/// One-for-one port of test_index_state.py against the F# State over SQLite.
module Fanion.Tests.IndexStateTests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Xunit
open Fanion.Canonical
open Fanion.IndexState

type Clock() =
    member val Value = 1000.0 with get, set
    member this.Now() = this.Value
    member this.Advance(?seconds: float) = this.Value <- this.Value + defaultArg seconds 1.0; this.Value

let field (value: J) (name: string) =
    match value with
    | Obj pairs -> pairs |> List.tryPick (fun (k, v) -> if k = name then Some v else None) |> Option.defaultWith (fun () -> failwithf "missing %s" name)
    | _ -> failwith "not an object"
let bool' value name = match field value name with Bool b -> b | _ -> failwith "not bool"
let str' value name = match field value name with Str s -> s | Null -> null | _ -> failwith "not string"
let strings value name = match field value name with Arr items -> [ for i in items -> (match i with Str s -> s | _ -> failwith "not string") ] | _ -> failwith "not array"

let rev (c: char) (n: int) = String(c, n)

type Fixture() =
    let temp = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../.scratch/tests", "index-state-" + Guid.NewGuid().ToString("N")))
    do Directory.CreateDirectory temp |> ignore
    member val Clock = Clock()
    member _.Path = Path.Combine(temp, "index.sqlite")
    member _.Allowed = [ "spec", []; "site", [ "spec" ]; "search", [ "site" ]; "other", [] ]
    member _.Heads = [ "spec", Some(rev 'a' 40); "site", Some(rev 'b' 40); "search", Some(rev 'c' 40); "other", Some(rev 'd' 64) ]
    member this.State = State(this.Path, this.Allowed, leaseSeconds = 10.0, clock = this.Clock.Now)
    member this.Reopen(?lease: float) = State(this.Path, this.Allowed, ?leaseSeconds = lease, clock = this.Clock.Now)
    member this.PublishInitial(state: State) =
        state.ObserveHeads(this.Heads, this.Clock.Now()) |> ignore
        let plan = state.PendingPlan()
        let token = state.StartBuild plan
        let result = state.CompleteBuild(token, rev '1' 64, [ for kv in plan.Targets -> kv.Key, kv.Value ])
        Assert.True(bool' result "published")
        Assert.True(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")
        token
    interface IDisposable with
        member _.Dispose() = try Directory.Delete(temp, true) with _ -> ()

let targets (plan: Plan) = [ for kv in plan.Targets -> kv.Key, kv.Value ]

[<Fact>]
let ``unknown sources and first generation fail closed`` () =
    use f = new Fixture()
    let state = f.State
    let plan = state.PendingPlan()
    Assert.Equal<string list>([ "other"; "search"; "site"; "spec" ], plan.ObservationRequired)
    Assert.Throws<StateError>(fun () -> state.StartBuild plan |> ignore) |> ignore
    let fresh = state.Freshness(rev '1' 64, 30.0)
    Assert.False(bool' fresh "fresh")
    Assert.Contains("unknown_snapshot", strings fresh "reasons")
    state.ObserveHeads([ "spec", Some(rev 'a' 40) ], f.Clock.Now()) |> ignore
    Assert.Throws<StateError>(fun () -> state.StartBuild(state.PendingPlan()) |> ignore) |> ignore
    Assert.Throws<StateError>(fun () -> state.ObserveHeads([ "unknown", Some(rev 'a' 40) ], f.Clock.Now()) |> ignore) |> ignore

[<Fact>]
let ``plan settle window follows real head changes and is bounded`` () =
    use f = new Fixture()
    let state = State(f.Path, f.Allowed, clock = f.Clock.Now, settleSeconds = 5.0, maxSettleSeconds = 60.0)
    Assert.True(state.PendingPlan().SettleUntil.IsNone)
    let first = f.Clock.Now()
    state.ObserveHeads(f.Heads, first) |> ignore
    Assert.Equal(Some(first + state.SettleSeconds), state.PendingPlan().SettleUntil)
    f.Clock.Advance() |> ignore
    state.ObserveHeads(f.Heads, f.Clock.Now()) |> ignore
    Assert.Equal(Some(first + state.SettleSeconds), state.PendingPlan().SettleUntil)
    f.Clock.Advance() |> ignore
    let changedAt = f.Clock.Now()
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], changedAt) |> ignore
    Assert.Equal(Some(min (changedAt + state.SettleSeconds) (first + state.MaxSettleSeconds)), state.PendingPlan().SettleUntil)
    Assert.Throws<StateError>(fun () -> State(f.Path, f.Allowed, clock = f.Clock.Now, settleSeconds = 10.0, maxSettleSeconds = 5.0) |> ignore) |> ignore

[<Fact>]
let ``default settle coalesces pushes briefly with a one second ceiling`` () =
    use f = new Fixture()
    let state = f.State
    Assert.Equal(0.25, state.SettleSeconds)
    Assert.Equal(1.0, state.MaxSettleSeconds)
    state.ObserveHeads(f.Heads, f.Clock.Now()) |> ignore
    Assert.Equal(Some 1000.25, state.PendingPlan().SettleUntil)
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance(0.9)) |> ignore
    Assert.Equal(Some 1001.0, state.PendingPlan().SettleUntil)

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``five same or mixed source pushes retain the first dirty deadline`` mixed =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    let first = f.Clock.Advance(10.0)
    let sources = if mixed then [ "spec"; "site"; "other"; "search"; "spec" ] else List.replicate 5 "spec"
    for i, source in List.indexed sources do
        f.Clock.Value <- first + float i * 0.225
        state.ObserveHeads([ source, Some(rev "ef012".[i] 40) ], f.Clock.Now()) |> ignore
        Assert.Equal(Some(min (f.Clock.Now() + 0.25) (first + 1.0)), state.PendingPlan().SettleUntil)
    let reopened = f.Reopen()
    Assert.Equal(Some(first + 1.0), reopened.PendingPlan().SettleUntil)
    let plan = reopened.PendingPlan()
    reopened.CompleteBuild(reopened.StartBuild plan, rev '2' 64, targets plan) |> ignore
    Assert.True(reopened.PendingPlan().SettleUntil.IsNone)
    let next = f.Clock.Advance(10.0)
    reopened.ObserveHeads([ "spec", Some(rev '0' 40) ], next) |> ignore
    Assert.Equal(Some(next + 0.25), reopened.PendingPlan().SettleUntil)

[<Fact>]
let ``reverted head clears its old batch while deletion and invalidation retain dirty cap`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance(10.0)) |> ignore
    state.ObserveHeads([ "spec", Some(rev 'a' 40) ], f.Clock.Advance(0.5)) |> ignore
    Assert.True(state.PendingPlan().SettleUntil.IsNone)
    let first = f.Clock.Advance(10.0)
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], first) |> ignore
    Assert.Equal(Some(first + 0.25), state.PendingPlan().SettleUntil)
    state.ObserveHeads([ "spec", None ], f.Clock.Advance(0.9)) |> ignore
    Assert.Equal(Some(first + 1.0), state.PendingPlan().SettleUntil)
    let plan = state.PendingPlan()
    state.CompleteBuild(state.StartBuild plan, rev '2' 64, targets plan) |> ignore
    let invalidated = f.Clock.Advance(10.0)
    state.InvalidateSources([ "spec" ], "policy changed") |> ignore
    Assert.Equal(Some(invalidated + 0.25), state.PendingPlan().SettleUntil)
    f.Clock.Advance(0.9) |> ignore
    state.InvalidateSources([ "spec" ], "another policy change") |> ignore
    Assert.Equal(Some(invalidated + 1.0), state.PendingPlan().SettleUntil)

[<Fact>]
let ``legacy state migration seeds first dirty time from changed time`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    let first = f.Clock.Advance(10.0)
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], first) |> ignore
    // Emulate the immediately previous on-disk schema without replacing its state.
    use connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=" + f.Path)
    connection.Open()
    use command = connection.CreateCommand()
    command.CommandText <- "ALTER TABLE sources DROP COLUMN first_changed_at"
    command.ExecuteNonQuery() |> ignore
    let reopened = f.Reopen()
    reopened.ObserveHeads([ "spec", Some(rev 'f' 40) ], f.Clock.Advance(0.9)) |> ignore
    Assert.Equal(Some(first + 1.0), reopened.PendingPlan().SettleUntil)

[<Fact>]
let ``separate state writer broadcasts even before observers await`` () = task {
    use f = new Fixture()
    let state, writer = f.State, f.Reopen()
    f.PublishInitial state |> ignore
    use changes = new Changes(f.Path)
    let first, second = changes.Watch(), changes.Watch()
    Assert.Same(first, second)
    Assert.True(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")
    writer.EventHint("separate-writer", "spec", "push") |> ignore
    Assert.True(SpinWait.SpinUntil((fun () -> first.IsCompleted), 3000), "SQLite write did not wake observers")
    do! first.WaitAsync(TimeSpan.FromSeconds 3.0)
    do! second.WaitAsync(TimeSpan.FromSeconds 3.0)
    Assert.Equal<string list>([ "spec" ], state.PendingPlan().ObservationRequired)
}

[<Fact>]
let ``readiness reads and unrelated files do not wake state observers`` () = task {
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    use changes = new Changes(f.Path)
    let changed = changes.Watch()
    for _ in 1..25 do
        Assert.True(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")
        Assert.True(state.PendingPlan().Targets.IsEmpty)
        Assert.True(state.ActiveGeneration().IsSome)
    File.WriteAllText(f.Path + ".unrelated", "not authoritative state")
    let! finished = Task.WhenAny(changed :> Task, Task.Delay 250)
    Assert.NotSame(changed, finished)
    Assert.False(changed.IsCompleted)
}

[<Fact>]
let ``late observer reads an already durable hint without waiting for another event`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    state.EventHint("before-subscription", "site", "push") |> ignore
    use changes = new Changes(f.Path)
    let changed = changes.Watch()
    Assert.Equal<string list>([ "site" ], state.PendingPlan().ObservationRequired)
    Assert.False(changed.IsCompleted)

[<Fact>]
let ``early filesystem notification cannot read through an uncommitted writer`` () = task {
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    use writer = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=" + f.Path)
    writer.Open()
    use command = writer.CreateCommand()
    command.CommandText <- "BEGIN IMMEDIATE; UPDATE sources SET indexed_known=0 WHERE source_id='spec'"
    command.ExecuteNonQuery() |> ignore
    use changes = new Changes(f.Path)
    let early = changes.Watch()
    // Deliver a filesystem hint before COMMIT. The read must acquire SQLite's
    // existing writer barrier, not consume this hint and return the old state.
    File.SetLastWriteTimeUtc(f.Path, DateTime.UtcNow.AddSeconds 1.0)
    do! early.WaitAsync(TimeSpan.FromSeconds 3.0)
    use entered = new ManualResetEventSlim()
    let read = Task.Run(fun () -> entered.Set(); state.PendingPlan())
    Assert.True(entered.Wait(TimeSpan.FromSeconds 3.0))
    do! Task.Delay 100
    Assert.False(read.IsCompleted, "An early notification bypassed the commit barrier")
    command.CommandText <- "COMMIT"
    command.ExecuteNonQuery() |> ignore
    let! result = read.WaitAsync(TimeSpan.FromSeconds 3.0)
    Assert.True(result.Targets.ContainsKey "spec")
}

[<Theory>]
[<InlineData("spec", false)>]
[<InlineData("all", false)>]
[<InlineData("spec", true)>]
let ``reindex CLI waits for matching receipt and rejects concurrent revisions`` selection concurrent = task {
    use f = new Fixture()
    let root = Path.GetDirectoryName f.Path
    let database, config = Path.Combine(root, "index-state.sqlite"), Path.Combine(root, "corpus.json")
    let state = State(database, f.Allowed)
    state.ObserveHeads(f.Heads, state.Now()) |> ignore
    let initial = state.PendingPlan()
    state.CompleteBuild(state.StartBuild initial, rev '1' 64, targets initial) |> ignore
    let source (id, upstream) = Obj [ "id", Str id; "repo", Str ("FidelityFramework/" + id); "ref", Str "refs/heads/main"; "depends_on", Arr (List.map Str upstream) ]
    File.WriteAllText(config, toText canonicalStyle (Obj [ "schema_version", Int 1L; "sources", Arr (List.map source f.Allowed) ]))
    use changes = new Changes(database)
    let before = changes.Watch()
    let info = Diagnostics.ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "retrieval-native"))
    for argument in [ "reindex"; "--root"; root; "--config"; config; "--sources"; selection; "--timeout-seconds"; "5" ] do info.ArgumentList.Add argument
    info.RedirectStandardOutput <- true
    info.RedirectStandardError <- true
    info.UseShellExecute <- false
    use child = Diagnostics.Process.Start info
    try
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 4.0)
        let rec accepted (signal: Task<unit>) = task {
            let plan = state.PendingPlan()
            if plan.DesiredEpoch > initial.DesiredEpoch then
                return plan
            else
                do! signal.WaitAsync(deadline.Token)
                return! accepted (changes.Watch())
        }
        let! pending = accepted before
        if concurrent then
            state.ObserveHeads([ "other", Some(rev 'e' 40) ], state.Now()) |> ignore
        else
            let result = state.CompleteBuild(state.StartBuild pending, rev '1' 64, targets pending)
            do! Task.Delay 100
            Assert.False(child.HasExited, "CLI returned before the matching phase receipt existed")
            let temporary = Path.Combine(root, "receipt.tmp")
            File.WriteAllText(temporary, toText canonicalStyle result)
            File.Move(temporary, Path.Combine(root, "last-build.json"))
        do! child.WaitForExitAsync(deadline.Token)
        let output, error = child.StandardOutput.ReadToEnd(), child.StandardError.ReadToEnd()
        if concurrent then
            Assert.NotEqual(0, child.ExitCode)
            Assert.Equal("", output)
            Assert.Contains("Concurrent source activity", error)
        else
            Assert.True(child.ExitCode = 0, error)
            let result = JsonInput.parse output
            Assert.Equal(2L, result |> JsonInput.property "generation" |> JsonInput.int64)
            Assert.Equal("operator_reindex_current_heads", result |> JsonInput.property "kind" |> JsonInput.string)
            Assert.Equal((if selection = "all" then 4 else 1), result |> JsonInput.property "requested_sources" |> JsonInput.items |> List.length)
            let number name = result |> JsonInput.property name |> JsonInput.number
            Assert.True(number "total_seconds" >= number "acceptance_seconds")
            Assert.InRange(abs (number "total_seconds" - number "acceptance_seconds" - number "accepted_to_publication_observed_seconds"), 0.0, 1e-9)
            Assert.True((result |> JsonInput.property "receipt_delay_seconds" |> JsonInput.number) >= 0.0)
    finally
        if not child.HasExited then child.Kill(true)
}

[<Fact>]
let ``closing change notifications releases current and future observers`` () = task {
    use f = new Fixture()
    f.State |> ignore
    let changes = new Changes(f.Path)
    let pending = changes.Watch()
    (changes :> IDisposable).Dispose()
    let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> pending :> Task)
    let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> changes.Watch() :> Task)
    (changes :> IDisposable).Dispose()
    return ()
}

[<Fact>]
let ``reordered hints and persistent dedupe are not revision authority`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    f.Clock.Advance() |> ignore
    state.EventHint("newer-delivery", "spec", "push") |> ignore
    state.EventHint("older-delivery", "spec", "push") |> ignore
    let pending = state.PendingPlan()
    Assert.True(pending.Targets.IsEmpty)
    Assert.Equal<string list>([ "spec" ], pending.ObservationRequired)
    Assert.False(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")
    f.Clock.Advance() |> ignore
    state.ObserveHeads([ "spec", Some(rev 'a' 40) ], f.Clock.Now()) |> ignore
    let reopened = f.Reopen()
    Assert.True(bool' (reopened.EventHint("older-delivery", "spec", "push")) "duplicate")
    Assert.True(bool' (reopened.Freshness(rev '1' 64, 30.0)) "fresh")
    Assert.Throws<StateError>(fun () -> reopened.EventHint("older-delivery", "site", "push") |> ignore) |> ignore

[<Fact>]
let ``missed webhook reconciliation targets transitive dependents`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    f.Clock.Advance() |> ignore
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Now()) |> ignore
    let plan = state.PendingPlan()
    Assert.Equal<string list>([ "spec" ], plan.ChangedSources)
    Assert.Equal<Map<string, string option>>(Map.ofList [ "spec", Some(rev 'e' 40); "site", Some(rev 'b' 40); "search", Some(rev 'c' 40) ], plan.Targets)
    Assert.Contains("indexing_pending", strings (state.Freshness(rev '1' 64, 30.0)) "reasons")

[<Fact>]
let ``force push and confirmed deletion need new generation`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    f.Clock.Advance() |> ignore
    state.ObserveHeads([ "spec", Some(rev '0' 40) ], f.Clock.Now()) |> ignore
    let plan = state.PendingPlan()
    state.CompleteBuild(state.StartBuild plan, rev '2' 64, targets plan) |> ignore
    f.Clock.Advance() |> ignore
    state.ObserveHeads([ "spec", None ], f.Clock.Now()) |> ignore
    let plan = state.PendingPlan()
    Assert.True(plan.Targets.["spec"].IsNone)
    Assert.Equal<Set<string>>(set [ "spec"; "site"; "search" ], plan.Targets.Keys |> Set.ofSeq)
    Assert.False(bool' (state.Freshness(rev '2' 64, 30.0)) "fresh")
    state.CompleteBuild(state.StartBuild plan, rev '3' 64, targets plan) |> ignore
    Assert.True(bool' (state.Freshness(rev '3' 64, 30.0, [ "spec", None ])) "fresh")
    Assert.False(bool' (state.Freshness(rev '3' 64, 30.0, [ "spec", Some(rev '0' 40) ])) "fresh")

[<Fact>]
let ``observations reject future and conflicts and ignore older results`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    let oldTime = f.Clock.Now()
    f.Clock.Advance(2.0) |> ignore
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Now()) |> ignore
    let result = state.ObserveHeads([ "spec", Some(rev 'a' 40) ], oldTime)
    Assert.Equal<string list>([ "spec" ], strings result "ignored_older")
    Assert.Throws<StateError>(fun () -> state.ObserveHeads([ "spec", Some(rev 'a' 40) ], f.Clock.Now()) |> ignore) |> ignore
    for timestamp in [ f.Clock.Now() + 6.0; Double.PositiveInfinity; Double.NaN; -1.0 ] do
        Assert.Throws<StateError>(fun () -> state.ObserveHeads([ "spec", Some(rev 'e' 40) ], timestamp) |> ignore) |> ignore
    Assert.Equal(Some(rev 'e' 40), state.PendingPlan().Targets.["spec"])

[<Fact>]
let ``hint arriving during fetch is not acknowledged by old observation`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    let fetchStarted = f.Clock.Advance()
    f.Clock.Advance() |> ignore
    state.EventHint("during-fetch", "spec", "push") |> ignore
    state.ObserveHeads([ "spec", Some(rev 'a' 40) ], fetchStarted) |> ignore
    Assert.Equal<string list>([ "spec" ], state.PendingPlan().ObservationRequired)
    state.ObserveHeads([ "spec", Some(rev 'a' 40) ], f.Clock.Advance()) |> ignore
    Assert.True(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")

[<Fact>]
let ``concurrent desired advancement supersedes build`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance()) |> ignore
    let plan = state.PendingPlan()
    let token = state.StartBuild plan
    let other = f.Reopen()
    other.ObserveHeads([ "other", Some(rev 'f' 64) ], f.Clock.Advance()) |> ignore
    let result = state.CompleteBuild(token, rev '2' 64, targets plan)
    Assert.Equal("superseded", str' result "status")
    Assert.False(bool' result "published")
    Assert.Equal(rev '1' 64, str' (state.Freshness(rev '1' 64, 30.0)) "current_snapshot_id")
    Assert.Equal<Set<string>>(set [ "spec"; "site"; "search"; "other" ], state.PendingPlan().Targets.Keys |> Set.ofSeq)

[<Fact>]
let ``advance then return still invalidates old build fence`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance()) |> ignore
    let plan = state.PendingPlan()
    let token = state.StartBuild plan
    state.ObserveHeads([ "spec", Some(rev 'f' 40) ], f.Clock.Advance()) |> ignore
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance()) |> ignore
    Assert.Equal("superseded", str' (state.CompleteBuild(token, rev '2' 64, targets plan)) "status")

[<Fact>]
let ``unobserved hint during build prevents publication`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance()) |> ignore
    let plan = state.PendingPlan()
    let token = state.StartBuild plan
    state.EventHint("late", "site", "site_deploy") |> ignore
    Assert.Equal("superseded", str' (state.CompleteBuild(token, rev '2' 64, targets plan)) "status")
    Assert.Throws<StateError>(fun () -> state.StartBuild(state.PendingPlan()) |> ignore) |> ignore

[<Fact>]
let ``failed build keeps old generation but latest rejects and retry works`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance()) |> ignore
    let plan = state.PendingPlan()
    let token = state.StartBuild plan
    state.FailBuild(token, "extractor failed") |> ignore
    let status = state.Freshness(rev '1' 64, 30.0)
    Assert.Equal(rev '1' 64, str' status "current_snapshot_id")
    Assert.False(bool' status "fresh")
    let retry = state.StartBuild(state.PendingPlan())
    Assert.NotEqual<string>(token, retry)
    Assert.True(bool' (state.CompleteBuild(retry, rev '2' 64, targets plan)) "published")
    Assert.True(bool' (state.Freshness(rev '2' 64, 30.0)) "fresh")

[<Fact>]
let ``restart lease expiry and late completion cannot publish`` () =
    use f = new Fixture()
    let state = f.State
    state.ObserveHeads(f.Heads, f.Clock.Now()) |> ignore
    let plan = state.PendingPlan()
    let token = state.StartBuild plan
    let reopened = f.Reopen(10.0)
    Assert.Throws<BuildInProgress>(fun () -> reopened.StartBuild plan |> ignore) |> ignore
    f.Clock.Advance(11.0) |> ignore
    let retry = reopened.StartBuild(reopened.PendingPlan())
    Assert.Equal("expired", str' (reopened.CompleteBuild(token, rev '1' 64, targets plan)) "status")
    Assert.True(bool' (reopened.CompleteBuild(retry, rev '1' 64, targets plan)) "published")

[<Fact>]
let ``renewal and exact target coverage`` () =
    use f = new Fixture()
    let state = f.State
    state.ObserveHeads(f.Heads, f.Clock.Now()) |> ignore
    let plan = state.PendingPlan()
    let token = state.StartBuild plan
    f.Clock.Advance(9.0) |> ignore
    Assert.Equal(f.Clock.Now() + 10.0, state.RenewBuild token)
    f.Clock.Advance(2.0) |> ignore
    Assert.Throws<StateError>(fun () -> state.CompleteBuild(token, rev '1' 64, [ "spec", Some(rev 'a' 40) ]) |> ignore) |> ignore
    Assert.Throws<StateError>(fun () -> state.CompleteBuild(token, rev '1' 64, [ "spec", Some(rev 'f' 40); "site", Some(rev 'b' 40); "search", Some(rev 'c' 40); "other", Some(rev 'd' 64) ]) |> ignore) |> ignore
    Assert.True(bool' (state.CompleteBuild(token, rev '1' 64, f.Heads)) "published")

[<Fact>]
let ``duplicate completion is a noop even after newer generation`` () =
    use f = new Fixture()
    let state = f.State
    let first = f.PublishInitial state
    state.ObserveHeads([ "other", Some(rev 'f' 64) ], f.Clock.Advance()) |> ignore
    let plan = state.PendingPlan()
    state.CompleteBuild(state.StartBuild plan, rev '2' 64, targets plan) |> ignore
    let replay = state.CompleteBuild(first, rev '1' 64, f.Heads)
    Assert.False(bool' replay "published")
    Assert.False(bool' replay "current")
    Assert.True(bool' replay "replayed")
    Assert.Equal(rev '2' 64, str' (state.Freshness(rev '2' 64, 30.0)) "current_snapshot_id")

[<Fact>]
let ``force push can restore identical historical snapshot as new generation`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    state.ObserveHeads([ "other", Some(rev 'f' 64) ], f.Clock.Advance()) |> ignore
    let plan = state.PendingPlan()
    state.CompleteBuild(state.StartBuild plan, rev '2' 64, targets plan) |> ignore
    state.ObserveHeads([ "other", Some(rev 'd' 64) ], f.Clock.Advance()) |> ignore
    let plan = state.PendingPlan()
    let restored = state.CompleteBuild(state.StartBuild plan, rev '1' 64, targets plan)
    Assert.Equal(Int 3L, field restored "generation")
    Assert.True(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")

[<Fact>]
let ``freshness fences observation age required revisions and history`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    Assert.False(bool' (state.Freshness(rev '1' 64, 30.0, [ "spec", Some(rev 'e' 40) ])) "fresh")
    f.Clock.Advance(31.0) |> ignore
    Assert.Contains("observation_stale_or_unknown", strings (state.Freshness(rev '1' 64, 30.0)) "reasons")
    state.ObserveHeads(f.Heads, f.Clock.Now()) |> ignore
    Assert.True(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")
    state.ObserveHeads([ "other", Some(rev 'e' 64) ], f.Clock.Advance()) |> ignore
    let plan = state.PendingPlan()
    state.CompleteBuild(state.StartBuild plan, rev '2' 64, targets plan) |> ignore
    Assert.Contains("not_latest_generation", strings (state.Freshness(rev '1' 64, 30.0)) "reasons")

[<Fact>]
let ``stale plans and invalid inputs cannot change state`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    state.ObserveHeads([ "other", Some(rev 'e' 64) ], f.Clock.Advance()) |> ignore
    let stale = state.PendingPlan()
    state.ObserveHeads([ "other", Some(rev 'f' 64) ], f.Clock.Advance()) |> ignore
    Assert.Throws<StateError>(fun () -> state.StartBuild stale |> ignore) |> ignore
    for heads in [ [ "not-allowed", Some(rev 'a' 40) ]; [ "spec", Some(rev 'A' 40) ]; [ "spec", Some(rev 'a' 39) ] ] do
        Assert.Throws<StateError>(fun () -> state.ObserveHeads(heads, f.Clock.Now()) |> ignore) |> ignore
    Assert.Throws<StateError>(fun () -> state.EventHint("bad event\n", "spec", "push") |> ignore) |> ignore
    Assert.Throws<StateError>(fun () -> state.EventHint("good", "spec", "arbitrary") |> ignore) |> ignore
    Assert.Throws<StateError>(fun () -> State(f.Path, [ "other", [] ], clock = f.Clock.Now) |> ignore) |> ignore

[<Fact>]
let ``dependency cycles and unlisted dependencies rejected`` () =
    use f = new Fixture()
    for graph in [ [ "a", [ "b" ]; "b", [ "a" ] ]; [ "a", [ "unknown" ] ]; [ "a", [ "a" ] ]; [ "a", [ "b"; "b" ]; "b", [] ] ] do
        Assert.Throws<StateError>(fun () -> State(Path.Combine(Path.GetDirectoryName f.Path, "invalid.sqlite"), graph) |> ignore) |> ignore

[<Fact>]
let ``competing schedulers get only one live lease`` () =
    use f = new Fixture()
    let state = f.State
    state.ObserveHeads(f.Heads, f.Clock.Now()) |> ignore
    let plan = state.PendingPlan()
    use barrier = new Barrier(2)
    let attempt () =
        barrier.SignalAndWait(5000) |> ignore
        try Some(state.StartBuild plan) with BuildInProgress _ -> None
    let results = [ Task.Run(attempt); Task.Run(attempt) ] |> List.map (fun t -> t.Result)
    Assert.Equal(1, results |> List.filter Option.isSome |> List.length)

[<Fact>]
let ``active generation and explicit invalidation fence`` () =
    use f = new Fixture()
    let state = f.State
    Assert.True(state.ActiveGeneration().IsNone)
    f.PublishInitial state |> ignore
    let active = state.ActiveGeneration().Value
    Assert.Equal(1L, active.Generation)
    Assert.Equal(rev '1' 64, active.SnapshotId)
    Assert.Equal<Map<string, string option>>(Map.ofList f.Heads, active.Revisions)
    Assert.Equal(f.Clock.Now(), active.PublishedAt)
    let invalidated = state.InvalidateSources([ "spec" ], "extractor configuration changed")
    Assert.Equal<string list>([ "search"; "site"; "spec" ], strings invalidated "pending_sources")
    Assert.False(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")
    let plan = state.PendingPlan()
    let token = state.StartBuild plan
    state.InvalidateSources([ "spec" ], "extractor changed again") |> ignore
    Assert.Equal("superseded", str' (state.CompleteBuild(token, rev '2' 64, targets plan)) "status")
    let retry = state.PendingPlan()
    state.CompleteBuild(state.StartBuild retry, rev '2' 64, targets retry) |> ignore
    Assert.True(bool' (state.Freshness(rev '2' 64, 30.0)) "fresh")
    Assert.Equal(2L, state.ActiveGeneration().Value.Generation)
    for (sources, reason) in [ [], "x"; [ "unknown" ], "x"; [ "spec"; "spec" ], "x"; [ "spec" ], ""; [ "spec" ], String('x', 1025) ] do
        Assert.Throws<StateError>(fun () -> state.InvalidateSources(sources, reason) |> ignore) |> ignore
