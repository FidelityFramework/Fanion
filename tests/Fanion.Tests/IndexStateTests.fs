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
    let temp = Path.Combine(Path.GetTempPath(), "index-state-" + Guid.NewGuid().ToString("N"))
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
    let state = f.State
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
