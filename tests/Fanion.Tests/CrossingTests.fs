// Crossing gate, trunk-only invariant, bounded FIFO retrieval queue, and snapshot-pinned read tests.
module Fanion.Tests.CrossingTests

open System
open System.Collections.Concurrent
open System.Diagnostics
open System.IO
open System.Net
open System.Net.Http
open System.Security.Cryptography
open System.Text
open System.Threading
open System.Threading.Tasks
open Microsoft.Data.Sqlite
open Xunit
open Fanion
open Fanion.Canonical
open Fanion.Query
open Fanion.English
open Fanion.Tests.IndexStateTests
open Fanion.Tests.EnglishTests
open Fanion.Tests.ServeTests

let private completed<'T> () = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
let private bounded (pending: Task<'T>) = pending.WaitAsync(TimeSpan.FromSeconds 20.0)
let private joined (pending: Task) = pending.WaitAsync(TimeSpan.FromSeconds 20.0)

let private sqliteScalar<'T> (path: string) (sql: string) : 'T =
    use connection = new SqliteConnection(sprintf "Data Source=%s;Mode=ReadOnly" path)
    connection.Open()
    use command = connection.CreateCommand()
    command.CommandText <- sql
    command.ExecuteScalar() :?> 'T

type private SseStream(response: HttpResponseMessage, reader: StreamReader) =
    member _.Next() : Task<J option> = task {
        let mutable result = None
        let mutable reading = true
        while reading do
            let! line = reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds 20.0)
            match line with
            | null -> reading <- false
            | "event: readiness" ->
                let! data = reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds 10.0)
                let! blank = reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds 10.0)
                Assert.StartsWith("data: ", data)
                Assert.Equal("", blank)
                result <- Some(parse (data.Substring 6))
                reading <- false
            | ": keepalive" | "" -> ()
            | other -> failwithf "unexpected event stream line %s" other
        return result
    }
    member this.NextFrame() = task {
        match! this.Next() with
        | Some frame -> return frame
        | None -> return failwith "event stream ended unexpectedly"
    }
    member this.ReadUntil(predicate: J -> bool) = task {
        let collected = ResizeArray<J>()
        let mutable found = None
        while found.IsNone do
            let! frame = this.NextFrame()
            collected.Add frame
            if predicate frame then found <- Some frame
        return List.ofSeq collected, found.Value
    }
    interface IDisposable with
        member _.Dispose() = reader.Dispose(); response.Dispose()

let private openObserve (http: HttpClient) (token: string) = task {
    use request = new HttpRequestMessage(HttpMethod.Get, "v1/retrieval/observe")
    request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + token) |> ignore
    let! response = http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead)
    Assert.Equal(HttpStatusCode.OK, response.StatusCode)
    let! stream = response.Content.ReadAsStreamAsync()
    return new SseStream(response, new StreamReader(stream))
}

let private postJson (http: HttpClient) (path: string) (token: string option) (body: string) (ct: CancellationToken) = task {
    use request = new HttpRequestMessage(HttpMethod.Post, path)
    request.Content <- new StringContent(body, Encoding.UTF8, "application/json")
    token |> Option.iter (fun t -> request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + t) |> ignore)
    use! response = http.SendAsync(request, ct)
    let! text = response.Content.ReadAsStringAsync(ct)
    return int response.StatusCode, (try parse text with _ -> Str text)
}

let private sendWebhook (server: Server) (secret: byte[]) (repoFullName: string) (refName: string) (delivery: string) =
    let payload = Obj [ "repository", Obj [ "full_name", Str repoFullName ]; "ref", Str refName; "after", Str (String('a', 40)) ]
    let raw = Encoding.UTF8.GetBytes(canon payload)
    let sigHex = Convert.ToHexString(HMACSHA256.HashData(secret, raw)).ToLowerInvariant()
    let status, body = server.Post("v1/index/events/forgejo", raw, [ "X-Forgejo-Signature", sigHex; "X-Forgejo-Event", "push"; "X-Forgejo-Delivery", delivery ])
    Assert.Equal(202, status)
    parse body

let private coordOptions (corpus: Corpus) (extensions: string) (libduckdb: string) (settle: float option) (maxSettle: float option) : Coordinator.Options =
    let lockPath name = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../config", name))
    let fixtures = Path.Combine(Path.GetDirectoryName corpus.Config, "fixtures")
    { Root = corpus.Root
      Config = corpus.Config
      Engine = engineOf extensions libduckdb
      Budgets = { MaxOutputBytes = 384L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxTreeEntries = 100_000L; MaxFiles = 20_000
                  MaxSourceBytes = 128L * 1024L * 1024L; MaxDerivedEdges = 50_000; MaxCapabilityNodes = 2048; MaxMetadataBytes = 32768 }
      ExtractLimits = { MaxRepositories = 64; MaxTreeEntries = 100_000L; MaxTreeBytes = 16 * 1024 * 1024; MaxFiles = 20_000
                        MaxFileBytes = 1024 * 1024; MaxSourceBytes = 128L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000
                        MaxOutputBytes = 384L * 1024L * 1024L; MaxLayerRules = 64; MaxRulePatterns = 32; MaxTotalRulePatterns = 256
                        MaxLinksPerFile = 1024; MaxBuildSeconds = 600.0; GitTimeoutSeconds = 15.0 }
      RemoteBase = fixtures
      LeaseSeconds = 900.0
      SettleSeconds = settle
      MaxSettleSeconds = maxSettle
      LockFiles = [ lockPath "duckpgq-lock.json"; lockPath "libduckdb-lock.json" ] }

let private awaitState (changes: IndexState.Changes) (running: Task<unit>) (condition: unit -> bool) = task {
    use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 20.0)
    let mutable matched = false
    while not matched do
        let changed = changes.Watch()
        matched <- condition ()
        if not matched then
            let! completed = Task.WhenAny(changed :> Task, running :> Task).WaitAsync(deadline.Token)
            if Object.ReferenceEquals(completed, running) then
                do! running
                failwith "Coordinator exited before expected state"
            else
                do! changed.WaitAsync(deadline.Token)
}

let private idOfPath (server: Server) (snapshot: string) (relative: string) =
    let status, found = server.Call(Obj [ "action", Str "find"; "query", Str relative; "snapshot_id", Str snapshot; "limit", Int 50L ])
    Assert.Equal(200, status)
    items (field found "results")
    |> List.pick (fun row -> if text (field row "path") = relative && text (field row "kind") = "file" then Some(text (field row "id")) else None)

[<Fact>]
let ``trunk branch invariant enforces main and master only and crossing endpoints fail closed`` () = task {
    let temp = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../.scratch/tests", "trunk-cfg-" + Guid.NewGuid().ToString("N")))
    Directory.CreateDirectory temp |> ignore
    try
        let writeCfg name refA refB =
            let p = Path.Combine(temp, name)
            File.WriteAllText(p, sprintf """{"schema_version":1,"sources":[{"id":"a","repo":"FidelityFramework/A","ref":"%s","depends_on":[]},{"id":"b","repo":"FidelityFramework/B","ref":"%s","depends_on":[]}]}""" refA refB)
            p
        // Positive: both refs/heads/main and refs/heads/master are admitted.
        let valid, _ = Coordinator.configSources (writeCfg "valid.json" "refs/heads/main" "refs/heads/master")
        Assert.Equal("refs/heads/main", valid.[0].Ref)
        Assert.Equal("refs/heads/master", valid.[1].Ref)
        // Negative: any non-trunk or wrong-case ref is refused.
        for badRef in [ "refs/heads/release"; "refs/heads/feature/x"; "refs/heads/MAIN"; "refs/heads/MASTER"; "refs/tags/v1"; "main" ] do
            Assert.Throws<ArgumentException>(fun () -> Coordinator.configSources (writeCfg ("bad-" + Guid.NewGuid().ToString("N") + ".json") badRef "refs/heads/main") |> ignore) |> ignore
    finally
        try Directory.Delete(temp, true) with _ -> ()

    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        use server = new Server(corpus, None, None)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        let holdBody = """{"fence_id":"f-auth","sources":["alpha"]}"""
        let releaseBody = """{"fence_id":"f-auth"}"""
        // 401 unauthorized on hold and release.
        for tok in [ None; Some(String('x', 48)) ] do
            let! s1, _ = postJson http "v1/crossing/hold" tok holdBody CancellationToken.None
            let! s2, _ = postJson http "v1/crossing/release" tok releaseBody CancellationToken.None
            Assert.Equal(401, s1)
            Assert.Equal(401, s2)
        // 422 unmanaged_source.
        let! sUnmanaged, bUnmanaged = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"f-unmanaged","sources":["alpha","unknown_repo"]}""" CancellationToken.None
        Assert.Equal(422, sUnmanaged)
        Assert.Equal("unmanaged_source", text (field bUnmanaged "error"))
        // 422 invalid_crossing_ref (feature branch, uppercase MAIN, or mismatched master).
        for badRef in [ "refs/heads/feature/x"; "refs/heads/MAIN"; "refs/heads/master" ] do
            let body = sprintf """{"fence_id":"f-bad-ref","sources":["alpha"],"refs":{"alpha":"%s"}}""" badRef
            let! st, res = postJson http "v1/crossing/hold" (Some server.Token) body CancellationToken.None
            Assert.Equal(422, st)
            Assert.Equal("invalid_crossing_ref", text (field res "error"))
        // 400 malformed JSON, empty/duplicate sources, invalid fence_id, invalid TTL.
        for badBody in [ "not json"; "[]"; """{"fence_id":"f","sources":[]}"""; """{"fence_id":"f","sources":["alpha","alpha"]}"""
                         """{"fence_id":"","sources":["alpha"]}"""; """{"fence_id":"bad id","sources":["alpha"]}"""
                         """{"fence_id":"bad id","sources":["unknown_repo"]}"""
                         """{"fence_id":"f","sources":["alpha"],"ttl_seconds":0}"""; """{"fence_id":"f","sources":["alpha"],"ttl_seconds":-5}"""
                         """{"fence_id":"f","sources":["alpha"],"ttl_seconds":300.1}"""; """{"fence_id":"f","sources":["alpha"],"ttl_seconds":"60"}"""
                         """{"fence_id":"f","sources":["alpha"],"unknown":1}""" ] do
            let! st, _ = postJson http "v1/crossing/hold" (Some server.Token) badBody CancellationToken.None
            Assert.Equal(400, st)
        // 404 on unknown fence release.
        let! sMissing, bMissing = postJson http "v1/crossing/release" (Some server.Token) """{"fence_id":"does-not-exist"}""" CancellationToken.None
        Assert.Equal(404, sMissing)
        Assert.Equal("unknown_fence", text (field bMissing "error"))
        // Hold idempotency, reuse conflict, maxActiveFences (8), release idempotency.
        let! sHold, rHold = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"f-1","sources":["alpha"],"refs":{"alpha":"refs/heads/main"},"ttl_seconds":60,"reason":"batch"}""" CancellationToken.None
        Assert.Equal(200, sHold)
        Assert.Equal("held", text (field rHold "status"))
        let! sIdem, rIdem = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"f-1","sources":["alpha"],"ttl_seconds":30,"reason":"batch"}""" CancellationToken.None
        Assert.Equal(200, sIdem)
        Assert.True(bool' rIdem "duplicate")
        let! sReuseDiff, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"f-1","sources":["beta"],"reason":"batch"}""" CancellationToken.None
        Assert.Equal(400, sReuseDiff)
        for i in 2..IndexState.maxActiveFences do
            let! st, _ = postJson http "v1/crossing/hold" (Some server.Token) (sprintf """{"fence_id":"f-%d","sources":["alpha"]}""" i) CancellationToken.None
            Assert.Equal(200, st)
        let! sOverCap, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"f-9","sources":["alpha"]}""" CancellationToken.None
        Assert.Equal(400, sOverCap)
        let! sRel, rRel = postJson http "v1/crossing/release" (Some server.Token) """{"fence_id":"f-1"}""" CancellationToken.None
        Assert.Equal(200, sRel)
        Assert.False(bool' rRel "already_released")
        let! sRelAgain, rRelAgain = postJson http "v1/crossing/release" (Some server.Token) """{"fence_id":"f-1"}""" CancellationToken.None
        Assert.Equal(200, sRelAgain)
        Assert.True(bool' rRelAgain "already_released")
        let! sReopenReleased, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"f-1","sources":["alpha"],"reason":"batch"}""" CancellationToken.None
        Assert.Equal(400, sReopenReleased)
}

[<Fact>]
let ``five repo sequential push train builds one generation and auto releases on fifth repo`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        let repos = [ for i in 1..5 -> sprintf "repo%d" i, standardFiles ]
        let secret = Encoding.UTF8.GetBytes(String('s', 40))
        use corpus = new Corpus(repos, engineOf extensions libduckdb, extensions)
        // Use realistic 3.0s / 15.0s default quiet windows to prove all_arrived bypasses both TTL and settle delay.
        let opts = coordOptions corpus extensions libduckdb None None
        use stop = new CancellationTokenSource()
        let coordinator = Coordinator.Coordinator(opts, stop.Token)
        Assert.Empty(coordinator.Observe())
        Assert.True(bool' (coordinator.RunOnce true) "published")
        Assert.Equal(1L, coordinator.State.ActiveGeneration().Value.Generation)

        use server = new Server(corpus, None, Some secret)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        use! stream = openObserve http server.Token
        let! initial = stream.NextFrame()
        Assert.Equal("fresh", text (field initial "state"))
        Assert.Equal(Int 1L, field initial "generation")

        use changes = new IndexState.Changes(Path.Combine(corpus.Root, "index-state.sqlite"))
        let running = Coordinator.run coordinator changes stop.Token
        try
            let holdBody = """{"fence_id":"train-5","sources":["repo1","repo2","repo3","repo4","repo5"],"ttl_seconds":60,"reason":"5-repo train"}"""
            let! holdStatus, holdReceipt = postJson http "v1/crossing/hold" (Some server.Token) holdBody CancellationToken.None
            Assert.Equal(200, holdStatus)
            Assert.Equal("held", text (field holdReceipt "status"))
            Assert.Equal<string list>([ "repo1"; "repo2"; "repo3"; "repo4"; "repo5" ], strings holdReceipt "pending_sources")

            let! heldFrame = stream.NextFrame()
            Assert.Equal("crossing_held", text (field heldFrame "state"))
            Assert.Equal<string list>([ "repo1"; "repo2"; "repo3"; "repo4"; "repo5" ], strings heldFrame "expected_sources")
            Assert.Equal<string list>([ "repo1"; "repo2"; "repo3"; "repo4"; "repo5" ], strings heldFrame "pending_sources")
            Assert.Equal(Int 0L, field heldFrame "queued_retrievals")

            let newRevs = ResizeArray<string * string>()
            for i in 1..4 do
                let name = sprintf "repo%d" i
                let revNew = corpus.Commit(name, "2.md", sprintf "Content 2 train-%d\nEvidence\n" i)
                newRevs.Add(name, revNew)
                let wh = sendWebhook server secret ("FidelityFramework/" + name) "refs/heads/main" (sprintf "train-deliv-%d" i)
                Assert.True(bool' wh "accepted")
                do! awaitState changes running (fun () ->
                    let plan = coordinator.State.PendingPlan()
                    plan.ObservationRequired.IsEmpty && plan.DesiredRevisions.[name] = Some revNew)
                Assert.Equal(1L, coordinator.State.ActiveGeneration().Value.Generation)
                Assert.Equal("crossing_held", str' (coordinator.RunOnce false) "status")
                Assert.Equal("crossing_held", str' (coordinator.RunOnce true) "status")

            let sw = Stopwatch.StartNew()
            let rev5 = corpus.Commit("repo5", "2.md", "Content 2 train-5\nEvidence\n")
            newRevs.Add("repo5", rev5)
            let wh5 = sendWebhook server secret "FidelityFramework/repo5" "refs/heads/main" "train-deliv-5"
            Assert.True(bool' wh5 "accepted")

            do! awaitState changes running (fun () ->
                coordinator.State.ActiveGeneration() |> Option.exists (fun g -> g.Generation = 2L))
            // Built promptly without waiting out the 3.0s settle or 60s TTL.
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds 2.8, sprintf "expected immediate build on all_arrived, took %A" sw.Elapsed)

            let gen2 = coordinator.State.ActiveGeneration().Value
            Assert.Equal(2L, gen2.Generation)
            for (name, revExpected) in newRevs do
                Assert.Equal(Some revExpected, gen2.Revisions.[name])

            let! frames, freshGen2 = stream.ReadUntil(fun f -> text (field f "state") = "fresh")
            Assert.Contains(frames, fun f -> text (field f "state") = "indexing")
            Assert.Equal(Int 2L, field freshGen2 "generation")
            let stateDb = Path.Combine(corpus.Root, "index-state.sqlite")
            Assert.Equal("all_arrived", sqliteScalar<string> stateDb "SELECT release_reason FROM crossing_fences WHERE fence_id='train-5'")
        finally
            stop.Cancel()
            joined running |> fun t -> t.GetAwaiter().GetResult()
}

[<Fact>]
let ``unannounced pushes within sliding quiet period coalesce and cap at max settle`` () = task {
    // Part A: virtual clock proves 3.0s quiet and 15.0s ceiling on IndexState defaults.
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    let first = f.Clock.Advance(10.0)
    for i in 0..5 do
        f.Clock.Value <- first + float i * 2.8
        state.ObserveHeads([ "spec", Some(rev "ef0123".[i] 40) ], f.Clock.Now()) |> ignore
        Assert.Equal(Some(min (f.Clock.Now() + 3.0) (first + 15.0)), state.PendingPlan().SettleUntil)
    Assert.Equal(Some(first + 15.0), state.PendingPlan().SettleUntil)

    // Part B: real Coordinator coalesces rapid pushes under sliding settle into one generation.
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        let maxSettleSeconds = 0.85
        let opts = coordOptions corpus extensions libduckdb (Some 0.25) (Some maxSettleSeconds)
        use stop = new CancellationTokenSource()
        let coordinator = Coordinator.Coordinator(opts, stop.Token)
        Assert.Empty(coordinator.Observe())
        Assert.True(bool' (coordinator.RunOnce true) "published")
        use changes = new IndexState.Changes(Path.Combine(corpus.Root, "index-state.sqlite"))
        let running = Coordinator.run coordinator changes stop.Token
        try
            let revA = corpus.Commit("alpha", "2.md", "Coalesced alpha\nEvidence\n")
            coordinator.State.EventHint("coal-1", "alpha", "push") |> ignore
            do! Task.Delay 80
            let revB = corpus.Commit("beta", "2.md", "Coalesced beta\nEvidence\n")
            coordinator.State.EventHint("coal-2", "beta", "push") |> ignore
            do! awaitState changes running (fun () ->
                coordinator.State.ActiveGeneration() |> Option.exists (fun g -> g.Generation = 2L))
            let stateDb = Path.Combine(corpus.Root, "index-state.sqlite")
            let firstEventAt = sqliteScalar<float> stateDb "SELECT created_at FROM events WHERE event_id='coal-1'"
            let gen2StartedAt = sqliteScalar<float> stateDb "SELECT b.started_at FROM builds b JOIN generations g ON g.build_token=b.token WHERE g.generation=2"
            let settleElapsed = gen2StartedAt - firstEventAt
            Assert.True(settleElapsed <= maxSettleSeconds, sprintf "expected elapsed settle time <= %.2fs, got %.3fs" maxSettleSeconds settleElapsed)
            let gen2 = coordinator.State.ActiveGeneration().Value
            Assert.Equal(2L, gen2.Generation)
            Assert.Equal(Some revA, gen2.Revisions.["alpha"])
            Assert.Equal(Some revB, gen2.Revisions.["beta"])
            do! Task.Delay 250
            Assert.Equal(2L, coordinator.State.ActiveGeneration().Value.Generation)
        finally
            stop.Cancel()
            joined running |> fun t -> t.GetAwaiter().GetResult()
}

[<Fact>]
let ``post fence noop event hint marks source arrived when observed while pre fence hint does not`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    // Pre-fence hint for site must NOT satisfy the later crossing fence.
    f.Clock.Advance(1.0) |> ignore
    state.EventHint("pre-fence-site", "site", "push") |> ignore
    f.Clock.Advance(1.0) |> ignore
    state.HoldCrossing("noop-fence", [ "spec"; "site" ], 60.0, "noop hint test") |> ignore
    // Observe pre-fence site hint with baseline revision -> site must stay pending!
    state.ObserveHeads([ "site", Some(rev 'b' 40) ], f.Clock.Advance(1.0)) |> ignore
    let afterPre = state.PendingPlan()
    Assert.True(afterPre.Crossing.IsSome)
    Assert.Equal<string list>([ "site"; "spec" ], afterPre.Crossing.Value.PendingSources)
    // spec pushes a real new revision.
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance(1.0)) |> ignore
    Assert.Equal<string list>([ "site" ], state.PendingPlan().Crossing.Value.PendingSources)
    // site gets a post-fence EventHint with unchanged baseline revision.
    f.Clock.Advance(1.0) |> ignore
    state.EventHint("post-fence-site", "site", "push") |> ignore
    state.ObserveHeads([ "site", Some(rev 'b' 40) ], f.Clock.Advance(1.0)) |> ignore
    let released = state.PendingPlan()
    Assert.True(released.Crossing.IsNone)
    Assert.True(released.SettleUntil.IsNone)
    Assert.Equal("all_arrived", sqliteScalar<string> f.Path "SELECT release_reason FROM crossing_fences WHERE fence_id='noop-fence'")

[<Fact>]
let ``fifo crossing queue parks default latest and ask while pinned queries drain concurrently`` () = task {
    // Part A: deterministic FIFO wakeup order verification on Requests.Service.
    let changed = ref (completed<unit> ())
    let mutable fresh = false
    let dispatchOrder = ConcurrentQueue<int>()
    let ctx : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> changed.Value.Task)
          Readiness = (fun _ ->
              Task.FromResult(Obj [ "fresh", Bool fresh; "mode", Str "latest"; "snapshot_id", Str "s2"; "current_snapshot_id", Str "s2" ]))
          Evaluate = (fun q _ _ -> Task.FromResult(Str q.Text)) }
    let svc = new Requests.Service(ctx, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxConcurrentReads = 1 })
    try
        let tasks =
            [ for i in 1..4 ->
                svc.ExecuteRead(None, (fun () -> dispatchOrder.Enqueue i; Int(int64 i)), CancellationToken.None) ]
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        while svc.QueuedRetrievals <> 4 do do! Task.Delay(1, deadline.Token)
        Assert.Equal(4, svc.QueuedRetrievals)
        fresh <- true
        let prev = changed.Value
        changed.Value <- completed<unit> ()
        prev.SetResult()
        let! results = bounded (Task.WhenAll tasks)
        Assert.Equal<int list>([ 1; 2; 3; 4 ], List.ofSeq dispatchOrder)
        Assert.Equal<J array>([| Int 1L; Int 2L; Int 3L; Int 4L |], results)
        Assert.Equal(0, svc.QueuedRetrievals)
    finally
        joined (svc.Close()) |> fun t -> t.GetAwaiter().GetResult()

    // Part B: full Kestrel HTTP + DuckPGQ + RecordingModel + SSE observe test.
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        let opts = coordOptions corpus extensions libduckdb (Some 0.0) (Some 0.0)
        let coordinator = Coordinator.Coordinator(opts, CancellationToken.None)
        Assert.Empty(coordinator.Observe())
        Assert.True(bool' (coordinator.RunOnce true) "published")

        let model = RecordingModel(fixtureSettings, [], None)
        let askPlan = Obj [ "lookups", Arr [ Obj [ "query", Str "0.md"; "repo", Str "FidelityFramework/alpha"; "corpus_layer", Null ] ] ]
        model.Responses.Add(Choice1Of2 askPlan)
        model.OnCall <- fun idx ->
            if idx = 1 then
                let userJson =
                    match model.Calls.[1] with
                    | Arr [ _; Obj pairs ] -> pairs |> List.pick (fun (k, v) -> if k = "content" then Some(match v with Str s -> parse s | _ -> failwith "bad content") else None)
                    | _ -> failwith "unexpected messages"
                let seedId =
                    items (field userJson "seed_lookups")
                    |> List.collect (fun l -> items (field l "results"))
                    |> List.pick (fun r -> if text (field r "path") = "0.md" then Some(text (field r "id")) else None)
                model.Responses.Add(Choice1Of2(generatedFor seedId))
        use server = new Server(corpus, Some(model :> ModelClient), None)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        let _, schema1 = server.Call(Obj [ "action", Str "schema" ])
        let snap1 = text (field schema1 "snapshot_id")
        let id0 = idOfPath server snap1 "0.md"
        let id1 = idOfPath server snap1 "1.md"

        use! stream = openObserve http server.Token
        let! initial = stream.NextFrame()
        Assert.Equal("fresh", text (field initial "state"))

        let! sHold, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"q-fence","sources":["alpha","beta"],"ttl_seconds":60,"reason":"queue"}""" CancellationToken.None
        Assert.Equal(200, sHold)
        let! held0 = stream.NextFrame()
        Assert.Equal("crossing_held", text (field held0 "state"))
        Assert.Equal(Int 0L, field held0 "queued_retrievals")

        // Explicit wait_for_fresh:false returns immediate 503 index_not_current.
        let sFailFast, bFailFast = server.Call(Obj [ "action", Str "schema"; "wait_for_fresh", Bool false ])
        Assert.Equal(503, sFailFast)
        Assert.Equal("index_not_current", text (field bFailFast "error"))
        Assert.Contains("crossing_held", strings (field bFailFast "freshness") "reasons")

        // Pinned negative controls: missing snapshot_id (400), mismatched snapshot_id (409), invalid field combinations (400).
        Assert.Equal(400, fst (server.Call(Obj [ "action", Str "prompt"; "text", Str question; "freshness_mode", Str "pinned" ])))
        Assert.Equal(400, fst (server.Call(Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str snap1; "freshness_mode", Str "pinned"; "wait_for_fresh", Bool true ])))
        Assert.Equal(400, fst (server.Call(Obj [ "action", Str "schema"; "freshness_mode", Str "pinned" ])))
        Assert.Equal(400, fst (server.Call(Obj [ "action", Str "ask"; "text", Str question; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])))
        Assert.Equal(400, fst (server.Call(Obj [ "action", Str "schema"; "freshness_mode", Str "invalid" ])))
        Assert.Equal(400, fst (server.Call(Obj [ "action", Str "schema"; "wait_for_fresh", Str "yes" ])))
        Assert.Equal(400, fst (server.Call(Obj [ "action", Str "schema"; "wait_timeout_ms", Int 0L ])))
        let sMismatch, bMismatch = server.Call(Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str (String('f', 64)); "freshness_mode", Str "pinned" ])
        Assert.Equal(409, sMismatch)
        Assert.Equal("snapshot_changed", text (field bMismatch "error"))

        // Default latest queries (WITHOUT passing wait_for_fresh) park behind the crossing gate; verify live queued_retrievals = 4.
        let qSchema = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema"}""" CancellationToken.None
        let qFindOldSnap = postJson http "v1/retrieval" (Some server.Token) (jsonOf (Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str snap1 ])) CancellationToken.None
        let qPrompt = postJson http "v1/retrieval" (Some server.Token) (jsonOf (Obj [ "action", Str "prompt"; "text", Str question ])) CancellationToken.None
        let qAsk = postJson http "v1/retrieval" (Some server.Token) (jsonOf (Obj [ "action", Str "ask"; "text", Str question ])) CancellationToken.None

        let! _, q4Frame = stream.ReadUntil(fun f -> text (field f "state") = "crossing_held" && int' (field f "queued_retrievals") = 4L)
        Assert.Equal(Int 4L, field q4Frame "queued_retrievals")

        // Concurrent control: snapshot-pinned find, pgq, sources, and prompt complete 200 against N WHILE latest waiters remain parked!
        let sFind, bFind = server.Call(Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])
        Assert.Equal(200, sFind)
        Assert.Equal("pinned", text (field (field bFind "freshness") "mode"))
        let sPgq, bPgq = server.Call(Obj [ "action", Str "pgq"; "query", Str examples.[0]; "parameters", Obj [ "seed", Str id0 ]; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])
        Assert.Equal(200, sPgq)
        Assert.Equal(id1, text (field (items (field bPgq "results")).[0] "id"))
        let sSrc, bSrc = server.Call(Obj [ "action", Str "sources"; "ids", Arr [ Str id1 ]; "snapshot_id", Str snap1; "max_chars", Int 400L; "freshness_mode", Str "pinned" ])
        Assert.Equal(200, sSrc)
        Assert.Equal("Content 1\nEvidence\n", text (field (items (field bSrc "sources")).[0] "text"))
        let sPrompt, bPrompt = server.Call(Obj [ "action", Str "prompt"; "text", Str question; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])
        Assert.Equal(200, sPrompt)
        Assert.Equal("pinned", text (field (field bPrompt "freshness") "mode"))
        Assert.False(qSchema.IsCompleted)
        Assert.False(qFindOldSnap.IsCompleted)
        Assert.False(qPrompt.IsCompleted)
        Assert.False(qAsk.IsCompleted)

        // Commit both repos -> observe -> auto-release -> build gen 2; prepare scripted model responses for gen 2.
        corpus.Commit("alpha", "2.md", "Content 2 gen2\nEvidence\n") |> ignore
        corpus.Commit("beta", "2.md", "Content 2 gen2\nEvidence\n") |> ignore
        Assert.Empty(coordinator.Observe())
        let! _, idxFrame = stream.ReadUntil(fun f -> text (field f "state") = "indexing")
        Assert.Equal("indexing", text (field idxFrame "state"))

        // Model responses for ask against gen 2 are populated dynamically via model.OnCall.
        let published = coordinator.RunOnce false
        Assert.True(bool' published "published")
        let snap2 = text (field published "snapshot_id")

        let! sSchema2, bSchema2 = bounded qSchema
        let! sFindOld, bFindOld = bounded qFindOldSnap
        let! sPrompt2, bPrompt2 = bounded qPrompt
        let! sAsk2, bAsk2 = bounded qAsk
        Assert.Equal(200, sSchema2)
        Assert.Equal(409, sFindOld)
        Assert.Equal("snapshot_changed", text (field bFindOld "error"))
        Assert.True(bool' bFindOld "retryable")
        Assert.Equal(200, sPrompt2)
        Assert.Equal(200, sAsk2)
        Assert.Equal(snap2, text (field bSchema2 "snapshot_id"))
        Assert.Equal(snap2, text (field bPrompt2 "snapshot_id"))
        Assert.Equal(snap2, text (field bAsk2 "snapshot_id"))

        // Verify SSE reached fresh gen 2.
        let! _, fresh2 = stream.ReadUntil(fun f -> text (field f "state") = "fresh" && int' (field f "generation") = 2L)
        Assert.Equal(snap2, text (field fresh2 "snapshot_id"))

        // Old snap1 in pinned mode now fails closed (409) since gen 2 is active.
        let sOldPin, bOldPin = server.Call(Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])
        Assert.Equal(409, sOldPin)
        Assert.Equal("snapshot_changed", text (field bOldPin "error"))
}

[<Fact>]
let ``crossing hold and mid staging commits during n+1 staging publish n+1 and coalesce n+2`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        let opts = coordOptions corpus extensions libduckdb (Some 0.0) (Some 0.0)
        let coordinator = Coordinator.Coordinator(opts, CancellationToken.None)
        Assert.Empty(coordinator.Observe())
        Assert.True(bool' (coordinator.RunOnce true) "published")
        Assert.Equal(1L, coordinator.State.ActiveGeneration().Value.Generation)

        // Commit alpha for N+1, then while N+1 is actively staging (*.building.duckdb on disk),
        // place fence-n2 and push an actual N+2 commit + push hint for alpha.
        let revN1 = corpus.Commit("alpha", "2.md", "N+1 commit\nEvidence\n")
        Assert.Empty(coordinator.Observe())
        let generationsDir = Path.Combine(corpus.Root, "generations")
        let mutable revN2a = ""
        let mutable stagingObserved = false

        let n1Result =
            coordinator.RunOnce(false, onStaged = fun () ->
                Assert.NotEmpty(Directory.GetFiles(generationsDir, "*.building.duckdb"))
                stagingObserved <- true
                coordinator.State.HoldCrossing("fence-n2", [ "alpha"; "beta" ], 60.0, "hold N+2 while N+1 stages") |> ignore
                revN2a <- corpus.Commit("alpha", "2.md", "N+2 alpha mid-staging\nEvidence\n")
                coordinator.State.EventHint("mid-staging-alpha", "alpha", "push") |> ignore)

        Assert.True(stagingObserved)
        Assert.NotEqual<string>("", revN2a)
        Assert.True(bool' n1Result "published")
        Assert.Equal("completed", str' n1Result "status")
        Assert.Equal(Int 2L, field n1Result "generation")
        let gen2 = coordinator.State.ActiveGeneration().Value
        Assert.Equal(2L, gen2.Generation)
        Assert.Equal(Some revN1, gen2.Revisions.["alpha"])
        // Mid-staging push for alpha was observed before CompleteBuild without aborting N+1,
        // and immediately appears in PendingPlan while fence-n2 holds until beta arrives.
        let afterN1Plan = coordinator.State.PendingPlan()
        Assert.Equal(Some 2L, afterN1Plan.BaseGeneration)
        Assert.Equal(Some revN2a, afterN1Plan.DesiredRevisions.["alpha"])
        Assert.Equal(Some revN2a, afterN1Plan.Targets.["alpha"])
        Assert.True(afterN1Plan.Crossing.IsSome)
        Assert.Equal<string list>([ "beta" ], afterN1Plan.Crossing.Value.PendingSources)
        let gen2Freshness = coordinator.State.Freshness(gen2.SnapshotId, 90.0)
        Assert.False(bool' gen2Freshness "fresh")
        Assert.Contains("crossing_held", strings gen2Freshness "reasons")
        Assert.Contains("indexing_pending", strings gen2Freshness "reasons")
        Assert.Equal("crossing_held", str' (coordinator.RunOnce false) "status")
        Assert.Equal("crossing_held", str' (coordinator.RunOnce true) "status")

        // Push N+2 commit to beta -> auto-releases fence-n2 and builds ONE gen 3 coalescing both mid-staging and post-staging commits.
        let revN2b = corpus.Commit("beta", "2.md", "N+2 beta\nEvidence\n")
        coordinator.State.EventHint("post-staging-beta", "beta", "push") |> ignore
        Assert.Empty(coordinator.Observe())
        let n2Result = coordinator.RunOnce false
        Assert.True(bool' n2Result "published")
        Assert.Equal(Int 3L, field n2Result "generation")
        let gen3 = coordinator.State.ActiveGeneration().Value
        Assert.Equal(Some revN2a, gen3.Revisions.["alpha"])
        Assert.Equal(Some revN2b, gen3.Revisions.["beta"])
        Assert.True(bool' (coordinator.State.Freshness(gen3.SnapshotId, 90.0)) "fresh")
}

[<Fact>]
let ``queue_full returns 429 and wait_timeout_ms returns 503 with clean queued_retrievals drain`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        let qOpts : Requests.QueueOptions =
            { MaxQueued = 2; DefaultWaitTimeout = TimeSpan.FromSeconds 10.0; MaxWaitTimeout = TimeSpan.FromSeconds 10.0; MaxConcurrentReads = 4 }
        use server = new Server(corpus, None, None, queueOptions = qOpts)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        use! stream = openObserve http server.Token
        let! _ = stream.NextFrame()

        let! sHold, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"cap-fence","sources":["test"],"ttl_seconds":60}""" CancellationToken.None
        Assert.Equal(200, sHold)
        let! _ = stream.NextFrame()

        // Test wait_timeout_ms first: park one caller with 60ms timeout, verify 503 wait_timeout and drain back to 0.
        let! sTimeout, bTimeout = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema","wait_for_fresh":true,"wait_timeout_ms":60}""" CancellationToken.None
        Assert.Equal(503, sTimeout)
        Assert.Equal("wait_timeout", text (field bTimeout "error"))
        Assert.True(bool' bTimeout "retryable")
        let! _, drainedFrame = stream.ReadUntil(fun f -> text (field f "state") = "crossing_held" && int' (field f "queued_retrievals") = 0L)
        Assert.Equal(Int 0L, field drainedFrame "queued_retrievals")

        // Fill queue to MaxQueued = 2, verify 3rd request fails immediately with 429 queue_full.
        let p1 = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema","wait_for_fresh":true}""" CancellationToken.None
        let p2 = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema","wait_for_fresh":true}""" CancellationToken.None
        let! _, fullFrame = stream.ReadUntil(fun f -> text (field f "state") = "crossing_held" && int' (field f "queued_retrievals") = 2L)
        Assert.Equal(Int 2L, field fullFrame "queued_retrievals")

        let! sFull, bFull = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema","wait_for_fresh":true}""" CancellationToken.None
        Assert.Equal(429, sFull)
        Assert.Equal("queue_full", text (field bFull "error"))
        Assert.True(bool' bFull "retryable")
        let! sAskFull, bAskFull = postJson http "v1/retrieval" (Some server.Token) """{"action":"ask","text":"Which files does 0.md link to?"}""" CancellationToken.None
        Assert.Equal(429, sAskFull)
        Assert.Equal("queue_full", text (field bAskFull "error"))

        // Release crossing and confirm the 2 parked queries succeed once the reconcile hint is observed.
        let! sRel, _ = postJson http "v1/crossing/release" (Some server.Token) """{"fence_id":"cap-fence"}""" CancellationToken.None
        Assert.Equal(200, sRel)
        Assert.Empty(corpus.Observe())
        let! r1s, _ = bounded p1
        let! r2s, _ = bounded p2
        Assert.Equal(200, r1s)
        Assert.Equal(200, r2s)
}

[<Fact>]
let ``expired fence ttl releases deterministically and wakes observe without polling`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        use server = new Server(corpus, None, None)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        use! stream = openObserve http server.Token
        let! initial = stream.NextFrame()
        Assert.Equal("fresh", text (field initial "state"))

        let! sHold, rHold = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"ttl-fence","sources":["test"],"ttl_seconds":0.25,"reason":"ttl test"}""" CancellationToken.None
        Assert.Equal(200, sHold)
        Assert.Equal("held", text (field rHold "status"))

        let! heldFrame = stream.NextFrame()
        Assert.Equal("crossing_held", text (field heldFrame "state"))
        Assert.True(has heldFrame "held_until_ms")

        // Without any polling or extra pushes, observe wakes at held_until and transitions back to fresh.
        let! recovered = stream.NextFrame()
        Assert.Equal("fresh", text (field recovered "state"))
        Assert.Equal(Int 1L, field recovered "generation")
        let stateDb = Path.Combine(corpus.Root, "index-state.sqlite")
        Assert.Equal("ttl_expired", sqliteScalar<string> stateDb "SELECT release_reason FROM crossing_fences WHERE fence_id='ttl-fence'")
}

[<Fact>]
let ``cancelling parked caller mid wait drains slot cleanly without leaking capacity`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        let qOpts : Requests.QueueOptions =
            { MaxQueued = 2; DefaultWaitTimeout = TimeSpan.FromSeconds 10.0; MaxWaitTimeout = TimeSpan.FromSeconds 10.0; MaxConcurrentReads = 4 }
        use server = new Server(corpus, None, None, queueOptions = qOpts)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        use! stream = openObserve http server.Token
        let! _ = stream.NextFrame()

        let! sHold, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"cancel-fence","sources":["test"],"ttl_seconds":60}""" CancellationToken.None
        Assert.Equal(200, sHold)
        let! _ = stream.NextFrame()

        use cts1 = new CancellationTokenSource()
        use cts2 = new CancellationTokenSource()
        let p1 = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema","wait_for_fresh":true}""" cts1.Token
        let p2 = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema","wait_for_fresh":true}""" cts2.Token
        let! _, fullFrame = stream.ReadUntil(fun f -> text (field f "state") = "crossing_held" && int' (field f "queued_retrievals") = 2L)
        Assert.Equal(Int 2L, field fullFrame "queued_retrievals")

        let! sRejected, _ = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema","wait_for_fresh":true}""" CancellationToken.None
        Assert.Equal(429, sRejected)

        // Cancel head waiter; queued_retrievals drops to 1 and frees the slot.
        cts1.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> p1 :> Task)
        let! _, oneLeft = stream.ReadUntil(fun f -> text (field f "state") = "crossing_held" && int' (field f "queued_retrievals") = 1L)
        Assert.Equal(Int 1L, field oneLeft "queued_retrievals")

        // New waiter p3 claims the freed slot cleanly.
        let p3 = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema","wait_for_fresh":true}""" CancellationToken.None
        let! _, backToTwo = stream.ReadUntil(fun f -> text (field f "state") = "crossing_held" && int' (field f "queued_retrievals") = 2L)
        Assert.Equal(Int 2L, field backToTwo "queued_retrievals")

        // Cancel p2 too, leaving only p3, then release crossing and confirm p3 completes cleanly.
        cts2.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> p2 :> Task)
        let! _, oneLeftAgain = stream.ReadUntil(fun f -> text (field f "state") = "crossing_held" && int' (field f "queued_retrievals") = 1L)
        Assert.Equal(Int 1L, field oneLeftAgain "queued_retrievals")

        let! sRel, _ = postJson http "v1/crossing/release" (Some server.Token) """{"fence_id":"cancel-fence"}""" CancellationToken.None
        Assert.Equal(200, sRel)
        Assert.Empty(corpus.Observe())
        let! s3, b3 = bounded p3
        Assert.Equal(200, s3)
        Assert.True(bool' (field b3 "freshness") "fresh")
}

[<Fact>]
let ``build failure and restart mid staging cleans temp files without wedging crossing gate`` () =
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        let opts = coordOptions corpus extensions libduckdb (Some 0.0) (Some 0.0)
        let coord1 = Coordinator.Coordinator(opts, CancellationToken.None)
        Assert.Empty(coord1.Observe())
        Assert.True(bool' (coord1.RunOnce true) "published")
        let gen1 = coord1.State.ActiveGeneration().Value

        // Part A: inject a failure mid-staging (unreachable remote during extract) and verify temp/lease cleanup.
        corpus.Commit("test", "2.md", "Content 2 faulted\nEvidence\n") |> ignore
        Assert.Empty(coord1.Observe())
        let badOpts = { opts with RemoteBase = Path.Combine(Path.GetDirectoryName corpus.Config, "missing-remote") }
        let failingCoord = Coordinator.Coordinator(badOpts, CancellationToken.None)
        Assert.ThrowsAny<Exception>(fun () -> failingCoord.RunOnce false |> ignore) |> ignore
        let generationsDir = Path.Combine(corpus.Root, "generations")
        Assert.Empty(Directory.GetFiles(generationsDir, "*.building.duckdb*"))
        Assert.Equal(gen1.SnapshotId, coord1.State.ActiveGeneration().Value.SnapshotId)
        let stateDb = Path.Combine(corpus.Root, "index-state.sqlite")
        Assert.Equal(0L, sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM builds WHERE status='running'")

        // Part B: simulate hard process crash mid-staging (orphan temp files + VALID future lease) while a crossing fence is held.
        let plan = coord1.State.PendingPlan()
        let crashedToken = coord1.State.StartBuild plan
        let orphanDb = Path.Combine(generationsDir, crashedToken + ".building.duckdb")
        let orphanWal = orphanDb + ".wal"
        File.WriteAllText(orphanDb, "partial")
        File.WriteAllText(orphanWal, "partial-wal")
        coord1.State.HoldCrossing("survives-restart", [ "test" ], 60.0, "survives restart") |> ignore
        let leaseUntil = sqliteScalar<float> stateDb (sprintf "SELECT lease_until FROM builds WHERE token='%s'" crashedToken)
        Assert.True(leaseUntil > coord1.State.Now() + 600.0, sprintf "expected valid future lease, got %.1f" leaseUntil)

        let restarted = Coordinator.Coordinator(opts, CancellationToken.None)
        // Constructor alone does not disrupt a live lease; exclusive-lock startup reclaim does immediately.
        Assert.Equal(1L, sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM builds WHERE status='running'")
        Assert.True(File.Exists orphanDb)
        let reclaimed = restarted.ReclaimOnStartup()
        Assert.Equal(1, reclaimed)
        Assert.False(File.Exists orphanDb)
        Assert.False(File.Exists orphanWal)
        Assert.Equal(0L, sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM builds WHERE status='running'")
        Assert.Equal("abandoned", sqliteScalar<string> stateDb (sprintf "SELECT status FROM builds WHERE token='%s'" crashedToken))
        Assert.Equal("indexer_restarted", sqliteScalar<string> stateDb (sprintf "SELECT error FROM builds WHERE token='%s'" crashedToken))
        Assert.Equal("crossing_held", str' (restarted.RunOnce false) "status")
        Assert.Equal("crossing_held", str' (restarted.RunOnce true) "status")

        // Releasing the crossing requires reconciling the unarrived source, then lets the subsequent build succeed cleanly as gen 2 without waiting out the 900s lease.
        restarted.State.ReleaseCrossing("survives-restart") |> ignore
        Assert.Empty(restarted.Observe())
        let recovered = restarted.RunOnce false
        Assert.True(bool' recovered "published")
        Assert.Equal(Int 2L, field recovered "generation")
        Assert.Equal(2L, restarted.State.ActiveGeneration().Value.Generation)

[<Fact>]
let ``observeSince replays fresh crossing_held indexing fresh and tolerates legacy transition rows`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    let initial = state.ObserveSince(90.0, None)
    Assert.True(bool' initial.Current.Freshness "fresh")
    let cursor0 = initial.Sequence

    // Transition 1: crossing_held
    f.Clock.Advance(1.0) |> ignore
    state.HoldCrossing("replay-fence", [ "spec" ], 60.0, "replay") |> ignore
    // Transition 2: spec arrives -> auto-releases fence, leaving spec indexing pending
    state.ObserveHeads([ "spec", Some(rev 'e' 40) ], f.Clock.Advance(1.0)) |> ignore
    // Transition 3: gen 2 publishes -> fresh
    let plan = state.PendingPlan()
    let token = state.StartBuild plan
    state.CompleteBuild(token, rev '2' 64, [ for kv in plan.Targets -> kv.Key, kv.Value ]) |> ignore

    let replay = state.ObserveSince(90.0, Some cursor0)
    match replay.History with
    | IndexState.Truncated -> failwith "Expected transitions, not Truncated"
    | IndexState.Transitions items ->
        Assert.Equal(2, items.Length)
        Assert.True(items.[0].Crossing.IsSome)
        Assert.Contains("crossing_held", strings items.[0].Freshness "reasons")
        Assert.True(items.[1].Crossing.IsNone)
        Assert.Contains("indexing_pending", strings items.[1].Freshness "reasons")
        Assert.DoesNotContain("crossing_held", strings items.[1].Freshness "reasons")
    Assert.True(bool' replay.Current.Freshness "fresh")
    Assert.Equal(Int 2L, field replay.Current.Freshness "generation")

    // Legacy transition row written without "crossing" property in facts JSON parses cleanly as Crossing = None.
    do
        use conn = new SqliteConnection(sprintf "Data Source=%s" f.Path)
        conn.Open()
        use cmd = conn.CreateCommand()
        cmd.CommandText <- "UPDATE readiness_transitions SET facts=$legacy WHERE seq=$seq"
        let legacyFacts =
            jsonOf (Obj [ "observed_at", Obj [ "site", Float (f.Clock.Now()); "spec", Float (f.Clock.Now()) ]
                          "required", Arr []
                          "pending", Arr [ Str "spec" ]
                          "changed_at", Float (f.Clock.Now()) ])
        cmd.Parameters.AddWithValue("$legacy", legacyFacts) |> ignore
        cmd.Parameters.AddWithValue("$seq", cursor0 + 1L) |> ignore
        Assert.Equal(1, cmd.ExecuteNonQuery())

    let legacyReplay = state.ObserveSince(90.0, Some cursor0)
    match legacyReplay.History with
    | IndexState.Truncated -> failwith "Expected legacy transitions"
    | IndexState.Transitions items ->
        Assert.True(items.Length >= 1)
        Assert.True(items.[0].Crossing.IsNone)
        Assert.Contains("indexing_pending", strings items.[0].Freshness "reasons")

[<Fact>]
let ``pinned mode fails closed with 409 corpus_configuration_changed when config identity changes`` () =
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        let published = corpus.Publish()
        let snap1 = text (field published "snapshot_id")
        let changedConfig = Path.Combine(Path.GetDirectoryName corpus.Config, "changed-corpus.json")
        File.WriteAllText(changedConfig, File.ReadAllText(corpus.Config).Replace("refs/heads/main", "refs/heads/master"))
        let reconfigured = ManagedStore(corpus.Root, changedConfig, corpus.Engine, promptFile)
        try
            let ex = Assert.Throws<PinnedSnapshotError>(fun () -> reconfigured.FindPinned("0.md", snap1, None, 10L, None) |> ignore)
            match ex :> exn with
            | PinnedSnapshotError(code, status, retryable) ->
                Assert.Equal("corpus_configuration_changed", code)
                Assert.Equal(409, status)
                Assert.True(retryable)
            | other -> raise other
        finally reconfigured.Close()

[<Fact>]
let ``runOnce force and startBuild refuse to build a partial batch while crossing fence is held`` () =
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        let opts = coordOptions corpus extensions libduckdb (Some 0.0) (Some 0.0)
        let coordinator = Coordinator.Coordinator(opts, CancellationToken.None)
        Assert.Empty(coordinator.Observe())
        Assert.True(bool' (coordinator.RunOnce true) "published")
        Assert.Equal(1L, coordinator.State.ActiveGeneration().Value.Generation)

        coordinator.State.HoldCrossing("no-force-bypass", [ "alpha"; "beta" ], 60.0, "enforce crossing for all callers") |> ignore
        corpus.Commit("alpha", "2.md", "Partial batch alpha\nEvidence\n") |> ignore
        Assert.Empty(coordinator.Observe())

        let forced = coordinator.RunOnce true
        Assert.Equal("crossing_held", str' forced "status")
        let regular = coordinator.RunOnce false
        Assert.Equal("crossing_held", str' regular "status")
        Assert.Equal(1L, coordinator.State.ActiveGeneration().Value.Generation)

        let plan = coordinator.State.PendingPlan()
        Assert.NotEmpty(plan.Targets)
        let ex = Assert.Throws<IndexState.StateError>(fun () -> coordinator.State.StartBuild plan |> ignore)
        Assert.Equal("Crossing fence is held; build cannot start until crossing releases", ex.Data0)

[<Fact>]
let ``reconcile hint and pre hold push observed after hold do not mark crossing arrival`` () =
    // Negative control (a): a failed-observation reconcile hint followed by an unchanged head
    // observation must NOT mark the source as arrived or auto-release the crossing fence.
    use f1 = new Fixture()
    let s1 = f1.State
    f1.PublishInitial s1 |> ignore
    f1.Clock.Advance(1.0) |> ignore
    s1.HoldCrossing("reconcile-fence", [ "spec" ], 60.0, "reconcile negative control") |> ignore
    f1.Clock.Advance(1.0) |> ignore
    s1.EventHint("observation-failed:spec:1", "spec", "reconcile") |> ignore
    s1.ObserveHeads([ "spec", Some(rev 'a' 40) ], f1.Clock.Advance(1.0)) |> ignore
    let afterReconcile = s1.PendingPlan()
    Assert.True(afterReconcile.Crossing.IsSome)
    Assert.Equal<string list>([ "spec" ], afterReconcile.Crossing.Value.PendingSources)

    // Negative control (b): a pre-hold push first observed AFTER hold (even with a new revision)
    // must NOT count as a post-hold arrival or auto-release the fence.
    use f2 = new Fixture()
    let s2 = f2.State
    f2.PublishInitial s2 |> ignore
    f2.Clock.Advance(1.0) |> ignore
    s2.EventHint("pre-hold-push-spec", "spec", "push") |> ignore
    f2.Clock.Advance(1.0) |> ignore
    s2.HoldCrossing("pre-hold-fence", [ "spec" ], 60.0, "pre-hold negative control") |> ignore
    // First observation of the pre-hold push arrives after hold with a changed revision.
    s2.ObserveHeads([ "spec", Some(rev 'e' 40) ], f2.Clock.Advance(1.0)) |> ignore
    let afterPreHoldObs = s2.PendingPlan()
    Assert.True(afterPreHoldObs.Crossing.IsSome)
    Assert.Equal<string list>([ "spec" ], afterPreHoldObs.Crossing.Value.PendingSources)
    // Positive control: a genuine post-hold push observed after hold DOES release the fence.
    f2.Clock.Advance(1.0) |> ignore
    s2.EventHint("post-hold-push-spec", "spec", "push") |> ignore
    s2.ObserveHeads([ "spec", Some(rev 'f' 40) ], f2.Clock.Advance(1.0)) |> ignore
    let released = s2.PendingPlan()
    Assert.True(released.Crossing.IsNone)
    Assert.Equal("all_arrived", sqliteScalar<string> f2.Path "SELECT release_reason FROM crossing_fences WHERE fence_id='pre-hold-fence'")

[<Fact>]
let ``overlapping short and long fences drop short fence sources at earliest ttl without waiting for long fence`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        use server = new Server(corpus, None, None)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        use! stream = openObserve http server.Token
        let! initial = stream.NextFrame()
        Assert.Equal("fresh", text (field initial "state"))

        let! sLong, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"long-fence","sources":["beta"],"ttl_seconds":60,"reason":"long"}""" CancellationToken.None
        Assert.Equal(200, sLong)
        let! heldLong = stream.NextFrame()
        Assert.Equal("crossing_held", text (field heldLong "state"))
        Assert.Equal<string list>([ "beta" ], strings heldLong "expected_sources")

        let sw = Stopwatch.StartNew()
        let! sShort, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"short-fence","sources":["alpha"],"ttl_seconds":0.25,"reason":"short"}""" CancellationToken.None
        Assert.Equal(200, sShort)
        let! heldBoth = stream.NextFrame()
        Assert.Equal("crossing_held", text (field heldBoth "state"))
        Assert.Equal<string list>([ "alpha"; "beta" ], strings heldBoth "expected_sources")

        // Without waiting for the 60s long-fence, observe wakes at 0.25s and drops alpha immediately.
        let! dropped = stream.NextFrame()
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds 5.0, sprintf "expected earliest TTL transition well before 60s, took %A" sw.Elapsed)
        Assert.Equal("crossing_held", text (field dropped "state"))
        Assert.Equal<string list>([ "beta" ], strings dropped "expected_sources")
        Assert.Equal<string list>([ "beta" ], strings dropped "pending_sources")
        let stateDb = Path.Combine(corpus.Root, "index-state.sqlite")
        Assert.Equal("ttl_expired", sqliteScalar<string> stateDb "SELECT release_reason FROM crossing_fences WHERE fence_id='short-fence'")
}

[<Fact>]
let ``parked latest caller wakes on crossing ttl expiry without sse observer or background indexer`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        let published = corpus.Publish()
        let snap1 = text (field published "snapshot_id")
        // No SSE subscriber and no background Coordinator.run; SQLite stays completely idle until TTL expiry.
        use server = new Server(corpus, None, None)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)

        let! sHold, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"idle-ttl","sources":["test"],"ttl_seconds":0.25,"reason":"idle ttl wakeup"}""" CancellationToken.None
        Assert.Equal(200, sHold)

        let sw = Stopwatch.StartNew()
        let qSchema = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema"}""" CancellationToken.None
        let qFind = postJson http "v1/retrieval" (Some server.Token) (jsonOf (Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str snap1 ])) CancellationToken.None

        let! sSchema, bSchema = bounded qSchema
        let! sFind, bFind = bounded qFind
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds 5.0, sprintf "expected idle TTL wakeup promptly, took %A" sw.Elapsed)
        Assert.Equal(200, sSchema)
        Assert.Equal(200, sFind)
        Assert.Equal(snap1, text (field bSchema "snapshot_id"))
        Assert.Equal(snap1, text (field bFind "snapshot_id"))
        Assert.True(bool' (field bSchema "freshness") "fresh")
        let stateDb = Path.Combine(corpus.Root, "index-state.sqlite")
        Assert.Equal("ttl_expired", sqliteScalar<string> stateDb "SELECT release_reason FROM crossing_fences WHERE fence_id='idle-ttl'")
}

[<Fact>]
let ``bounded intake before readiness and front requeue on raced index not current preserve strict fifo`` () = task {
    // Part A: intake enforces MaxQueued atomically BEFORE ReadinessAsync completes.
    let readinessEntered = completed<unit> ()
    let releaseReadiness = completed<unit> ()
    let ctxBound : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> (completed<unit> ()).Task)
          Readiness = (fun ct -> task {
              readinessEntered.TrySetResult() |> ignore
              do! releaseReadiness.Task.WaitAsync ct
              return Obj [ "fresh", Bool true; "mode", Str "latest"; "snapshot_id", Str "s1"; "current_snapshot_id", Str "s1" ] })
          Evaluate = (fun q _ _ -> Task.FromResult(Str q.Text)) }
    let svcBound = new Requests.Service(ctxBound, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 2; MaxConcurrentReads = 2 })
    try
        let r1 = svcBound.ExecuteRead(None, (fun () -> Int 1L), CancellationToken.None)
        do! bounded readinessEntered.Task
        let r2 = svcBound.ExecuteRead(None, (fun () -> Int 2L), CancellationToken.None)
        let! fullEx = Assert.ThrowsAsync<EnglishError>(fun () -> svcBound.ExecuteRead(None, (fun () -> Int 3L), CancellationToken.None) :> Task)
        Assert.Equal(("queue_full", 429, true), englishError fullEx)
        releaseReadiness.SetResult()
        let! v1 = bounded r1
        let! v2 = bounded r2
        Assert.Equal(Int 1L, v1)
        Assert.Equal(Int 2L, v2)
    finally
        joined (svcBound.Close()) |> fun t -> t.GetAwaiter().GetResult()

    // Part B: a dequeued waiter hitting a raced IndexNotCurrent re-enqueues at the FRONT (AddFirst),
    // completing strictly ahead of later queued waiters.
    let changed = ref (completed<unit> ())
    let mutable fresh = false
    let signalFresh value =
        fresh <- value
        let prev = changed.Value
        changed.Value <- completed<unit> ()
        prev.SetResult()
    let completedOrder = ConcurrentQueue<int>()
    let w1RacedOnce = ref false
    let w1Failed = completed<unit> ()
    let ctxFifo : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> changed.Value.Task)
          Readiness = (fun _ ->
              Task.FromResult(Obj [ "fresh", Bool fresh; "mode", Str "latest"; "snapshot_id", Str "s2"; "current_snapshot_id", Str "s2" ]))
          Evaluate = (fun q _ _ -> Task.FromResult(Str q.Text)) }
    let svcFifo = new Requests.Service(ctxFifo, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxConcurrentReads = 1 })
    try
        let w1 =
            svcFifo.ExecuteRead(None, (fun () ->
                if not (Interlocked.Exchange(&w1RacedOnce.contents, true)) then
                    fresh <- false
                    w1Failed.TrySetResult() |> ignore
                    raise (IndexNotCurrent(Obj [ "fresh", Bool false; "reasons", Arr [ Str "indexing_pending" ] ]))
                completedOrder.Enqueue 1
                Int 1L), CancellationToken.None)
        let w2 =
            svcFifo.ExecuteRead(None, (fun () ->
                completedOrder.Enqueue 2
                Int 2L), CancellationToken.None)
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        while svcFifo.QueuedRetrievals <> 2 do do! Task.Delay(1, deadline.Token)
        signalFresh true
        do! bounded w1Failed.Task
        // Wait briefly for w1 to re-enqueue at the front before flipping fresh back to true.
        while svcFifo.QueuedRetrievals <> 2 do do! Task.Delay(1, deadline.Token)
        signalFresh true
        let! out = bounded (Task.WhenAll [ w1; w2 ])
        Assert.Equal<int list>([ 1; 2 ], List.ofSeq completedOrder)
        Assert.Equal<J array>([| Int 1L; Int 2L |], out)
    finally
        joined (svcFifo.Close()) |> fun t -> t.GetAwaiter().GetResult()
}

[<Fact>]
let ``callPinned surfaces duckdb load failure as 503 instead of masking as 409`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        let published = corpus.Publish()
        let snap1 = text (field published "snapshot_id")
        let artifact = Path.Combine(corpus.Root, "generations", snap1 + ".duckdb")
        File.Delete artifact
        Assert.False(File.Exists artifact)

        // Direct ManagedStore.FindPinned propagates DuckDBException (never masking as PinnedSnapshotError 409).
        let freshStore = corpus.Open()
        try
            Assert.Throws<DuckDB.NET.Data.DuckDBException>(fun () -> freshStore.FindPinned("0.md", snap1, None, 10L, None) |> ignore) |> ignore
        finally freshStore.Close()

        // HTTP gateway returns 503 (never 409 snapshot_changed) when the generation artifact is unreadable/corrupt.
        File.WriteAllText(artifact, "not-a-valid-duckdb-file")
        use server = new Server(corpus, None, None)
        let status, body = server.Call(Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])
        Assert.Equal(503, status)
        Assert.NotEqual<string>("snapshot_changed", text (field body "error"))
}

[<Fact>]
let ``released fence retention stays bounded and settle bypass scopes to released fence sources only`` () =
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore

    // Part A: bounded released fence retention (256) never deletes active fences.
    state.HoldCrossing("active-1", [ "spec" ], 300.0, "keep active 1") |> ignore
    state.HoldCrossing("active-2", [ "site" ], 300.0, "keep active 2") |> ignore
    for i in 1 .. IndexState.releasedFenceRetention + 14 do
        f.Clock.Advance(0.01) |> ignore
        let fid = sprintf "hist-%04d" i
        state.HoldCrossing(fid, [ "other" ], 60.0, "retention test") |> ignore
        state.ReleaseCrossing(fid) |> ignore
    Assert.Equal(int64 IndexState.releasedFenceRetention, sqliteScalar<int64> f.Path "SELECT COUNT(*) FROM crossing_fences WHERE released_at IS NOT NULL")
    Assert.Equal(2L, sqliteScalar<int64> f.Path "SELECT COUNT(*) FROM crossing_fences WHERE released_at IS NULL")
    state.ReleaseCrossing("active-1") |> ignore
    state.ReleaseCrossing("active-2") |> ignore

    // Part B: releasing a fence bypasses settle ONLY for the released fence's sources,
    // leaving unrelated dirty sources in their quiet period.
    use g = new Fixture()
    let s = g.State
    g.PublishInitial s |> ignore
    let t0 = g.Clock.Advance(10.0)
    s.HoldCrossing("scoped-fence", [ "spec" ], 60.0, "scoped settle") |> ignore
    // Unrelated source 'other' gets an unannounced push while scoped-fence is held on 'spec'.
    let tOther = g.Clock.Advance(1.0)
    s.ObserveHeads([ "other", Some(rev 'e' 64) ], tOther) |> ignore
    // 'spec' arrives and auto-releases scoped-fence.
    let tSpec = g.Clock.Advance(1.0)
    s.ObserveHeads([ "spec", Some(rev 'e' 40) ], tSpec) |> ignore
    Assert.True(tSpec > t0)
    let afterRelease = s.PendingPlan()
    Assert.True(afterRelease.Crossing.IsNone)
    Assert.Equal(1L, sqliteScalar<int64> g.Path "SELECT settle_bypassed FROM sources WHERE source_id='spec'")
    Assert.Equal(0L, sqliteScalar<int64> g.Path "SELECT settle_bypassed FROM sources WHERE source_id='other'")
    // SettleUntil still reflects 'other's quiet window (tOther + settleSeconds), not cleared!
    Assert.Equal(Some(tOther + s.SettleSeconds), afterRelease.SettleUntil)

[<Fact>]
let ``durable manifest identity rebinds cleanly on coordinator upgrade while old serve fails closed`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        let secret = Encoding.UTF8.GetBytes(String('s', 40))

        // Part A: fresh unindexed root — serve is check-only and never grabs manifest_identity ownership.
        use unindexed = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        Directory.CreateDirectory unindexed.Root |> ignore
        let unindexedDb = Path.Combine(unindexed.Root, "index-state.sqlite")
        let checkOnlyStore = unindexed.Open()
        try
            Assert.True(checkOnlyStore.ManifestIdentityMatches())
            Assert.Equal(0L, sqliteScalar<int64> unindexedDb "SELECT COUNT(*) FROM metadata WHERE key='manifest_identity'")
            checkOnlyStore.State.HoldCrossing("pre-index-fence", [ "alpha" ], 60.0, "check-only hold", expectedManifestIdentity = checkOnlyStore.Identity) |> ignore
            checkOnlyStore.State.EventHint("pre-index-push", "alpha", "push", expectedManifestIdentity = checkOnlyStore.Identity) |> ignore
            checkOnlyStore.State.ReleaseCrossing("pre-index-fence", expectedManifestIdentity = checkOnlyStore.Identity) |> ignore
            Assert.Equal(0L, sqliteScalar<int64> unindexedDb "SELECT COUNT(*) FROM metadata WHERE key='manifest_identity'")
        finally checkOnlyStore.Close()

        // Part B: publish gen 1, start old serve, upgrade alpha's policy and restart Coordinator.
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        let pub1 = corpus.Publish()
        let snap1 = text (field pub1 "snapshot_id")
        let oldIdentity = corpus.Coordinator.Identity
        let stateDb = Path.Combine(corpus.Root, "index-state.sqlite")
        Assert.Equal(oldIdentity, sqliteScalar<string> stateDb "SELECT value FROM metadata WHERE key='manifest_identity'")

        use oldServer = new Server(corpus, None, Some secret)
        use oldHttp = oldServer.CreateClient(TimeSpan.FromSeconds 30.0)
        let sBefore, bBefore = oldServer.Call(Obj [ "action", Str "schema"; "wait_for_fresh", Bool false ])
        Assert.Equal(200, sBefore)
        Assert.Equal(snap1, text (field bBefore "snapshot_id"))

        // Upgrade corpus config: add a valid policy on alpha (distinct manifest_identity, same repos/refs).
        let upgradedConfig = Path.Combine(Path.GetDirectoryName corpus.Config, "upgraded-corpus.json")
        let upgradedJson =
            File.ReadAllText(corpus.Config)
                .Replace(""""id":"alpha","repo":"FidelityFramework/alpha","ref":"refs/heads/main","depends_on":[]""",
                         """"id":"alpha","repo":"FidelityFramework/alpha","ref":"refs/heads/main","depends_on":[],"policy":{"role":"central","authority":80}""")
        File.WriteAllText(upgradedConfig, upgradedJson)
        let upgradedOpts = { coordOptions corpus extensions libduckdb (Some 0.0) (Some 0.0) with Config = upgradedConfig }

        // Positive control: Coordinator rebinds manifest_identity atomically, invalidates sources, and publishes gen 2.
        let upgradedCoord = Coordinator.Coordinator(upgradedOpts, CancellationToken.None)
        let newIdentity = upgradedCoord.Identity
        Assert.NotEqual<string>(oldIdentity, newIdentity)
        Assert.Equal(newIdentity, sqliteScalar<string> stateDb "SELECT value FROM metadata WHERE key='manifest_identity'")
        Assert.True(sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM invalidations WHERE reason='Corpus manifest identity changed'" >= 2L)
        let pendingAfterRebind = upgradedCoord.State.PendingPlan()
        Assert.True(pendingAfterRebind.Targets.ContainsKey "alpha")
        Assert.True(pendingAfterRebind.Targets.ContainsKey "beta")

        Assert.Empty(upgradedCoord.Observe())
        let pub2 = upgradedCoord.RunOnce true
        Assert.True(bool' pub2 "published")
        let snap2 = text (field pub2 "snapshot_id")
        Assert.NotEqual<string>(snap1, snap2)
        let active2 = upgradedCoord.State.ActiveGeneration().Value
        Assert.Equal(2L, active2.Generation)
        Assert.Equal(Some newIdentity, active2.ConfigurationSha256)
        Assert.True(bool' (upgradedCoord.State.Freshness(snap2, 90.0)) "fresh")

        use upgradedServer = new Server(corpus, None, Some secret, config = upgradedConfig)
        let sNew, bNew = upgradedServer.Call(Obj [ "action", Str "schema"; "wait_for_fresh", Bool false ])
        Assert.Equal(200, sNew)
        Assert.Equal(snap2, text (field bNew "snapshot_id"))
        Assert.True(bool' (field bNew "freshness") "fresh")

        // Exact-reason negative control: old serve holding oldIdentity fails closed immediately with 409 on latest, fail-fast, ask, pinned, and writes.
        let swDrift = Stopwatch.StartNew()
        let sDefaultOld, bDefaultOld = oldServer.Call(Obj [ "action", Str "schema" ])
        Assert.Equal(409, sDefaultOld)
        Assert.Equal("corpus_configuration_changed", text (field bDefaultOld "error"))
        Assert.True(swDrift.Elapsed < TimeSpan.FromSeconds 5.0, sprintf "expected immediate 409 on default latest read, took %A" swDrift.Elapsed)

        let sReadOld, bReadOld = oldServer.Call(Obj [ "action", Str "schema"; "wait_for_fresh", Bool false ])
        Assert.Equal(409, sReadOld)
        Assert.Equal("corpus_configuration_changed", text (field bReadOld "error"))

        let sAskDefaultOld, bAskDefaultOld = oldServer.Call(Obj [ "action", Str "ask"; "text", Str question ])
        Assert.Equal(409, sAskDefaultOld)
        Assert.Equal("corpus_configuration_changed", text (field bAskDefaultOld "error"))

        let sAskFastOld, bAskFastOld = oldServer.Call(Obj [ "action", Str "ask"; "text", Str question; "wait_for_fresh", Bool false ])
        Assert.Equal(409, sAskFastOld)
        Assert.Equal("corpus_configuration_changed", text (field bAskFastOld "error"))

        let sPinOld, bPinOld = oldServer.Call(Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str snap2; "freshness_mode", Str "pinned" ])
        Assert.Equal(409, sPinOld)
        Assert.Equal("corpus_configuration_changed", text (field bPinOld "error"))

        // Mid-read manifest rebind control: CallPinned re-verifies manifest_identity after action and fails closed with 409.
        use midRebindCorpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        let midPub = midRebindCorpus.Publish()
        let midSnap = text (field midPub "snapshot_id")
        let midStore = midRebindCorpus.Open()
        try
            let midEx =
                Assert.Throws<PinnedSnapshotError>(fun () ->
                    midStore.CallPinned((fun _ ->
                        midRebindCorpus.Coordinator.State.RebindManifestIdentity(String('b', 64), [ "test" ], "mid-read manifest rebind") |> ignore
                        Null), midSnap) |> ignore)
            match midEx :> exn with
            | PinnedSnapshotError(code, status, retryable) ->
                Assert.Equal("corpus_configuration_changed", code)
                Assert.Equal(409, status)
                Assert.True(retryable)
            | other -> raise other
        finally midStore.Close()

        let oldStore = corpus.Open()
        try
            let! obsOld = bounded (oldStore.ObserveAsync(None, CancellationToken.None))
            Assert.False(bool' obsOld.Current.Freshness "fresh")
            Assert.Equal<string list>([ "corpus_configuration_changed" ], strings obsOld.Current.Freshness "reasons")
        finally oldStore.Close()

        upgradedCoord.State.HoldCrossing("valid-fence", [ "alpha" ], 60.0, "from upgraded index", expectedManifestIdentity = newIdentity) |> ignore

        let! sHold, bHold = postJson oldHttp "v1/crossing/hold" (Some oldServer.Token) """{"fence_id":"drifted-hold","sources":["alpha"],"ttl_seconds":60}""" CancellationToken.None
        Assert.Equal(409, sHold)
        Assert.Equal("corpus_configuration_changed", text (field bHold "error"))
        Assert.Equal(0L, sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM crossing_fences WHERE fence_id='drifted-hold'")

        let! sRel, bRel = postJson oldHttp "v1/crossing/release" (Some oldServer.Token) """{"fence_id":"valid-fence"}""" CancellationToken.None
        Assert.Equal(409, sRel)
        Assert.Equal("corpus_configuration_changed", text (field bRel "error"))
        Assert.Equal(1L, sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM crossing_fences WHERE fence_id='valid-fence' AND released_at IS NULL")

        let payload = Obj [ "repository", Obj [ "full_name", Str "FidelityFramework/alpha" ]; "ref", Str "refs/heads/main"; "after", Str (String('a', 40)) ]
        let raw = Encoding.UTF8.GetBytes(canon payload)
        let sigHex = Convert.ToHexString(HMACSHA256.HashData(secret, raw)).ToLowerInvariant()
        let sHook, bHookText = oldServer.Post("v1/index/events/forgejo", raw, [ "X-Forgejo-Signature", sigHex; "X-Forgejo-Event", "push"; "X-Forgejo-Delivery", "drifted-push-1" ])
        let bHook = parse bHookText
        Assert.Equal(409, sHook)
        Assert.False(bool' bHook "accepted")
        Assert.Equal("corpus_configuration_changed", text (field bHook "error"))
        Assert.Equal(0L, sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM events WHERE event_id='drifted-push-1'")
}

[<Fact>]
let ``concurrent raced index not current retries preserve ticket fifo and never exceed max queued`` () = task {
    let runScenario (failFirstTicket: int) = task {
        let gate = obj ()
        let changed = ref (completed<unit> ())
        let mutable admitBudget = 0
        let signalAdmit count =
            lock gate (fun () ->
                admitBudget <- count
                let prev = changed.Value
                changed.Value <- completed<unit> ()
                prev.SetResult())
        let w1Entered, w2Entered = completed<unit> (), completed<unit> ()
        let releaseW1Fail, releaseW2Fail = completed<unit> (), completed<unit> ()
        let w1FailedOnce, w2FailedOnce = ref false, ref false
        let completedOrder = ConcurrentQueue<int>()
        let ctx : Requests.Ctx =
            { Sources = []
              Observe = (fun _ _ -> failwith "unused")
              WatchFailure = (completed<exn> ()).Task
              WhileWatchHealthy = (fun a -> a ())
              WatchReadiness = (fun () -> lock gate (fun () -> changed.Value.Task))
              Readiness = (fun _ ->
                  let isFresh =
                      lock gate (fun () ->
                          if admitBudget > 0 then
                              admitBudget <- admitBudget - 1
                              true
                          else false)
                  Task.FromResult(Obj [ "fresh", Bool isFresh; "mode", Str "latest"; "snapshot_id", Str "s2"; "current_snapshot_id", Str "s2" ]))
              Evaluate = (fun q _ _ -> Task.FromResult(Str q.Text)) }
        let svc = new Requests.Service(ctx, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 2; MaxConcurrentReads = 2 })
        try
            let makeAction ticket (entered: TaskCompletionSource<unit>) (releaseFail: TaskCompletionSource<unit>) (failedOnce: bool ref) =
                fun (ct: CancellationToken) -> task {
                    if not (Interlocked.Exchange(&failedOnce.contents, true)) then
                        entered.TrySetResult() |> ignore
                        do! releaseFail.Task.WaitAsync ct
                        return raise (IndexNotCurrent(Obj [ "fresh", Bool false; "reasons", Arr [ Str "indexing_pending" ] ]))
                    else
                        completedOrder.Enqueue ticket
                        return Int(int64 ticket)
                }
            let w1 = svc.ExecuteReadAsync(None, None, makeAction 1 w1Entered releaseW1Fail w1FailedOnce, CancellationToken.None)
            use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
            while svc.QueuedRetrievals <> 1 do do! Task.Delay(1, deadline.Token)
            let w2 = svc.ExecuteReadAsync(None, None, makeAction 2 w2Entered releaseW2Fail w2FailedOnce, CancellationToken.None)
            while svc.QueuedRetrievals <> 2 do do! Task.Delay(1, deadline.Token)

            // Admit BOTH Ticket 1 and Ticket 2 concurrently into action.
            signalAdmit 2
            do! bounded w1Entered.Task
            do! bounded w2Entered.Task

            // While both waiters are in-flight (waitQueue empty, 2 retry reservations held), 3rd caller gets 429 queue_full.
            let! fullWhileInFlight = Assert.ThrowsAsync<EnglishError>(fun () -> svc.ExecuteRead(None, (fun () -> Int 3L), CancellationToken.None) :> Task)
            Assert.Equal(("queue_full", 429, true), englishError fullWhileInFlight)

            // Fail the chosen ticket first, verify 4th caller still gets 429 queue_full, then fail the other ticket.
            if failFirstTicket = 1 then
                releaseW1Fail.SetResult()
                while svc.QueuedRetrievals <> 1 do do! Task.Delay(1, deadline.Token)
                let! fullMid = Assert.ThrowsAsync<EnglishError>(fun () -> svc.ExecuteRead(None, (fun () -> Int 4L), CancellationToken.None) :> Task)
                Assert.Equal(("queue_full", 429, true), englishError fullMid)
                releaseW2Fail.SetResult()
            else
                releaseW2Fail.SetResult()
                while svc.QueuedRetrievals <> 1 do do! Task.Delay(1, deadline.Token)
                let! fullMid = Assert.ThrowsAsync<EnglishError>(fun () -> svc.ExecuteRead(None, (fun () -> Int 4L), CancellationToken.None) :> Task)
                Assert.Equal(("queue_full", 429, true), englishError fullMid)
                releaseW1Fail.SetResult()

            while svc.QueuedRetrievals <> 2 do do! Task.Delay(1, deadline.Token)
            let! fullBothParked = Assert.ThrowsAsync<EnglishError>(fun () -> svc.ExecuteRead(None, (fun () -> Int 5L), CancellationToken.None) :> Task)
            Assert.Equal(("queue_full", 429, true), englishError fullBothParked)

            // Admit strictly ONE waiter: Ticket 1 must complete while Ticket 2 stays parked.
            signalAdmit 1
            let! r1 = bounded w1
            Assert.Equal(Int 1L, r1)
            Assert.False(w2.IsCompleted)
            Assert.Equal(1, svc.QueuedRetrievals)

            // Admit the second waiter: Ticket 2 completes second.
            signalAdmit 1
            let! r2 = bounded w2
            Assert.Equal(Int 2L, r2)
            Assert.Equal<int list>([ 1; 2 ], List.ofSeq completedOrder)
            Assert.Equal(0, svc.QueuedRetrievals)

            // Positive follow-up: reservations were cleanly released.
            signalAdmit 1
            let! r6 = bounded (svc.ExecuteRead(None, (fun () -> Int 6L), CancellationToken.None))
            Assert.Equal(Int 6L, r6)
        finally
            joined (svc.Close()) |> fun t -> t.GetAwaiter().GetResult()
    }
    // Both failure orders preserve Ticket 1 < Ticket 2 and never exceed MaxQueued = 2.
    do! runScenario 1
    do! runScenario 2
}

[<Fact>]
let ``ask with wait_for_fresh false fails fast at admission and mid inference without parking`` () = task {
    let askQuestion text : Requests.Question =
        { Text = text; Snapshot = None; Limit = 8L; MaxChars = 2400L; IncludeContext = false; IncludeQuery = true; IncludeResults = true }

    // Part A: unit controls on Requests.Service.Ask(..., waitForFresh = false).
    let changed = ref (completed<unit> ())
    let mutable fresh = false
    let evalEntered, evalRelease = completed<unit> (), completed<unit> ()
    let mutable calls = 0
    let ctx : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> changed.Value.Task)
          Readiness = (fun _ ->
              let reasons = if fresh then Arr [] else Arr [ Str "crossing_held" ]
              Task.FromResult(Obj [ "fresh", Bool fresh; "mode", Str "latest"; "snapshot_id", Str "s1"; "current_snapshot_id", Str "s1"; "reasons", reasons ]))
          Evaluate = (fun _ snapshot ct -> task {
              Interlocked.Increment(&calls) |> ignore
              evalEntered.TrySetResult() |> ignore
              do! evalRelease.Task.WaitAsync ct
              return Obj [ "snapshot_id", Str snapshot; "answer", Str "ok" ] }) }
    let svc = new Requests.Service(ctx, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 1; MaxConcurrentReads = 1 })
    try
        // Freshness lost at admission (even with MaxQueued = 1 saturated by a waiting caller): fails fast with IndexNotCurrent.
        let parked = svc.Ask(askQuestion "parked waiter", CancellationToken.None)
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        while svc.QueuedRetrievals <> 1 do do! Task.Delay(1, deadline.Token)
        let! admEx = Assert.ThrowsAsync<IndexNotCurrent>(fun () -> svc.Ask(askQuestion "fail-fast admission", CancellationToken.None, waitForFresh = false) :> Task)
        match admEx :> exn with
        | IndexNotCurrent freshness -> Assert.Contains("crossing_held", strings freshness "reasons")
        | other -> raise other
        Assert.Equal(0, calls)
        Assert.Equal(1, svc.QueuedRetrievals)

        // Drain the parked waiter, then test mid-inference freshness loss with waitForFresh = false.
        fresh <- true
        evalRelease.TrySetResult() |> ignore
        let prev = changed.Value
        changed.Value <- completed<unit> ()
        prev.SetResult()
        let! _ = bounded parked
        Assert.Equal(0, svc.QueuedRetrievals)
    finally
        joined (svc.Close()) |> fun t -> t.GetAwaiter().GetResult()

    // Freshness lost mid-inference: Ask(waitForFresh = false) raises IndexNotCurrent immediately without parking.
    let evalEntered2, evalRelease2 = completed<unit> (), completed<unit> ()
    let mutable fresh2 = true
    let mutable calls2 = 0
    let ctxMid : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> (completed<unit> ()).Task)
          Readiness = (fun _ ->
              let reasons = if fresh2 then Arr [] else Arr [ Str "crossing_held" ]
              Task.FromResult(Obj [ "fresh", Bool fresh2; "mode", Str "latest"; "snapshot_id", Str "s1"; "current_snapshot_id", Str "s1"; "reasons", reasons ]))
          Evaluate = (fun _ snapshot ct -> task {
              Interlocked.Increment(&calls2) |> ignore
              evalEntered2.TrySetResult() |> ignore
              do! evalRelease2.Task.WaitAsync ct
              return Obj [ "snapshot_id", Str snapshot; "answer", Str "ok" ] }) }
    let svcMid = new Requests.Service(ctxMid, Requests.defaultObserverOptions, Requests.defaultQueueOptions)
    try
        let inFlight = svcMid.Ask(askQuestion "mid-inference loss", CancellationToken.None, waitForFresh = false)
        do! bounded evalEntered2.Task
        fresh2 <- false
        evalRelease2.SetResult()
        let! midEx = Assert.ThrowsAsync<IndexNotCurrent>(fun () -> inFlight :> Task)
        match midEx :> exn with
        | IndexNotCurrent freshness -> Assert.Contains("crossing_held", strings freshness "reasons")
        | other -> raise other
        Assert.Equal(1, calls2)
        Assert.Equal(0, svcMid.QueuedRetrievals)
    finally
        joined (svcMid.Close()) |> fun t -> t.GetAwaiter().GetResult()

    // Part B: real Kestrel HTTP + RecordingModel proves wait_for_fresh:false never parks on crossing hold mid-inference or at admission.
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        let model = RecordingModel(fixtureSettings, [], None)
        let askPlan = Obj [ "lookups", Arr [ Obj [ "query", Str "0.md"; "repo", Str "FidelityFramework/test"; "corpus_layer", Null ] ] ]
        model.Responses.Add(Choice1Of2 askPlan)
        model.OnCall <- fun idx ->
            if idx = 0 then
                // Crossing hold lands mid-inference right after HTTP dispatch admitted ask!
                corpus.Coordinator.State.HoldCrossing("mid-ask-fence", [ "test" ], 60.0, "mid-inference crossing hold") |> ignore
        use server = new Server(corpus, Some(model :> ModelClient), None)
        let sMid, bMid = server.Call(Obj [ "action", Str "ask"; "text", Str question; "wait_for_fresh", Bool false ])
        Assert.Equal(503, sMid)
        Assert.Equal("index_not_current", text (field bMid "error"))
        Assert.Contains("crossing_held", strings (field bMid "freshness") "reasons")

        // Subsequent ask with wait_for_fresh:false while hold is active also fails fast with 503 without calling the model.
        let callsBefore = model.Calls.Count
        let sHeld, bHeld = server.Call(Obj [ "action", Str "ask"; "text", Str question; "wait_for_fresh", Bool false ])
        Assert.Equal(503, sHeld)
        Assert.Equal("index_not_current", text (field bHeld "error"))
        Assert.Contains("crossing_held", strings (field bHeld "freshness") "reasons")
        Assert.Equal(callsBefore, model.Calls.Count)
}

[<Fact>]
let ``pinned reads enforce bounded intake and async gate wait while draining concurrently during crossing hold`` () = task {
    // Part A: unit control on Requests.Service.ExecutePinnedAsync (bounded intake, wait_timeout, and concurrent drain).
    let changed = ref (completed<unit> ())
    let mutable fresh = false
    let ctx : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> changed.Value.Task)
          Readiness = (fun _ ->
              Task.FromResult(Obj [ "fresh", Bool fresh; "mode", Str "latest"; "snapshot_id", Str "s1"; "current_snapshot_id", Str "s1" ]))
          Evaluate = (fun q _ _ -> Task.FromResult(Str q.Text)) }
    let svc = new Requests.Service(ctx, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 2; MaxConcurrentReads = 1 })
    try
        // Park 2 latest callers (MaxQueued = 2 full behind crossing hold); pinned reads STILL drain immediately.
        let latest1 = svc.ExecuteRead(None, (fun () -> Str "latest-1"), CancellationToken.None)
        let latest2 = svc.ExecuteRead(None, (fun () -> Str "latest-2"), CancellationToken.None)
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        while svc.QueuedRetrievals <> 2 do do! Task.Delay(1, deadline.Token)

        let! pinImmediate = bounded (svc.ExecutePinnedAsync(None, (fun _ -> Task.FromResult(Str "pinned-ok")), CancellationToken.None))
        Assert.Equal(Str "pinned-ok", pinImmediate)
        Assert.False(latest1.IsCompleted)
        Assert.False(latest2.IsCompleted)

        // Saturate readConcurrency (1 active slow pinned read) + MaxQueued = 2 waiting pinned reads -> 4th fails closed 429 queue_full.
        let pinEntered, pinRelease = completed<unit> (), completed<unit> ()
        let slowPin =
            svc.ExecutePinnedAsync(None, (fun ct -> task {
                pinEntered.TrySetResult() |> ignore
                do! pinRelease.Task.WaitAsync ct
                return Str "pin-active"
            }), CancellationToken.None)
        do! bounded pinEntered.Task

        let waitingPin1 = svc.ExecutePinnedAsync(None, (fun _ -> Task.FromResult(Str "pin-wait-1")), CancellationToken.None)
        // Verify wait_timeout_ms on a waiting pinned read returns 503 wait_timeout and releases its waiting slot.
        let! pinTimeoutEx =
            Assert.ThrowsAsync<EnglishError>(fun () ->
                svc.ExecutePinnedAsync(Some(TimeSpan.FromMilliseconds 40.0), (fun _ -> Task.FromResult(Str "pin-timeout")), CancellationToken.None) :> Task)
        Assert.Equal(("wait_timeout", 503, true), englishError pinTimeoutEx)

        let waitingPin2 = svc.ExecutePinnedAsync(None, (fun _ -> Task.FromResult(Str "pin-wait-2")), CancellationToken.None)
        let! pinFullEx =
            Assert.ThrowsAsync<EnglishError>(fun () ->
                svc.ExecutePinnedAsync(None, (fun _ -> Task.FromResult(Str "pin-overflow")), CancellationToken.None) :> Task)
        Assert.Equal(("queue_full", 429, true), englishError pinFullEx)

        // Release active pinned read -> waiting pinned reads drain cleanly while latest waiters remain parked.
        pinRelease.SetResult()
        let! pActive = bounded slowPin
        let! pWait1 = bounded waitingPin1
        let! pWait2 = bounded waitingPin2
        Assert.Equal(Str "pin-active", pActive)
        Assert.Equal(Str "pin-wait-1", pWait1)
        Assert.Equal(Str "pin-wait-2", pWait2)
        Assert.False(latest1.IsCompleted)
        Assert.False(latest2.IsCompleted)

        // Release latest callers cleanly.
        fresh <- true
        let prev = changed.Value
        changed.Value <- completed<unit> ()
        prev.SetResult()
        let! l1 = bounded latest1
        let! l2 = bounded latest2
        Assert.Equal(Str "latest-1", l1)
        Assert.Equal(Str "latest-2", l2)
    finally
        joined (svc.Close()) |> fun t -> t.GetAwaiter().GetResult()

    // Part B: real ManagedStore.CallPinnedAsync cancels without blocking gate.Wait(3000) and HTTP pinned endpoints drain while latest waits.
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        let published = corpus.Publish()
        let snap1 = text (field published "snapshot_id")

        // Prove CallPinnedAsync uses non-blocking gate.WaitAsync(ct) and cancels promptly when gate is held.
        let store = corpus.Open()
        let gateEntered, gateRelease = completed<unit> (), completed<unit> ()
        let holder = Task.Run(fun () ->
            store.Call(fun _ -> gateEntered.TrySetResult() |> ignore; gateRelease.Task.GetAwaiter().GetResult(); Null))
        try
            do! bounded gateEntered.Task
            use cancelCts = new CancellationTokenSource(TimeSpan.FromMilliseconds 40.0)
            let sw = Stopwatch.StartNew()
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> store.FindPinnedAsync("0.md", snap1, None, 10L, None, cancelCts.Token) :> Task)
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds 1.0, sprintf "expected prompt async cancellation, took %A" sw.Elapsed)
            gateRelease.SetResult()
            let! _ = bounded holder
            let! okPin = bounded (store.FindPinnedAsync("0.md", snap1, None, 10L, None, CancellationToken.None))
            Assert.Equal("pinned", text (field (field okPin "freshness") "mode"))
        finally
            gateRelease.TrySetResult() |> ignore
            (try holder.GetAwaiter().GetResult() |> ignore with _ -> ())
            store.Close()

        // HTTP server with MaxQueued = 1, MaxConcurrentReads = 1: latest caller fills wait queue while pinned endpoints drain concurrently.
        let qOpts : Requests.QueueOptions =
            { MaxQueued = 1; DefaultWaitTimeout = TimeSpan.FromSeconds 10.0; MaxWaitTimeout = TimeSpan.FromSeconds 10.0; MaxConcurrentReads = 1 }
        use server = new Server(corpus, None, None, queueOptions = qOpts)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        let id0 = idOfPath server snap1 "0.md"
        let id1 = idOfPath server snap1 "1.md"

        use! stream = openObserve http server.Token
        let! _ = stream.NextFrame()
        let! sHold, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"pin-drain-fence","sources":["test"],"ttl_seconds":60}""" CancellationToken.None
        Assert.Equal(200, sHold)
        let! _ = stream.NextFrame()

        let parkedLatest = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema"}""" CancellationToken.None
        let! _, q1Frame = stream.ReadUntil(fun f -> text (field f "state") = "crossing_held" && int' (field f "queued_retrievals") = 1L)
        Assert.Equal(Int 1L, field q1Frame "queued_retrievals")

        // Latest queue is saturated (429), yet pinned find/pgq/sources/prompt all drain 200 concurrently via *PinnedAsync!
        let! sLatestFull, bLatestFull = postJson http "v1/retrieval" (Some server.Token) """{"action":"schema"}""" CancellationToken.None
        Assert.Equal(429, sLatestFull)
        Assert.Equal("queue_full", text (field bLatestFull "error"))

        let sFind, bFind = server.Call(Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])
        Assert.Equal(200, sFind)
        Assert.Equal("pinned", text (field (field bFind "freshness") "mode"))
        let sPgq, bPgq = server.Call(Obj [ "action", Str "pgq"; "query", Str examples.[0]; "parameters", Obj [ "seed", Str id0 ]; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])
        Assert.Equal(200, sPgq)
        Assert.Equal(id1, text (field (items (field bPgq "results")).[0] "id"))
        let sSrc, bSrc = server.Call(Obj [ "action", Str "sources"; "ids", Arr [ Str id1 ]; "snapshot_id", Str snap1; "max_chars", Int 400L; "freshness_mode", Str "pinned" ])
        Assert.Equal(200, sSrc)
        Assert.Equal("Content 1\nEvidence\n", text (field (items (field bSrc "sources")).[0] "text"))
        let sPrompt, bPrompt = server.Call(Obj [ "action", Str "prompt"; "text", Str question; "snapshot_id", Str snap1; "freshness_mode", Str "pinned" ])
        Assert.Equal(200, sPrompt)
        Assert.Equal("pinned", text (field (field bPrompt "freshness") "mode"))
        Assert.False(parkedLatest.IsCompleted)

        let! sRel, _ = postJson http "v1/crossing/release" (Some server.Token) """{"fence_id":"pin-drain-fence"}""" CancellationToken.None
        Assert.Equal(200, sRel)
        Assert.Empty(corpus.Observe())
        let! sLatestOk, _ = bounded parkedLatest
        Assert.Equal(200, sLatestOk)
}

[<Fact>]
let ``manual crossing release requires reconcile observation for unarrived sources and coalesces one generation`` () = task {
    // Part A: when all expected sources already arrived, ReleaseCrossing inserts zero extra reconcile hints;
    // when only a subset arrived, only unarrived expected sources get a reconcile hint.
    use f = new Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    f.Clock.Advance(1.0) |> ignore
    state.HoldCrossing("all-arrived-edge", [ "site"; "spec" ], 60.0, "covered edge") |> ignore
    f.Clock.Advance(1.0) |> ignore
    state.EventHint("p-site-1", "site", "push") |> ignore
    state.EventHint("p-spec-1", "spec", "push") |> ignore
    let tObs = f.Clock.Advance(1.0)
    // Inject an extra push on 'site' after tObs so allExpectedArrived waits for obsHint >= lastHint while both are in arrived_sources.
    f.Clock.Advance(1.0) |> ignore
    state.EventHint("p-site-2", "site", "push") |> ignore
    state.ObserveHeads([ "site", Some(rev 'd' 40); "spec", Some(rev 'e' 40) ], tObs) |> ignore
    Assert.True(state.PendingPlan().Crossing.IsSome)
    state.ReleaseCrossing("all-arrived-edge") |> ignore
    Assert.Equal(0L, sqliteScalar<int64> f.Path "SELECT COUNT(*) FROM events WHERE event_kind='reconcile'")

    // Part B: real multi-repo Coordinator.run + manual POST /v1/crossing/release coalesces A and B into ONE generation without reintroducing the quiet period.
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus([ "alpha", standardFiles; "beta", standardFiles ], engineOf extensions libduckdb, extensions)
        let opts = coordOptions corpus extensions libduckdb (Some 10.0) (Some 15.0)
        use stop = new CancellationTokenSource()
        let coordinator = Coordinator.Coordinator(opts, stop.Token)
        Assert.Empty(coordinator.Observe())
        Assert.True(bool' (coordinator.RunOnce true) "published")
        Assert.Equal(1L, coordinator.State.ActiveGeneration().Value.Generation)

        use server = new Server(corpus, None, None)
        use http = server.CreateClient(TimeSpan.FromSeconds 30.0)
        let! sHold, _ = postJson http "v1/crossing/hold" (Some server.Token) """{"fence_id":"manual-coal","sources":["alpha","beta"],"ttl_seconds":60,"reason":"manual coal"}""" CancellationToken.None
        Assert.Equal(200, sHold)

        let revA = corpus.Commit("alpha", "2.md", "Alpha manual release\nEvidence\n")
        let revB = corpus.Commit("beta", "2.md", "Beta manual release without webhook\nEvidence\n")
        Assert.Empty(coordinator.Observe([ "alpha" ]))
        Assert.Equal<string list>([ "beta" ], coordinator.State.PendingPlan().Crossing.Value.PendingSources)

        let! sRel, rRel = postJson http "v1/crossing/release" (Some server.Token) """{"fence_id":"manual-coal"}""" CancellationToken.None
        Assert.Equal(200, sRel)
        Assert.False(bool' rRel "already_released")
        let afterRelease = coordinator.State.PendingPlan()
        Assert.True(afterRelease.Crossing.IsNone)
        Assert.Equal<string list>([ "beta" ], afterRelease.ObservationRequired)
        Assert.Equal("observation_required", str' (coordinator.RunOnce false) "status")
        Assert.Equal(1L, coordinator.State.ActiveGeneration().Value.Generation)

        let stateDb = Path.Combine(corpus.Root, "index-state.sqlite")
        Assert.Equal(1L, sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM events WHERE source_id='beta' AND event_kind='reconcile'")
        Assert.Equal(0L, sqliteScalar<int64> stateDb "SELECT COUNT(*) FROM events WHERE source_id='alpha' AND event_kind='reconcile'")

        let sw = Stopwatch.StartNew()
        use changes = new IndexState.Changes(stateDb)
        let running = Coordinator.run coordinator changes stop.Token
        try
            do! awaitState changes running (fun () ->
                coordinator.State.ActiveGeneration() |> Option.exists (fun g -> g.Generation = 2L))
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds 5.0, sprintf "manual release reintroduced 10s settle period; took %A" sw.Elapsed)
            do! Task.Delay 200
            let gen2 = coordinator.State.ActiveGeneration().Value
            Assert.Equal(2L, gen2.Generation)
            Assert.Equal(Some revA, gen2.Revisions.["alpha"])
            Assert.Equal(Some revB, gen2.Revisions.["beta"])
        finally
            stop.Cancel()
            joined running |> fun t -> t.GetAwaiter().GetResult()
}

[<Fact>]
let ``cancelled or timed out waiters drain cleanly and retries preserve strict fifo even when fresh`` () = task {
    // Part A: 40 cancelled/timed-out waiters while WatchReadiness stays uncompleted; subsequent waiter wakes cleanly.
    let changed = ref (completed<unit> ())
    let mutable fresh = false
    let ctxWait : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> changed.Value.Task)
          Readiness = (fun _ -> Task.FromResult(Obj [ "fresh", Bool fresh; "mode", Str "latest"; "snapshot_id", Str "s1"; "current_snapshot_id", Str "s1" ]))
          Evaluate = (fun q _ _ -> Task.FromResult(Str q.Text)) }
    let svcWait = new Requests.Service(ctxWait, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 64 })
    try
        let timedOutTasks =
            [ for _ in 1..20 ->
                svcWait.ExecuteRead(Some(TimeSpan.FromMilliseconds 25.0), (fun () -> Int 0L), CancellationToken.None) ]
        let cancelCts = [ for _ in 1..20 -> new CancellationTokenSource() ]
        let cancelledTasks =
            [ for cts in cancelCts ->
                svcWait.ExecuteRead(None, (fun () -> Int 0L), cts.Token) ]
        for cts in cancelCts do cts.Cancel()
        for t in timedOutTasks do
            let! ex = Assert.ThrowsAsync<EnglishError>(fun () -> t :> Task)
            Assert.Equal(("wait_timeout", 503, true), englishError ex)
        for t in cancelledTasks do
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> t :> Task)
            ()
        for cts in cancelCts do cts.Dispose()
        Assert.Equal(0, svcWait.QueuedRetrievals)
        fresh <- true
        let prev = changed.Value
        changed.Value <- completed<unit> ()
        prev.SetResult()
        let! ok = bounded (svcWait.ExecuteRead(None, (fun () -> Int 99L), CancellationToken.None))
        Assert.Equal(Int 99L, ok)
    finally
        joined (svcWait.Close()) |> fun t -> t.GetAwaiter().GetResult()

    // Part B: Ticket 1 and Ticket 2 both enter action concurrently while fresh = true;
    // Ticket 2 fails first with IndexNotCurrent while Ticket 1 is still in-flight, and MUST wait for Ticket 1 rather than skipping ahead.
    let w1Entered, w2Entered, releaseW1 = completed<unit> (), completed<unit> (), completed<unit> ()
    let w1FailedOnce, w2FailedOnce = ref false, ref false
    let order = ConcurrentQueue<int>()
    let ctxFifo : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> (completed<unit> ()).Task)
          Readiness = (fun _ -> Task.FromResult(Obj [ "fresh", Bool true; "mode", Str "latest"; "snapshot_id", Str "s2"; "current_snapshot_id", Str "s2" ]))
          Evaluate = (fun q _ _ -> Task.FromResult(Str q.Text)) }
    let svcFifo = new Requests.Service(ctxFifo, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 4; MaxConcurrentReads = 2 })
    try
        let w1 =
            svcFifo.ExecuteReadAsync(None, None, (fun ct -> task {
                if not (Interlocked.Exchange(&w1FailedOnce.contents, true)) then
                    w1Entered.TrySetResult() |> ignore
                    do! releaseW1.Task.WaitAsync ct
                    return raise (IndexNotCurrent(Obj [ "fresh", Bool false; "reasons", Arr [ Str "indexing_pending" ] ]))
                else
                    order.Enqueue 1
                    return Int 1L
            }), CancellationToken.None)
        do! bounded w1Entered.Task
        let w2 =
            svcFifo.ExecuteReadAsync(None, None, (fun _ -> task {
                if not (Interlocked.Exchange(&w2FailedOnce.contents, true)) then
                    w2Entered.TrySetResult() |> ignore
                    return raise (IndexNotCurrent(Obj [ "fresh", Bool false; "reasons", Arr [ Str "indexing_pending" ] ]))
                else
                    order.Enqueue 2
                    return Int 2L
            }), CancellationToken.None)
        do! bounded w2Entered.Task
        // Even with fresh = true, Ticket 2's retry stays parked while Ticket 1 is still in-flight.
        do! Task.Delay 60
        Assert.False(w2.IsCompleted)
        Assert.Empty(order)
        releaseW1.SetResult()
        let! res = bounded (Task.WhenAll [ w1; w2 ])
        Assert.Equal<int list>([ 1; 2 ], List.ofSeq order)
        Assert.Equal<J array>([| Int 1L; Int 2L |], res)
    finally
        joined (svcFifo.Close()) |> fun t -> t.GetAwaiter().GetResult()
}

[<Fact>]
let ``pinned gate wait budget and saturated opt out ask admission fail closed within budget`` () = task {
    // Part A: with 2 readConcurrency permits and a held ManagedStore gate, a 40ms-budget pinned read times out with 503 wait_timeout.
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        let published = corpus.Publish()
        let snap1 = text (field published "snapshot_id")
        let store = corpus.Open()
        let ctxPin : Requests.Ctx =
            { Sources = []
              Observe = (fun _ _ -> failwith "unused")
              WatchFailure = (completed<exn> ()).Task
              WhileWatchHealthy = (fun a -> a ())
              WatchReadiness = (fun () -> (completed<unit> ()).Task)
              Readiness = (fun ct -> store.ReadinessAsync ct)
              Evaluate = (fun _ _ _ -> failwith "unused") }
        let svcPin = new Requests.Service(ctxPin, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 4; MaxConcurrentReads = 2 })
        let gateEntered, gateRelease = completed<unit> (), completed<unit> ()
        let holder = Task.Run(fun () ->
            store.Call(fun _ -> gateEntered.TrySetResult() |> ignore; gateRelease.Task.GetAwaiter().GetResult(); Null))
        try
            do! bounded gateEntered.Task
            let sw = Stopwatch.StartNew()
            let! pinEx =
                Assert.ThrowsAsync<EnglishError>(fun () ->
                    svcPin.ExecutePinnedWithBudgetAsync(
                        Some(TimeSpan.FromMilliseconds 40.0),
                        (fun gateCt execCt -> store.FindPinnedAsync("0.md", snap1, None, 10L, None, execCt, gateCancellation = gateCt)),
                        CancellationToken.None) :> Task)
            Assert.Equal(("wait_timeout", 503, true), englishError pinEx)
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds 1.0, sprintf "expected prompt pinned gate wait_timeout, took %A" sw.Elapsed)

            gateRelease.SetResult()
            let! _ = bounded holder
            let! okPin =
                bounded (svcPin.ExecutePinnedWithBudgetAsync(
                    Some(TimeSpan.FromMilliseconds 500.0),
                    (fun gateCt execCt -> store.FindPinnedAsync("0.md", snap1, None, 10L, None, execCt, gateCancellation = gateCt)),
                    CancellationToken.None))
            Assert.Equal("pinned", text (field (field okPin "freshness") "mode"))
        finally
            gateRelease.TrySetResult() |> ignore
            (try holder.GetAwaiter().GetResult() |> ignore with _ -> ())
            joined (svcPin.Close()) |> fun t -> t.GetAwaiter().GetResult()
            store.Close()

    // Part B: saturated wait_for_fresh:false ask admission rejects with 503 wait_timeout and 429 queue_full instead of queueing 240s.
    let evalEntered, evalRelease = completed<unit> (), completed<unit> ()
    let ctxAsk : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> (completed<unit> ()).Task)
          Readiness = (fun _ -> Task.FromResult(Obj [ "fresh", Bool true; "mode", Str "latest"; "snapshot_id", Str "s1"; "current_snapshot_id", Str "s1" ]))
          Evaluate = (fun _ snapshot ct -> task {
              evalEntered.TrySetResult() |> ignore
              do! evalRelease.Task.WaitAsync ct
              return Obj [ "snapshot_id", Str snapshot; "answer", Str "ok" ] }) }
    let svcAsk = new Requests.Service(ctxAsk, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 2 })
    let q text : Requests.Question =
        { Text = text; Snapshot = None; Limit = 8L; MaxChars = 2400L; IncludeContext = false; IncludeQuery = true; IncludeResults = true }
    try
        let admitted = [ for _ in 1..64 -> svcAsk.Ask(q "shared opt-out", None, CancellationToken.None, waitForFresh = false) ]
        do! bounded evalEntered.Task
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        while svcAsk.Consumers <> 64 do do! Task.Delay(1, deadline.Token)

        let sw = Stopwatch.StartNew()
        let! timeoutEx =
            Assert.ThrowsAsync<EnglishError>(fun () ->
                svcAsk.Ask(q "opt-out-timeout", Some(TimeSpan.FromMilliseconds 40.0), CancellationToken.None, waitForFresh = false) :> Task)
        Assert.Equal(("wait_timeout", 503, true), englishError timeoutEx)
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds 1.0, sprintf "expected prompt opt-out admission wait_timeout, took %A" sw.Elapsed)

        let queued1 = svcAsk.Ask(q "opt-out-q1", None, CancellationToken.None, waitForFresh = false)
        let queued2 = svcAsk.Ask(q "opt-out-q2", None, CancellationToken.None, waitForFresh = false)
        let! fullEx =
            Assert.ThrowsAsync<EnglishError>(fun () ->
                svcAsk.Ask(q "opt-out-q3", None, CancellationToken.None, waitForFresh = false) :> Task)
        Assert.Equal(("queue_full", 429, true), englishError fullEx)

        evalRelease.SetResult()
        let! _ = bounded (Task.WhenAll admitted)
        let! _ = bounded queued1
        let! _ = bounded queued2
        ()
    finally
        evalRelease.TrySetResult() |> ignore
        joined (svcAsk.Close()) |> fun t -> t.GetAwaiter().GetResult()
}

[<Fact>]
let ``retrying ask inference does not head of line block latest reads`` () = task {
    let mutable currentSnap = "snap-1"
    let askAFirstEntered, askAFirstRelease = completed<unit> (), completed<unit> ()
    let askASecondEntered, askASecondRelease = completed<unit> (), completed<unit> ()
    let askBFirstEntered, askBFirstRelease = completed<unit> (), completed<unit> ()
    let ctx : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> (completed<unit> ()).Task)
          Readiness = (fun _ ->
              let s = Volatile.Read(&currentSnap)
              Task.FromResult(Obj [ "fresh", Bool true; "mode", Str "latest"; "snapshot_id", Str s; "current_snapshot_id", Str s ]))
          Evaluate = (fun q snap ct -> task {
              match q.Text, snap with
              | "ask-a", "snap-1" ->
                  askAFirstEntered.TrySetResult() |> ignore
                  do! askAFirstRelease.Task.WaitAsync ct
              | "ask-a", "snap-2" ->
                  askASecondEntered.TrySetResult() |> ignore
                  do! askASecondRelease.Task.WaitAsync ct
              | "ask-b", "snap-1" ->
                  askBFirstEntered.TrySetResult() |> ignore
                  do! askBFirstRelease.Task.WaitAsync ct
              | _ -> ()
              return Obj [ "snapshot_id", Str snap; "answer", Str (q.Text + ":" + snap) ] }) }
    let svc = new Requests.Service(ctx, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 8; MaxConcurrentReads = 4 })
    let q text : Requests.Question =
        { Text = text; Snapshot = None; Limit = 8L; MaxChars = 2400L; IncludeContext = false; IncludeQuery = true; IncludeResults = true }
    try
        let askA = svc.Ask(q "ask-a", None, CancellationToken.None, waitForFresh = true)
        do! bounded askAFirstEntered.Task
        let askB = svc.Ask(q "ask-b", None, CancellationToken.None, waitForFresh = true)
        // Publish snap-2 while askA is running on snap-1 and askB is waiting on inference.
        Volatile.Write(&currentSnap, "snap-2")
        askAFirstRelease.SetResult()
        // Wait for askB's snap-1 inference to start, then finish askB on snap-1 so askB retries behind askA (which is running on snap-2).
        do! bounded askBFirstEntered.Task
        askBFirstRelease.SetResult()
        do! bounded askASecondEntered.Task
        do! Task.Delay 50
        Assert.False(askA.IsCompleted)
        Assert.False(askB.IsCompleted)

        // A fresh latest read MUST complete immediately against snap-2 without waiting for askA's snap-2 inference or askB's retry!
        let sw = Stopwatch.StartNew()
        let! readResult =
            bounded (svc.ExecuteReadAsync(
                Some(TimeSpan.FromMilliseconds 500.0),
                None,
                (fun _ -> Task.FromResult(Obj [ "snapshot_id", Str (Volatile.Read(&currentSnap)) ])),
                CancellationToken.None))
        Assert.Equal("snap-2", text (field readResult "snapshot_id"))
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds 1.0, sprintf "latest read was blocked by ask retry; took %A" sw.Elapsed)
        Assert.False(askA.IsCompleted)
        Assert.False(askB.IsCompleted)

        askASecondRelease.SetResult()
        let! resA = bounded askA
        let! resB = bounded askB
        Assert.Equal("ask-a:snap-2", text (field resA "answer"))
        Assert.Equal("ask-b:snap-2", text (field resB "answer"))
    finally
        askAFirstRelease.TrySetResult() |> ignore
        askBFirstRelease.TrySetResult() |> ignore
        askASecondRelease.TrySetResult() |> ignore
        joined (svc.Close()) |> fun t -> t.GetAwaiter().GetResult()
}

[<Fact>]
let ``opt out ask intake is bounded before readiness and epoch capacity preserves ticket order and wait budget`` () = task {
    // Part A: when readiness blocks (e.g., pinned read holding DuckDB gate), opt-out Ask intake rejects at MaxQueued BEFORE queueing unboundedly.
    let readyBlocked = completed<unit> ()
    let mutable readyCalls = 0
    let ctxBound : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> (completed<unit> ()).Task)
          Readiness = (fun ct -> task {
              Interlocked.Increment(&readyCalls) |> ignore
              do! readyBlocked.Task.WaitAsync ct
              return Obj [ "fresh", Bool true; "mode", Str "latest"; "snapshot_id", Str "s1"; "current_snapshot_id", Str "s1" ] })
          Evaluate = (fun q snap _ -> Task.FromResult(Obj [ "snapshot_id", Str snap; "answer", Str q.Text ])) }
    let svcBound = new Requests.Service(ctxBound, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 2 })
    let q text : Requests.Question =
        { Text = text; Snapshot = None; Limit = 8L; MaxChars = 2400L; IncludeContext = false; IncludeQuery = true; IncludeResults = true }
    try
        let p1 = svcBound.Ask(q "opt-1", None, CancellationToken.None, waitForFresh = false)
        let p2 = svcBound.Ask(q "opt-2", None, CancellationToken.None, waitForFresh = false)
        let! fullEx =
            Assert.ThrowsAsync<EnglishError>(fun () ->
                svcBound.Ask(q "opt-3", None, CancellationToken.None, waitForFresh = false) :> Task)
        Assert.Equal(("queue_full", 429, true), englishError fullEx)
        Assert.Equal(2, Volatile.Read(&readyCalls))

        readyBlocked.SetResult()
        let! _ = bounded (Task.WhenAll [ p1; p2 ])
        ()
    finally
        readyBlocked.TrySetResult() |> ignore
        joined (svcBound.Close()) |> fun t -> t.GetAwaiter().GetResult()

    // Part B: when the 128-entry epoch is full with an active consumer, wait_timeout_ms bounds binding (503 wait_timeout)
    // and two queued waiting tickets preserve strict ticket order across epoch rotation.
    let holderEntered, holderRelease = completed<unit> (), completed<unit> ()
    let evalOrder = ConcurrentQueue<string>()
    let ctxEpoch : Requests.Ctx =
        { Sources = []
          Observe = (fun _ _ -> failwith "unused")
          WatchFailure = (completed<exn> ()).Task
          WhileWatchHealthy = (fun a -> a ())
          WatchReadiness = (fun () -> (completed<unit> ()).Task)
          Readiness = (fun _ -> Task.FromResult(Obj [ "fresh", Bool true; "mode", Str "latest"; "snapshot_id", Str "s1"; "current_snapshot_id", Str "s1" ]))
          Evaluate = (fun question snap ct -> task {
              if question.Text = "epoch-holder" then
                  holderEntered.TrySetResult() |> ignore
                  do! holderRelease.Task.WaitAsync ct
              elif question.Text = "epoch-wait-1" || question.Text = "epoch-wait-2" then
                  evalOrder.Enqueue question.Text
              return Obj [ "snapshot_id", Str snap; "answer", Str question.Text ] }) }
    let svcEpoch = new Requests.Service(ctxEpoch, Requests.defaultObserverOptions, { Requests.defaultQueueOptions with MaxQueued = 8 })
    try
        for i in 1..127 do
            let! _ = bounded (svcEpoch.Ask(q (sprintf "fill-%d" i), None, CancellationToken.None, waitForFresh = true))
            ()
        let holder = svcEpoch.Ask(q "epoch-holder", None, CancellationToken.None, waitForFresh = true)
        do! bounded holderEntered.Task

        // Short admission budget must fail with 503 wait_timeout rather than waiting 240s.
        let sw = Stopwatch.StartNew()
        let! timeoutEx =
            Assert.ThrowsAsync<EnglishError>(fun () ->
                svcEpoch.Ask(q "epoch-timeout", Some(TimeSpan.FromMilliseconds 40.0), CancellationToken.None, waitForFresh = true) :> Task)
        Assert.Equal(("wait_timeout", 503, true), englishError timeoutEx)
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds 1.0, sprintf "epoch capacity wait ignored wait_timeout_ms; took %A" sw.Elapsed)

        let wait1 = svcEpoch.Ask(q "epoch-wait-1", Some(TimeSpan.FromSeconds 10.0), CancellationToken.None, waitForFresh = true)
        let wait2 = svcEpoch.Ask(q "epoch-wait-2", Some(TimeSpan.FromSeconds 10.0), CancellationToken.None, waitForFresh = true)
        do! Task.Delay 40
        Assert.False(wait1.IsCompleted)
        Assert.False(wait2.IsCompleted)

        holderRelease.SetResult()
        let! _ = bounded holder
        let! r1 = bounded wait1
        let! r2 = bounded wait2
        Assert.Equal("epoch-wait-1", text (field r1 "answer"))
        Assert.Equal("epoch-wait-2", text (field r2 "answer"))
        Assert.Equal<string list>([ "epoch-wait-1"; "epoch-wait-2" ], List.ofSeq evalOrder)
    finally
        holderRelease.TrySetResult() |> ignore
        joined (svcEpoch.Close()) |> fun t -> t.GetAwaiter().GetResult()
}

[<Fact>]
let ``contended gate manifest drift returns immediate 409 on latest ask and pinned paths`` () = task {
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        let published = corpus.Publish()
        let snap1 = text (field published "snapshot_id")
        let store = corpus.Open()
        let gateEntered, gateRelease = completed<unit> (), completed<unit> ()
        let holder = Task.Run(fun () ->
            store.Call(fun _ -> gateEntered.TrySetResult() |> ignore; gateRelease.Task.GetAwaiter().GetResult(); Null))
        try
            do! bounded gateEntered.Task
            // Publisher rebinds manifest_identity while the old server's DuckDB gate is actively held.
            corpus.Coordinator.State.RebindManifestIdentity(String('c', 64), [ "test" ], "contended gate manifest rebind") |> ignore

            let assert409 (ex: PinnedSnapshotError) =
                match ex :> exn with
                | PinnedSnapshotError(code, status, retryable) ->
                    Assert.Equal("corpus_configuration_changed", code)
                    Assert.Equal(409, status)
                    Assert.True(retryable)
                | other -> raise other

            // Synchronous Call (latest fail-fast), CallAsync, CallPinned, CallPinnedAsync, and ReadinessAsync MUST all return immediate 409 (not 503 Graph is busy / wait_timeout).
            let sw = Stopwatch.StartNew()
            assert409 (Assert.Throws<PinnedSnapshotError>(fun () -> store.Schema() |> ignore))
            let! exAsync = Assert.ThrowsAsync<PinnedSnapshotError>(fun () -> store.SchemaAsync(CancellationToken.None) :> Task)
            assert409 exAsync
            use shortWait = new CancellationTokenSource(TimeSpan.FromMilliseconds 40.0)
            let! exPin =
                Assert.ThrowsAsync<PinnedSnapshotError>(fun () ->
                    store.FindPinnedAsync("0.md", snap1, None, 10L, None, CancellationToken.None, gateCancellation = shortWait.Token) :> Task)
            assert409 exPin
            let! exReady = Assert.ThrowsAsync<PinnedSnapshotError>(fun () -> store.ReadinessAsync(shortWait.Token) :> Task)
            assert409 exReady
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds 1.0, sprintf "contended manifest drift waited on DuckDB gate; took %A" sw.Elapsed)
        finally
            gateRelease.TrySetResult() |> ignore
            (try holder.GetAwaiter().GetResult() |> ignore with _ -> ())
            store.Close()
}
