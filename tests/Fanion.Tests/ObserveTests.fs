// Readiness observation: the Requests-owned producer and per-observer delivery
// over IndexState notifications, and the authenticated /v1/retrieval/observe
// server-sent event transport.
module Fanion.Tests.ObserveTests

open System
open System.Buffers.Binary
open System.Collections.Concurrent
open System.Diagnostics
open System.IO
open System.Net
open System.Net.Http
open System.Text
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open System.Runtime.ExceptionServices
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Server.Kestrel.Core
open Microsoft.Extensions.Logging
open Xunit
open Fanion
open Fanion.Canonical
open Fanion.English
open Fanion.Requests
open Fanion.Tests.IndexStateTests
open Fanion.Tests.EnglishTests
open Fanion.Tests.ServeTests

let private completed<'T> () = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
let private bounded (pending: Task<'T>) = pending.WaitAsync(TimeSpan.FromSeconds 10.0)
let private joined (pending: Task) = pending.WaitAsync(TimeSpan.FromSeconds 10.0)
// The exact key set State.Freshness returned at 4885af6; observer timing stays out of it.
let freshnessKeys =
    [ "fresh"; "mode"; "snapshot_id"; "current_snapshot_id"; "generation"; "reasons"; "observation_required"
      "stale_sources"; "pending_sources"; "checked_at"; "max_observation_age" ]
let private keysOf (value: J) = match value with Obj pairs -> List.map fst pairs | _ -> failwith "not an object"

let private snapshotA, snapshotB = String('a', 64), String('b', 64)
let private generationOf number snapshot (published: float) revisions : IndexState.Generation =
    { Generation = number; SnapshotId = snapshot; Revisions = Map.ofList revisions; PublishedAt = published; ConfigurationSha256 = None }
let private freshJ (generation: int64) snapshot =
    Obj [ "fresh", Bool true; "mode", Str "latest"; "snapshot_id", Str snapshot; "current_snapshot_id", Str snapshot
          "generation", Int generation; "reasons", Arr []; "observation_required", Arr []; "stale_sources", Arr []; "pending_sources", Arr []
          "checked_at", Float 100.0; "max_observation_age", Float 90.0 ]
let private notFreshJ (generation: int64) snapshot reasons pending stale =
    Obj [ "fresh", Bool false; "mode", Str "latest"; "snapshot_id", Str snapshot; "current_snapshot_id", Str snapshot
          "generation", Int generation; "reasons", Arr (reasons |> List.map Str); "observation_required", Arr []
          "stale_sources", Arr (stale |> List.map Str); "pending_sources", Arr (pending |> List.map Str)
          "checked_at", Float 100.0; "max_observation_age", Float 90.0 ]
let private sampleOf freshness active changedAt deadline : IndexState.Observation =
    { Freshness = freshness; Active = active; ChangedAt = changedAt; Deadline = deadline; CheckedAt = 100.0; Crossing = None }
let private batch current : IndexState.ObservationBatch =
    { Sequence = 0L; History = IndexState.Transitions []; Current = current; SampledAt = Stopwatch.GetTimestamp() }
let private unpublished = sampleOf (Obj [ "fresh", Bool false; "reasons", Arr [ Str "no_published_generation" ] ]) None None None
let private first = generationOf 1L snapshotA 100.25 [ "test", Some(String('1', 40)) ]
let private second = generationOf 2L snapshotB 101.5 [ "test", Some(String('2', 40)) ]
let private freshFirst = sampleOf (freshJ 1L snapshotA) (Some first) None None
let private freshSecond = sampleOf (freshJ 2L snapshotB) (Some second) None None
let private pendingFirst = sampleOf (notFreshJ 1L snapshotA [ "indexing_pending" ] [ "test" ] []) (Some first) (Some 100.5) None
let private frameOfFirst = Fresh { Generation = 1L; SnapshotId = snapshotA; PublishedAtMs = 100250L; Revisions = first.Revisions }
let private frameOfSecond = Fresh { Generation = 2L; SnapshotId = snapshotB; PublishedAtMs = 101500L; Revisions = second.Revisions }
let private framePending = Indexing { Generation = 1L; SnapshotId = snapshotA; PendingSources = [ "test" ]; StaleSources = []; Reasons = [ "indexing_pending" ]; ChangedAtMs = Some 100500L; QueuedRetrievals = 0 }
let private quiet = { defaultObserverOptions with KeepaliveInterval = TimeSpan.FromMinutes 10.0 }

// Deterministic coherent samples and publication fences around the real Service.
type private Source() =
    let mutable sample = unpublished
    let gate = obj ()
    let failed = completed<exn> ()
    let mutable changed = completed<unit> ()
    let mutable beforeRead : (unit -> Task) = fun () -> Task.CompletedTask
    let reads = ref 0
    let read = ConcurrentDictionary<int, TaskCompletionSource<unit>>()
    let projected = ConcurrentDictionary<int, TaskCompletionSource<unit>>()
    let checkpoint (signals: ConcurrentDictionary<int, TaskCompletionSource<unit>>) number =
        signals.GetOrAdd(number, fun _ -> completed<unit> ())
    let stamps = ConcurrentQueue<int64>()
    member _.Set(next: IndexState.Observation) = sample <- next
    member this.Publish(next: IndexState.Observation) =
        lock gate (fun () ->
            let previous = changed
            changed <- completed<unit> ()
            this.Set next
            previous.SetResult())
    member this.Notify() = this.Publish sample
    member _.Fail(error: exn) =
        lock gate (fun () ->
            failed.SetResult error
            changed.SetException error
            changed.Task.Exception |> ignore)
    member _.BeforeRead with set action = beforeRead <- action
    member _.Reads = Volatile.Read(&reads.contents)
    member _.Stamps = stamps.ToArray()
    member _.Read(number) = bounded (checkpoint read number).Task
    member _.Projected(number) = bounded (checkpoint projected number).Task
    member _.Ctx : Requests.Ctx =
        { Sources = []; WatchReadiness = (fun () -> lock gate (fun () -> changed.Task))
          WatchFailure = failed.Task
          WhileWatchHealthy = (fun action -> lock gate (fun () ->
              if not failed.Task.IsCompleted then
                  action ()
                  (checkpoint projected (Volatile.Read(&reads.contents))).TrySetResult() |> ignore))
          Readiness = (fun _ -> failwith "Observers read through Observe")
          Observe = (fun _ ct -> task {
              stamps.Enqueue(Stopwatch.GetTimestamp())
              let captured = batch sample
              let number = Interlocked.Increment(&reads.contents)
              (checkpoint read number).TrySetResult() |> ignore
              do! (beforeRead ()).WaitAsync ct
              return captured })
          Evaluate = fun _ _ _ -> failwith "Observers never start inference" }

// Records frames after an optional per-emit hold (1-based emit number).
type private Collector(?hold: int -> CancellationToken -> Task) =
    let frames = Channel.CreateUnbounded<RetrievalReadiness>()
    let keepalives = ref 0
    let emits = ref 0
    let entered = completed<unit> ()
    let heartbeat = Channel.CreateUnbounded<unit>()
    member _.Sink : ObserverSink =
        { Emit = (fun ready ct ->
              let number = Interlocked.Increment(&emits.contents)
              task {
                  entered.TrySetResult() |> ignore
                  match hold with
                  | Some wait -> do! wait number ct
                  | None -> ()
                  frames.Writer.TryWrite ready |> ignore } :> Task)
          Keepalive = (fun _ ->
              Interlocked.Increment(&keepalives.contents) |> ignore
              heartbeat.Writer.TryWrite() |> ignore
              Task.CompletedTask) }
    member _.Next() = bounded (frames.Reader.ReadAsync().AsTask())
    member _.Entered = bounded entered.Task
    member _.Heartbeat() = bounded (heartbeat.Reader.ReadAsync().AsTask())
    member _.Pending = frames.Reader.Count
    member _.Keepalives = Volatile.Read(&keepalives.contents)

// Every test joins the service shutdown, including after a failed assertion.
let private joinAfter (service: Service) (body: unit -> Task) = task {
    let! outcome = task {
        try
            do! body ()
            return None
        with error -> return Some error }
    do! joined (service.Close())
    match outcome with Some error -> ExceptionDispatchInfo.Capture(error).Throw() | None -> ()
}
let private withService (source: Source) (options: ObserverOptions) (body: Service -> Task) =
    let service = new Service(source.Ctx, options)
    joinAfter service (fun () -> body service)

[<Fact>]
let ``the readiness wire codec is exact and observer options are validated`` () =
    let encode frame = toText orderedCompactStyle (readinessJ frame)
    Assert.Equal(
        """{"state":"fresh","generation":7,"snapshot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","published_at_ms":1763600000250,"revisions":{"site":null,"spec":"1111111111111111111111111111111111111111"}}""",
        encode (Fresh { Generation = 7L; SnapshotId = snapshotA; PublishedAtMs = 1763600000250L; Revisions = Map.ofList [ "spec", Some(String('1', 40)); "site", None ] }))
    Assert.Equal(
        """{"state":"indexing","generation":7,"snapshot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pending_sources":["search","spec"],"stale_sources":[],"reasons":["indexing_pending"],"changed_at_ms":1763600001000,"queued_retrievals":0}""",
        encode (Indexing { Generation = 7L; SnapshotId = snapshotA; PendingSources = [ "search"; "spec" ]; StaleSources = []; Reasons = [ "indexing_pending" ]; ChangedAtMs = Some 1763600001000L; QueuedRetrievals = 0 }))
    Assert.Equal(
        """{"state":"indexing","generation":7,"snapshot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pending_sources":[],"stale_sources":["spec"],"reasons":["authoritative_observation_required"],"changed_at_ms":null,"queued_retrievals":0}""",
        encode (Indexing { Generation = 7L; SnapshotId = snapshotA; PendingSources = []; StaleSources = [ "spec" ]; Reasons = [ "authoritative_observation_required" ]; ChangedAtMs = None; QueuedRetrievals = 0 }))
    Assert.Equal(
        """{"state":"crossing_held","generation":7,"snapshot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expected_sources":["search","spec"],"pending_sources":["spec"],"stale_sources":[],"reasons":["crossing_held"],"held_until_ms":1763600060000,"queued_retrievals":2,"changed_at_ms":1763600001000}""",
        encode (CrossingHeld { Generation = 7L; SnapshotId = snapshotA; ExpectedSources = [ "search"; "spec" ]; PendingSources = [ "spec" ]; StaleSources = []; Reasons = [ "crossing_held" ]; HeldUntilMs = Some 1763600060000L; QueuedRetrievals = 2; ChangedAtMs = Some 1763600001000L }))
    Assert.Equal("""{"state":"unavailable","reason":"no_published_generation"}""", encode (Unavailable "no_published_generation"))
    for invalid in [ { defaultObserverOptions with Capacity = 0 }; { defaultObserverOptions with QueueDepth = 0 }
                     { defaultObserverOptions with KeepaliveInterval = TimeSpan.Zero }; { defaultObserverOptions with WriteTimeout = TimeSpan.Zero } ] do
        Assert.Throws<ArgumentException>(fun () -> Service(Source().Ctx, invalid) |> ignore) |> ignore

[<Fact>]
let ``observe pushes the initial frame then only transitions`` () =
    let source = Source()
    source.Set freshFirst
    withService source quiet (fun service -> task {
        let sink = Collector()
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        let! initial = sink.Next()
        Assert.Equal(frameOfFirst, initial)
        // A notification without a state change is read and suppressed.
        source.Notify()
        do! source.Read 2
        source.Publish pendingFirst
        let! indexing = sink.Next()
        Assert.Equal(framePending, indexing)
        Assert.Equal(3, source.Reads)
        source.Publish freshSecond
        let! republished = sink.Next()
        Assert.Equal(frameOfSecond, republished)
        Assert.Equal(0, sink.Pending)
        Assert.Equal(1, service.Observers)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        Assert.Equal(0, service.Observers)
    })

[<Fact>]
let ``unpublished samples wait and malformed samples fail the observation`` () =
    let source = Source()
    // A null generation and an absent one are the same unpublished state.
    source.Set(sampleOf (Obj [ "fresh", Bool false; "generation", Null; "reasons", Arr [ Str "unknown_snapshot" ] ]) None None None)
    withService source quiet (fun service -> task {
        let sink = Collector()
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        let! initial = sink.Next()
        Assert.Equal(Unavailable "no_published_generation", initial)
        source.Publish(sampleOf (notFreshJ 1L snapshotA [] [] [] |> function Obj pairs -> Obj [ for k, v in pairs -> k, (if k = "reasons" then Arr [ Int 3L ] else v) ] | other -> other) (Some first) None None)
        let! malformed = sink.Next()
        Assert.Equal(Unavailable "observation_failed", malformed)
        let! error = Assert.ThrowsAsync<InvalidOperationException>(fun () -> joined observing)
        Assert.Equal("Malformed retrieval readiness sample", error.Message)
    })

[<Fact>]
let ``an observation deadline wakes a time only transition without polling`` () =
    let source = Source()
    source.Set(sampleOf (freshJ 1L snapshotA) (Some first) None (Some 100.05))
    withService source quiet (fun service -> task {
        let sink = Collector()
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        let! initial = sink.Next()
        Assert.Equal(frameOfFirst, initial)
        // No SQLite write: only the sampled deadline can surface expiry.
        source.Set(sampleOf (notFreshJ 1L snapshotA [ "observation_stale_or_unknown" ] [] [ "test" ]) (Some first) (Some 100.05) None)
        let! stale = sink.Next()
        Assert.Equal(Indexing { Generation = 1L; SnapshotId = snapshotA; PendingSources = []; StaleSources = [ "test" ]; Reasons = [ "observation_stale_or_unknown" ]; ChangedAtMs = Some 100050L; QueuedRetrievals = 0 }, stale)
        let reads = source.Reads
        do! Task.Delay 300
        Assert.Equal(reads, source.Reads)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        ()
    })

[<Fact>]
let ``expiry is anchored to the sample and not shifted by a slow write`` () =
    let source = Source()
    source.Set(sampleOf (freshJ 1L snapshotA) (Some first) None (Some 100.05))
    withService source quiet (fun service -> task {
        // The first write takes 400 ms; the 50 ms deadline must not wait for it.
        let sink = Collector(fun number ct -> if number = 1 then Task.Delay(400, ct) else Task.CompletedTask)
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        do! source.Read 1
        source.Set(sampleOf (notFreshJ 1L snapshotA [ "observation_stale_or_unknown" ] [] [ "test" ]) (Some first) (Some 100.05) None)
        let! initial = sink.Next()
        Assert.Equal(frameOfFirst, initial)
        let! stale = sink.Next()
        Assert.Equal("indexing", match stale with Indexing _ -> "indexing" | _ -> "other")
        let stamps = source.Stamps
        Assert.True(Stopwatch.GetElapsedTime(stamps.[0], stamps.[1]) < TimeSpan.FromMilliseconds 300.0)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        ()
    })

[<Fact>]
let ``a held write still receives every committed transition in order`` () =
    let source = Source()
    source.Set freshFirst
    let release = completed<unit> ()
    withService source quiet (fun service -> task {
        let sink = Collector(fun number ct -> if number = 1 then release.Task.WaitAsync ct else Task.CompletedTask)
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        do! source.Read 1
        source.Publish pendingFirst
        do! source.Read 2
        source.Publish freshSecond
        do! source.Read 3
        release.SetResult()
        let! a = sink.Next()
        let! b = sink.Next()
        let! c = sink.Next()
        Assert.Equal<RetrievalReadiness list>([ frameOfFirst; framePending; frameOfSecond ], [ a; b; c ])
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        ()
    })

[<Fact>]
let ``an overflowing observer is evicted explicitly and releases its slot`` () =
    let source = Source()
    source.Set freshFirst
    let options = { quiet with QueueDepth = 2; WriteTimeout = TimeSpan.FromMinutes 1.0 }
    withService source options (fun service -> task {
        let release = completed<unit> ()
        let sink = Collector(fun number ct -> if number = 1 then release.Task.WaitAsync ct else Task.CompletedTask)
        let observing = service.Observe(sink.Sink, CancellationToken.None)
        do! sink.Entered
        let pendingOther = sampleOf (notFreshJ 2L snapshotB [ "indexing_pending" ] [ "test" ] []) (Some second) (Some 101.75) None
        for next, reads in [ pendingFirst, 2; freshSecond, 3; pendingOther, 4 ] do
            source.Publish next
            do! source.Projected reads
        release.SetResult()
        let! initial = sink.Next()
        Assert.Equal(frameOfFirst, initial)
        let! terminal = sink.Next()
        Assert.Equal(Unavailable "observer_lagged", terminal)
        do! joined observing
        Assert.Equal(0, service.Observers)
        let late = Collector()
        use cancel = new CancellationTokenSource()
        let replacement = service.Observe(late.Sink, cancel.Token)
        let! current = late.Next()
        Assert.Equal(Indexing { Generation = 2L; SnapshotId = snapshotB; PendingSources = [ "test" ]; StaleSources = []; Reasons = [ "indexing_pending" ]; ChangedAtMs = Some 101750L; QueuedRetrievals = 0 }, current)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined replacement)
        ()
    })

[<Fact>]
let ``a stalled write times out and releases its slot`` () =
    let source = Source()
    source.Set freshFirst
    withService source { quiet with WriteTimeout = TimeSpan.FromMilliseconds 50.0 } (fun service -> task {
        let sink = Collector(fun _ ct -> Task.Delay(Timeout.Infinite, ct))
        let! error = Assert.ThrowsAsync<EnglishError>(fun () -> joined (service.Observe(sink.Sink, CancellationToken.None)))
        Assert.Equal(("observer_write_timeout", 504, true), englishError error)
        Assert.Equal(0, sink.Pending)
        Assert.Equal(0, service.Observers)
    })

[<Fact>]
let ``keepalive is anchored to the last write and never reset by notifications`` () =
    let source = Source()
    source.Set freshFirst
    withService source { quiet with KeepaliveInterval = TimeSpan.FromMilliseconds 150.0 } (fun service -> task {
        let sink = Collector()
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        let! _ = sink.Next()
        // Unchanged notifications every 40 ms, faster than the keepalive interval.
        for read in 2..18 do
            source.Notify()
            do! source.Projected read
            do! Task.Delay 40
        Assert.True(sink.Keepalives >= 2, sprintf "keepalives %d" sink.Keepalives)
        Assert.True(source.Reads > 5, sprintf "reads %d" source.Reads)
        Assert.Equal(0, sink.Pending)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        ()
    })

[<Fact>]
let ``keepalive frames carry liveness only`` () =
    let source = Source()
    source.Set freshFirst
    withService source { quiet with KeepaliveInterval = TimeSpan.FromMilliseconds 20.0 } (fun service -> task {
        let sink = Collector()
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        let! _ = sink.Next()
        for _ in 1..3 do do! sink.Heartbeat()
        Assert.Equal(1, source.Reads)
        Assert.Equal(0, sink.Pending)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        ()
    })

[<Fact>]
let ``saturated observers are refused and a disconnect drains its slot`` () =
    let source = Source()
    source.Set freshFirst
    withService source { quiet with Capacity = 3 } (fun service -> task {
        Assert.Equal(3, service.ObserverCapacity)
        let cancels = [ for _ in 1..service.ObserverCapacity -> new CancellationTokenSource() ]
        try
            let streams = [ for cancel in cancels -> let sink = Collector() in sink, service.Observe(sink.Sink, cancel.Token) ]
            for sink, _ in streams do
                let! _ = sink.Next()
                ()
            Assert.Equal(service.ObserverCapacity, service.Observers)
            let reads = source.Reads
            let refused = Collector()
            let! error = Assert.ThrowsAsync<EnglishError>(fun () -> joined (service.Observe(refused.Sink, CancellationToken.None)))
            Assert.Equal(("retrieval_observers_saturated", 429, true), englishError error)
            Assert.Equal(0, refused.Pending)
            Assert.Equal(reads, source.Reads)
            cancels.Head.Cancel()
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined (snd streams.Head))
            Assert.Equal(service.ObserverCapacity - 1, service.Observers)
            let admitted = Collector()
            use late = new CancellationTokenSource()
            let observing = service.Observe(admitted.Sink, late.Token)
            let! _ = admitted.Next()
            Assert.Equal(service.ObserverCapacity, service.Observers)
            late.Cancel()
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
            ()
        finally for cancel in cancels do cancel.Cancel(); cancel.Dispose()
    })

[<Fact>]
let ``shutdown ends observers with a stopping frame and joins them`` () = task {
    let source = Source()
    source.Set freshFirst
    let service = new Service(source.Ctx, quiet)
    let sink = Collector()
    let observing = service.Observe(sink.Sink, CancellationToken.None)
    let! _ = sink.Next()
    do! joined (service.Close())
    Assert.True(observing.IsCompletedSuccessfully)
    let! stopping = sink.Next()
    Assert.Equal(Unavailable "retrieval_stopping", stopping)
    Assert.Equal(0, service.Observers)
    let! error = Assert.ThrowsAsync<EnglishError>(fun () -> joined (service.Observe(Collector().Sink, CancellationToken.None)))
    Assert.Equal(("retrieval_stopping", 503, false), englishError error)
}

[<Fact>]
let ``a failed publication watch ends the stream and propagates its error`` () =
    let source = Source()
    source.Set freshFirst
    withService source quiet (fun service -> task {
        let sink = Collector()
        let observing = service.Observe(sink.Sink, CancellationToken.None)
        let! _ = sink.Next()
        source.Fail(IOException "publication notification failed")
        let! final = sink.Next()
        Assert.Equal(Unavailable "publication_notifications_failed", final)
        let! error = Assert.ThrowsAsync<IOException>(fun () -> joined observing)
        Assert.Equal("publication notification failed", error.Message)
        Assert.Equal(0, service.Observers)
        // Already failed: no sample is read and nothing claims a state.
        let reads = source.Reads
        let late = Collector()
        let! again = Assert.ThrowsAsync<IOException>(fun () -> joined (service.Observe(late.Sink, CancellationToken.None)))
        Assert.Equal("publication notification failed", again.Message)
        let! only = late.Next()
        Assert.Equal(Unavailable "publication_notifications_failed", only)
        Assert.Equal(reads, source.Reads)
    })

[<Fact>]
let ``a watcher failure during an in flight write outranks the shutdown it triggers`` () = task {
    let source = Source()
    source.Set freshFirst
    let service = new Service(source.Ctx, quiet)
    let release = completed<unit> ()
    let sink = Collector(fun number ct -> if number = 1 then release.Task.WaitAsync ct else Task.CompletedTask)
    let observing = service.Observe(sink.Sink, CancellationToken.None)
    do! sink.Entered
    source.Fail(IOException "publication notification failed")
    let closing = service.Close()
    release.SetResult()
    let! error = Assert.ThrowsAsync<IOException>(fun () -> joined observing)
    Assert.Equal("publication notification failed", error.Message)
    let! initial = sink.Next()
    Assert.Equal(frameOfFirst, initial)
    let! final = sink.Next()
    Assert.Equal(Unavailable "publication_notifications_failed", final)
    do! joined closing
    Assert.Equal(0, service.Observers)
}

[<Fact>]
let ``a terminal write error never masks the watcher failure`` () =
    let source = Source()
    source.Set freshFirst
    withService source quiet (fun service -> task {
        let sink = Collector(fun number _ -> if number = 1 then Task.CompletedTask else Task.FromException(IOException "connection reset"))
        let observing = service.Observe(sink.Sink, CancellationToken.None)
        let! _ = sink.Next()
        source.Fail(IOException "publication notification failed")
        let! error = Assert.ThrowsAsync<IOException>(fun () -> joined observing)
        Assert.Equal("publication notification failed", error.Message)
        Assert.Equal(0, sink.Pending)
    })

[<Fact>]
let ``freshness keeps its response key set and observe reads one coherent sample`` () =
    use f = new IndexStateTests.Fixture()
    let state = f.State
    let none = state.Observe 30.0
    Assert.True(none.Active.IsNone)
    Assert.Equal(canon (Obj [ "fresh", Bool false; "reasons", Arr [ Str "no_published_generation" ] ]), canon none.Freshness)
    f.PublishInitial state |> ignore
    Assert.Equal<string list>(freshnessKeys, keysOf (state.Freshness(String('1', 64), 30.0)))
    let published = state.Observe 30.0
    Assert.Equal<string list>(freshnessKeys, keysOf published.Freshness)
    Assert.Equal(canon (state.Freshness(String('1', 64), 30.0)), canon published.Freshness)
    Assert.Equal(Some 1L, published.Active |> Option.map (fun g -> g.Generation))
    Assert.Equal(None, published.ChangedAt)
    Assert.Equal(Some 1030.0, published.Deadline)
    let changedAt = f.Clock.Advance()
    state.ObserveHeads([ "spec", Some(String('e', 40)) ], changedAt) |> ignore
    let pending = state.Observe 30.0
    Assert.Equal(Some changedAt, pending.ChangedAt)
    Assert.Equal(Some 1030.0, pending.Deadline)
    Assert.Equal(Some 1L, pending.Active |> Option.map (fun g -> g.Generation))

// Real SQLite publication fence and FileSystemWatcher; no graph engine required.
[<Fact>]
let ``sqlite publication pushes fresh then indexing then the next generation`` () = task {
    use f = new IndexStateTests.Fixture()
    let state = f.State
    use changes = new IndexState.Changes(f.Path)
    let holding, release = completed<unit> (), completed<unit> ()
    let hold = ref false
    let ctx : Requests.Ctx =
        { Sources = []; WatchReadiness = changes.Watch; WatchFailure = changes.Failure; WhileWatchHealthy = changes.WhileHealthy
          Readiness = (fun _ -> failwith "Observers read through Observe")
          Observe = (fun cursor ct -> task {
              if Volatile.Read(&hold.contents) then
                  holding.TrySetResult() |> ignore
                  do! release.Task.WaitAsync ct
              return state.ObserveSince(30.0, cursor) })
          Evaluate = fun _ _ _ -> failwith "Observers never start inference" }
    let service = new Service(ctx, quiet)
    do! joinAfter service (fun () -> task {
        let sink = Collector()
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        let! initial = sink.Next()
        Assert.Equal(Unavailable "no_published_generation", initial)
        f.PublishInitial state |> ignore
        let! fresh = sink.Next()
        Assert.Equal(Fresh { Generation = 1L; SnapshotId = String('1', 64); PublishedAtMs = 1000000L; Revisions = Map.ofList f.Heads }, fresh)
        Volatile.Write(&hold.contents, true)
        state.ObserveHeads(f.Heads, f.Clock.Advance()) |> ignore
        do! bounded holding.Task
        let changedAt = f.Clock.Advance()
        state.ObserveHeads([ "spec", Some(String('e', 40)) ], changedAt) |> ignore
        let plan = state.PendingPlan()
        let token = state.StartBuild plan
        Assert.True(bool' (state.CompleteBuild(token, String('2', 64), targets plan)) "published")
        Volatile.Write(&hold.contents, false)
        release.SetResult()
        let! indexing = sink.Next()
        Assert.Equal(Indexing { Generation = 1L; SnapshotId = String('1', 64); PendingSources = [ "search"; "site"; "spec" ]; StaleSources = []
                                Reasons = [ "indexing_pending" ]; ChangedAtMs = Some 1002000L; QueuedRetrievals = 0 }, indexing)
        let! next = sink.Next()
        let revisions = f.Heads |> List.map (fun (source, revision) -> source, (if source = "spec" then Some(String('e', 40)) else revision)) |> Map.ofList
        Assert.Equal(Fresh { Generation = 2L; SnapshotId = String('2', 64); PublishedAtMs = 1002000L; Revisions = revisions }, next)
        Assert.Equal(0, sink.Pending)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        Assert.Equal(0, service.Observers)
    })
}

// HTTP transport over the real published graph (requires the pinned engine).
let private withPublished (body: Corpus -> Task) = task {
    match environment () with
    | None -> ()
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        do! body corpus
}

type private EventStream(response: HttpResponseMessage, reader: StreamReader) =
    member _.Response = response
    // The next readiness data line; keepalive comments are skipped, and any
    // other framing fails the test. None is the end of the stream.
    member _.Next() : Task<string option> = task {
        let mutable result = None
        let mutable reading = true
        while reading do
            let! line = reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds 60.0)
            match line with
            | null -> reading <- false
            | "event: readiness" ->
                let! data = reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds 10.0)
                let! blank = reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds 10.0)
                Assert.StartsWith("data: ", data)
                Assert.Equal("", blank)
                result <- Some(data.Substring 6); reading <- false
            | ": keepalive" | "" -> ()
            | other -> failwithf "unexpected event stream line %s" other
        return result
    }
    interface IDisposable with
        member _.Dispose() = reader.Dispose(); response.Dispose()

let private observe (http: HttpClient) (authorization: string option) = task {
    use request = new HttpRequestMessage(HttpMethod.Get, "v1/retrieval/observe")
    authorization |> Option.iter (fun value -> request.Headers.TryAddWithoutValidation("Authorization", value) |> ignore)
    let! response = http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead)
    if response.StatusCode <> HttpStatusCode.OK then
        let! body = response.Content.ReadAsStringAsync()
        response.Dispose()
        return Error(int response.StatusCode, parse body)
    else
        let! stream = response.Content.ReadAsStreamAsync()
        return Ok(new EventStream(response, new StreamReader(stream)))
}
let private opened = function Ok(stream: EventStream) -> stream | Error(status, body) -> failwithf "observe refused %d %s" status (jsonOf body)
let private frameOf (stream: EventStream) = task {
    match! stream.Next() with
    | Some data -> return parse data
    | None -> return failwith "event stream ended"
}
let private stateOf (frame: J) = text (field frame "state")

[<Fact>]
let ``observe endpoint authenticates pushes the next generation and bounds admission`` () = withPublished (fun corpus -> task {
    use server = new Server(corpus, None, None)
    use http = server.CreateClient(Timeout.InfiniteTimeSpan)
    // Exact-reason refusals: no bearer, a wrong bearer, and the token without its scheme.
    for authorization in [ None; Some("Bearer " + String('x', 48)); Some server.Token ] do
        match! observe http authorization with
        | Error(status, body) ->
            Assert.Equal(401, status)
            Assert.Equal("""{"error":"Unauthorized"}""", toText orderedCompactStyle body)
        | Ok stream -> (stream :> IDisposable).Dispose(); failwith "unauthenticated observer admitted"
    let bearer = Some("Bearer " + server.Token)
    let! admitted = observe http bearer
    use stream = opened admitted
    Assert.Equal("text/event-stream; charset=utf-8", string stream.Response.Content.Headers.ContentType)
    Assert.Equal("no", stream.Response.Headers.GetValues("X-Accel-Buffering") |> Seq.exactlyOne)
    let! initial = frameOf stream
    let schema = snd (server.Call(Obj [ "action", Str "schema" ]))
    // Existing response contracts keep the base freshness shape.
    Assert.Equal<string list>(freshnessKeys, keysOf (field schema "freshness"))
    Assert.Equal("fresh", stateOf initial)
    Assert.Equal(Int 1L, field initial "generation")
    Assert.Equal(field schema "snapshot_id", field initial "snapshot_id")
    Assert.Equal(canon (Obj [ "test", Str (corpus.Head "test") ]), canon (field initial "revisions"))
    let revision = corpus.Commit("test", "2.md", "Content 2 changed\nEvidence\n")
    Assert.Empty(corpus.Observe())
    let! indexing = frameOf stream
    Assert.Equal("indexing", stateOf indexing)
    Assert.Equal(Int 1L, field indexing "generation")
    Assert.Equal(canon (Arr [ Str "test" ]), canon (field indexing "pending_sources"))
    Assert.Equal(canon (Arr [ Str "indexing_pending" ]), canon (field indexing "reasons"))
    Assert.True(bool' (corpus.RunOnce()) "published")
    let mutable previous = indexing
    while stateOf previous <> "fresh" do
        let! frame = frameOf stream
        Assert.NotEqual<string>(canon previous, canon frame)
        previous <- frame
    Assert.Equal(Int 2L, field previous "generation")
    Assert.Equal(canon (Obj [ "test", Str revision ]), canon (field previous "revisions"))
    // Saturation is surfaced as an explicit admission failure.
    let others = ResizeArray<EventStream>()
    try
        for _ in 2..16 do
            let! next = observe http bearer
            let next = opened next
            others.Add next
            let! _ = frameOf next
            ()
        match! observe http bearer with
        | Error(status, body) ->
            Assert.Equal(429, status)
            Assert.Equal("""{"error":"retrieval_observers_saturated","retryable":true}""", toText orderedCompactStyle body)
        | Ok extra -> (extra :> IDisposable).Dispose(); failwith "observer admitted beyond capacity"
    finally for other in others do (other :> IDisposable).Dispose()
})

[<Fact>]
let ``observe stream reports a failed publication watch and ends`` () = withPublished (fun corpus -> task {
    let changes = new IndexState.Changes(Path.Combine(corpus.Root, "index-state.sqlite"))
    use server = new Server(corpus, None, None, changes = changes)
    use http = server.CreateClient(Timeout.InfiniteTimeSpan)
    let! admitted = observe http (Some("Bearer " + server.Token))
    use stream = opened admitted
    let! initial = frameOf stream
    Assert.Equal("fresh", stateOf initial)
    // Fault injection: the real Changes owner fails terminally.
    (changes :> IDisposable).Dispose()
    let! final = frameOf stream
    Assert.Equal("""{"state":"unavailable","reason":"publication_notifications_failed"}""", toText orderedCompactStyle final)
    let! ended = stream.Next()
    Assert.True(ended.IsNone)
    let! exitCode = bounded server.Stopped
    Assert.NotEqual(0, exitCode)
    let! failure = changes.Failure
    Assert.IsType<ObjectDisposedException>(failure) |> ignore
})

[<Fact>]
let ``a sample that expires during binding is replaced before its initial frame`` () =
    let source = Source()
    let release = completed<unit> ()
    source.Set { freshFirst with Deadline = Some 100.025 }
    source.BeforeRead <- fun () -> release.Task
    withService source quiet (fun service -> task {
        let sink = Collector()
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        do! source.Read 1
        do! Task.Delay 50
        source.Set(sampleOf (notFreshJ 1L snapshotA [ "observation_stale_or_unknown" ] [] [ "test" ]) (Some first) (Some 100.025) None)
        release.SetResult()
        let! initial = sink.Next()
        Assert.Equal(Indexing { Generation = 1L; SnapshotId = snapshotA; PendingSources = []; StaleSources = [ "test" ]
                                Reasons = [ "observation_stale_or_unknown" ]; ChangedAtMs = Some 100025L; QueuedRetrievals = 0 }, initial)
        Assert.Equal(2, source.Reads)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        ()
    })

[<Fact>]
let ``watch failure fences a held sample and a concurrent observer`` () =
    let source = Source()
    source.Set freshFirst
    let release = completed<unit> ()
    let fault = IOException "publication watch failed while binding"
    withService source quiet (fun service -> task {
        let original = Collector()
        let observing = service.Observe(original.Sink, CancellationToken.None)
        let! initial = original.Next()
        Assert.Equal(frameOfFirst, initial)
        source.BeforeRead <- fun () -> release.Task
        source.Publish freshSecond
        do! source.Read 2
        source.Fail fault
        let late = Collector()
        let concurrent = service.Observe(late.Sink, CancellationToken.None)
        release.SetResult()
        for sink, stream in [ original, observing; late, concurrent ] do
            let! last = sink.Next()
            Assert.Equal(Unavailable "publication_notifications_failed", last)
            let! error = Assert.ThrowsAsync<IOException>(fun () -> joined stream)
            Assert.Same(fault, error)
            Assert.Equal(0, sink.Pending)
        Assert.Equal(2, source.Reads)
    })

[<Theory>]
[<InlineData("graph")>]
[<InlineData("sqlite")>]
[<InlineData("io")>]
[<InlineData("invariant")>]
[<InlineData("disposal")>]
let ``sampling errors end the stream and a new observer can recover without a notification`` kind =
    let fault, reason =
        match kind with
        | "graph" -> Query.GraphUnavailable "artifact unavailable", "graph_generation_unavailable"
        | "sqlite" -> Microsoft.Data.Sqlite.SqliteException("database busy", 5) :> exn, "observation_failed"
        | "io" -> IOException "read failed" :> exn, "observation_failed"
        | "invariant" -> InvalidOperationException "broken invariant" :> exn, "observation_failed"
        | _ -> ObjectDisposedException "sample" :> exn, "observation_failed"
    let source = Source()
    source.Set freshFirst
    withService source quiet (fun service -> task {
        let sink = Collector()
        let observing = service.Observe(sink.Sink, CancellationToken.None)
        let! initial = sink.Next()
        Assert.Equal(frameOfFirst, initial)
        source.BeforeRead <- fun () -> Task.FromException fault
        source.Notify()
        let! last = sink.Next()
        Assert.Equal(Unavailable reason, last)
        let! error = Assert.ThrowsAnyAsync<Exception>(fun () -> joined observing)
        Assert.Same(fault, error)
        Assert.Equal(0, service.Observers)
        source.BeforeRead <- fun () -> Task.CompletedTask
        let recovered = Collector()
        use cancel = new CancellationTokenSource()
        let observingAgain = service.Observe(recovered.Sink, cancel.Token)
        let! current = recovered.Next()
        Assert.Equal(frameOfFirst, current)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observingAgain)
        ()
    })

// HTTP/2 flow control lets the client hold an actual Kestrel response flush.
let private withKestrel source options (body: (unit -> Stream) -> string -> Service -> (unit -> Task<unit>) -> Task) = task {
    let token = String('t', 48)
    let service = new Service((source: Source).Ctx, options)
    let transport = WireTransport()
    let builder = WebApplication.CreateBuilder()
    builder.Logging.ClearProviders() |> ignore
    transport.Configure builder
    builder.WebHost.ConfigureKestrel(fun (kestrel: KestrelServerOptions) ->
        kestrel.Listen(IPAddress.Loopback, 0, fun listen -> listen.Protocols <- HttpProtocols.Http2)) |> ignore
    use app = builder.Build()
    use stopping = app.Lifetime.ApplicationStopping.Register(Action service.BeginStop)
    let drained = Channel.CreateUnbounded<unit>()
    let endpoint (context: HttpContext) = task {
        try do! Fanion.Serve.observe token service app.Logger context
        finally drained.Writer.TryWrite() |> ignore
    }
    app.MapGet("/v1/retrieval/observe", RequestDelegate(fun context -> endpoint context)) |> ignore
    let! outcome = task {
        try
            do! app.StartAsync()
            do! body transport.Connect token service (fun () -> bounded (drained.Reader.ReadAsync().AsTask()))
            return None
        with error -> return Some error
    }
    service.BeginStop()
    do! app.StopAsync()
    do! joined (service.Close())
    match outcome with Some error -> ExceptionDispatchInfo.Capture(error).Throw() | None -> ()
}

let private h2Frame frameType flags streamId (payload: byte[]) =
    let bytes = Array.zeroCreate (9 + payload.Length)
    bytes.[0] <- byte (payload.Length >>> 16)
    bytes.[1] <- byte (payload.Length >>> 8)
    bytes.[2] <- byte payload.Length
    bytes.[3] <- frameType
    bytes.[4] <- flags
    BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(5, 4), streamId)
    payload.CopyTo(bytes, 9)
    bytes

let private readH2Frame (wire: Stream) cancellation = task {
    let header = Array.zeroCreate 9
    do! wire.ReadExactlyAsync(header.AsMemory(), cancellation)
    let length = (int header.[0] <<< 16) ||| (int header.[1] <<< 8) ||| int header.[2]
    let payload = Array.zeroCreate length
    do! wire.ReadExactlyAsync(payload.AsMemory(), cancellation)
    return header.[3], header.[4], BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(5, 4)), payload
}

let private openH2 (wire: Stream) token cancellation = task {
    let literal name (value: string) = Array.concat [ name; [| byte value.Length |]; Encoding.ASCII.GetBytes value ]
    let headers =
        Array.concat [
            [| 0x82uy; 0x86uy |] // :method GET, :scheme http (HPACK static table).
            literal [| 0x01uy |] "localhost" // :authority
            literal [| 0x04uy |] "/v1/retrieval/observe" // :path
            literal [| 0x0Fuy; 0x08uy |] ("Bearer " + token) ] // authorization
    let initialWindow = [| 0uy; 4uy; 0uy; 0uy; 0uy; 1uy |]
    let preface = Encoding.ASCII.GetBytes("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
    let request = Array.concat [ preface; h2Frame 4uy 0uy 0 initialWindow; h2Frame 1uy 5uy 1 headers ]
    do! wire.WriteAsync(request.AsMemory(), cancellation)
}

[<Theory>]
[<InlineData("observer_lagged")>]
[<InlineData("publication_notifications_failed")>]
[<InlineData("retrieval_stopping")>]
let ``Kestrel sends the terminal readiness event after a backpressured flush recovers`` reason =
    let source = Source()
    let generation = { first with Revisions = Map.ofList [ "test", Some(String('a', 131072)) ] }
    source.Set { freshFirst with Active = Some generation }
    withKestrel source { quiet with QueueDepth = 1 } (fun connect token service _ -> task {
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        let ct = deadline.Token
        use wire = connect ()
        do! openH2 wire token ct
        use data = new MemoryStream()
        let mutable awaitingData = true
        while awaitingData do
            let! kind, flags, id, payload = readH2Frame wire ct
            match kind with
            | 0uy ->
                Assert.Equal(1, id)
                Assert.Equal(0uy, flags &&& 1uy)
                Assert.Single payload |> ignore
                data.Write payload
                awaitingData <- false
            | 4uy when flags = 0uy -> do! wire.WriteAsync((h2Frame 4uy 1uy 0 [||]).AsMemory(), ct)
            | 1uy | 4uy | 8uy -> ()
            | _ -> failwithf "unexpected HTTP/2 frame %d before readiness data" kind

        match reason with
        | "observer_lagged" ->
            source.Publish pendingFirst
            do! source.Projected 2
            source.Publish freshSecond
            do! source.Projected 3
        | "publication_notifications_failed" ->
            source.Fail(IOException "publication watcher interrupted under backpressure")
            service.BeginStop()
        | _ -> service.BeginStop()

        // A canceled flush would reset the stream before the client restores credit.
        do! Task.Delay(60, ct)
        let increment = [| 0uy; 16uy; 0uy; 0uy |]
        let credit = Array.concat [ h2Frame 8uy 0uy 0 increment; h2Frame 8uy 0uy 1 increment ]
        do! wire.WriteAsync(credit.AsMemory(), ct)
        let mutable ended = false
        while not ended do
            let! kind, flags, id, payload = readH2Frame wire ct
            match kind with
            | 0uy ->
                Assert.Equal(1, id)
                data.Write payload
                ended <- flags &&& 1uy <> 0uy
            | 4uy | 7uy | 8uy -> ()
            | _ -> failwithf "unexpected HTTP/2 frame %d while draining readiness data" kind
        let expected =
            [ for frame in [ Fresh { Generation = generation.Generation; SnapshotId = generation.SnapshotId
                                     PublishedAtMs = 100250L; Revisions = generation.Revisions }
                             Unavailable reason ] ->
                  "event: readiness\ndata: " + toText orderedCompactStyle (readinessJ frame) + "\n\n" ]
            |> String.concat ""
        Assert.Equal(expected, Encoding.UTF8.GetString(data.ToArray()))
        do! joined (service.Close())
        Assert.Equal(0, service.Observers)
    })

[<Fact>]
let ``Kestrel keeps an unstarted observer out of SSE during shutdown`` () =
    let source = Source()
    let release = completed<unit> ()
    source.BeforeRead <- fun () -> release.Task
    source.Set freshFirst
    withKestrel source { quiet with KeepaliveInterval = TimeSpan.FromMilliseconds 10.0 } (fun connect token service _ -> task {
        use handler = new SocketsHttpHandler(UseProxy = false)
        handler.ConnectCallback <- fun _ _ -> ValueTask<Stream>(connect ())
        use http = new HttpClient(handler, disposeHandler = false)
        use request = new HttpRequestMessage(HttpMethod.Get, "http://localhost/v1/retrieval/observe")
        request.Version <- HttpVersion.Version20
        request.VersionPolicy <- HttpVersionPolicy.RequestVersionExact
        request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + token) |> ignore
        let receiving = http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead)
        do! source.Read 1
        do! Task.Delay 80
        Assert.False(receiving.IsCompleted)
        service.BeginStop()
        use! response = bounded receiving
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode)
        Assert.Equal("application/json", response.Content.Headers.ContentType.MediaType)
        let! body = response.Content.ReadAsStringAsync()
        Assert.Equal(canon (Obj [ "error", Str "retrieval_stopping"; "retryable", Bool false ]), canon (parse body))
        do! joined (service.Close())
    })


[<Fact>]
let ``a Kestrel client reset drains an unstarted observer and frees its slot`` () =
    let source = Source()
    let release = completed<unit> ()
    source.BeforeRead <- fun () -> release.Task
    source.Set freshFirst
    withKestrel source { quiet with Capacity = 1 } (fun connect token service drained -> task {
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        let ct = deadline.Token
        use wire = connect ()
        do! openH2 wire token ct
        do! source.Read 1
        Assert.Equal(1, service.Observers)
        // HTTP/2 CANCEL resets just this stream and signals RequestAborted.
        let reset = h2Frame 3uy 0uy 1 [| 0uy; 0uy; 0uy; 8uy |]
        do! wire.WriteAsync(reset.AsMemory(), ct)
        do! drained ()
        Assert.Equal(0, service.Observers)
        release.SetResult()
        use peer = connect ()
        do! openH2 peer token ct
        do! source.Read 2
        Assert.Equal(1, service.Observers)
        do! peer.WriteAsync(reset.AsMemory(), ct)
        do! drained ()
        Assert.Equal(0, service.Observers)
    })

let private askQuestion text : Requests.Question =
    { Text = text; Snapshot = None; Limit = 8L; MaxChars = 2400L; IncludeContext = false; IncludeQuery = true; IncludeResults = true }

let private sqliteScalar<'T> (path: string) (sql: string) : 'T =
    use connection = new Microsoft.Data.Sqlite.SqliteConnection(sprintf "Data Source=%s;Mode=ReadOnly" path)
    connection.Open()
    use command = connection.CreateCommand()
    command.CommandText <- sql
    command.ExecuteScalar() :?> 'T

[<Fact>]
let ``begin stop with zero active callers rejects ask with retrieval stopping before and after close`` () = task {
    let source = Source()
    let evalCalls = ref 0
    let ctx : Requests.Ctx =
        { source.Ctx with
            Readiness = fun _ -> Task.FromResult(freshJ 1L snapshotA)
            Evaluate = fun _ _ _ ->
                Interlocked.Increment(&evalCalls.contents) |> ignore
                Task.FromResult(Obj [ "answer", Str "unexpected" ]) }
    let service = new Service(ctx, quiet)
    Assert.Equal(0, service.Observers)
    service.BeginStop()
    // Give any eager background teardown time to run if it were mistakenly started on BeginStop.
    do! Task.Delay 30
    // Projection remains alive until Close() joins shutdown.
    Assert.NotEqual(Null, service.Remit())
    let! beforeClose = Assert.ThrowsAsync<EnglishError>(fun () -> joined (service.Ask(askQuestion "during drain", CancellationToken.None)))
    Assert.Equal(("retrieval_stopping", 503, false), englishError beforeClose)
    do! joined (service.Close())
    let! afterClose = Assert.ThrowsAsync<EnglishError>(fun () -> joined (service.Ask(askQuestion "after close", CancellationToken.None)))
    Assert.Equal(("retrieval_stopping", 503, false), englishError afterClose)
    // Close remains idempotent after BeginStop.
    do! joined (service.Close())
    Assert.Equal(0, Volatile.Read(&evalCalls.contents))
}

[<Fact>]
let ``ask hitting the post admission closing window returns retrieval stopping`` () = task {
    let source = Source()
    let evalCalls = ref 0
    let serviceRef : Service option ref = ref None
    let ctx : Requests.Ctx =
        { source.Ctx with
            Readiness = fun _ ->
                // Trigger BeginStop after admission.WaitAsync has already granted a permit.
                serviceRef.Value |> Option.iter (fun s -> s.BeginStop())
                Task.FromResult(freshJ 1L snapshotA)
            Evaluate = fun _ _ _ ->
                Interlocked.Increment(&evalCalls.contents) |> ignore
                Task.FromResult(Obj [ "answer", Str "unexpected" ]) }
    let service = new Service(ctx, quiet)
    serviceRef.Value <- Some service
    let! error = Assert.ThrowsAsync<EnglishError>(fun () -> joined (service.Ask(askQuestion "post admission race", CancellationToken.None)))
    Assert.Equal(("retrieval_stopping", 503, false), englishError error)
    Assert.Equal(0, Volatile.Read(&evalCalls.contents))
    do! joined (service.Close())
}

let private journalRow (path: string) =
    let facts = sqliteScalar<string> path "SELECT facts FROM readiness_transitions ORDER BY seq DESC LIMIT 1"
    let observedAt = Canonical.JsonInput.parse facts |> Canonical.JsonInput.property "observed_at"
    sqliteScalar<float> path "SELECT committed_at FROM readiness_transitions ORDER BY seq DESC LIMIT 1",
    observedAt |> Canonical.JsonInput.property "spec" |> Canonical.JsonInput.number
let private journalCount (path: string) = sqliteScalar<int64> path "SELECT COUNT(*) FROM readiness_transitions"

[<Fact>]
let ``steady state observe heads dedupes readiness transitions while advancing observed at`` () =
    use f = new IndexStateTests.Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    Assert.Equal(1L, journalCount f.Path)
    Assert.Equal(1L, sqliteScalar<int64> f.Path "SELECT MAX(seq) FROM readiness_transitions")
    for _ in 1..20 do
        let at = f.Clock.Advance(30.0)
        state.ObserveHeads(f.Heads, at) |> ignore
        // In place: the latest row's facts and commit clock advance together.
        Assert.Equal((at, at), journalRow f.Path)
    // Steady-state ticks advance sources.observed_at without bloating readiness_transitions.
    Assert.Equal(1L, journalCount f.Path)
    Assert.Equal(1L, sqliteScalar<int64> f.Path "SELECT MAX(seq) FROM readiness_transitions")
    Assert.True(bool' (state.Freshness(rev '1' 64, 30.0)) "fresh")
    // Real readiness transitions (hint arrival and hint clearance) still append rows.
    state.EventHint("evt-steady-1", "spec", "push") |> ignore
    Assert.Equal(2L, journalCount f.Path)
    // The refreshed row replays at its own commit clock as Fresh, not as a future observation.
    match (state.ObserveSince(30.0, Some 0L)).History with
    | IndexState.Transitions [ refreshed ] ->
        Assert.True(bool' refreshed.Freshness "fresh")
        Assert.Equal(Float 1600.0, field refreshed.Freshness "checked_at")
    | other -> failwithf "expected the refreshed row in history, got %A" other
    state.ObserveHeads(f.Heads, f.Clock.Advance(1.0)) |> ignore
    Assert.Equal(3L, journalCount f.Path)

[<Fact>]
let ``a same revision refresh after expiry journals the restored fresh state before the next hint`` () =
    use f = new IndexStateTests.Fixture()
    Assert.Throws<IndexState.StateError>(fun () ->
        IndexState.State(f.Path, f.Allowed, clock = f.Clock.Now, maxObservationAge = 0.0) |> ignore) |> ignore
    let state = IndexState.State(f.Path, f.Allowed, leaseSeconds = 10.0, clock = f.Clock.Now, maxObservationAge = 30.0)
    Assert.Equal(30.0, state.MaxObservationAge)
    f.PublishInitial state |> ignore
    let cursor = (state.ObserveSince(30.0, None)).Sequence
    Assert.Equal(1L, cursor)
    // Every observation expires; the observer's live sample is stale.
    f.Clock.Advance(31.0) |> ignore
    let expired = state.ObserveSince(30.0, Some cursor)
    Assert.Equal<string list>([ "observation_stale_or_unknown" ], strings expired.Current.Freshness "reasons")
    Assert.Equal(cursor, expired.Sequence)
    // A same-revision refresh restores freshness and must journal it.
    state.ObserveHeads(f.Heads, f.Clock.Advance()) |> ignore
    Assert.Equal(2L, journalCount f.Path)
    // A hint commits before any observer samples the restored state.
    state.EventHint("evt-after-expiry", "spec", "push") |> ignore
    let hinted = state.ObserveSince(30.0, Some cursor)
    match hinted.History with
    | IndexState.Transitions [ restored ] -> Assert.True(bool' restored.Freshness "fresh")
    | other -> failwithf "expected the restored fresh transition, got %A" other
    Assert.Equal<string list>([ "authoritative_observation_required" ], strings hinted.Current.Freshness "reasons")
    // Once the hint clears, replay carries both the restored Fresh and the Indexing commit.
    state.ObserveHeads(f.Heads, f.Clock.Advance()) |> ignore
    let cleared = state.ObserveSince(30.0, Some cursor)
    match cleared.History with
    | IndexState.Transitions [ restored; indexing ] ->
        Assert.True(bool' restored.Freshness "fresh")
        Assert.Equal<string list>([ "authoritative_observation_required" ], strings indexing.Freshness "reasons")
    | other -> failwithf "expected restored fresh then indexing, got %A" other
    Assert.True(bool' cleared.Current.Freshness "fresh")

    // Fault injection: a journal horizon beyond the observer's age reproduces the
    // age-blind dedupe; the refresh stays in place and replay skips the restored Fresh.
    use g = new IndexStateTests.Fixture()
    let lax = g.State
    Assert.Equal(IndexState.maxObservationAgeDefault, lax.MaxObservationAge)
    g.PublishInitial lax |> ignore
    g.Clock.Advance(31.0) |> ignore
    lax.ObserveHeads(g.Heads, g.Clock.Advance()) |> ignore
    Assert.Equal(1L, journalCount g.Path)
    lax.EventHint("evt-after-expiry", "spec", "push") |> ignore
    Assert.Equal(IndexState.Transitions [], (lax.ObserveSince(30.0, Some 1L)).History)

[<Fact>]
let ``the newest unseen refresh replays after it expires without another hint`` () =
    use f = new IndexStateTests.Fixture()
    f.Clock.Value <- 0.0
    let age = 30.0
    let state = IndexState.State(f.Path, f.Allowed, clock = f.Clock.Now, maxObservationAge = age)
    f.PublishInitial state |> ignore
    let cursor = (state.ObserveSince(age, None)).Sequence
    Assert.Equal(1L, cursor)

    let refreshedAt = f.Clock.Advance(31.0)
    let expired = state.ObserveSince(age, Some cursor)
    Assert.Equal(cursor, expired.Sequence)
    Assert.Equal<string list>([ "observation_stale_or_unknown" ], strings expired.Current.Freshness "reasons")
    state.ObserveHeads(f.Heads, refreshedAt) |> ignore

    f.Clock.Advance(4.0) |> ignore
    let fresh = state.ObserveSince(age, Some cursor)
    Assert.Equal(2L, fresh.Sequence)
    Assert.Equal(35.0, fresh.Current.CheckedAt)
    Assert.True(bool' fresh.Current.Freshness "fresh")
    Assert.Equal(IndexState.Transitions [], fresh.History)

    f.Clock.Advance(27.0) |> ignore
    let late = state.ObserveSince(age, Some cursor)
    Assert.Equal(2L, late.Sequence)
    match late.History with
    | IndexState.Transitions [ refreshed ] ->
        Assert.True(bool' refreshed.Freshness "fresh")
        Assert.Equal(31.0, refreshed.CheckedAt)
        Assert.Equal(late.Current.Active, refreshed.Active)
    | other -> failwithf "expected the seq-2 refresh in history, got %A" other
    Assert.False(bool' late.Current.Freshness "fresh")
    Assert.Equal(62.0, late.Current.CheckedAt)
    Assert.Equal(Some 61.0, late.Current.ChangedAt)
    Assert.Equal<string list>([ "observation_stale_or_unknown" ], strings late.Current.Freshness "reasons")

[<Fact>]
let ``legacy readiness transitions table without autoincrement is migrated on open`` () =
    use f = new IndexStateTests.Fixture()
    let state = f.State
    f.PublishInitial state |> ignore
    // Simulate a pre-v4 schema without AUTOINCREMENT while preserving row seq.
    do
        use connection = new Microsoft.Data.Sqlite.SqliteConnection(sprintf "Data Source=%s" f.Path)
        connection.Open()
        let exec sql = (use c = connection.CreateCommand() in c.CommandText <- sql; c.ExecuteNonQuery() |> ignore)
        exec "CREATE TABLE readiness_transitions_legacy(seq INTEGER PRIMARY KEY, generation INTEGER NOT NULL REFERENCES generations(generation), committed_at REAL NOT NULL, facts TEXT NOT NULL)"
        exec "INSERT INTO readiness_transitions_legacy SELECT seq,generation,committed_at,facts FROM readiness_transitions"
        exec "DROP TABLE readiness_transitions"
        exec "ALTER TABLE readiness_transitions_legacy RENAME TO readiness_transitions"
    let reopened = f.Reopen()
    let ddl = sqliteScalar<string> f.Path "SELECT sql FROM sqlite_master WHERE type='table' AND name='readiness_transitions'"
    Assert.Contains("AUTOINCREMENT", ddl, StringComparison.OrdinalIgnoreCase)
    Assert.Equal(1L, sqliteScalar<int64> f.Path "SELECT COUNT(*) FROM readiness_transitions")
    reopened.EventHint("evt-after-migrate", "spec", "push") |> ignore
    Assert.Equal(2L, sqliteScalar<int64> f.Path "SELECT MAX(seq) FROM readiness_transitions")

[<Fact>]
let ``bounded readiness transitions prune old rows keep monotonic seq and lag pruned observers`` () = task {
    use f = new IndexStateTests.Fixture()
    Assert.Throws<IndexState.StateError>(fun () ->
        IndexState.State(f.Path, f.Allowed, leaseSeconds = 10.0, clock = f.Clock.Now, transitionRetention = 0) |> ignore) |> ignore
    let retention = 4
    let state = IndexState.State(f.Path, f.Allowed, leaseSeconds = 10.0, clock = f.Clock.Now, transitionRetention = retention)
    Assert.Equal(retention, state.TransitionRetention)
    f.PublishInitial state |> ignore
    let initial = state.ObserveSince(30.0, None)
    Assert.Equal(1L, initial.Sequence)
    Assert.Equal(IndexState.Transitions [], initial.History)

    // Force 8 more distinct readiness transitions (9 total) across a 4-row retention window.
    let mutable lastSeq = 1L
    for i in 1..4 do
        let digit = char (int '2' + i)
        state.ObserveHeads([ "spec", Some(rev digit 40) ], f.Clock.Advance()) |> ignore
        let seqAfterChange = sqliteScalar<int64> f.Path "SELECT MAX(seq) FROM readiness_transitions"
        Assert.True(seqAfterChange > lastSeq)
        Assert.True(sqliteScalar<int64> f.Path "SELECT COUNT(*) FROM readiness_transitions" <= int64 retention)
        lastSeq <- seqAfterChange
        let plan = state.PendingPlan()
        let token = state.StartBuild plan
        Assert.True(bool' (state.CompleteBuild(token, rev digit 64, targets plan)) "published")
        let seqAfterBuild = sqliteScalar<int64> f.Path "SELECT MAX(seq) FROM readiness_transitions"
        Assert.True(seqAfterBuild > lastSeq)
        Assert.True(sqliteScalar<int64> f.Path "SELECT COUNT(*) FROM readiness_transitions" <= int64 retention)
        lastSeq <- seqAfterBuild

    Assert.Equal(9L, lastSeq)
    Assert.Equal(4L, sqliteScalar<int64> f.Path "SELECT COUNT(*) FROM readiness_transitions")
    Assert.Equal(6L, sqliteScalar<int64> f.Path "SELECT MIN(seq) FROM readiness_transitions")

    // Cursors whose intermediate rows were pruned report explicit Truncated.
    let prunedOld = state.ObserveSince(30.0, Some 1L)
    Assert.Equal(IndexState.Truncated, prunedOld.History)
    Assert.Equal(9L, prunedOld.Sequence)
    let prunedEdge = state.ObserveSince(30.0, Some 4L)
    Assert.Equal(IndexState.Truncated, prunedEdge.History)

    // Cursor 5 still has every subsequent transition (6, 7, 8 in history; 9 as current).
    let intact = state.ObserveSince(30.0, Some 5L)
    match intact.History with
    | IndexState.Transitions items -> Assert.Equal(3, items.Length)
    | IndexState.Truncated -> failwith "cursor 5 should not be truncated when oldest retained seq is 6"

    // A brand-new read (None) always receives the current state cleanly after pruning;
    // Some 0L is an established cursor whose successor seq 1 was pruned.
    let freshInitial = state.ObserveSince(30.0, None)
    Assert.Equal(IndexState.Transitions [], freshInitial.History)
    Assert.Equal(9L, freshInitial.Sequence)
    Assert.True(bool' freshInitial.Current.Freshness "fresh")
    Assert.Equal(IndexState.Truncated, (state.ObserveSince(30.0, Some 0L)).History)

    // End-to-end: an observer suspended across > retention transitions receives observer_lagged.
    use changes = new IndexState.Changes(f.Path)
    let holding, release = completed<unit> (), completed<unit> ()
    let hold = ref false
    let ctx : Requests.Ctx =
        { Sources = []; WatchReadiness = changes.Watch; WatchFailure = changes.Failure; WhileWatchHealthy = changes.WhileHealthy
          Readiness = (fun _ -> failwith "Observers read through Observe")
          Observe = (fun cursor ct -> task {
              if Volatile.Read(&hold.contents) then
                  holding.TrySetResult() |> ignore
                  do! release.Task.WaitAsync ct
              return state.ObserveSince(30.0, cursor) })
          Evaluate = fun _ _ _ -> failwith "Observers never start inference" }
    let service = new Service(ctx, quiet)
    do! joinAfter service (fun () -> task {
        let sink = Collector()
        let observing = service.Observe(sink.Sink, CancellationToken.None)
        let! firstFrame = sink.Next()
        match firstFrame with
        | Fresh ready -> Assert.Equal(5L, ready.Generation)
        | other -> failwithf "expected fresh generation 5, got %A" other
        Volatile.Write(&hold.contents, true)
        state.EventHint("evt-trigger-hold", "spec", "push") |> ignore
        do! bounded holding.Task
        // Advance beyond the 4-row retention window while the producer is paused.
        for j in 1..3 do
            let digit = char (int 'a' + j)
            state.ObserveHeads([ "spec", Some(rev digit 40) ], f.Clock.Advance()) |> ignore
            let plan = state.PendingPlan()
            let token = state.StartBuild plan
            Assert.True(bool' (state.CompleteBuild(token, rev digit 64, targets plan)) "published")
        Volatile.Write(&hold.contents, false)
        release.SetResult()
        let! lagged = sink.Next()
        Assert.Equal(Unavailable "observer_lagged", lagged)
        do! joined observing
        Assert.Equal(0, service.Observers)
        // Reconnecting initial observer cleanly receives the latest published generation.
        let reconnected = Collector()
        use cancel = new CancellationTokenSource()
        let resumed = service.Observe(reconnected.Sink, cancel.Token)
        let! latest = reconnected.Next()
        match latest with
        | Fresh ready -> Assert.Equal(8L, ready.Generation)
        | other -> failwithf "expected fresh generation 8, got %A" other
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined resumed)
        ()
    })
}

[<Fact>]
let ``an observer held at cursor zero before publication is lagged when seq one is pruned`` () = task {
    use f = new IndexStateTests.Fixture()
    let state = IndexState.State(f.Path, f.Allowed, leaseSeconds = 10.0, clock = f.Clock.Now, transitionRetention = 2)
    use changes = new IndexState.Changes(f.Path)
    let holding, release = completed<unit> (), completed<unit> ()
    let hold = ref false
    let held = ConcurrentQueue<int64 option>()
    let ctx : Requests.Ctx =
        { Sources = []; WatchReadiness = changes.Watch; WatchFailure = changes.Failure; WhileWatchHealthy = changes.WhileHealthy
          Readiness = (fun _ -> failwith "Observers read through Observe")
          Observe = (fun cursor ct -> task {
              if Volatile.Read(&hold.contents) then
                  held.Enqueue cursor
                  holding.TrySetResult() |> ignore
                  do! release.Task.WaitAsync ct
              return state.ObserveSince(30.0, cursor) })
          Evaluate = fun _ _ _ -> failwith "Observers never start inference" }
    let service = new Service(ctx, quiet)
    do! joinAfter service (fun () -> task {
        let sink = Collector()
        let observing = service.Observe(sink.Sink, CancellationToken.None)
        let! unpublishedFrame = sink.Next()
        Assert.Equal(Unavailable "no_published_generation", unpublishedFrame)
        Volatile.Write(&hold.contents, true)
        // Publication (seq 1), a hint (seq 2) and its clearance (seq 3) prune seq 1 at retention 2.
        f.PublishInitial state |> ignore
        do! bounded holding.Task
        state.EventHint("evt-cursor-zero", "spec", "push") |> ignore
        state.ObserveHeads(f.Heads, f.Clock.Advance()) |> ignore
        Assert.Equal(2L, sqliteScalar<int64> f.Path "SELECT MIN(seq) FROM readiness_transitions")
        Assert.Equal<int64 option list>([ Some 0L ], List.ofSeq held)
        Assert.Equal(IndexState.Truncated, (state.ObserveSince(30.0, Some 0L)).History)
        Volatile.Write(&hold.contents, false)
        release.SetResult()
        let! lagged = sink.Next()
        Assert.Equal(Unavailable "observer_lagged", lagged)
        do! joined observing
        Assert.Equal(0, service.Observers)
        // A brand-new observer (cursor None) still receives the current Fresh state.
        let reconnected = Collector()
        use cancel = new CancellationTokenSource()
        let resumed = service.Observe(reconnected.Sink, cancel.Token)
        let! latest = reconnected.Next()
        match latest with
        | Fresh ready -> Assert.Equal(1L, ready.Generation)
        | other -> failwithf "expected fresh generation 1, got %A" other
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined resumed)
        ()
    })
}

[<Theory>]
[<InlineData("throw")>]
[<InlineData("faulted")>]
[<InlineData("async_fault")>]
let ``watch readiness throw and faulted task report publication notifications failed`` mode = task {
    let fault = IOException("watch fault " + mode)
    let neverFailed = completed<exn> ()
    let asyncWatch = completed<unit> ()
    let ctx : Requests.Ctx =
        { Sources = []
          WatchFailure = neverFailed.Task
          WhileWatchHealthy = fun action -> action ()
          Readiness = fun _ -> failwith "Observers read through Observe"
          Evaluate = fun _ _ _ -> failwith "Observers never start inference"
          Observe = fun _ _ -> Task.FromResult(batch freshFirst)
          WatchReadiness = fun () ->
              match mode with
              | "throw" -> raise fault
              | "faulted" -> Task.FromException<unit> fault
              | _ -> asyncWatch.Task }
    let service = new Service(ctx, quiet)
    do! joinAfter service (fun () -> task {
        let sink = Collector()
        let observing = service.Observe(sink.Sink, CancellationToken.None)
        if mode = "async_fault" then
            let! initial = sink.Next()
            Assert.Equal(frameOfFirst, initial)
            asyncWatch.SetException fault
            asyncWatch.Task.Exception |> ignore
        let! terminal = sink.Next()
        Assert.Equal(Unavailable "publication_notifications_failed", terminal)
        let! error = Assert.ThrowsAsync<IOException>(fun () -> joined observing)
        Assert.Same(fault, error)
        Assert.Equal(0, service.Observers)
    })
}

// Runs a continuation inside the antecedent's completion call. A
// RunContinuationsAsynchronously source still hands a ContinueWith continuation
// to its scheduler synchronously, so QueueTask observes the exact fault moment.
type private CompletionProbe() =
    inherit TaskScheduler()
    override this.QueueTask(work: Task) = this.TryExecuteTask work |> ignore
    override _.TryExecuteTaskInline(_, _) = false
    override _.GetScheduledTasks() = Seq.empty

let private failureSeenAtFault (watched: Task<unit>) (failure: Task<exn>) =
    let seen = ref None
    let probe =
        watched.ContinueWith((fun (faulted: Task<unit>) -> seen.Value <- Some(faulted.IsFaulted, failure.IsCompleted)),
                             CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, CompletionProbe())
    seen, probe

[<Fact>]
let ``changes fail publishes failure before faulting watch`` () = task {
    use f = new IndexStateTests.Fixture()
    use changes = new IndexState.Changes(f.Path)
    let watched = changes.Watch()
    let seen, probe = failureSeenAtFault watched changes.Failure
    (changes :> IDisposable).Dispose()
    // Captured inside the faulting call itself: Failure had already completed.
    Assert.Equal(Some(true, true), seen.Value)
    do! joined probe
    let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> joined (watched :> Task))
    Assert.True(changes.Failure.IsCompletedSuccessfully)
    Assert.IsType<ObjectDisposedException>(changes.Failure.Result) |> ignore
    // Fault injection: the same probe catches the reversed order.
    let reversedWatch = completed<unit> ()
    let reversedFailure = completed<exn> ()
    let reversedSeen, reversedProbe = failureSeenAtFault reversedWatch.Task reversedFailure.Task
    let error = ObjectDisposedException "reversed"
    reversedWatch.SetException error
    reversedFailure.SetResult error
    Assert.Equal(Some(true, false), reversedSeen.Value)
    do! joined reversedProbe
}

[<Theory>]
[<InlineData(100.0)>]
[<InlineData(99.0)>]
let ``frozen clock at or past deadline does not spin the producer and wakes on notification`` (deadline: float) =
    let source = Source()
    source.Set { freshFirst with Deadline = Some deadline }
    withService source quiet (fun service -> task {
        let sink = Collector()
        use cancel = new CancellationTokenSource()
        let observing = service.Observe(sink.Sink, cancel.Token)
        do! Task.Delay 120
        // At most the initial sample plus one confirmation retry while clock stays frozen.
        Assert.True(source.Reads <= 2, sprintf "expected bounded reads for frozen clock, got %d" source.Reads)
        Assert.Equal(0, sink.Pending)
        source.Publish freshSecond
        let! recovered = sink.Next()
        Assert.Equal(frameOfSecond, recovered)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observing)
        ()
    })

[<Fact>]
let ``terminal emit swallows transport errors but does not swallow programming errors`` () =
    let options = { quiet with QueueDepth = 1; WriteTimeout = TimeSpan.FromMinutes 1.0 }
    let runWithTerminalError (terminalFault: exn) =
        let source = Source()
        source.Set freshFirst
        withService source options (fun service -> task {
            let release = completed<unit> ()
            let sink = Collector(fun number ct ->
                if number = 1 then release.Task.WaitAsync ct
                else Task.FromException terminalFault)
            let observing = service.Observe(sink.Sink, CancellationToken.None)
            do! sink.Entered
            source.Publish pendingFirst
            do! source.Projected 2
            source.Publish freshSecond
            do! source.Projected 3
            release.SetResult()
            let! _ = sink.Next()
            match terminalFault with
            | :? IOException | :? ObjectDisposedException -> do! joined observing
            | _ ->
                let! propagated = Assert.ThrowsAnyAsync<Exception>(fun () -> joined observing)
                Assert.Same(terminalFault, propagated)
            Assert.Equal(0, service.Observers)
        })
    task {
        // Transport teardown is absorbed.
        do! runWithTerminalError (IOException "broken pipe")
        do! runWithTerminalError (ObjectDisposedException "response body")
        // Programming errors propagate, invalidOp included.
        do! runWithTerminalError (InvalidOperationException "programming bug")
        do! runWithTerminalError (ArgumentException "programming bug")
    }

[<Fact>]
let ``malformed readiness sample fails observe without poisoning ask on the shared graph`` () = task {
    let source = Source()
    source.Set freshFirst
    let ctx : Requests.Ctx =
        { source.Ctx with
            Readiness = fun _ -> Task.FromResult(freshJ 1L snapshotA)
            Evaluate = fun q _ _ -> Task.FromResult(Obj [ "answer", Str ("ok:" + q.Text) ]) }
    let service = new Service(ctx, quiet)
    do! joinAfter service (fun () -> task {
        let sink = Collector()
        let observing = service.Observe(sink.Sink, CancellationToken.None)
        let! initial = sink.Next()
        Assert.Equal(frameOfFirst, initial)
        // Malformed sample: non-string entry in reasons array.
        let malformedReasons =
            notFreshJ 1L snapshotA [] [] []
            |> function Obj pairs -> Obj [ for k, v in pairs -> k, (if k = "reasons" then Arr [ Int 3L ] else v) ] | other -> other
        source.Publish(sampleOf malformedReasons (Some first) None None)
        let! failed = sink.Next()
        Assert.Equal(Unavailable "observation_failed", failed)
        let! error = Assert.ThrowsAsync<InvalidOperationException>(fun () -> joined observing)
        Assert.Equal("Malformed retrieval readiness sample", error.Message)
        // Ask on the SAME Service instance still computes published cleanly through Ranvier.Graph.
        let! answer = bounded (service.Ask(askQuestion "still healthy", CancellationToken.None))
        Assert.Equal(Str "ok:still healthy", field answer "answer")
        Assert.True(bool' (field answer "freshness") "fresh")
        // A subsequent observer also recovers cleanly on the same graph.
        source.Set freshSecond
        let recovered = Collector()
        use cancel = new CancellationTokenSource()
        let observingAgain = service.Observe(recovered.Sink, cancel.Token)
        let! current = recovered.Next()
        Assert.Equal(frameOfSecond, current)
        cancel.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> joined observingAgain)
        ()
    })
}

let private withStoreAsync (store: Query.ManagedStore) (body: Query.ManagedStore -> Task) = task {
    let! outcome = task {
        try
            do! body store
            return None
        with error -> return Some error }
    do! joined (store.CloseAsync())
    match outcome with Some error -> ExceptionDispatchInfo.Capture(error).Throw() | None -> ()
}

[<Fact>]
let ``managed store observe async replays coalesced history without reopening older duckdb artifacts`` () = withPublished (fun corpus -> task {
    do! withStoreAsync (corpus.Open()) (fun store -> task {
        let! initial = store.ObserveAsync(None, CancellationToken.None)
        Assert.Equal(IndexState.Transitions [], initial.History)
        Assert.True(bool' initial.Current.Freshness "fresh")
        Assert.Equal(Some 1L, initial.Current.Active |> Option.map (fun g -> g.Generation))

        // Coalesce two full build cycles while the observer does not read between them.
        corpus.Commit("test", "2.md", "Content 2 gen 2\nEvidence\n") |> ignore
        Assert.Empty(corpus.Observe())
        let gen2 = corpus.RunOnce()
        Assert.True(bool' gen2 "published")
        let gen2Snapshot = text (field gen2 "snapshot_id")

        corpus.Commit("test", "2.md", "Content 2 gen 3\nEvidence\n") |> ignore
        Assert.Empty(corpus.Observe())
        let gen3 = corpus.RunOnce()
        Assert.True(bool' gen3 "published")
        let gen3Snapshot = text (field gen3 "snapshot_id")

        // Fault injection: delete the superseded gen 2 artifact before replaying history.
        // ObserveAsync must not reopen older historical DuckDB files.
        let gen2Artifact = Path.Combine(corpus.Root, "generations", gen2Snapshot + ".duckdb")
        if File.Exists gen2Artifact then File.Delete gen2Artifact
        Assert.False(File.Exists gen2Artifact)

        let! replayed = store.ObserveAsync(Some initial.Sequence, CancellationToken.None)
        match replayed.History with
        | IndexState.Transitions [ indexing1; fresh2; indexing2 ] ->
            Assert.False(bool' indexing1.Freshness "fresh")
            Assert.Equal(Some 1L, indexing1.Active |> Option.map (fun g -> g.Generation))
            Assert.True(bool' fresh2.Freshness "fresh")
            Assert.Equal(Some 2L, fresh2.Active |> Option.map (fun g -> g.Generation))
            Assert.False(bool' indexing2.Freshness "fresh")
            Assert.Equal(Some 2L, indexing2.Active |> Option.map (fun g -> g.Generation))
        | other -> failwithf "expected 3 historical transitions, got %A" other

        Assert.True(bool' replayed.Current.Freshness "fresh")
        Assert.Equal(Some 3L, replayed.Current.Active |> Option.map (fun g -> g.Generation))
        Assert.Equal(Some gen3Snapshot, replayed.Current.Active |> Option.map (fun g -> g.SnapshotId))
        Assert.Equal(Str gen3Snapshot, field (store.Schema()) "snapshot_id")

        // Negative control: removing the active generation artifact still fails binding on a fresh store.
        do! joined (store.CloseAsync())
        let gen3Artifact = Path.Combine(corpus.Root, "generations", gen3Snapshot + ".duckdb")
        File.Delete gen3Artifact
        do! withStoreAsync (corpus.Open()) (fun freshStore -> task {
            let! missing = Assert.ThrowsAsync<DuckDB.NET.Data.DuckDBException>(fun () -> freshStore.ObserveAsync(None, CancellationToken.None) :> Task)
            Assert.StartsWith("Catalog Error: Table with name metadata does not exist", missing.Message)
        })
    })
})

let private historicalGenerations (batch: IndexState.ObservationBatch) =
    match batch.History with
    | IndexState.Transitions items -> [ for o in items -> o.Active.Value.Generation, bool' o.Freshness "fresh", strings o.Freshness "reasons" ]
    | IndexState.Truncated -> failwith "unexpected journal truncation"

[<Fact>]
let ``historical replay rejects a generation published under a different corpus configuration`` () = withPublished (fun corpus -> task {
    corpus.Commit("test", "2.md", "Content 2 gen 2\nEvidence\n") |> ignore
    Assert.Empty(corpus.Observe())
    Assert.True(bool' (corpus.RunOnce()) "published")
    let statePath = Path.Combine(corpus.Root, "index-state.sqlite")
    // Each Coordinator publication records the configuration identity it bound.
    let identity = Coordinator.configIdentity (fst (Coordinator.configSources corpus.Config))
    Assert.Equal(2L, sqliteScalar<int64> statePath (sprintf "SELECT COUNT(*) FROM generations WHERE configuration_sha256='%s'" identity))
    let generationOne (batch: IndexState.ObservationBatch) =
        historicalGenerations batch |> List.filter (fun (generation, _, _) -> generation = 1L) |> List.map (fun (_, fresh, reasons) -> fresh, reasons)

    // Positive control: under configuration A, generation 1 replays as Fresh.
    do! withStoreAsync (corpus.Open()) (fun store -> task {
        let! replay = store.ObserveAsync(Some 0L, CancellationToken.None)
        Assert.Contains((true, []), generationOne replay)
        Assert.True(bool' replay.Current.Freshness "fresh")
    })

    // Configuration B keeps the source IDs and dependencies and changes only the tracked ref.
    let configB = Path.Combine(Path.GetDirectoryName corpus.Config, "corpus-b.json")
    File.WriteAllText(configB, File.ReadAllText(corpus.Config).Replace("refs/heads/main", "refs/heads/master"))
    Assert.NotEqual<string>(identity, Coordinator.configIdentity (fst (Coordinator.configSources configB)))
    do! withStoreAsync (Query.ManagedStore(corpus.Root, configB, corpus.Engine, promptFile)) (fun store -> task {
        let! replay = store.ObserveAsync(Some 0L, CancellationToken.None)
        let rejected = generationOne replay |> List.filter fst
        Assert.Empty(rejected)
        Assert.Contains((false, [ "corpus_configuration_changed" ]), generationOne replay)
        Assert.Equal<string list>([ "corpus_configuration_changed" ], strings replay.Current.Freshness "reasons")
    })

    // Fault injection: legacy rows without a recorded identity fail closed on replay,
    // while the generation this call binds and verifies still serves.
    do
        use connection = new Microsoft.Data.Sqlite.SqliteConnection(sprintf "Data Source=%s" statePath)
        connection.Open()
        use command = connection.CreateCommand()
        command.CommandText <- "UPDATE generations SET configuration_sha256=NULL"
        command.ExecuteNonQuery() |> ignore
    do! withStoreAsync (corpus.Open()) (fun store -> task {
        let! replay = store.ObserveAsync(Some 0L, CancellationToken.None)
        Assert.Contains((false, [ "corpus_configuration_changed" ]), generationOne replay)
        Assert.Empty(generationOne replay |> List.filter fst)
        Assert.True(bool' replay.Current.Freshness "fresh")
        Assert.Equal(Some 2L, replay.Current.Active |> Option.map (fun g -> g.Generation))
    })
})

[<Fact>]
let ``recorded identity fallback does not verify legacy history sharing the snapshot`` () = withPublished (fun corpus -> task {
    let coordinator = corpus.Coordinator
    let state = coordinator.State
    let first = state.ActiveGeneration().Value
    let snapshot = first.SnapshotId
    let identity = coordinator.Identity

    // Republish the same artifact with a recorded identity, then simulate a legacy first publication.
    state.InvalidateSources([ "test" ], "Republish the same snapshot") |> ignore
    let plan = state.PendingPlan()
    Assert.True(bool' (state.CompleteBuild(state.StartBuild plan, snapshot, targets plan, configurationSha256 = identity)) "published")
    do
        use connection = new Microsoft.Data.Sqlite.SqliteConnection(sprintf "Data Source=%s" (Path.Combine(corpus.Root, "index-state.sqlite")))
        connection.Open()
        use command = connection.CreateCommand()
        command.CommandText <- "UPDATE generations SET configuration_sha256=NULL WHERE generation=1"
        Assert.Equal(1, command.ExecuteNonQuery())

    do! withStoreAsync (corpus.Open()) (fun store -> task {
        let! live = bounded (store.ObserveAsync(Some 0L, CancellationToken.None))
        Assert.True(bool' live.Current.Freshness "fresh")
        Assert.Contains((1L, true, []), historicalGenerations live)

        // Queue the newer bind first while Call holds the gate. ObserveAsync then samples
        // generation 2 before publication, but reaches the gate only after generation 3 binds.
        let queued = ref None
        let error = Assert.Throws<Query.IndexNotCurrent>(fun () ->
            store.Call(fun _ ->
                let binding = store.ReadinessAsync(CancellationToken.None)
                let replaying = store.ObserveAsync(Some 0L, CancellationToken.None)
                Assert.False(binding.IsCompleted)
                Assert.False(replaying.IsCompleted)
                queued.Value <- Some(binding, replaying)
                corpus.Commit("test", "2.md", "Content 2 gen 3\nEvidence\n") |> ignore
                let published = corpus.Publish()
                Assert.Equal(Int 3L, field published "generation")
                Null) |> ignore)
        match error :> exn with
        | Query.IndexNotCurrent freshness ->
            Assert.Equal<string list>([ "not_latest_generation" ], strings freshness "reasons")
        | other -> raise other

        let binding, replaying = queued.Value.Value
        let! newer = bounded binding
        Assert.True(bool' newer "fresh")
        Assert.Equal(Int 3L, field newer "generation")
        let! fallback = bounded replaying
        Assert.True(bool' fallback.Current.Freshness "fresh")
        Assert.Equal(Some (2L, snapshot, Some identity), fallback.Current.Active |> Option.map (fun g -> g.Generation, g.SnapshotId, g.ConfigurationSha256))
        let historical = historicalGenerations fallback
        Assert.Contains((1L, false, [ "corpus_configuration_changed" ]), historical)
        Assert.DoesNotContain((1L, true, []), historical)
    })
})
