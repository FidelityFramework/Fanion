/// Demand and lifetime ownership for text retrieval. The index owns freshness;
/// Incremental owns ready/running work. HTTP callers own only their demands.
module Fanion.Requests

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Threading
open System.Threading.Channels
open System.Runtime.ExceptionServices
open System.Threading.Tasks
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting
open Ranvier
open Fanion.Canonical
open Fanion.Query
open Fanion.English

[<StructuralEquality; StructuralComparison>]
type Question =
    { Text: string; Snapshot: string option; Limit: int64; MaxChars: int64
      IncludeContext: bool; IncludeQuery: bool; IncludeResults: bool }

type Ctx =
    { Readiness: CancellationToken -> Task<J>
      WatchReadiness: unit -> Task<unit>
      WatchFailure: Task<exn>
      WhileWatchHealthy: (unit -> unit) -> unit
      Observe: int64 option -> CancellationToken -> Task<IndexState.ObservationBatch>
      Evaluate: Question -> string -> CancellationToken -> Task<J>
      Sources: Coordinator.Source list }

// Immutable readiness frames for observers: one on connect, then one per transition.
type FreshReadiness =
    { Generation: int64; SnapshotId: string; PublishedAtMs: int64; Revisions: Map<string, string option> }

type IndexingReadiness =
    { Generation: int64; SnapshotId: string; PendingSources: string list; StaleSources: string list
      Reasons: string list; ChangedAtMs: int64 option }

type RetrievalReadiness =
    | Fresh of FreshReadiness
    | Indexing of IndexingReadiness
    | Unavailable of Reason: string

// Explicit wire codec; keys are emitted in this order.
let readinessJ (frame: RetrievalReadiness) : J =
    let strings values = Arr (values |> List.map Str)
    match frame with
    | Fresh ready ->
        Obj [ "state", Str "fresh"; "generation", Int ready.Generation; "snapshot_id", Str ready.SnapshotId
              "published_at_ms", Int ready.PublishedAtMs
              "revisions", Obj [ for kv in ready.Revisions -> kv.Key, (match kv.Value with Some r -> Str r | None -> Null) ] ]
    | Indexing pending ->
        Obj [ "state", Str "indexing"; "generation", Int pending.Generation; "snapshot_id", Str pending.SnapshotId
              "pending_sources", strings pending.PendingSources; "stale_sources", strings pending.StaleSources
              "reasons", strings pending.Reasons
              "changed_at_ms", (match pending.ChangedAtMs with Some at -> Int at | None -> Null) ]
    | Unavailable reason -> Obj [ "state", Str "unavailable"; "reason", Str reason ]

// The transport owns framing; the service owns admission, ordering, deadlines and lifetime.
type ObserverSink =
    { Emit: RetrievalReadiness -> CancellationToken -> Task
      Keepalive: CancellationToken -> Task }

type ObserverOptions =
    { Capacity: int; QueueDepth: int; KeepaliveInterval: TimeSpan; WriteTimeout: TimeSpan }

let defaultObserverOptions =
    { Capacity = 16; QueueDepth = 32; KeepaliveInterval = TimeSpan.FromSeconds 15.0; WriteTimeout = TimeSpan.FromSeconds 10.0 }

// Why the producer ended one observer's delivery.
type private Terminal =
    | Lagged
    | Failed of reason: string * error: exn

// Projected transition replay for publish, or truncation when the journal pruned older rows.
type private ProjectedHistory =
    | Replayed of RetrievalReadiness list
    | JournalTruncated

// One observer: an ordered bounded queue fed by the single producer.
type private Observer =
    { Queue: Channel<RetrievalReadiness>; Terminal: TaskCompletionSource<Terminal>; mutable HasInitial: bool }

let private milliseconds (seconds: float) = int64 (Math.Round(seconds * 1000.0))

type private Job =
    { Id: uint64; Question: Question; Snapshot: string; mutable Consumers: int }

/// One bounded epoch retains at most 128 exact answers. Rotation joins the old
/// host before discarding its payloads, including canceled attempts and tombstones.
/// No caller can cancel another caller's demand or occupy the GPU while indexing.
type Service(ctx: Ctx, observerOptions: ObserverOptions) =
    do
        if observerOptions.Capacity < 1 || observerOptions.QueueDepth < 1 || observerOptions.KeepaliveInterval <= TimeSpan.Zero || observerOptions.WriteTimeout <= TimeSpan.Zero then
            invalidArg (nameof observerOptions) "Observer capacity, queue depth, keepalive interval and write timeout must be positive"
    let gate = new SemaphoreSlim(1, 1)
    let admission = new SemaphoreSlim(64, 64)
    let observers = new SemaphoreSlim(observerOptions.Capacity, observerOptions.Capacity)
    let lifetime = new CancellationTokenSource()
    let jobs = Dictionary<Question * string, Job>()
    let inputs = ConcurrentDictionary<ValueToken, Job>()
    let outputs = ConcurrentDictionary<ValueToken, Result<J, exn>>()
    let mutable serial, epoch, allocated, consumers, observing = 0UL, 0UL, 0, 0, 0
    let mutable closing = false
    let isClosing () = Volatile.Read(&closing)
    let enterObserver () = Interlocked.Increment(&observing) |> ignore
    let leaveObserver () = Interlocked.Decrement(&observing) |> ignore
    let signal () = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let stopping = signal ()
    let cancelLifetime = Lazy<Task>((fun () -> lifetime.CancelAsync()), LazyThreadSafetyMode.ExecutionAndPublication)
    let beginStop () =
        Volatile.Write(&closing, true)
        stopping.TrySetResult() |> ignore
        cancelLifetime.Value |> ignore
    let mutable epochReleased = signal ()
    let field value name = match value with Obj pairs -> pairs |> List.tryPick (fun (k, v) -> if k = name then Some v else None) | _ -> None
    let next () = serial <- serial + 1UL; serial
    let require = function Ok v -> v | Error e -> raise (InvalidOperationException(sprintf "Retrieval coordinator: %A" e))
    let evaluate invocation cancellation = async {
        match invocation with
        | StepInvocation.Start request ->
            let token = match request.Reads with [{ Value = ReadValue.Input(_, _, value) }] -> value | _ -> invalidOp "One immutable question input is required"
            let job = inputs.[token]
            try
                let! result = ClrInterop.fromTask (ctx.Evaluate job.Question job.Snapshot) cancellation
                outputs.[token] <- Ok result
            with error -> outputs.[token] <- Error error
            return StepOutcome.Complete(Completion.Succeeded token)
        | StepInvocation.Resume _ -> return invalidOp "Retrieval has no external suspension responses"
    }
    let create () =
        epoch <- epoch + 1UL
        let h = AsyncMailbox.create { Epoch = EpochId epoch; MaxConcurrency = 1; CommandCapacity = 1024 } evaluate |> require
        AsyncMailbox.start h |> require
        h
    let mutable handle = create ()
    let send h action = task {
        let operation = AsyncMailbox.admit action h |> require
        let! result = ClrInterop.toTask CancellationToken.None (AsyncMailbox.observe operation)
        require result |> ignore
        // Only this registry, under gate, drains owner notifications.
        AsyncMailbox.drainEvents h |> ignore
        AsyncMailbox.drainDiagnostics h |> ignore
    }
    // Ranvier projects configuration on demand; it never owns asynchronous work.
    let projectionGate = obj ()
    let projection = new Ranvier.Graph({ Ranvier.GraphOptions.Default with ThreadAffinity = Ranvier.ThreadAffinity.Serialised })
    let observed, published = projection.Run(fun () ->
        let observed = createSignal Null
        let published = createMemo(fun _ ->
            let ready = observed.Value
            match field ready "fresh", field ready "mode", field ready "snapshot_id", field ready "current_snapshot_id" with
            | Some(Bool true), Some(Str "latest"), Some(Str snapshot), Some(Str current) when snapshot = current -> Some snapshot
            | _ -> None)
        observed, published)
    let readiness ct = task {
        let! current = ctx.Readiness ct
        return lock projectionGate (fun () -> projection.Run(fun () -> observed.Value <- current; current, published.Value))
    }
    // Observers share the same signal and fence. The memo shapes frames only;
    // the coherent sample carries the generation row and timing beside it.
    let sampled = projection.Run(fun () -> createSignal (None: IndexState.Observation option))
    let frame = projection.Run(fun () -> createMemo(fun _ ->
        let ready = observed.Value
        let strings name =
            match field ready name with
            | Some(Arr items) -> let names = items |> List.choose (function Str s -> Some s | _ -> None) in if names.Length = items.Length then Some names else None
            | _ -> None
        match sampled.Value with
        | None -> Error "Malformed retrieval readiness sample"
        | Some sample ->
            match sample.Active, published.Value with
            | None, _ -> Ok(Unavailable "no_published_generation")
            | Some active, Some snapshot when snapshot = active.SnapshotId ->
                Ok(Fresh { Generation = active.Generation; SnapshotId = snapshot; PublishedAtMs = milliseconds active.PublishedAt; Revisions = active.Revisions })
            | Some active, None ->
                match strings "pending_sources", strings "stale_sources", strings "reasons" with
                | Some pending, Some stale, Some reasons ->
                    Ok(Indexing { Generation = active.Generation; SnapshotId = active.SnapshotId; PendingSources = pending; StaleSources = stale
                                  Reasons = reasons; ChangedAtMs = sample.ChangedAt |> Option.map milliseconds })
                | _ -> Error "Malformed retrieval readiness sample"
            | Some _, Some _ -> Error "Malformed retrieval readiness sample"))
    let sampleOnce cursor ct = task {
        try
            let! sample = ctx.Observe cursor ct
            return Ok sample
        with
        | GraphUnavailable _ | Load.LoadError _ | :? DuckDB.NET.Data.DuckDBException as error ->
            return Error(Failed("graph_generation_unavailable", error))
    }
    let project (sample: IndexState.Observation) =
        let projected =
            lock projectionGate (fun () ->
                projection.Run(fun () ->
                    observed.Value <- sample.Freshness
                    sampled.Value <- Some sample
                    frame.Value))
        match projected with
        | Ok ready -> ready
        | Error message -> invalidOp message
    // A monotonic elapsed time works with both the production clock and injected SQLite clocks.
    let remaining (batch: IndexState.ObservationBatch) =
        batch.Current.Deadline
        |> Option.map (fun deadline -> deadline - batch.Current.CheckedAt - Stopwatch.GetElapsedTime(batch.SampledAt).TotalSeconds)

    // Admission fails loud and synchronously; a closing service never admits.
    let admitObserver () =
        if isClosing () then raise (EnglishError("retrieval_stopping", 503, false))
        if not (observers.Wait 0) then
            if isClosing () then raise (EnglishError("retrieval_stopping", 503, false))
            raise (EnglishError("retrieval_observers_saturated", 429, true))
        if isClosing () then
            observers.Release() |> ignore
            raise (EnglishError("retrieval_stopping", 503, false))
        enterObserver ()
    let releaseObserver () =
        leaveObserver ()
        observers.Release() |> ignore
    // One producer reads IndexState for every observer and fans transitions out
    // to bounded per-observer queues; a slow client never stalls it, and an
    // overflowing queue evicts its observer instead of dropping a transition.
    let registryGate = obj ()
    let registry = Dictionary<int64, Observer>()
    let mutable registered = 0L
    let mutable current : (RetrievalReadiness * IndexState.ObservationBatch) option = None
    let mutable producer : Task option = None
    let mutable membership = signal ()
    let membershipChanged () =
        let previous = membership
        membership <- signal ()
        previous.TrySetResult() |> ignore
    let finish (observer: Observer) terminal = observer.Terminal.TrySetResult terminal |> ignore
    let failAll terminal =
        lock registryGate (fun () ->
            for observer in registry.Values do finish observer terminal
            registry.Clear()
            current <- None
            membershipChanged ())
    let publish (batch: IndexState.ObservationBatch) (history: ProjectedHistory) ready =
        let mutable accepted = false
        ctx.WhileWatchHealthy(fun () ->
            lock registryGate (fun () ->
                if not (isClosing ()) && (remaining batch |> Option.forall (fun seconds -> seconds >= 0.0)) then
                    let lagged = ResizeArray<int64>()
                    match history with
                    | JournalTruncated ->
                        for KeyValue(id, observer) in registry do
                            if observer.HasInitial then
                                finish observer Lagged
                                lagged.Add id
                            elif observer.Queue.Writer.TryWrite ready then
                                observer.HasInitial <- true
                            else
                                finish observer Lagged
                                lagged.Add id
                        for id in lagged do registry.Remove id |> ignore
                    | Replayed frames ->
                        let mutable previous = current |> Option.map fst
                        let broadcast isCurrent frame =
                            let changed = previous <> Some frame
                            if isCurrent || changed then
                                lagged.Clear()
                                for KeyValue(id, observer) in registry do
                                    if (observer.HasInitial && changed) || (isCurrent && not observer.HasInitial) then
                                        if observer.Queue.Writer.TryWrite frame then observer.HasInitial <- true
                                        else
                                            finish observer Lagged
                                            lagged.Add id
                                for id in lagged do registry.Remove id |> ignore
                            previous <- Some frame
                        for frame in frames do broadcast false frame
                        broadcast true ready
                    current <- if registry.Count = 0 then None else Some(ready, { batch with History = IndexState.Transitions [] })
                    if registry.Count = 0 then membershipChanged ()
                    accepted <- true))
        accepted
    let watcherFailed () = ctx.WatchFailure.IsCompletedSuccessfully
    let watchFailureOf (fault: exn) =
        if watcherFailed () then ctx.WatchFailure.Result
        else
            match fault with
            | :? AggregateException as agg when not (isNull agg.InnerException) -> agg.InnerException
            | other -> other

    // The only wakeups are registry activity, a SQLite notification, or a sampled deadline.
    let produce () : Task = task {
        let ct = lifetime.Token
        let mutable cursor = None
        let mutable stalledCheckedAt : float option = None
        while not ct.IsCancellationRequested && not (isClosing ()) && not (watcherFailed ()) do
            let active, members = lock registryGate (fun () -> registry.Count > 0, membership.Task)
            if active then
                let mutable changed : Task<unit> option = None
                let mutable watchError : exn option = None
                try
                    let next = ctx.WatchReadiness()
                    changed <- Some next
                    if next.IsCompleted then do! next
                with
                | :? OperationCanceledException when isClosing () -> ()
                | error -> watchError <- Some error
                match watchError, changed with
                | Some error, _ ->
                    failAll (Failed("publication_notifications_failed", watchFailureOf error))
                    cursor <- None
                    stalledCheckedAt <- None
                | None, Some next when not (watcherFailed ()) && not (isClosing ()) ->
                    try
                        match! sampleOnce cursor ct with
                        | Error terminal ->
                            failAll terminal
                            cursor <- None
                            stalledCheckedAt <- None
                        | Ok batch when not (watcherFailed ()) && not (isClosing ()) ->
                            let history =
                                match batch.History with
                                | IndexState.Transitions items -> Replayed (List.map project items)
                                | IndexState.Truncated -> JournalTruncated
                            let ready = project batch.Current
                            if publish batch history ready then
                                cursor <- Some batch.Sequence
                                stalledCheckedAt <- None
                                use pause = CancellationTokenSource.CreateLinkedTokenSource ct
                                let delay =
                                    match remaining batch with
                                    | Some seconds -> TimeSpan.FromMilliseconds(Math.Min(Math.Ceiling(max seconds 0.0 * 1000.0) + 1.0, 86_400_000.0))
                                    | None -> Timeout.InfiniteTimeSpan
                                try
                                    let expiry = Task.Delay(delay, pause.Token)
                                    let! _ = Task.WhenAny(next :> Task, expiry, members :> Task, ctx.WatchFailure :> Task, stopping.Task :> Task)
                                    ()
                                finally pause.Cancel()
                            elif not (isClosing ()) && not (watcherFailed ()) then
                                let immediateExpiry =
                                    batch.Current.Deadline |> Option.exists (fun deadline -> deadline <= batch.Current.CheckedAt)
                                let frozen = stalledCheckedAt = Some batch.Current.CheckedAt
                                stalledCheckedAt <- Some batch.Current.CheckedAt
                                if frozen || immediateExpiry then
                                    use pause = CancellationTokenSource.CreateLinkedTokenSource ct
                                    let delay = if frozen then Timeout.InfiniteTimeSpan else TimeSpan.FromMilliseconds 15.0
                                    try
                                        let guard = Task.Delay(delay, pause.Token)
                                        let! _ = Task.WhenAny(next :> Task, guard, members :> Task, ctx.WatchFailure :> Task, stopping.Task :> Task)
                                        if next.IsCompleted || members.IsCompleted then
                                            stalledCheckedAt <- None
                                    finally pause.Cancel()
                        | Ok _ -> ()
                    with
                    | :? OperationCanceledException when isClosing () -> ()
                    | error ->
                        if watcherFailed () then failAll (Failed("publication_notifications_failed", ctx.WatchFailure.Result))
                        else failAll (Failed("observation_failed", error))
                        cursor <- None
                        stalledCheckedAt <- None
                    if next.IsFaulted then
                        failAll (Failed("publication_notifications_failed", watchFailureOf next.Exception))
                        cursor <- None
                        stalledCheckedAt <- None
                    elif next.IsCanceled && not (isClosing ()) then
                        failAll (Failed("publication_notifications_failed", TaskCanceledException("Publication watch was canceled")))
                        cursor <- None
                        stalledCheckedAt <- None
                | _ -> ()
            else
                cursor <- None
                stalledCheckedAt <- None
                use pause = CancellationTokenSource.CreateLinkedTokenSource ct
                try
                    let idle = members.WaitAsync(pause.Token)
                    let! _ = Task.WhenAny(idle, ctx.WatchFailure :> Task, stopping.Task :> Task)
                    ()
                finally pause.Cancel()
        if watcherFailed () then failAll (Failed("publication_notifications_failed", ctx.WatchFailure.Result))
    }
    let register () =
        let observer =
            { Queue = Channel.CreateBounded<RetrievalReadiness>(BoundedChannelOptions(observerOptions.QueueDepth, SingleReader = true, FullMode = BoundedChannelFullMode.Wait))
              Terminal = TaskCompletionSource<Terminal>(TaskCreationOptions.RunContinuationsAsynchronously)
              HasInitial = false }
        let mutable id = 0L
        ctx.WhileWatchHealthy(fun () ->
            lock registryGate (fun () ->
                if registry.Count = 0 then membershipChanged ()
                registered <- registered + 1L
                id <- registered
                registry.Add(id, observer)
                match current with
                | Some(ready, batch) when remaining batch |> Option.forall (fun seconds -> seconds >= 0.0) ->
                    observer.HasInitial <- observer.Queue.Writer.TryWrite ready
                | _ -> ()
                if producer.IsNone then producer <- Some(Task.Run(Func<Task>(produce)))))
        id, observer
    let unregister id =
        lock registryGate (fun () ->
            if registry.Remove id && registry.Count = 0 then
                current <- None
                membershipChanged ())

    // Eviction, notification failure and shutdown stop delivery. An in-flight Kestrel
    // flush keeps its transport token until the write or the bounded drain expires.
    let drainTime = TimeSpan.FromSeconds 1.0
    let write (observer: Observer) (action: CancellationToken -> Task) (cancellation: CancellationToken) = task {
        cancellation.ThrowIfCancellationRequested()
        use bound = CancellationTokenSource.CreateLinkedTokenSource cancellation
        let writeStarted = Stopwatch.GetTimestamp()
        bound.CancelAfter observerOptions.WriteTimeout
        try
            bound.Token.ThrowIfCancellationRequested()
            let pending = action bound.Token
            if not pending.IsCompleted then
                let! first = Task.WhenAny(pending, observer.Terminal.Task :> Task, stopping.Task :> Task, ctx.WatchFailure :> Task)
                if first <> pending then
                    // Keep the earlier of the original deadline and the drain deadline.
                    let remaining = observerOptions.WriteTimeout - Stopwatch.GetElapsedTime(writeStarted)
                    bound.CancelAfter(max TimeSpan.Zero (min remaining drainTime))
            do! pending
        with :? OperationCanceledException when not cancellation.IsCancellationRequested ->
            raise (EnglishError("observer_write_timeout", 504, true))
    }
    let deliver (observer: Observer) (sink: ObserverSink) (wrote: bool ref) (cancellation: CancellationToken) = task {
        use linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token)
        let ct = linked.Token
        let mutable lastWrite = Stopwatch.GetTimestamp()
        while not observer.Terminal.Task.IsCompleted && not (watcherFailed ()) && not (isClosing ()) do
            ct.ThrowIfCancellationRequested()
            match observer.Queue.Reader.TryRead() with
            | true, ready ->
                do! write observer (sink.Emit ready) cancellation
                wrote.Value <- true
                lastWrite <- Stopwatch.GetTimestamp()
            | _ ->
                let due = observerOptions.KeepaliveInterval - Stopwatch.GetElapsedTime lastWrite
                if wrote.Value && due <= TimeSpan.Zero then
                    do! write observer sink.Keepalive cancellation
                    lastWrite <- Stopwatch.GetTimestamp()
                else
                    use pause = CancellationTokenSource.CreateLinkedTokenSource ct
                    try
                        let keepalive = Task.Delay((if wrote.Value then due else Timeout.InfiniteTimeSpan), pause.Token)
                        let readable = observer.Queue.Reader.WaitToReadAsync(pause.Token).AsTask()
                        let! _ = Task.WhenAny(readable :> Task, observer.Terminal.Task :> Task, keepalive, stopping.Task :> Task, ctx.WatchFailure :> Task)
                        ()
                    finally pause.Cancel()
    }
    let terminal (sink: ObserverSink) ready cancellation = task {
        use bound = CancellationTokenSource.CreateLinkedTokenSource(cancellation: CancellationToken)
        bound.CancelAfter drainTime
        try
            bound.Token.ThrowIfCancellationRequested()
            do! sink.Emit ready bound.Token
        // Transport teardown only; a programming error (invalidOp included) propagates.
        with
        | :? OperationCanceledException
        | :? IOException
        | :? ObjectDisposedException -> ()
    }
    let conclude (sink: ObserverSink) (observer: Observer) wrote cancellation (writeError: exn option) = task {
        let reason =
            if watcherFailed () then Some(Failed("publication_notifications_failed", ctx.WatchFailure.Result))
            elif observer.Terminal.Task.IsCompletedSuccessfully then Some observer.Terminal.Task.Result
            else None
        match reason, writeError with
        | Some(Failed(code, error)), _ ->
            if writeError.IsNone then do! terminal sink (Unavailable code) cancellation
            ExceptionDispatchInfo.Capture(error).Throw()
        | _, Some error -> ExceptionDispatchInfo.Capture(error).Throw()
        | Some Lagged, None -> do! terminal sink (Unavailable "observer_lagged") cancellation
        | None, None when isClosing () ->
            if not wrote then raise (EnglishError("retrieval_stopping", 503, false))
            do! terminal sink (Unavailable "retrieval_stopping") cancellation
        | None, None -> ()
    }
    let manifest = projection.Run(fun () -> createMemo(fun _ ->
        Obj [ "repositories", Arr [ for source in ctx.Sources -> Obj [ "repo", Str source.Repo; "ref", Str source.Ref; "policy", Coordinator.policyJ source.Normalized ] ]
              "configuration_sha256", Str (Coordinator.configIdentity ctx.Sources)
              "materials", Str "Latest pushed file/chunk text, capabilities and recorded graph relationships, with source revision/path/lines. No working-tree state or proof of runtime semantics."
              "scope", Str "Configured pushed heads and indexed paths/layers. Direct reads require an explicit exception for working-tree changes, repositories outside this corpus, or materials excluded by its policy. Empty matches, pagination, truncation and response omissions do not change coverage."
              "routing", Str "Use text retrieval for covered evidence. Direct repository reads are allowed for sources or materials outside this remit; queueing, indexing, timeout or transport failure never changes coverage."
              "wait_budget_seconds", Int 240L
              "waiting", Str "Keep the original request pending during model contention and snapshot publication. Do not resubmit or fall back to repository scraping while waiting." ]))

    let close = lazy (task {
        do! cancelLifetime.Value
        // Join admitted HTTP callers and observers before closing their demand host.
        // Waiters outside admission are canceled by the same service lifetime.
        for _ in 1..64 do do! admission.WaitAsync()
        for _ in 1..observerOptions.Capacity do do! observers.WaitAsync()
        // The producer exits on the same lifetime; join it before the projection goes.
        match lock registryGate (fun () -> producer) with
        | Some running -> do! running
        | None -> ()
        try
            let! closed = ClrInterop.toTask CancellationToken.None (AsyncMailbox.close handle)
            require closed |> ignore
            lock projectionGate (fun () -> (projection :> IDisposable).Dispose())
        finally lifetime.Dispose()
    })

    member _.Remit() : J = lock projectionGate (fun () -> projection.Run(fun () -> manifest.Value))
    member _.Snapshot() = AsyncMailbox.snapshot handle
    member _.Consumers = Volatile.Read(&consumers)

    member _.Ask(question: Question, cancellation: CancellationToken) = task {
        if isNull question.Text || question.Text.Trim().Length < 1 || question.Text.Trim().Length > 2000
           || question.Limit < 1L || question.Limit > 20L || question.MaxChars < 4L || question.MaxChars > 4000L then
            raise (QueryError "Invalid text retrieval request")
        if isClosing () then raise (EnglishError("retrieval_stopping", 503, false))
        try
            use budget = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token)
            budget.CancelAfter(TimeSpan.FromSeconds 240.0)
            let ct = budget.Token
            do! admission.WaitAsync ct
            try
                if isClosing () then raise (EnglishError("retrieval_stopping", 503, false))
                let mutable answer = None
                while answer.IsNone do
                    ct.ThrowIfCancellationRequested()
                    // Subscribe before reading the durable publication fence: a
                    // publication during that read must not leave this caller asleep.
                    let changed = ctx.WatchReadiness()
                    if changed.IsCompleted then do! changed.WaitAsync ct
                    let! _, snapshot = readiness ct
                    match snapshot with
                    | None -> do! changed.WaitAsync ct
                    | Some snapshot ->
                        match question.Snapshot with Some pinned when pinned <> snapshot -> raise (EnglishError("snapshot_changed", 409, true)) | _ -> ()
                        do! gate.WaitAsync ct
                        let mutable bound = None
                        let mutable capacity = Task.FromResult(())
                        try
                            if isClosing () then raise (EnglishError("retrieval_stopping", 503, false))
                            if allocated >= 128 && consumers = 0 then
                                let! closed = ClrInterop.toTask CancellationToken.None (AsyncMailbox.close handle)
                                require closed |> ignore
                                jobs.Clear(); inputs.Clear(); outputs.Clear(); allocated <- 0
                                handle <- create ()
                            let key = question, snapshot
                            if jobs.ContainsKey key || allocated < 128 then
                                let h = handle
                                let job =
                                    match jobs.TryGetValue key with
                                    | true, existing -> existing
                                    | _ ->
                                        let job = { Id = next (); Question = question; Snapshot = snapshot; Consumers = 0 }
                                        inputs.[ValueToken job.Id] <- job
                                        jobs.Add(key, job); allocated <- allocated + 1
                                        job
                                if job.Consumers = 0 && (AsyncMailbox.tryResult (WorkId job.Id) h).IsNone then
                                    do! send h (Action.SetInputs [{ Input = InputId job.Id; Stamp = InputStamp 1UL; Value = ValueToken job.Id }])
                                    do! send h (Action.ReserveScope(ScopeId job.Id, RevisionId 1UL))
                                    do! send h (Action.ReplaceScope(ScopeId job.Id, RevisionId 1UL, [ScopeEntry.Define {
                                        Work = WorkId job.Id; Stamp = DefinitionStamp 1UL
                                        Reads = [{ Slot = ReadSlotId 1UL; Source = ReadSource.Input(InputId job.Id) }] }]))
                                let demand = DemandId(next ())
                                do! send h (Action.Demand(demand, WorkId job.Id))
                                job.Consumers <- job.Consumers + 1; consumers <- consumers + 1
                                bound <- Some(h, job, demand)
                            else capacity <- epochReleased.Task
                        finally gate.Release() |> ignore
                        match bound with
                        | None -> do! capacity.WaitAsync ct
                        | Some(h, job, demand) ->
                            // Async disposal releases this demand even when its observer
                            // is canceled. The host still owns physical attempt cleanup.
                            use release = { new IAsyncDisposable with
                                member _.DisposeAsync() = ValueTask(task {
                                    do! gate.WaitAsync()
                                    try
                                        do! send h (Action.Release demand)
                                        job.Consumers <- job.Consumers - 1; consumers <- consumers - 1
                                        let failed = match outputs.TryGetValue(ValueToken job.Id) with true, Error _ -> true | _ -> false
                                        if job.Consumers = 0 && (failed || (AsyncMailbox.tryResult (WorkId job.Id) h).IsNone) then jobs.Remove((question, snapshot)) |> ignore
                                        if consumers = 0 then
                                            let released = epochReleased
                                            epochReleased <- signal ()
                                            released.TrySetResult() |> ignore
                                    finally gate.Release() |> ignore
                                }) }
                            let mutable result = None
                            while result.IsNone do
                                let _, changed = AsyncMailbox.watch h
                                match AsyncMailbox.tryResult (WorkId job.Id) h with
                                | Some offered when AsyncMailbox.isEligible offered h -> result <- Some outputs.[ValueToken job.Id]
                                | _ -> do! ClrInterop.toTask ct changed
                            let! after, published = readiness ct
                            // Eligibility is necessary, but IndexState alone authorizes
                            // current publication. A raced update rebinds this same caller.
                            if published = Some snapshot then
                                answer <- Some(match result.Value with
                                               | Ok(Obj fields) -> Obj ((fields |> List.filter (fun (k, _) -> k <> "freshness")) @ [ "freshness", after ])
                                               | Ok value -> value
                                               | Error error -> raise error)
                return answer.Value
            finally admission.Release() |> ignore
        with
        | :? OperationCanceledException when isClosing () -> return raise (EnglishError("retrieval_stopping", 503, false))
        | :? ObjectDisposedException when isClosing () -> return raise (EnglishError("retrieval_stopping", 503, false))
        | :? OperationCanceledException when not cancellation.IsCancellationRequested ->
            return raise (EnglishError("retrieval_wait_budget_exceeded", 504, false))
    }

    member _.Observers = Volatile.Read(&observing)
    member _.ObserverCapacity = observerOptions.Capacity

    // One ordered stream per admitted observer: the producer's current frame,
    // then every transition, until the client leaves, the service stops, the
    // queue overflows or the publication watch fails.
    member _.Observe(sink: ObserverSink, cancellation: CancellationToken) : Task = task {
        admitObserver ()
        try
            let id, observer = register ()
            try
                let wrote = ref false
                let! writeError = task {
                    try
                        do! deliver observer sink wrote cancellation
                        return None
                    with
                    | :? OperationCanceledException when isClosing () && not cancellation.IsCancellationRequested -> return None
                    | error -> return Some error }
                do! conclude sink observer wrote.Value cancellation writeError
            finally unregister id
        finally releaseObserver ()
    }

    member _.BeginStop() = beginStop ()
    member _.Close() =
        beginStop ()
        close.Value

    new(ctx: Ctx) = Service(ctx, defaultObserverOptions)
