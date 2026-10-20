/// Demand and lifetime ownership for text retrieval. The index owns freshness;
/// Incremental owns ready/running work. HTTP callers own only their demands.
module Retrieval.Native.Requests

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting
open Ranvier
open Retrieval.Native.Canonical
open Retrieval.Native.Query
open Retrieval.Native.English

[<StructuralEquality; StructuralComparison>]
type Question =
    { Text: string; Snapshot: string option; Limit: int64; MaxChars: int64
      IncludeContext: bool; IncludeQuery: bool; IncludeResults: bool }

type Ctx =
    { Readiness: unit -> J
      Evaluate: Question -> string -> CancellationToken -> Task<J>
      Sources: Coordinator.Source list }

type private Job =
    { Id: uint64; Question: Question; Snapshot: string; mutable Consumers: int }

/// One bounded epoch retains at most 128 exact answers. Rotation joins the old
/// host before discarding its payloads, including canceled attempts and tombstones.
/// No caller can cancel another caller's demand or occupy the GPU while indexing.
type Service(ctx: Ctx) =
    let gate = new SemaphoreSlim(1, 1)
    let admission = new SemaphoreSlim(64, 64)
    let lifetime = new CancellationTokenSource()
    let jobs = Dictionary<Question * string, Job>()
    let inputs = ConcurrentDictionary<ValueToken, Job>()
    let outputs = ConcurrentDictionary<ValueToken, Result<J, exn>>()
    let mutable serial, epoch, allocated, consumers = 0UL, 0UL, 0, 0
    let mutable closing = false
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
    let readiness () =
        let current = try ctx.Readiness() with QueryBudgetError _ -> Obj [ "fresh", Bool false ]
        lock projectionGate (fun () -> projection.Run(fun () -> observed.Value <- current; current, published.Value))
    let manifest = projection.Run(fun () -> createMemo(fun _ ->
        Obj [ "repositories", Arr [ for source in ctx.Sources -> Obj [ "repo", Str source.Repo; "ref", Str source.Ref; "policy", Coordinator.policyJ source.Normalized ] ]
              "configuration_sha256", Str (Coordinator.configIdentity ctx.Sources)
              "materials", Str "Latest pushed file/chunk text, capabilities and recorded graph relationships, with source revision/path/lines. No working-tree state or proof of runtime semantics."
              "scope", Str "Configured pushed heads and indexed paths/layers. Direct reads require an explicit exception for working-tree changes, repositories outside this corpus, or materials excluded by its policy. Empty matches, pagination, truncation and response omissions do not change coverage."
              "routing", Str "Use text retrieval for covered evidence. Direct repository reads are allowed for sources or materials outside this remit; queueing, indexing, timeout or transport failure never changes coverage."
              "wait_budget_seconds", Int 240L
              "waiting", Str "Keep the original request pending during model contention and snapshot publication. Do not resubmit or fall back to repository scraping while waiting." ]))

    let close = lazy (task {
        do! gate.WaitAsync()
        try closing <- true
        finally gate.Release() |> ignore
        lifetime.Cancel()
        // Join admitted HTTP observers before closing their demand host. Waiters
        // outside admission are canceled by the same service lifetime.
        for _ in 1..64 do do! admission.WaitAsync()
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
        use budget = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token)
        budget.CancelAfter(TimeSpan.FromSeconds 240.0)
        let ct = budget.Token
        try
            do! admission.WaitAsync ct
            try
                let mutable answer = None
                while answer.IsNone do
                    ct.ThrowIfCancellationRequested()
                    let _, snapshot = readiness ()
                    match snapshot with
                    | None -> do! Task.Delay(250, ct)
                    | Some snapshot ->
                        match question.Snapshot with Some pinned when pinned <> snapshot -> raise (EnglishError("snapshot_changed", 409, true)) | _ -> ()
                        do! gate.WaitAsync ct
                        let mutable bound = None
                        try
                            if closing then invalidOp "Retrieval is stopping"
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
                        finally gate.Release() |> ignore
                        match bound with
                        | None -> do! Task.Delay(250, ct)
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
                                    finally gate.Release() |> ignore
                                }) }
                            let mutable result = None
                            while result.IsNone do
                                let _, changed = AsyncMailbox.watch h
                                match AsyncMailbox.tryResult (WorkId job.Id) h with
                                | Some offered when AsyncMailbox.isEligible offered h -> result <- Some outputs.[ValueToken job.Id]
                                | _ -> do! ClrInterop.toTask ct changed
                            let after, published = readiness ()
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
        | :? OperationCanceledException when lifetime.IsCancellationRequested -> return raise (EnglishError("retrieval_stopping", 503, false))
        | :? OperationCanceledException when not cancellation.IsCancellationRequested ->
            return raise (EnglishError("retrieval_wait_budget_exceeded", 504, false))
    }

    member _.Close() = close.Value
