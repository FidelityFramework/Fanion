/// The indexer: authoritative observation, mirrors, settle window, in-process
/// extraction and generation, fenced publication. Replaces retrieval.pipeline.
module Fanion.Coordinator

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.IO
open System.Runtime.InteropServices
open System.Security.Cryptography
open System.Text
open Fidelity.Data.JSON
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting
open Fanion.Canonical
open Fanion.IndexState

exception PipelineStopped of string

// Linux host adapter: the generation filename must be durable before SQLite
// can publish it. Corpus and publication semantics remain in their existing owners.
[<DllImport("libc", EntryPoint = "open", SetLastError = true)>]
extern int private openDirectory(string path, int flags)
[<DllImport("libc", EntryPoint = "fsync", SetLastError = true)>]
extern int private syncDirectory(int descriptor)
[<DllImport("libc", EntryPoint = "close")>]
extern int private closeDirectory(int descriptor)

let private persistDirectory path =
    let descriptor = openDirectory(path, 0x10000 ||| 0x80000) // O_DIRECTORY | O_CLOEXEC, read-only
    if descriptor < 0 then raise (IOException("Cannot open generation directory for durability: " + string (Marshal.GetLastPInvokeError())))
    try
        if syncDirectory descriptor <> 0 then raise (IOException("Cannot persist generation directory: " + string (Marshal.GetLastPInvokeError())))
    finally
        closeDirectory descriptor |> ignore

type Source =
    { Id: string; Repo: string; Ref: string; DependsOn: string list; LayerRules: JsonValue option; Policy: JsonValue option
      Normalized: Policy.Normalized; Raw: JsonValue }

let private idPattern = Regex(@"\A[a-z][a-z0-9_\-]{0,47}\z", RegexOptions.Compiled)
let private repoPattern = Regex(@"\AFidelityFramework/[A-Za-z0-9_.\-]{1,80}\z", RegexOptions.Compiled)
let private refPattern = Regex(@"\Arefs/heads/[A-Za-z0-9][A-Za-z0-9_./\-]{0,100}\z", RegexOptions.Compiled)
let private revisionPattern = Regex(@"\A[0-9a-f]{40}\z", RegexOptions.Compiled)

/// config_sources: validated, ordered as configured.
let configSources (configPath: string) : Source list * JsonValue =
    let root = File.ReadAllBytes configPath |> JsonInput.parseBytes
    let keys = JsonInput.fields root |> List.map fst |> set
    let versionOne =
        match Json.prop "schema_version" root with
        | Some(JsonValue.NumberLiteral value) -> JsonNumberLiteral.text value = "1"
        | _ -> false
    if keys <> set [ "schema_version"; "sources" ] || not versionOne then raise (ArgumentException "Unsupported corpus configuration")
    let sources =
        match JsonInput.property "sources" root with
        | JsonValue.Array sources when not sources.IsEmpty && sources.Length <= 64 -> sources
        | _ -> raise (ArgumentException "Configure 1\u201364 explicit sources")
    let seen = HashSet<string>()
    let result =
        [ for source in sources do
            let fields = JsonInput.fields source |> List.map fst |> set
            if not (Set.isSubset (set [ "id"; "repo"; "ref"; "depends_on" ]) fields) || not (Set.isSubset fields (set [ "id"; "repo"; "ref"; "depends_on"; "layer_rules"; "policy" ])) then raise (ArgumentException "Invalid source fields")
            let id = JsonInput.property "id" source |> JsonInput.string
            if isNull id || not (idPattern.IsMatch id) || not (seen.Add id) then raise (ArgumentException "Invalid or repeated source ID")
            let repo = JsonInput.property "repo" source |> JsonInput.string
            if isNull repo || not (repoPattern.IsMatch repo) then raise (ArgumentException "Only explicit FidelityFramework repositories are accepted")
            let reference = JsonInput.property "ref" source |> JsonInput.string
            if isNull reference || not (refPattern.IsMatch reference) || reference.Contains ".." || reference.EndsWith("/", StringComparison.Ordinal) then raise (ArgumentException "A canonical explicit branch ref is required")
            let policy =
                match Json.prop "policy" source with
                | Some JsonValue.Null -> raise (ArgumentException "Repository policy must be an object")
                | policy -> policy
            let normalized = try Policy.normalize policy with Snapshot.RelationshipError m -> raise (ArgumentException m)
            yield { Id = id; Repo = repo; Ref = reference; DependsOn = JsonInput.property "depends_on" source |> JsonInput.items |> List.map JsonInput.string
                    LayerRules = Json.prop "layer_rules" source; Policy = policy; Normalized = normalized; Raw = source } ]
    if (result |> List.map (fun s -> s.Repo.ToLowerInvariant()) |> List.distinct).Length <> result.Length then raise (ArgumentException "First slice supports one tracked ref per repository")
    result, root

let policyJ (p: Policy.Normalized) : J =
    Obj [ "role", Str p.Role; "authority", Int p.Authority; "capabilities", Arr (p.Capabilities |> List.map Str)
          "include_generated", Arr (p.IncludeGenerated |> List.map Str); "include_paths", Arr (p.IncludePaths |> List.map Str)
          "exclude_paths", Arr (p.ExcludePaths |> List.map Str) ]

let private defaultExtractLimits : Extract.Limits =
    { MaxRepositories = 64; MaxTreeEntries = 100_000L; MaxTreeBytes = 16 * 1024 * 1024; MaxFiles = 20_000; MaxFileBytes = 1024 * 1024
      MaxSourceBytes = 128L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxOutputBytes = 384L * 1024L * 1024L
      MaxLayerRules = 64; MaxRulePatterns = 32; MaxTotalRulePatterns = 256; MaxLinksPerFile = 1024; MaxBuildSeconds = 600.0; GitTimeoutSeconds = 15.0 }

/// canonical_config_identity: identical bytes to the Python coordinator (the gateway compares it).
let configIdentity (sources: Source list) : string =
    let normalized =
        [ for source in sources |> List.sortWith (fun a b -> String.CompareOrdinal(a.Id, b.Id)) ->
            let rules = Extract.layerRules source.LayerRules defaultExtractLimits |> List.filter (fun r -> r.Origin = "manifest")
            Obj [ "id", Str source.Id; "repo", Str source.Repo; "ref", Str source.Ref
                  "depends_on", Arr (source.DependsOn |> List.sortWith (fun a b -> String.CompareOrdinal(a, b)) |> List.map Str)
                  "layer_rules", Arr [ for r in rules -> Obj [ "name", Str r.Name; "patterns", Arr (r.Patterns |> List.map Str) ] ]
                  "policy", policyJ source.Normalized ] ]
    fst (digest canonicalStyle (Obj [ "version", Int 2L; "sources", Arr normalized ]))

/// Extraction fingerprint: the configured sources plus the toolchain binary and engine locks.
let extractionFingerprint (sources: Source list) (lockFiles: string list) : string =
    use hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256)
    let configured = Obj [ for s in sources -> s.Id, Raw s.Raw ]
    hash.AppendData(Encoding.UTF8.GetBytes(toText canonicalStyle configured))
    hash.AppendData(File.ReadAllBytes(Environment.ProcessPath))
    for file in lockFiles do hash.AppendData(File.ReadAllBytes file)
    Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant()

let private atomicJson (path: string) (value: J) =
    let temporary = Path.Combine(Path.GetDirectoryName path, "." + Path.GetFileName path + "-" + Guid.NewGuid().ToString("N"))
    (use stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None)
     File.SetUnixFileMode(temporary, UnixFileMode.UserRead ||| UnixFileMode.UserWrite)
     let bytes = Encoding.UTF8.GetBytes(toText canonicalStyle value + "\n")
     stream.Write(bytes, 0, bytes.Length)
     stream.Flush(true))
    File.Move(temporary, path, true)

type Options =
    { Root: string; Config: string; Engine: Load.Engine; Budgets: Generation.Budgets; ExtractLimits: Extract.Limits
      RemoteBase: string; LeaseSeconds: float; SettleSeconds: float option; MaxSettleSeconds: float option; LockFiles: string list }

type Coordinator(options: Options, stop: CancellationToken, ?maxObservationAge: float) =
    let root = Path.GetFullPath options.Root
    let sources, _ = configSources options.Config
    let byId = sources |> List.map (fun s -> s.Id, s) |> Map.ofList
    let identity = configIdentity sources
    let fingerprint = extractionFingerprint sources options.LockFiles
    do for directory in [ "mirrors"; "components"; "generations" ] do Directory.CreateDirectory(Path.Combine(root, directory)) |> ignore
    let state =
        State(Path.Combine(root, "index-state.sqlite"), [ for s in sources -> s.Id, s.DependsOn ], leaseSeconds = options.LeaseSeconds,
              ?settleSeconds = options.SettleSeconds, ?maxSettleSeconds = options.MaxSettleSeconds, ?maxObservationAge = maxObservationAge)
    do
        let marker = Path.Combine(root, "extractor-fingerprint.json")
        let previous = if File.Exists marker then Some(File.ReadAllText marker) else None
        let expected = toText canonicalStyle (Obj [ "sha256", Str fingerprint ]) + "\n"
        if previous <> Some expected then
            state.InvalidateSources([ for s in sources -> s.Id ], "Extractor or corpus configuration changed") |> ignore
            atomicJson marker (Obj [ "sha256", Str fingerprint ])
    let observationLock = obj ()
    let checkpoint () = if stop.IsCancellationRequested then raise (PipelineStopped "Indexer stopping; no further generation will be published")
    let remoteUrl (source: Source) =
        if options.RemoteBase.StartsWith("/", StringComparison.Ordinal) then Path.Combine(options.RemoteBase, source.Repo)
        else options.RemoteBase.TrimEnd('/') + "/" + source.Repo + ".git"
    let localRemote = options.RemoteBase.StartsWith("/", StringComparison.Ordinal)
    let fileProtocol = if localRemote then [ "-c"; "protocol.file.allow=always" ] else []

    member _.State = state
    member _.Identity = identity
    member _.Fingerprint = fingerprint
    member _.Sources = sources

    /// Authoritative head of the tracked ref, None when the ref is absent.
    member _.RemoteHead(source: Source) : string option =
        let output = Encoding.UTF8.GetString(Git.runNetwork null (fileProtocol @ [ "ls-remote"; "--refs"; remoteUrl source; source.Ref ]) 65536 20.0)
        let lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
        if lines.Length = 0 then None
        elif lines.Length <> 1 then raise (InvalidOperationException "Authoritative ref query was ambiguous")
        else
            let parts = lines.[0].Split('\t')
            if parts.Length <> 2 || parts.[1] <> source.Ref || not (revisionPattern.IsMatch parts.[0]) then raise (InvalidOperationException "Unexpected authoritative Git ref")
            Some parts.[0]

    /// Observe every source concurrently; an outage is a reconcile hint, never a deletion.
    member this.Observe(?sourceIds: string list) : J list =
        checkpoint ()
        lock observationLock (fun () ->
            checkpoint ()
            let errors = ConcurrentBag<J>()
            let observed = match sourceIds with None -> sources | Some ids -> ids |> List.map (fun id -> byId.[id])
            Parallel.ForEach(observed, ParallelOptions(MaxDegreeOfParallelism = 6), fun (source: Source) ->
                if not stop.IsCancellationRequested then
                    let started = state.Now()
                    try
                        let revision = this.RemoteHead source
                        state.ObserveHeads([ source.Id, revision ], started) |> ignore
                    with
                    | PipelineStopped _ -> ()
                    | error ->
                        errors.Add(Obj [ "source_id", Str source.Id; "error_type", Str (error.GetType().Name) ])
                        state.EventHint("observation-failed:" + source.Id + ":" + Guid.NewGuid().ToString("N"), source.Id, "reconcile") |> ignore) |> ignore
            checkpoint ()
            let list = List.ofSeq errors
            atomicJson (Path.Combine(root, "last-observation.json")) (Obj [ "time", Float (state.Now()); "errors", Arr list ])
            list)

    member _.Fetch(source: Source, revision: string) : string =
        let mirror = Path.Combine(root, "mirrors", source.Id + ".git")
        if not (Directory.Exists mirror) then Git.runNetwork null [ "init"; "--bare"; mirror ] 65536 90.0 |> ignore
        Git.runNetwork mirror (fileProtocol @ [ "fetch"; "--no-tags"; "--depth=1"; remoteUrl source; revision ]) 65536 180.0 |> ignore
        mirror

    member _.ComponentPath(sourceId: string, revision: string) =
        let key = fst (digest canonicalStyle (Arr [ Str sourceId; Str revision; Str fingerprint ]))
        Path.Combine(root, "components", key + ".json")

    /// Fetch changed mirrors concurrently, then extract each component in-process.
    member this.Extract(targets: (Source * string * string) list) =
        let clock = Diagnostics.Stopwatch.StartNew()
        let mutable fetched = 0.0
        if not targets.IsEmpty then
            let mirrors = ConcurrentDictionary<string, string>()
            Parallel.ForEach(targets, ParallelOptions(MaxDegreeOfParallelism = 4), fun (source: Source, revision: string, _) ->
                mirrors.[source.Id] <- this.Fetch(source, revision)) |> ignore
            checkpoint ()
            fetched <- clock.Elapsed.TotalSeconds
            Parallel.ForEach(targets, fun (source: Source, revision: string, path: string) ->
                let fields : (string * J) list =
                    [ ("path", Str mirrors.[source.Id]); ("name", Str source.Repo); ("commit", Str revision); ("ref", Str source.Ref)
                      ("source_url", Str ("https://forge.spkez.dev/" + source.Repo)) ]
                    @ (match source.LayerRules with Some r -> [ ("layer_rules", Raw r) ] | None -> [])
                    @ (match source.Policy with Some p -> [ ("policy", Raw p) ] | None -> [])
                let manifest = Obj [ ("repositories", Arr [ Obj fields ]) ]
                let manifest = toText canonicalStyle manifest |> JsonInput.parse
                let _, content = Extract.build manifest options.ExtractLimits
                Extract.writeAtomic path content) |> ignore
            checkpoint ()
        fetched, clock.Elapsed.TotalSeconds - fetched

    /// Open the staged generation and exercise sources and a PGQ hop once.
    member _.Verify(database: string, snapshotId: string) =
        use connection = Load.openEngine options.Engine database
        let scalar (sql: string) = (use c = connection.CreateCommand() in c.CommandText <- sql; c.ExecuteScalar())
        let metadata = string (scalar "SELECT value FROM metadata")
        let metadata = JsonInput.parse metadata
        if (JsonInput.property "snapshot_id" metadata |> JsonInput.string) <> snapshotId then raise (InvalidOperationException "Staged generation identity differs")
        Load.verifyGenerationEngine options.Engine metadata
        match scalar "SELECT id FROM nodes WHERE kind='file' ORDER BY id LIMIT 1" with
        | :? string as seed ->
            scalar (sprintf "SELECT length(text) FROM nodes WHERE id='%s'" seed) |> ignore
            use c = connection.CreateCommand()
            c.CommandText <- sprintf "FROM GRAPH_TABLE(corpus MATCH (a:resource WHERE a.id = '%s')-[e:relation]->(b:resource) COLUMNS (b.id AS id, b.path AS path, b.kind AS kind, e.kind AS edge_kind)) ORDER BY ALL LIMIT 2" seed
            use reader = c.ExecuteReader()
            while reader.Read() do ()
        | _ -> ()

    /// One build pass; `force` skips the settle window (operator --once builds).
    member this.RunOnce(force: bool) : J =
        checkpoint ()
        let plan = state.PendingPlan()
        if not plan.ObservationRequired.IsEmpty then Obj [ "status", Str "observation_required" ]
        elif plan.Targets.IsEmpty then Obj [ "status", Str "idle" ]
        else
        let remaining = match plan.SettleUntil with Some until when not force && plan.BaseGeneration.IsSome -> until - state.Now() | _ -> 0.0
        if remaining > 0.0 then
            Obj [ "status", Str "settling"; "settle_seconds_remaining", Float (Math.Round(remaining, 3)); "pending_sources", Arr [ for s in plan.Targets.Keys -> Str s ] ]
        else
        let startedAt = DateTimeOffset.UtcNow
        let clock = Diagnostics.Stopwatch.StartNew()
        let timeline = ResizeArray<string * float * float>()
        let mutable boundary = 0.0
        let mark name =
            let now = clock.Elapsed.TotalSeconds
            timeline.Add(name, boundary, now)
            boundary <- now
        let token = state.StartBuild plan
        let reused, rebuilt = ResizeArray<string>(), ResizeArray<string>()
        let temporary = Path.Combine(root, "generations", token + ".building.duckdb")
        try
            try
                let components = ResizeArray<Generation.ComponentSpec>()
                let pending = ResizeArray<Source * string * string>()
                let changed = Set.ofList plan.ChangedSources
                for kv in plan.DesiredRevisions do
                    checkpoint ()
                    match kv.Value with
                    | None -> ()
                    | Some revision ->
                        let source = byId.[kv.Key]
                        let path = this.ComponentPath(kv.Key, revision)
                        // Dependency targets need fresh cross-links, not another
                        // extraction of an unchanged, fingerprint-bound component.
                        if File.Exists path && not (changed.Contains kv.Key) then reused.Add kv.Key
                        else
                            pending.Add((source, revision, path))
                            rebuilt.Add kv.Key
                        components.Add { Path = path; SourceId = kv.Key; Repo = source.Repo; Commit = revision }
                mark "prepare"
                let fetchSeconds, extractSeconds = this.Extract(List.ofSeq pending)
                mark "fetch_and_extract"
                state.RenewBuild token |> ignore
                let trackedEntry (sourceId: string) (revision: string option) : J =
                    Obj [ ("repo", Str byId.[sourceId].Repo); ("ref", Str byId.[sourceId].Ref); ("revision", (match revision with Some r -> Str r | None -> Null)) ]
                let tracked = Obj [ for kv in plan.DesiredRevisions -> (kv.Key, trackedEntry kv.Key kv.Value) ]
                let report = Generation.generation options.Engine options.Budgets (List.ofSeq components) identity tracked temporary
                mark "generation"
                checkpoint ()
                let target = Path.Combine(root, "generations", report.SnapshotId + ".duckdb")
                let exists = File.Exists target
                let alreadyActive = exists && (state.ActiveGeneration() |> Option.exists (fun current -> current.SnapshotId = report.SnapshotId))
                if not exists then
                    this.Verify(temporary, report.SnapshotId)
                    checkpoint ()
                    (use handle = File.Open(temporary, FileMode.Open, FileAccess.Read) in handle.Flush(true))
                    File.Move(temporary, target)
                // The serving process owns the active database's RW lock. Its
                // engine-bound identity was verified at publication and Store open.
                // A rename preserves the already-verified file. Only a retained
                // non-active target needs another open; never verify new bytes twice.
                if exists && not alreadyActive then this.Verify(target, report.SnapshotId)
                persistDirectory (Path.GetDirectoryName target)
                mark "verify_and_persist"
                if not (this.Observe()).IsEmpty then raise (InvalidOperationException "Authoritative recheck failed; staged generation remains unpublished")
                mark "observe_heads"
                checkpoint ()
                let result = state.CompleteBuild(token, report.SnapshotId, [ for kv in plan.Targets -> kv.Key, kv.Value ], configurationSha256 = identity)
                mark "publish"
                let phases = Obj [ for (name, seconds) in report.Phases -> name, Float (Math.Round(seconds, 3)) ]
                let generationTiming =
                    Obj [ for name, seconds in report.Phases do
                              if name = "relationships" then
                                  yield name, Obj [ "seconds", Float seconds
                                                    "phases", Obj [ for child, elapsed in report.Phases do
                                                                        if child.StartsWith("relationship_", StringComparison.Ordinal) then
                                                                            yield child.Substring(13), Float elapsed ] ]
                              elif not (name.StartsWith("relationship_", StringComparison.Ordinal)) then
                                  yield name, Float seconds ]
                let stages =
                    [ for name, start, finish in timeline ->
                        Obj [ "name", Str name; "start_seconds", Float start; "end_seconds", Float finish; "seconds", Float (finish - start) ] ]
                let timing =
                    Obj [ "started_at", Str (startedAt.ToString("O"))
                          "seconds", Float boundary
                          "stages", Arr stages
                          "nested", Obj [ "fetch_and_extract", Obj [ "fetch", Float fetchSeconds; "extract", Float extractSeconds ]
                                          "generation", generationTiming ] ]
                let result = match result with Obj pairs -> Obj (pairs @ [ "rebuilt", Arr [ for s in rebuilt -> Str s ]; "reused", Arr [ for s in reused -> Str s ]; "phases", phases; "timing", timing ]) | other -> other
                atomicJson (Path.Combine(root, "last-build.json")) result
                result
            finally
                if File.Exists temporary then File.Delete temporary
                if File.Exists(temporary + ".wal") then File.Delete(temporary + ".wal")
        with error ->
            let cancelled = (match error with PipelineStopped _ -> true | _ -> false)
            let kind = if cancelled then "cancelled" else error.GetType().Name
            (try state.FailBuild(token, kind) |> ignore with _ -> ())
            atomicJson (Path.Combine(root, "last-build.json")) (Obj [ "status", Str (if cancelled then "cancelled" else "failed"); "error_type", Str (error.GetType().Name); "rebuilt", Arr [ for s in rebuilt -> Str s ] ])
            reraise ()

/// One Incremental work slot owns effects. Durable state notifications wake the
/// pull loop; timers are deadlines for debounce and missed-hook reconciliation.
let run (coordinator: Coordinator) (changes: Changes) (stop: CancellationToken) = task {
    let require = function Ok value -> value | Error error -> invalidOp (sprintf "Indexer host: %A" error)
    let actions = ConcurrentDictionary<ValueToken, unit -> J>()
    let results = ConcurrentDictionary<ValueToken, Result<J, exn>>()
    let evaluate invocation _ = async {
        match invocation with
        | StepInvocation.Start request ->
            let token = match request.Reads with [{ Value = ReadValue.Input(_, _, value) }] -> value | _ -> invalidOp "One indexing input required"
            results.[token] <- try Ok(actions.[token]()) with error -> Error error
            return StepOutcome.Complete(Completion.Succeeded token)
        | StepInvocation.Resume _ -> return invalidOp "Indexing has no external suspension response"
    }
    let host = AsyncMailbox.create { Epoch = EpochId 1UL; MaxConcurrency = 1; CommandCapacity = 16 } evaluate |> require
    AsyncMailbox.start host |> require
    let send action = task {
        let operation = AsyncMailbox.admit action host |> require
        let! result = ClrInterop.toTask CancellationToken.None (AsyncMailbox.observe operation)
        require result |> ignore
        AsyncMailbox.drainEvents host |> ignore
        AsyncMailbox.drainDiagnostics host |> ignore
    }
    let mutable serial = 0UL
    let execute action = task {
        serial <- serial + 1UL
        let token = ValueToken serial
        actions.[token] <- action
        do! send (Action.SetInputs [{ Input = InputId 1UL; Stamp = InputStamp serial; Value = token }])
        if serial = 1UL then
            do! send (Action.ReserveScope(ScopeId 1UL, RevisionId 1UL))
            do! send (Action.ReplaceScope(ScopeId 1UL, RevisionId 1UL, [ScopeEntry.Define {
                Work = WorkId 1UL; Stamp = DefinitionStamp 1UL
                Reads = [{ Slot = ReadSlotId 1UL; Source = ReadSource.Input(InputId 1UL) }] }]))
            do! send (Action.Demand(DemandId 1UL, WorkId 1UL))
        let mutable completed = false
        while not completed do
            let _, changed = AsyncMailbox.watch host
            match AsyncMailbox.tryResult (WorkId 1UL) host with
            | Some result when AsyncMailbox.isEligible result host && results.ContainsKey token -> completed <- true
            | _ -> do! ClrInterop.toTask stop changed
        let result = results.[token]
        actions.TryRemove token |> ignore
        results.TryRemove token |> ignore
        return match result with Ok value -> value | Error error -> raise error
    }
    let print value = Console.Out.WriteLine(toText canonicalStyle value); Console.Out.Flush()
    let mutable reconcileAt = 0.0
    let mutable failedPlan = None
    try
        try
            while not stop.IsCancellationRequested do
                // Subscribe before reading the only publication authority.
                let changed = changes.Watch()
                let state = coordinator.State
                let now = state.Now()
                let plan = state.PendingPlan()
                let due = now >= reconcileAt
                if due || (not plan.ObservationRequired.IsEmpty && failedPlan <> Some plan.PlanId) then
                    let observe () =
                        let errors = if due then coordinator.Observe() else coordinator.Observe(plan.ObservationRequired)
                        Obj [ "errors", Arr errors ]
                    let! result = execute observe
                    reconcileAt <- if due then state.Now() + 30.0 else reconcileAt
                    match result with
                    | Obj ["errors", Arr []] -> failedPlan <- None
                    | _ -> print result; failedPlan <- Some(state.PendingPlan().PlanId)
                elif not plan.Targets.IsEmpty && failedPlan <> Some plan.PlanId
                     && (plan.BaseGeneration.IsNone || plan.SettleUntil |> Option.forall (fun until -> until <= now)) then
                    try
                        let! result = execute (fun () -> coordinator.RunOnce false)
                        print result
                        failedPlan <- None
                    with
                    | PipelineStopped _ -> stop.ThrowIfCancellationRequested()
                    | error ->
                        print (Obj [ "index_error", Str (error.GetType().Name) ])
                        failedPlan <- Some(state.PendingPlan().PlanId)
                else
                    let deadline =
                        if not plan.Targets.IsEmpty && failedPlan <> Some plan.PlanId then
                            min reconcileAt (defaultArg plan.SettleUntil reconcileAt)
                        else reconcileAt
                    use timer = CancellationTokenSource.CreateLinkedTokenSource stop
                    let delay = Task.Delay(TimeSpan.FromSeconds(max 0.001 (deadline - state.Now())), timer.Token)
                    let! wake = Task.WhenAny(changed :> Task, delay)
                    timer.Cancel()
                    // Surface watcher failure instead of silently reverting to polling.
                    if Object.ReferenceEquals(wake, changed) then do! changed
                    stop.ThrowIfCancellationRequested()
        with :? OperationCanceledException when stop.IsCancellationRequested -> ()
    finally
        // Join the one physical worker before releasing the indexer lock.
        ClrInterop.toTask CancellationToken.None (AsyncMailbox.close host) |> fun closing -> closing.GetAwaiter().GetResult() |> require |> ignore
}

/// Lock the root for the service lifetime; a push wakes the existing owner.
let serve (options: Options) (once: bool) : int =
    let root = Path.GetFullPath options.Root
    Directory.CreateDirectory root |> ignore
    use lockFile = new FileStream(Path.Combine(root, "indexer.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None)
    use stop = new CancellationTokenSource()
    use _term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, fun context -> context.Cancel <- true; stop.Cancel())
    use _int = PosixSignalRegistration.Create(PosixSignal.SIGINT, fun context -> context.Cancel <- true; stop.Cancel())
    let coordinator = Coordinator(options, stop.Token)
    let print (value: J) = Console.Out.WriteLine(toText canonicalStyle value); Console.Out.Flush()
    try
        if once then
            coordinator.Observe() |> ignore
            print (coordinator.RunOnce true)
            0
        else
            use changes = new Changes(Path.Combine(root, "index-state.sqlite"))
            run coordinator changes stop.Token |> fun running -> running.GetAwaiter().GetResult()
            0
    with
    | PipelineStopped _ -> 0
