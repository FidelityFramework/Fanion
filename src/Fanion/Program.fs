/// retrieval-native <relationships|load|generation> ... Exit 2 on a validation or budget
/// error (message on stderr), 1 on an unexpected failure, 64 on usage.
module Fanion.Program

open System
open System.Collections.Generic
open System.IO
open Fidelity.Data.JSON
open System.Threading.Tasks
open Fanion.Snapshot
open Fanion.Canonical

let private writeRelationships (output: string) (root: JsonValue) (result: Enrich.Result) =
    let properties =
        [ for name, value in JsonInput.fields root do
            if name <> "nodes" && name <> "edges" && name <> "relationships" then
                yield name, Raw value ]
    let nodeEntries =
        Array.append (result.Nodes |> Array.map (fun n -> n.Id, n.Raw)) (result.Capabilities |> Array.map (fun c -> c.Id, capabilityJson c))
        |> Array.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b))
    let field (edge: JsonValue) name = match Json.prop name edge with Some(JsonValue.String value) -> value | _ -> ""
    let edgeEntries =
        Array.append
            (result.PassthroughEdges |> Array.map (fun e -> struct (field e "src", field e "dst", field e "kind", field e "evidence_id", field e "rule"), e))
            (result.Derived |> Array.map (fun e -> struct (e.Src, e.Dst, e.Kind, e.EvidenceId, e.Rule), edgeJson e))
        |> Array.sortWith (fun (struct (a1, a2, a3, a4, a5), _) (struct (b1, b2, b3, b4, b5), _) ->
            let c1 = String.CompareOrdinal(a1, b1)
            if c1 <> 0 then c1 else
            let c2 = String.CompareOrdinal(a2, b2)
            if c2 <> 0 then c2 else
            let c3 = String.CompareOrdinal(a3, b3)
            if c3 <> 0 then c3 else
            let c4 = String.CompareOrdinal(a4, b4)
            if c4 <> 0 then c4 else String.CompareOrdinal(a5, b5))
    let relationships =
        Obj [ "version", Str relationshipVersion
              "derived_edges", Int (int64 result.Derived.Length)
              "capability_nodes", Int (int64 result.Capabilities.Length)
              "confidence_note", Str "Fixed heuristic strengths, not calibrated probabilities or implementation proofs." ]
    let document =
        Obj (properties @ [ "nodes", Arr [ for _, node in nodeEntries -> Raw node ]
                            "edges", Arr [ for _, edge in edgeEntries -> Raw edge ]
                            "relationships", relationships ])
    use stream = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, 1 <<< 20)
    writeTo stream orderedCompactStyle document

type private Args(argv: string[]) =
    let positional = ResizeArray<string>()
    let options = Dictionary<string, string>()
    do
        let mutable i = 0
        while i < argv.Length do
            if argv.[i].StartsWith("--", StringComparison.Ordinal) && i + 1 < argv.Length then
                options.[argv.[i]] <- argv.[i + 1]; i <- i + 2
            else
                positional.Add argv.[i]; i <- i + 1
    member _.Positional = positional.ToArray()
    member _.Text(name: string, fallback: string) = match options.TryGetValue name with | true, v -> v | _ -> fallback
    member _.Required(name: string) = match options.TryGetValue name with | true, v -> v | _ -> raise (ArgumentException("missing " + name))
    member _.Int64(name: string, fallback: int64) = match options.TryGetValue name with | true, v -> Int64.Parse(v, Globalization.CultureInfo.InvariantCulture) | _ -> fallback
    member this.Int(name: string, fallback: int) = int (this.Int64(name, int64 fallback))

let private budgets (args: Args) : Generation.Budgets =
    { MaxOutputBytes = args.Int64("--max-output-bytes", 384L * 1024L * 1024L); MaxNodes = args.Int("--max-nodes", 100_000)
      MaxEdges = args.Int("--max-edges", 200_000); MaxTreeEntries = args.Int64("--max-tree-entries", 100_000L)
      MaxFiles = args.Int("--max-files", 20_000); MaxSourceBytes = args.Int64("--max-source-bytes", 128L * 1024L * 1024L)
      MaxDerivedEdges = args.Int("--max-derived-edges", maxDerivedEdges); MaxCapabilityNodes = args.Int("--max-capability-nodes", maxCapabilityNodes)
      MaxMetadataBytes = args.Int("--max-metadata-bytes", 32768) }

let private engine (args: Args) : Load.Engine =
    let here = Path.GetDirectoryName(Environment.ProcessPath)
    let lib = File.ReadAllBytes(args.Text("--libduckdb-lock", Path.Combine(here, "libduckdb-lock.json"))) |> JsonInput.parseBytes
    let pgq = File.ReadAllBytes(args.Text("--duckpgq-lock", Path.Combine(here, "duckpgq-lock.json"))) |> JsonInput.parseBytes
    let text name value = value |> JsonInput.property name |> JsonInput.string
    if text "linkage" pgq <> "static"
       || text "duckdb_version" pgq <> text "duckdb_version" lib
       || text "duckdb_source_revision" pgq <> text "duckdb_source_revision" lib then
        raise (InvalidOperationException "DuckPGQ must match the reviewed DuckDB source and static artifact")
    { LibDuckDb = args.Text("--libduckdb", Path.Combine(here, "libduckdb.so"))
      LibDuckDbSha256 = text "libduckdb_so_sha256" lib
      DuckDbVersion = text "duckdb_version" lib
      DuckDbSourceId = text "duckdb_source_id" lib
      DuckPgqVersion = text "extension_version" pgq
      Threads = args.Int("--threads", 8); MemoryLimit = args.Text("--memory-limit", "2GB") }

let private extractLimits (args: Args) : Extract.Limits =
    { MaxRepositories = args.Int("--max-repositories", 64); MaxTreeEntries = args.Int64("--max-tree-entries", 100_000L)
      MaxTreeBytes = args.Int("--max-tree-bytes", 16 * 1024 * 1024); MaxFiles = args.Int("--max-files", 20_000)
      MaxFileBytes = args.Int("--max-file-bytes", 1024 * 1024); MaxSourceBytes = args.Int64("--max-source-bytes", 128L * 1024L * 1024L)
      MaxNodes = args.Int("--max-nodes", 100_000); MaxEdges = args.Int("--max-edges", 200_000)
      MaxOutputBytes = args.Int64("--max-output-bytes", 384L * 1024L * 1024L); MaxLayerRules = args.Int("--max-layer-rules", 64)
      MaxRulePatterns = args.Int("--max-rule-patterns", 32); MaxTotalRulePatterns = args.Int("--max-total-rule-patterns", 256)
      MaxLinksPerFile = args.Int("--max-links-per-file", 1024)
      MaxBuildSeconds = Double.Parse(args.Text("--max-build-seconds", "600"), Globalization.CultureInfo.InvariantCulture)
      GitTimeoutSeconds = Double.Parse(args.Text("--git-timeout-seconds", "15"), Globalization.CultureInfo.InvariantCulture) }

let private report (r: Generation.Report) =
    let phases = Obj [ for (name, seconds) in r.Phases -> name, Float (Math.Round(seconds, 3)) ]
    let value = Obj [ "snapshot_id", Str r.SnapshotId; "nodes", Int (int64 r.Nodes); "edges", Int (int64 r.Edges)
                      "derived_edges", Int (int64 r.DerivedEdges); "capability_nodes", Int (int64 r.CapabilityNodes)
                      "threads", Int (int64 r.Threads); "stats", r.Stats; "phases", phases ]
    Console.Out.WriteLine(toText canonicalStyle value)

/// Operator re-extraction of current heads, not a simulated code push.
let private reindex (args: Args) =
    let root = Path.GetFullPath(args.Required "--root")
    let database = Path.Combine(root, "index-state.sqlite")
    if not (File.Exists database) then raise (InvalidOperationException "Reindex requires an existing published index")
    let sources, _ = Coordinator.configSources (args.Required "--config")
    let configured = sources |> List.map (fun source -> source.Id) |> set
    let requested =
        match args.Required "--sources" with
        | "all" -> sources |> List.map (fun source -> source.Id)
        | csv -> csv.Split(',') |> Array.toList
    if requested.IsEmpty || requested.Length <> (set requested).Count || requested |> List.exists (fun id -> not (configured.Contains id)) then
        raise (ArgumentException "--sources must be all or a comma-separated list of distinct configured source IDs")
    let timeout = args.Int("--timeout-seconds", 120)
    if timeout <= 0 then raise (ArgumentException "--timeout-seconds must be positive")
    use deadline = new Threading.CancellationTokenSource(TimeSpan.FromSeconds(float timeout))
    use changes = new IndexState.Changes(database)
    let receiptPath = Path.Combine(root, "last-build.json")
    use receipts = new IndexState.Changes(receiptPath)
    let state = IndexState.State(database, [ for source in sources -> source.Id, source.DependsOn ])
    let changed, receiptChanged = changes.Watch(), receipts.Watch()
    if changed.IsCompleted then changed.GetAwaiter().GetResult()
    if receiptChanged.IsCompleted then receiptChanged.GetAwaiter().GetResult()
    let field name = function Obj fields -> fields |> List.find (fun (key, _) -> key = name) |> snd | _ -> raise (InvalidOperationException "Invalid state response")
    let fresh snapshot = field "fresh" (state.Freshness(snapshot, 90.0)) = Bool true
    let baseline = state.ActiveGeneration() |> Option.defaultWith (fun () -> raise (InvalidOperationException "Reindex requires a published generation"))
    let plan = state.PendingPlan()
    if not plan.Targets.IsEmpty || not plan.ObservationRequired.IsEmpty || plan.DesiredRevisions <> baseline.Revisions || not (fresh baseline.SnapshotId) then
        raise (InvalidOperationException "Reindex requires a fresh publication without pending work or observations")
    let now () = float (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) / 1000.0
    let startedAt = now ()
    let clock = Diagnostics.Stopwatch.StartNew()
    let accepted = state.InvalidateSources(requested, "Operator timing: re-extract current published heads")
    let acceptedAt, acceptanceSeconds = now (), clock.Elapsed.TotalSeconds
    let epoch = match field "desired_epoch" accepted with Int value -> value | _ -> raise (InvalidOperationException "Missing accepted epoch")
    if epoch <> plan.DesiredEpoch + 1L then raise (InvalidOperationException "Concurrent invalidation changed the timing trial")
    let fence () =
        let current = state.PendingPlan()
        if current.DesiredEpoch <> epoch || current.DesiredRevisions <> baseline.Revisions || not current.ObservationRequired.IsEmpty then
            raise (InvalidOperationException "Concurrent source activity changed the timing trial; reindex remains owned by the coordinator")
    let rec awaitChange (watcher: IndexState.Changes) (signal: Task<unit>) read =
        deadline.Token.ThrowIfCancellationRequested()
        if signal.IsCompleted then signal.GetAwaiter().GetResult()
        match read () with
        | Some result -> result
        | None ->
            signal.WaitAsync(deadline.Token).GetAwaiter().GetResult()
            awaitChange watcher (watcher.Watch()) read
    let published =
        awaitChange changes changed (fun () ->
            fence ()
            match state.ActiveGeneration() with
            | Some current when current.Generation > baseline.Generation ->
                if current.Generation <> baseline.Generation + 1L || current.Revisions <> baseline.Revisions then
                    raise (InvalidOperationException "Publication does not belong to this isolated reindex trial")
                if fresh current.SnapshotId then Some current else None
            | _ -> None)
    let publicationObservedSeconds = clock.Elapsed.TotalSeconds
    let build =
        awaitChange receipts receiptChanged (fun () ->
            fence ()
            if not (File.Exists receiptPath) then None else
            let value = File.ReadAllBytes receiptPath |> JsonInput.parseBytes
            match Json.prop "generation" value |> Option.bind Json.tryAsInt64 with
            | Some generation when generation > published.Generation -> raise (InvalidOperationException "A later build replaced this trial's receipt")
            | Some generation when generation = published.Generation ->
                if value |> JsonInput.property "snapshot_id" |> JsonInput.string <> published.SnapshotId
                   || value |> JsonInput.property "status" |> JsonInput.string <> "completed"
                   || not (value |> JsonInput.property "published" |> JsonInput.boolean) then
                    raise (InvalidOperationException "The completed build receipt does not match the publication")
                Some value
            | _ -> None)
    let receiptDelay = clock.Elapsed.TotalSeconds - publicationObservedSeconds
    Console.Out.WriteLine(toText canonicalStyle (Obj [
        "kind", Str "operator_reindex_current_heads"; "requested_sources", Arr (List.map Str requested)
        "base_generation", Int baseline.Generation; "generation", Int published.Generation; "snapshot_id", Str published.SnapshotId
        "started_at", Float startedAt; "accepted_at", Float acceptedAt; "recorded_publication_at", Float published.PublishedAt
        "acceptance_seconds", Float acceptanceSeconds
        "accepted_to_recorded_publication_seconds", Float (published.PublishedAt - acceptedAt)
        "accepted_to_publication_observed_seconds", Float (publicationObservedSeconds - acceptanceSeconds)
        "total_seconds", Float publicationObservedSeconds
        "receipt_delay_seconds", Float receiptDelay; "build", Raw build ]))

[<EntryPoint>]
let main argv =
    try
        if argv.Length = 0 then
            eprintfn "       retrieval-native reindex --root DIR --config FILE --sources all|id,id [--timeout-seconds 120]"
            eprintfn "usage: retrieval-native serve --root DIR --config FILE --token-file FILE --prompt-file FILE [--webhook-secret-file FILE] [--host H --port P]\n       retrieval-native index --root DIR --config FILE [--once] [--remote-base URL] [--duckpgq-lock FILE --libduckdb-lock FILE]\n       retrieval-native fingerprint --config FILE [--duckpgq-lock FILE --libduckdb-lock FILE]\n       retrieval-native probe [--base-url http://127.0.0.1:8080] [--only resources|http|engine] [--config FILE --marker FILE --token-file FILE --duckpgq-lock FILE --libduckdb-lock FILE]\n       retrieval-native schema --root DIR --config FILE --prompt-file FILE [--duckpgq-lock FILE --libduckdb-lock FILE]\n       retrieval-native extract <manifest.json> <component.json> [limits]\n       retrieval-native extract-components <batch.json> [limits]\n       retrieval-native relationships <snapshot.json> <output.json> [budgets]\n       retrieval-native load <snapshot.json> <output.duckdb> [--libduckdb-lock FILE --duckpgq-lock FILE --libduckdb PATH] [budgets]\n       retrieval-native generation <manifest.json> <output.duckdb> (same engine options)"
            64
        else
            let argv = argv |> Array.collect (fun a -> if a = "--once" then [| "--once"; "true" |] else [| a |])
            let args = Args(argv.[1..])
            let positional = args.Positional
            match argv.[0] with
            | "reindex" when positional.Length = 0 -> reindex args; 0
            | "prime" when positional.Length = 0 ->
                let settings = English.settingsFromEnvironment () |> Option.defaultWith (fun () -> raise (InvalidOperationException "Gateway model is not configured"))
                if settings.ResponseFormat <> "magnitude_gbnf" || settings.EnableThinking <> Some false then
                    raise (InvalidOperationException "Gateway priming requires constrained non-thinking Magnitude")
                Console.Out.WriteLine(toText canonicalStyle (Probe.prime (args.Text("--base-url", "http://127.0.0.1:8080")) (args.Required "--token-file") settings.Model))
                0
            | "relationships" when positional.Length = 2 ->
                let b = budgets args
                let bytes = File.ReadAllBytes positional.[0]
                let document = JsonInput.parseBytes bytes
                let result = Enrich.run (engine args) document { MaxFiles = b.MaxFiles; MaxSourceBytes = b.MaxSourceBytes; MaxDerivedEdges = b.MaxDerivedEdges; MaxCapabilityNodes = b.MaxCapabilityNodes }
                writeRelationships positional.[1] document result
                0
            | "load" when positional.Length = 2 ->
                report (Generation.load (engine args) (budgets args) positional.[0] positional.[1])
                0
            | "extract-components" when positional.Length = 1 ->
                // Batch: [{"manifest": {...}, "output": path}, ...]; each job is one component,
                // built and written independently and in parallel; a failing job fails the call.
                let limits = extractLimits args
                let jobs = File.ReadAllBytes positional.[0] |> JsonInput.parseBytes |> JsonInput.items |> List.toArray
                let reports : J[] = Array.zeroCreate jobs.Length
                Parallel.For(0, jobs.Length, fun i ->
                    let manifest = JsonInput.property "manifest" jobs.[i]
                    let output = jobs.[i] |> JsonInput.property "output" |> JsonInput.string
                    let snapshotId, content = Extract.build manifest limits
                    Extract.writeAtomic output content
                    reports.[i] <- Obj [ "output", Str output; "snapshot_id", Str snapshotId; "bytes", Int (int64 content.Length + 1L) ]) |> ignore
                Console.Out.WriteLine(toText canonicalStyle (Arr (List.ofArray reports)))
                0
            | "extract" when positional.Length = 2 ->
                let limits = extractLimits args
                let raw = File.ReadAllBytes positional.[0]
                if raw.Length > args.Int("--max-manifest-bytes", 128 * 1024) then raise (Git.IngestError "manifest exceeded byte limit")
                let manifest = (try JsonInput.parseBytes raw with JsonInputError _ -> raise (Git.IngestError "manifest must contain only a repositories list"))
                let snapshotId, content = Extract.build manifest limits
                Extract.writeAtomic positional.[1] content
                Console.Out.WriteLine(toText canonicalStyle (Obj [ "snapshot_id", Str snapshotId; "bytes", Int (int64 content.Length + 1L) ]))
                0
            | "fingerprint" when positional.Length = 0 ->
                // The one extraction fingerprint formula: configured sources + this binary + the engine locks.
                // The F# coordinator and the guest readiness probe call this instead of computing their own.
                let lockPath name fallback = args.Text(name, Path.Combine(Path.GetDirectoryName(Environment.ProcessPath), fallback))
                let locks = [ lockPath "--duckpgq-lock" "duckpgq-lock.json"; lockPath "--libduckdb-lock" "libduckdb-lock.json" ]
                let sources, _ = Coordinator.configSources (args.Required "--config")
                Console.Out.WriteLine(toText canonicalStyle (Obj [ "sha256", Str (Coordinator.extractionFingerprint sources locks) ]))
                0
            | "probe" when positional.Length = 0 && args.Text("--only", "") = "engine" ->
                let configured = engine args
                use connection = Load.openEngine configured ":memory:"
                Console.Out.WriteLine(toText canonicalStyle (Load.engineIdentity configured))
                0
            | "probe" when positional.Length = 0 ->
                // Guest readiness probe (replaces guest_probe.py): fingerprint marker, allocated
                // resources, then health / 401 / schema / find / sources / pgq over loopback.
                let lockPath name fallback = args.Text(name, Path.Combine(Path.GetDirectoryName(Environment.ProcessPath), fallback))
                Probe.run
                    { BaseUrl = args.Text("--base-url", "http://127.0.0.1:8080")
                      Config = args.Text("--config", "/etc/retrieval/corpus.json")
                      Marker = args.Text("--marker", "/var/lib/retrieval/extractor-fingerprint.json")
                      TokenFile = args.Text("--token-file", "/etc/retrieval/api.token")
                      LockFiles = [ lockPath "--duckpgq-lock" "duckpgq-lock.json"; lockPath "--libduckdb-lock" "libduckdb-lock.json" ]
                      ExpectedIp = args.Text("--expected-ip", "10.0.50.232/24")
                      ExpectedMac = args.Text("--expected-mac", "02:00:0a:00:32:e8")
                      ExpectedGateway = args.Text("--expected-gateway", "10.0.50.1")
                      MinRootBytes = args.Int64("--min-root-bytes", 75L * 1024L * 1024L * 1024L)
                      RequireGuestIdentity = args.Text("--guest-identity", "required") <> "none"
                      Only = (match args.Text("--only", null) with null -> None | only -> Some only) }
            | "schema" when positional.Length = 0 ->
                // The published schema of one index root (upgrade.py validates the side index with it).
                let engine = { engine args with MemoryLimit = args.Text("--memory-limit", "768MB") }
                let store = Query.ManagedStore(args.Required "--root", args.Required "--config", engine, args.Required "--prompt-file")
                try Console.Out.WriteLine(toText orderedStyle (store.Schema())) finally store.Close()
                0
            | "serve" when positional.Length = 0 ->
                let lockPath name fallback = args.Text(name, Path.Combine(Path.GetDirectoryName(Environment.ProcessPath), fallback))
                let duckpgq = File.ReadAllBytes(lockPath "--duckpgq-lock" "duckpgq-lock.json") |> JsonInput.parseBytes
                let libduckdb = File.ReadAllBytes(lockPath "--libduckdb-lock" "libduckdb-lock.json") |> JsonInput.parseBytes
                if not (duckpgq |> JsonInput.property "accepted" |> JsonInput.boolean) || not (libduckdb |> JsonInput.property "accepted" |> JsonInput.boolean) then
                    raise (InvalidOperationException "Current DuckDB/DuckPGQ pair has not passed acceptance; deployment is disabled")
                let engine = { engine args with MemoryLimit = args.Text("--memory-limit", "768MB") }
                let token = File.ReadAllText(args.Required "--token-file").Trim()
                if token.Length < 32 then raise (InvalidOperationException "Retrieval API token must contain at least 32 characters")
                let secret =
                    match args.Text("--webhook-secret-file", null) with
                    | null -> None
                    | file ->
                        let bytes = File.ReadAllBytes file
                        let trimmed = Text.Encoding.UTF8.GetString(bytes).Trim()
                        if trimmed.Length < 32 then raise (InvalidOperationException "Webhook secret must contain at least 32 random characters")
                        Some(Text.Encoding.UTF8.GetBytes trimmed)
                let model = English.settingsFromEnvironment () |> Option.map (fun s -> English.ModelClient(s))
                Serve.run { Root = args.Required "--root"; Config = args.Required "--config"; Engine = engine; PromptFile = args.Required "--prompt-file"
                            Token = token; WebhookSecret = secret; Host = args.Text("--host", "0.0.0.0"); Port = args.Int("--port", 8080); Model = model
                            Observers = Requests.defaultObserverOptions; Queue = Requests.defaultQueueOptions }
            | "index" when positional.Length = 0 ->
                let lockPath name fallback = args.Text(name, Path.Combine(Path.GetDirectoryName(Environment.ProcessPath), fallback))
                let duckpgqLock, libduckdbLock = lockPath "--duckpgq-lock" "duckpgq-lock.json", lockPath "--libduckdb-lock" "libduckdb-lock.json"
                let duckpgq = File.ReadAllBytes duckpgqLock |> JsonInput.parseBytes
                let libduckdb = File.ReadAllBytes libduckdbLock |> JsonInput.parseBytes
                if not (duckpgq |> JsonInput.property "accepted" |> JsonInput.boolean) || not (libduckdb |> JsonInput.property "accepted" |> JsonInput.boolean) then
                    raise (InvalidOperationException "Current DuckDB/DuckPGQ pair has not passed acceptance; deployment is disabled")
                let engine = { engine args with MemoryLimit = args.Text("--memory-limit", "2GB") }
                let optionalFloat name = match args.Text(name, null) with null -> None | v -> Some(Double.Parse(v, Globalization.CultureInfo.InvariantCulture))
                let options : Coordinator.Options =
                    { Root = args.Required "--root"; Config = args.Required "--config"; Engine = engine
                      Budgets = budgets args; ExtractLimits = extractLimits args; RemoteBase = args.Text("--remote-base", "https://forge.spkez.dev/")
                      LeaseSeconds = Double.Parse(args.Text("--lease-seconds", "900"), Globalization.CultureInfo.InvariantCulture)
                      SettleSeconds = optionalFloat "--settle-seconds"; MaxSettleSeconds = optionalFloat "--max-settle-seconds"
                      LockFiles = [ duckpgqLock; libduckdbLock ] }
                Coordinator.serve options (args.Text("--once", "false") = "true")
            | "generation" when positional.Length = 2 ->
                let manifest = File.ReadAllBytes positional.[0] |> JsonInput.parseBytes
                let components =
                    [ for c in manifest |> JsonInput.property "components" |> JsonInput.items ->
                        ({ Path = c |> JsonInput.property "path" |> JsonInput.string
                           SourceId = c |> JsonInput.property "source_id" |> JsonInput.string
                           Repo = c |> JsonInput.property "repo" |> JsonInput.string
                           Commit = c |> JsonInput.property "commit" |> JsonInput.string } : Generation.ComponentSpec) ]
                let identity = manifest |> JsonInput.property "corpus_config_sha256" |> JsonInput.string
                report (Generation.generation (engine args) (budgets args) components identity (Raw (JsonInput.property "tracked_sources" manifest)) positional.[1])
                0
            | _ ->
                eprintfn "unknown subcommand or wrong arguments"
                64
    with
    | RelationshipError message | Load.LoadError message | Git.IngestError message -> eprintfn "%s" message; 2
    | :? AggregateException as error when
        (error.Flatten().InnerExceptions |> Seq.exists (function RelationshipError _ | Load.LoadError _ | Git.IngestError _ -> true | _ -> false)) ->
        // Parallel phases wrap validation errors (possibly nested); report the first one as a rejection.
        let message = error.Flatten().InnerExceptions |> Seq.pick (function RelationshipError m | Load.LoadError m | Git.IngestError m -> Some m | _ -> None)
        eprintfn "%s" message; 2
    | error -> eprintfn "%s" (error.ToString()); 1
