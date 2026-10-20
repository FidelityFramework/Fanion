/// retrieval-native <relationships|load|generation> ... Exit 2 on a validation or budget
/// error (message on stderr), 1 on an unexpected failure, 64 on usage.
module Retrieval.Native.Program

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open System.Threading.Tasks
open Retrieval.Native.Snapshot
open Retrieval.Native.Canonical

let private writeRelationships (output: string) (root: JsonElement) (result: Enrich.Result) =
    let options = JsonWriterOptions(Encoder = Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping, Indented = false)
    use stream = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, 1 <<< 20)
    use writer = new Utf8JsonWriter(stream, options)
    writer.WriteStartObject()
    for property in root.EnumerateObject() do
        if property.Name <> "nodes" && property.Name <> "edges" && property.Name <> "relationships" then property.WriteTo(writer)
    let nodeEntries =
        Array.append (result.Nodes |> Array.map (fun n -> n.Id, Choice1Of2 n.Raw)) (result.Capabilities |> Array.map (fun c -> c.Id, Choice2Of2 c))
        |> Array.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b))
    writer.WritePropertyName("nodes")
    writer.WriteStartArray()
    for (_, entry) in nodeEntries do
        match entry with
        | Choice1Of2 raw -> raw.WriteTo(writer)
        | Choice2Of2 cap -> writeCapability writer cap
    writer.WriteEndArray()
    let field (e: JsonElement) name = match e.TryGetProperty(name: string) with | true, v when v.ValueKind = JsonValueKind.String -> v.GetString() | _ -> ""
    let edgeEntries =
        Array.append
            (result.PassthroughEdges |> Array.map (fun e -> struct (field e "src", field e "dst", field e "kind", field e "evidence_id", field e "rule"), Choice1Of2 e))
            (result.Derived |> Array.map (fun e -> struct (e.Src, e.Dst, e.Kind, e.EvidenceId, e.Rule), Choice2Of2 e))
        |> Array.sortWith (fun (struct (a1, a2, a3, a4, a5), _) (struct (b1, b2, b3, b4, b5), _) ->
            let c1 = String.CompareOrdinal(a1, b1)
            if c1 <> 0 then c1 else
            let c2 = String.CompareOrdinal(a2, b2)
            if c2 <> 0 then c2 else
            let c3 = String.CompareOrdinal(a3, b3)
            if c3 <> 0 then c3 else
            let c4 = String.CompareOrdinal(a4, b4)
            if c4 <> 0 then c4 else String.CompareOrdinal(a5, b5))
    writer.WritePropertyName("edges")
    writer.WriteStartArray()
    for (_, entry) in edgeEntries do
        match entry with
        | Choice1Of2 raw -> raw.WriteTo(writer)
        | Choice2Of2 e -> writeEdge writer e
    writer.WriteEndArray()
    writer.WritePropertyName("relationships")
    writer.WriteStartObject()
    writer.WriteString("version", relationshipVersion)
    writer.WriteNumber("derived_edges", result.Derived.Length)
    writer.WriteNumber("capability_nodes", result.Capabilities.Length)
    writer.WriteString("confidence_note", "Fixed heuristic strengths, not calibrated probabilities or implementation proofs.")
    writer.WriteEndObject()
    writer.WriteEndObject()
    writer.Flush()

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
    { Extensions = args.Required "--extensions"
      LibDuckDb = args.Text("--libduckdb", Path.Combine(here, "libduckdb.so"))
      LibDuckDbSha256 = args.Required "--libduckdb-sha256"
      DuckPgqVersion = args.Required "--duckpgq-version"; DuckPgqSha256 = args.Required "--duckpgq-sha256"
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

[<EntryPoint>]
let main argv =
    try
        if argv.Length = 0 then
            eprintfn "usage: retrieval-native serve --root DIR --config FILE --extensions DIR --token-file FILE --prompt-file FILE [--webhook-secret-file FILE] [--host H --port P]\n       retrieval-native index --root DIR --config FILE --extensions DIR [--once] [--remote-base URL] [--duckpgq-lock FILE --libduckdb-lock FILE]\n       retrieval-native fingerprint --config FILE [--duckpgq-lock FILE --libduckdb-lock FILE]\n       retrieval-native probe [--base-url http://127.0.0.1:8080] [--only resources] [--config FILE --marker FILE --token-file FILE --duckpgq-lock FILE --libduckdb-lock FILE]\n       retrieval-native schema --root DIR --config FILE --extensions DIR --prompt-file FILE [--duckpgq-lock FILE --libduckdb-lock FILE]\n       retrieval-native extract <manifest.json> <component.json> [limits]\n       retrieval-native extract-components <batch.json> [limits]\n       retrieval-native relationships <snapshot.json> <output.json> [budgets]\n       retrieval-native load <snapshot.json> <output.duckdb> --extensions DIR --libduckdb-sha256 HEX --duckpgq-version V --duckpgq-sha256 HEX [--libduckdb PATH] [budgets]\n       retrieval-native generation <manifest.json> <output.duckdb> (same engine options)"
            64
        else
            let argv = argv |> Array.collect (fun a -> if a = "--once" then [| "--once"; "true" |] else [| a |])
            let args = Args(argv.[1..])
            let positional = args.Positional
            match argv.[0] with
            | "prime" when positional.Length = 0 ->
                let settings = English.settingsFromEnvironment () |> Option.defaultWith (fun () -> raise (InvalidOperationException "Gateway model is not configured"))
                if settings.ResponseFormat <> "magnitude_gbnf" || settings.EnableThinking <> Some false then
                    raise (InvalidOperationException "Gateway priming requires constrained non-thinking Magnitude")
                Console.Out.WriteLine(toText canonicalStyle (Probe.prime (args.Text("--base-url", "http://127.0.0.1:8080")) (args.Required "--token-file") settings.Model))
                0
            | "relationships" when positional.Length = 2 ->
                let b = budgets args
                let bytes = File.ReadAllBytes positional.[0]
                use document = JsonDocument.Parse(ReadOnlyMemory(bytes), JsonDocumentOptions(MaxDepth = 64))
                let result = Enrich.run document.RootElement { MaxFiles = b.MaxFiles; MaxSourceBytes = b.MaxSourceBytes; MaxDerivedEdges = b.MaxDerivedEdges; MaxCapabilityNodes = b.MaxCapabilityNodes }
                writeRelationships positional.[1] document.RootElement result
                0
            | "load" when positional.Length = 2 ->
                report (Generation.load (engine args) (budgets args) positional.[0] positional.[1])
                0
            | "extract-components" when positional.Length = 1 ->
                // Batch: [{"manifest": {...}, "output": path}, ...]; each job is one component,
                // built and written independently and in parallel; a failing job fails the call.
                let limits = extractLimits args
                use batch = JsonDocument.Parse(ReadOnlyMemory(File.ReadAllBytes positional.[0]))
                let jobs = [| for job in batch.RootElement.EnumerateArray() -> job |]
                let reports : J[] = Array.zeroCreate jobs.Length
                Parallel.For(0, jobs.Length, fun i ->
                    let manifest = jobs.[i].GetProperty("manifest")
                    let output = jobs.[i].GetProperty("output").GetString()
                    let snapshotId, content = Extract.build manifest limits
                    Extract.writeAtomic output content
                    reports.[i] <- Obj [ "output", Str output; "snapshot_id", Str snapshotId; "bytes", Int (int64 content.Length + 1L) ]) |> ignore
                Console.Out.WriteLine(toText canonicalStyle (Arr (List.ofArray reports)))
                0
            | "extract" when positional.Length = 2 ->
                let limits = extractLimits args
                let raw = File.ReadAllBytes positional.[0]
                if raw.Length > args.Int("--max-manifest-bytes", 128 * 1024) then raise (Git.IngestError "manifest exceeded byte limit")
                use manifest = (try JsonDocument.Parse(ReadOnlyMemory(raw)) with :? JsonException -> raise (Git.IngestError "manifest must contain only a repositories list"))
                let snapshotId, content = Extract.build manifest.RootElement limits
                Extract.writeAtomic positional.[1] content
                Console.Out.WriteLine(toText canonicalStyle (Obj [ "snapshot_id", Str snapshotId; "bytes", Int (int64 content.Length + 1L) ]))
                0
            | "fingerprint" when positional.Length = 0 ->
                // The one extraction fingerprint formula: configured sources + this binary + the engine locks.
                // The Python coordinator and the guest readiness probe call this instead of computing their own.
                let lockPath name fallback = args.Text(name, Path.Combine(Path.GetDirectoryName(Environment.ProcessPath), fallback))
                let locks = [ lockPath "--duckpgq-lock" "duckpgq-lock.json"; lockPath "--libduckdb-lock" "libduckdb-lock.json" ]
                let sources, _ = Coordinator.configSources (args.Required "--config")
                Console.Out.WriteLine(toText canonicalStyle (Obj [ "sha256", Str (Coordinator.extractionFingerprint sources locks) ]))
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
                let lockPath name fallback = args.Text(name, Path.Combine(Path.GetDirectoryName(Environment.ProcessPath), fallback))
                use duckpgq = JsonDocument.Parse(File.ReadAllBytes(lockPath "--duckpgq-lock" "duckpgq-lock.json"))
                use libduckdb = JsonDocument.Parse(File.ReadAllBytes(lockPath "--libduckdb-lock" "libduckdb-lock.json"))
                let here = Path.GetDirectoryName(Environment.ProcessPath)
                let engine : Load.Engine =
                    { Extensions = args.Required "--extensions"; LibDuckDb = args.Text("--libduckdb", Path.Combine(here, "libduckdb.so"))
                      LibDuckDbSha256 = libduckdb.RootElement.GetProperty("libduckdb_so_sha256").GetString()
                      DuckPgqVersion = duckpgq.RootElement.GetProperty("extension_version").GetString(); DuckPgqSha256 = duckpgq.RootElement.GetProperty("sha256").GetString()
                      Threads = args.Int("--threads", 8); MemoryLimit = args.Text("--memory-limit", "768MB") }
                let store = Query.ManagedStore(args.Required "--root", args.Required "--config", engine, args.Required "--prompt-file")
                try Console.Out.WriteLine(toText orderedStyle (store.Schema())) finally store.Close()
                0
            | "serve" when positional.Length = 0 ->
                let lockPath name fallback = args.Text(name, Path.Combine(Path.GetDirectoryName(Environment.ProcessPath), fallback))
                use duckpgq = JsonDocument.Parse(File.ReadAllBytes(lockPath "--duckpgq-lock" "duckpgq-lock.json"))
                use libduckdb = JsonDocument.Parse(File.ReadAllBytes(lockPath "--libduckdb-lock" "libduckdb-lock.json"))
                if not (duckpgq.RootElement.GetProperty("accepted").GetBoolean()) || not (libduckdb.RootElement.GetProperty("accepted").GetBoolean()) then
                    raise (InvalidOperationException "Current DuckDB/DuckPGQ pair has not passed acceptance; deployment is disabled")
                let here = Path.GetDirectoryName(Environment.ProcessPath)
                let engine : Load.Engine =
                    { Extensions = args.Required "--extensions"; LibDuckDb = args.Text("--libduckdb", Path.Combine(here, "libduckdb.so"))
                      LibDuckDbSha256 = libduckdb.RootElement.GetProperty("libduckdb_so_sha256").GetString()
                      DuckPgqVersion = duckpgq.RootElement.GetProperty("extension_version").GetString(); DuckPgqSha256 = duckpgq.RootElement.GetProperty("sha256").GetString()
                      Threads = args.Int("--threads", 8); MemoryLimit = args.Text("--memory-limit", "768MB") }
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
                            Token = token; WebhookSecret = secret; Host = args.Text("--host", "0.0.0.0"); Port = args.Int("--port", 8080); Model = model }
            | "index" when positional.Length = 0 ->
                let lockPath name fallback = args.Text(name, Path.Combine(Path.GetDirectoryName(Environment.ProcessPath), fallback))
                let duckpgqLock, libduckdbLock = lockPath "--duckpgq-lock" "duckpgq-lock.json", lockPath "--libduckdb-lock" "libduckdb-lock.json"
                use duckpgq = JsonDocument.Parse(File.ReadAllBytes duckpgqLock)
                use libduckdb = JsonDocument.Parse(File.ReadAllBytes libduckdbLock)
                if duckpgq.RootElement.GetProperty("accepted").GetBoolean() <> true || libduckdb.RootElement.GetProperty("accepted").GetBoolean() <> true then
                    raise (InvalidOperationException "Current DuckDB/DuckPGQ pair has not passed acceptance; deployment is disabled")
                let here = Path.GetDirectoryName(Environment.ProcessPath)
                let engine : Load.Engine =
                    { Extensions = args.Required "--extensions"; LibDuckDb = args.Text("--libduckdb", Path.Combine(here, "libduckdb.so"))
                      LibDuckDbSha256 = libduckdb.RootElement.GetProperty("libduckdb_so_sha256").GetString()
                      DuckPgqVersion = duckpgq.RootElement.GetProperty("extension_version").GetString(); DuckPgqSha256 = duckpgq.RootElement.GetProperty("sha256").GetString()
                      Threads = args.Int("--threads", 8); MemoryLimit = args.Text("--memory-limit", "2GB") }
                let optionalFloat name = match args.Text(name, null) with null -> None | v -> Some(Double.Parse(v, Globalization.CultureInfo.InvariantCulture))
                let options : Coordinator.Options =
                    { Root = args.Required "--root"; Config = args.Required "--config"; Extensions = args.Required "--extensions"; Engine = engine
                      Budgets = budgets args; ExtractLimits = extractLimits args; RemoteBase = args.Text("--remote-base", "https://forge.spkez.dev/")
                      LeaseSeconds = Double.Parse(args.Text("--lease-seconds", "900"), Globalization.CultureInfo.InvariantCulture)
                      SettleSeconds = optionalFloat "--settle-seconds"; MaxSettleSeconds = optionalFloat "--max-settle-seconds"
                      LockFiles = [ duckpgqLock; libduckdbLock ] }
                Coordinator.serve options (args.Text("--once", "false") = "true")
            | "generation" when positional.Length = 2 ->
                use manifest = JsonDocument.Parse(File.ReadAllBytes positional.[0])
                let root = manifest.RootElement
                let components =
                    [ for c in root.GetProperty("components").EnumerateArray() ->
                        ({ Path = c.GetProperty("path").GetString(); SourceId = c.GetProperty("source_id").GetString()
                           Repo = c.GetProperty("repo").GetString(); Commit = c.GetProperty("commit").GetString() } : Generation.ComponentSpec) ]
                let identity = root.GetProperty("corpus_config_sha256").GetString()
                report (Generation.generation (engine args) (budgets args) components identity (Raw (root.GetProperty("tracked_sources"))) positional.[1])
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
