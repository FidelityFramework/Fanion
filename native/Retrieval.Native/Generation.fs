/// Two entry points over the loader: `load` (an already-enriched snapshot file, the
/// old Python initialize contract) and `generation` (component files → relationships
/// → finished DuckDB generation, with no intermediate snapshot JSON).
module Retrieval.Native.Generation

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Text.Json
open System.Text.RegularExpressions
open System.Threading.Tasks
open Retrieval.Native.Snapshot
open Retrieval.Native.Canonical
open Retrieval.Native.Load

let private hex64 = Regex(@"\A[0-9a-f]{64}\z", RegexOptions.Compiled)

type Budgets =
    { MaxOutputBytes: int64; MaxNodes: int; MaxEdges: int; MaxTreeEntries: int64; MaxFiles: int; MaxSourceBytes: int64
      MaxDerivedEdges: int; MaxCapabilityNodes: int; MaxMetadataBytes: int }

type Report =
    { SnapshotId: string; Nodes: int; Edges: int; DerivedEdges: int; CapabilityNodes: int; Threads: int
      Stats: J; Phases: (string * float) list }

let private stopwatch () = Stopwatch.StartNew()

let private capabilityJ (cap: CapabilityNode) : J =
    Obj [ "id", Str cap.Id; "repo", Str cap.Repo; "revision", Str cap.Revision; "path", Str ""; "kind", Str "capability"
          "title", Str cap.Label; "line_start", Int 0L; "line_end", Int 0L; "text", Str (capabilityText cap.Label cap.Curated)
          "source_url", Str cap.SourceUrl; "corpus_layer", Str "capability"
          "classification_rule", Str (relationshipVersion + ":" + (if cap.Curated then "curated" else "api")) ]

let private derivedJ (e: DerivedEdge) : J =
    Obj [ "src", Str e.Src; "dst", Str e.Dst; "kind", Str e.Kind; "basis", Str e.Basis; "confidence", Float e.Confidence
          "evidence_id", Str e.EvidenceId; "evidence_line_start", Int (int64 e.Line); "evidence_line_end", Int (int64 e.Line)
          "rule", Str e.Rule; "explanation", Str e.Explanation ]

let private edgeKey (e: JsonElement) =
    let f name = match e.TryGetProperty(name: string) with | true, v when v.ValueKind = JsonValueKind.String -> v.GetString() | _ -> ""
    struct (f "src", f "dst", f "kind", f "evidence_id", f "rule")

let private compareKeys (struct (a1: string, a2: string, a3: string, a4: string, a5: string)) (struct (b1, b2, b3, b4, b5)) =
    let c1 = String.CompareOrdinal(a1, b1)
    if c1 <> 0 then c1 else
    let c2 = String.CompareOrdinal(a2, b2)
    if c2 <> 0 then c2 else
    let c3 = String.CompareOrdinal(a3, b3)
    if c3 <> 0 then c3 else
    let c4 = String.CompareOrdinal(a4, b4)
    if c4 <> 0 then c4 else String.CompareOrdinal(a5, b5)

/// Validate node rows in parallel, then the edge rows; return rows in the given order.
let private validateRows (repositories: Dictionary<string, string>) (nodes: Choice<JsonElement, CapabilityNode>[]) (edges: Choice<JsonElement, DerivedEdge>[]) =
    let nodeRows : (NodeRow * int64 * int64)[] = Array.zeroCreate nodes.Length
    Parallel.For(0, nodes.Length, fun i ->
        nodeRows.[i] <- (match nodes.[i] with Choice1Of2 raw -> rowOfRawNode repositories raw | Choice2Of2 cap -> rowOfCapability cap)) |> ignore
    let ranges = Dictionary<string, struct (int64 * int64)>(nodes.Length)
    for (row, s, e) in nodeRows do
        if ranges.ContainsKey row.Id then raise (LoadError "Invalid node identity or repository revision")
        ranges.[row.Id] <- struct (s, e)
    let edgeRows : EdgeRow[] = Array.zeroCreate edges.Length
    Parallel.For(0, edges.Length, fun i ->
        edgeRows.[i] <- (match edges.[i] with Choice1Of2 raw -> rowOfRawEdge ranges raw | Choice2Of2 d -> rowOfDerivedEdge ranges d)) |> ignore
    nodeRows |> Array.map (fun (row, _, _) -> row), edgeRows

/// `load`: the reference loader contract over an enriched snapshot file.
let load (engine: Engine) (budgets: Budgets) (snapshotPath: string) (database: string) : Report =
    let phases = ResizeArray<string * float>()
    let sw = stopwatch ()
    if FileInfo(snapshotPath).Length > budgets.MaxOutputBytes then raise (LoadError "Snapshot exceeds ingestion output budget")
    // Python's json module writes NaN/Infinity tokens; they are not JSON and the
    // reference loader rejects such snapshots, so parse failures are validation errors.
    use document =
        try JsonDocument.Parse(File.ReadAllBytes snapshotPath, JsonDocumentOptions(MaxDepth = 64))
        with :? JsonException -> raise (LoadError "Invalid snapshot metadata")
    let root = document.RootElement
    let property name = match root.TryGetProperty(name: string) with | true, v -> Some v | _ -> None
    let formatOk = match property "format_version" with Some v when v.ValueKind = JsonValueKind.Number && v.GetRawText() = "1" -> true | _ -> false
    let snapshotId = match property "snapshot_id" with Some v when v.ValueKind = JsonValueKind.String -> v.GetString() | _ -> null
    if root.ValueKind <> JsonValueKind.Object || not formatOk || isNull snapshotId || not (hex64.IsMatch snapshotId) then raise (LoadError "Invalid snapshot metadata")
    phases.Add("parse", sw.Elapsed.TotalSeconds); sw.Restart()
    let unsigned = Obj [ for p in root.EnumerateObject() do if p.Name <> "snapshot_id" then yield p.Name, Raw p.Value ]
    let digestHex, _ = digest canonicalStyle unsigned
    if digestHex <> snapshotId then raise (LoadError "Snapshot contents do not match their declared identity")
    phases.Add("digest", sw.Elapsed.TotalSeconds); sw.Restart()
    let isList name = match property name with Some v when v.ValueKind = JsonValueKind.Array -> true | _ -> false
    if not (isList "repositories" && isList "nodes" && isList "edges") then raise (LoadError "Snapshot repositories, nodes and edges must be lists")
    let repositories = validateRepositories [ for r in root.GetProperty("repositories").EnumerateArray() -> r ]
    let nodes = [| for n in root.GetProperty("nodes").EnumerateArray() -> Choice1Of2 n |]
    let edges = [| for e in root.GetProperty("edges").EnumerateArray() -> Choice1Of2 e |]
    if nodes.Length > budgets.MaxNodes || edges.Length > budgets.MaxEdges then raise (LoadError "Snapshot exceeds corpus bounds")
    let metadata = Obj [ for p in root.EnumerateObject() do if p.Name <> "nodes" && p.Name <> "edges" then yield p.Name, Raw p.Value ]
    if snd (digest defaultUnicodeStyle metadata) > int64 budgets.MaxMetadataBytes then raise (LoadError "Snapshot metadata exceeds the schema response budget")
    let metadataText = toText defaultStyle metadata
    let nodeRows, edgeRows = validateRows repositories nodes edges
    phases.Add("validate", sw.Elapsed.TotalSeconds); sw.Restart()
    build engine database metadataText nodeRows edgeRows
    phases.Add("duckdb", sw.Elapsed.TotalSeconds)
    { SnapshotId = snapshotId; Nodes = nodes.Length; Edges = edges.Length; DerivedEdges = 0; CapabilityNodes = 0
      Threads = engine.Threads; Stats = Null; Phases = List.ofSeq phases }

type ComponentSpec = { Path: string; SourceId: string; Repo: string; Commit: string }

/// Recursive add_counts over component statistics (non-negative ints and nested objects).
let rec private addCounts (target: Dictionary<string, obj>) (source: JsonElement) =
    for p in source.EnumerateObject() do
        match p.Value.ValueKind with
        | JsonValueKind.Number when (match p.Value.TryGetInt64() with | true, v -> v >= 0L && not (p.Value.GetRawText().Contains '.') | _ -> false) ->
            let v = p.Value.GetInt64()
            target.[p.Name] <- box (v + (match target.TryGetValue p.Name with | true, (:? int64 as existing) -> existing | _ -> 0L))
        | JsonValueKind.Object ->
            let nested = match target.TryGetValue p.Name with | true, (:? Dictionary<string, obj> as d) -> d | _ -> (let d = Dictionary<string, obj>() in target.[p.Name] <- box d; d)
            addCounts nested p.Value
        | _ -> raise (LoadError "Invalid corpus statistics")

let rec private statsJ (table: Dictionary<string, obj>) : J =
    Obj [ for kv in table -> kv.Key, (match kv.Value with :? int64 as v -> Int v | :? Dictionary<string, obj> as d -> statsJ d | _ -> Null) ]

/// `generation`: component files → combined snapshot → relationships → DuckDB, hashing without materializing JSON.
let generation (engine: Engine) (budgets: Budgets) (components: ComponentSpec list) (configIdentity: string) (trackedSources: J) (database: string) : Report =
    if components.IsEmpty then raise (LoadError "No committed source remains; latest retrieval stays unavailable")
    let phases = ResizeArray<string * float>()
    let sw = stopwatch ()
    // 1. Parse and verify every cached component in parallel (identity, checksum, single repository).
    let documents : JsonDocument[] = Array.zeroCreate components.Length
    let specs = Array.ofList components
    Parallel.For(0, specs.Length, fun i ->
        let spec = specs.[i]
        let document =
            try JsonDocument.Parse(File.ReadAllBytes spec.Path, JsonDocumentOptions(MaxDepth = 64))
            with :? JsonException -> raise (LoadError("Cached component identity or checksum differs: " + spec.SourceId))
        let root = document.RootElement
        let unsigned = Obj [ for p in root.EnumerateObject() do if p.Name <> "snapshot_id" then yield p.Name, Raw p.Value ]
        let declared = match root.TryGetProperty("snapshot_id") with | true, v when v.ValueKind = JsonValueKind.String -> v.GetString() | _ -> ""
        let repositories = match root.TryGetProperty("repositories") with | true, v when v.ValueKind = JsonValueKind.Array -> [ for r in v.EnumerateArray() -> r ] | _ -> []
        let name = match repositories with [ r ] -> (match r.TryGetProperty("name") with | true, v -> v.GetString() | _ -> "") | _ -> ""
        let commit = match repositories with [ r ] -> (match r.TryGetProperty("commit") with | true, v -> v.GetString() | _ -> "") | _ -> ""
        if fst (digest canonicalStyle unsigned) <> declared || repositories.Length <> 1 || name <> spec.Repo || commit <> spec.Commit then
            raise (LoadError("Cached component identity or checksum differs: " + spec.SourceId))
        documents.[i] <- document) |> ignore
    phases.Add("components", sw.Elapsed.TotalSeconds); sw.Restart()
    let roots = documents |> Array.map (fun d -> d.RootElement)
    let first = roots.[0]
    let formatFields =
        [ for name in [ "format_version"; "extractor_version"; "classification_version"; "classification_defaults"; "policy_version" ] do
            match first.TryGetProperty(name) with | true, v -> yield name, Raw v | _ -> () ]
    let repositoryElements = [ for root in roots do yield! root.GetProperty("repositories").EnumerateArray() ]
    let repositories = repositoryElements |> List.map parseRepository
    let allNodes = [| for root in roots do for n in root.GetProperty("nodes").EnumerateArray() -> parseNode n |]
    let allEdges = [| for root in roots do yield! root.GetProperty("edges").EnumerateArray() |]
    let stats = Dictionary<string, obj>()
    for root in roots do
        match root.TryGetProperty("stats") with | true, v when v.ValueKind = JsonValueKind.Object -> addCounts stats v | _ -> ()
    // 2. Relationships over typed arrays.
    let result =
        Enrich.runParsed repositories allNodes allEdges
            { MaxFiles = budgets.MaxFiles; MaxSourceBytes = budgets.MaxSourceBytes; MaxDerivedEdges = budgets.MaxDerivedEdges; MaxCapabilityNodes = budgets.MaxCapabilityNodes }
    phases.Add("relationships", sw.Elapsed.TotalSeconds); sw.Restart()
    let statValue name = match stats.TryGetValue(name: string) with | true, (:? int64 as v) -> v | _ -> 0L
    let nodeCount, edgeCount = result.Nodes.Length + result.Capabilities.Length, result.PassthroughEdges.Length + result.Derived.Length
    if nodeCount > budgets.MaxNodes || edgeCount > budgets.MaxEdges || statValue "tree_entries" > budgets.MaxTreeEntries
       || statValue "candidate_files" > int64 budgets.MaxFiles || statValue "candidate_bytes" > budgets.MaxSourceBytes then
        raise (LoadError "Combined corpus exceeds extraction bounds")
    // 3. Deterministic order: repositories by name, nodes by id, edges by (src, dst, kind, evidence_id, rule).
    let sortedRepositories = repositoryElements |> List.sortWith (fun a b -> String.CompareOrdinal(a.GetProperty("name").GetString(), b.GetProperty("name").GetString()))
    let nodes =
        Array.append (result.Nodes |> Array.map (fun n -> n.Id, Choice1Of2 n.Raw)) (result.Capabilities |> Array.map (fun c -> c.Id, Choice2Of2 c))
        |> Array.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b))
    let edges =
        Array.append (result.PassthroughEdges |> Array.map (fun e -> edgeKey e, Choice1Of2 e))
                     (result.Derived |> Array.map (fun e -> struct (e.Src, e.Dst, e.Kind, e.EvidenceId, e.Rule), Choice2Of2 e))
        |> Array.sortWith (fun (a, _) (b, _) -> compareKeys a b)
    let relationshipsJ =
        Obj [ "version", Str relationshipVersion; "derived_edges", Int (int64 result.Derived.Length); "capability_nodes", Int (int64 result.Capabilities.Length)
              "confidence_note", Str "Fixed heuristic strengths, not calibrated probabilities or implementation proofs." ]
    let body =
        formatFields @
        [ "repositories", Arr (sortedRepositories |> List.map Raw)
          "corpus_config_sha256", Str configIdentity
          "tracked_sources", trackedSources
          "stats", statsJ stats
          "relationships", relationshipsJ ]
    let nodesJ = Arr [ for (_, n) in nodes -> (match n with Choice1Of2 raw -> Raw raw | Choice2Of2 cap -> capabilityJ cap) ]
    let edgesJ = Arr [ for (_, e) in edges -> (match e with Choice1Of2 raw -> Raw raw | Choice2Of2 d -> derivedJ d) ]
    let snapshotJ = Obj (body @ [ ("nodes", nodesJ); ("edges", edgesJ) ])
    let snapshotId, bytesWithoutId = digest canonicalStyle snapshotJ
    // The published serialization also carries ,"snapshot_id":"<64 hex>" (81 bytes).
    if bytesWithoutId + 81L > budgets.MaxOutputBytes then raise (LoadError "Combined corpus exceeds its serialized output bound")
    phases.Add("hash", sw.Elapsed.TotalSeconds); sw.Restart()
    let metadata = Obj (body @ [ "snapshot_id", Str snapshotId ])
    if snd (digest defaultUnicodeStyle metadata) > int64 budgets.MaxMetadataBytes then raise (LoadError "Snapshot metadata exceeds the schema response budget")
    let metadataText = toText defaultStyle metadata
    let repositoryCommits = validateRepositories sortedRepositories
    let nodeRows, edgeRows = validateRows repositoryCommits (nodes |> Array.map snd) (edges |> Array.map snd)
    phases.Add("validate", sw.Elapsed.TotalSeconds); sw.Restart()
    build engine database metadataText nodeRows edgeRows
    phases.Add("duckdb", sw.Elapsed.TotalSeconds)
    for d in documents do d.Dispose()
    { SnapshotId = snapshotId; Nodes = nodeCount; Edges = edgeCount; DerivedEdges = result.Derived.Length; CapabilityNodes = result.Capabilities.Length
      Threads = engine.Threads; Stats = statsJ stats; Phases = List.ofSeq phases }
