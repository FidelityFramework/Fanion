/// Snapshot validation and the DuckDB generation build: the same checks the
/// Python loader enforced, then a columnar load into a fresh database with the
/// pinned DuckDB engine with DuckPGQ built into the same native library.
module Retrieval.Native.Load

open System
open System.Collections.Generic
open System.IO
open System.Reflection
open System.Runtime.InteropServices
open System.Security.Cryptography
open System.Text.Json
open System.Text.RegularExpressions
open DuckDB.NET.Data
open DuckDB.NET.Native
open Retrieval.Native.Snapshot
open Retrieval.Native.Canonical

exception LoadError of string

let nodeFields = [| "id"; "repo"; "revision"; "path"; "kind"; "title"; "line_start"; "line_end"; "source_url"; "corpus_layer"; "classification_rule" |]
let provenanceFields = [| "basis"; "confidence"; "evidence_id"; "evidence_line_start"; "evidence_line_end"; "rule"; "explanation" |]
let structuralKinds = set [ "contains"; "chunks_of"; "links_to" ]
let edgeKinds = Set.union structuralKinds (set [ "references_project"; "generated_by"; "generated_from"; "mentions"; "declares_capability"; "suggests_capability"; "shares_capability" ])
let private hex64 = Regex(@"\A[0-9a-f]{64}\z", RegexOptions.Compiled)
let private hex40 = Regex(@"\A[0-9a-f]{40}\z", RegexOptions.Compiled)

type Engine =
    { LibDuckDb: string; LibDuckDbSha256: string; DuckDbVersion: string; DuckDbSourceId: string; DuckPgqVersion: string
      Threads: int; MemoryLimit: string }

/// One identity for the native artifact used to build and read a generation.
let engineIdentity (engine: Engine) =
    Obj [ "duckdb_version", Str engine.DuckDbVersion; "duckdb_source_id", Str engine.DuckDbSourceId
          "libduckdb_sha256", Str engine.LibDuckDbSha256; "duckpgq_version", Str engine.DuckPgqVersion
          "duckpgq_linkage", Str "static" ]

type Limits = { MaxNodes: int; MaxEdges: int; MaxMetadataBytes: int }

/// One edge ready for the appender: raw or derived, with legacy structural fill applied.
[<Struct>]
type EdgeRow =
    { Src: string; Dst: string; Kind: string; Basis: string; Confidence: float
      EvidenceId: string; LineStart: int64 voption; LineEnd: int64 voption; Rule: string; Explanation: string }

let private str (e: JsonElement) name = match e.TryGetProperty(name: string) with | true, v when v.ValueKind = JsonValueKind.String -> v.GetString() | _ -> null
let private isInt (v: JsonElement) = v.ValueKind = JsonValueKind.Number && not (v.GetRawText() |> Seq.exists (fun c -> c = '.' || c = 'e' || c = 'E'))
let private int64Of (v: JsonElement) = match v.TryGetInt64() with | true, x -> x | _ -> raise (LoadError "Invalid node identity or repository revision")

/// Validate repositories exactly as the reference loader did; returns name → commit.
let validateRepositories (repositories: JsonElement list) =
    let result = Dictionary<string, string>()
    for repository in repositories do
        let name, commit = str repository "name", str repository "commit"
        if repository.ValueKind <> JsonValueKind.Object || isNull name || name.Length < 1 || name.Length > 256 || isNull commit || not (hex40.IsMatch commit) then
            raise (LoadError "Invalid repository metadata")
        (try Policy.validate repository with RelationshipError message -> raise (LoadError message))
        result.[name] <- commit
    if result.Count < 1 || result.Count > 64 || result.Count <> repositories.Length then raise (LoadError "Snapshot exceeds corpus bounds")
    result

/// A node as the appender needs it.
type NodeRow = { Id: string; Values: obj[] }  // values ordered as nodeFields then text

let private nodeKinds = set [ "repository"; "file"; "chunk"; "capability" ]

let rowOfRawNode (repositories: Dictionary<string, string>) (node: JsonElement) : NodeRow * int64 * int64 =
    if node.ValueKind <> JsonValueKind.Object then raise (LoadError "Invalid node identity or repository revision")
    let has name = (match node.TryGetProperty(name: string) with | true, _ -> true | _ -> false)
    if not (Array.forall has nodeFields && has "text") then raise (LoadError "Invalid node identity or repository revision")
    let id, repo, kind, text = str node "id", str node "repo", str node "kind", str node "text"
    let revision = node.GetProperty("revision")
    let lineStart, lineEnd = node.GetProperty("line_start"), node.GetProperty("line_end")
    if isNull id || not (hex64.IsMatch id) || isNull repo || not (repositories.ContainsKey repo)
       || revision.ValueKind <> JsonValueKind.String || revision.GetString() <> repositories.[repo]
       || isNull kind || not (nodeKinds.Contains kind) || isNull text
       || not (isInt lineStart) || not (isInt lineEnd) then
        raise (LoadError "Invalid node identity or repository revision")
    let s, e = int64Of lineStart, int64Of lineEnd
    if not (0L <= s && s <= e) then raise (LoadError "Invalid node identity or repository revision")
    let value (name: string) : obj =
        let v = node.GetProperty(name)
        match v.ValueKind with
        | JsonValueKind.String -> box (v.GetString())
        | JsonValueKind.Number -> box (int64Of v)
        | JsonValueKind.Null | JsonValueKind.Undefined -> null
        | _ -> raise (LoadError "Invalid node identity or repository revision")
    { Id = id; Values = Array.append (nodeFields |> Array.map value) [| box text |] }, s, e

let rowOfCapability (cap: CapabilityNode) : NodeRow * int64 * int64 =
    { Id = cap.Id
      Values = [| box cap.Id; box cap.Repo; box cap.Revision; box ""; box "capability"; box cap.Label; box 0L; box 0L
                  box cap.SourceUrl; box "capability"; box (relationshipVersion + ":" + (if cap.Curated then "curated" else "api"))
                  box (capabilityText cap.Label cap.Curated) |] }, 0L, 0L

let private bounded (name: string) (value: JsonElement) maximum =
    if value.ValueKind <> JsonValueKind.String then raise (LoadError("Invalid bounded edge " + name))
    let text = value.GetString()
    if text.Length < 1 || text.Length > maximum || text |> Seq.exists (fun c -> int c < 32 || int c = 127) then raise (LoadError("Invalid bounded edge " + name))
    text

/// validated_edge for a raw edge.
let rowOfRawEdge (nodes: Dictionary<string, struct (int64 * int64)>) (edge: JsonElement) : EdgeRow =
    let allowed = Set.union (set [ "src"; "dst"; "kind" ]) (Set.ofArray provenanceFields)
    let present = if edge.ValueKind = JsonValueKind.Object then [ for p in edge.EnumerateObject() -> p.Name ] |> set else Set.empty
    let src, dst, kind = str edge "src", str edge "dst", str edge "kind"
    if edge.ValueKind <> JsonValueKind.Object || not (Set.isSubset (set [ "src"; "dst"; "kind" ]) present) || not (Set.isSubset present allowed)
       || isNull src || not (nodes.ContainsKey src) || isNull dst || not (nodes.ContainsKey dst) || isNull kind || not (edgeKinds.Contains kind) then
        raise (LoadError "Invalid graph edge")
    let provenancePresent = provenanceFields |> Array.exists present.Contains
    if not provenancePresent && structuralKinds.Contains kind then
        { Src = src; Dst = dst; Kind = kind; Basis = "structural"; Confidence = 1.0; EvidenceId = null
          LineStart = ValueNone; LineEnd = ValueNone; Rule = "legacy-structural"; Explanation = "Legacy edge without recorded line provenance." }
    else
        if not (provenanceFields |> Array.forall present.Contains) then raise (LoadError "New graph edges require complete provenance")
        let basis = str edge "basis"
        let confidenceElement = edge.GetProperty("confidence")
        let confidence =
            if confidenceElement.ValueKind = JsonValueKind.Number then confidenceElement.GetDouble() else Double.NaN
        if isNull basis || not (set [ "structural"; "extracted"; "inferred"; "curated" ] |> Set.contains basis)
           || Double.IsNaN confidence || Double.IsInfinity confidence || confidence < 0.0 || confidence > 1.0 then
            raise (LoadError "Invalid edge basis or confidence")
        if basis = "structural" && not (structuralKinds.Contains kind) then raise (LoadError "New relationships require extracted, inferred or curated provenance")
        let rule = bounded "rule" (edge.GetProperty("rule")) 120
        let explanation = bounded "explanation" (edge.GetProperty("explanation")) 600
        let evidenceId = str edge "evidence_id"
        if isNull evidenceId || not (nodes.ContainsKey evidenceId) then raise (LoadError "Edge evidence must identify a node in this snapshot")
        let struct (evidenceStart, evidenceEnd) = nodes.[evidenceId]
        let startElement, endElement = edge.GetProperty("evidence_line_start"), edge.GetProperty("evidence_line_end")
        if not (isInt startElement) || not (isInt endElement) then raise (LoadError "Edge evidence line range must lie within its cited node")
        let s, e = (match startElement.TryGetInt64() with | true, v -> v | _ -> -1L), (match endElement.TryGetInt64() with | true, v -> v | _ -> -1L)
        if not (evidenceStart <= s && s <= e && e <= evidenceEnd) then raise (LoadError "Edge evidence line range must lie within its cited node")
        { Src = src; Dst = dst; Kind = kind; Basis = basis; Confidence = confidence; EvidenceId = evidenceId
          LineStart = ValueSome s; LineEnd = ValueSome e; Rule = rule; Explanation = explanation }

/// validated_edge for a derived edge (already well-formed by construction; keep the range check).
let rowOfDerivedEdge (nodes: Dictionary<string, struct (int64 * int64)>) (edge: DerivedEdge) : EdgeRow =
    if not (nodes.ContainsKey edge.Src) || not (nodes.ContainsKey edge.Dst) then raise (LoadError "Invalid graph edge")
    if not (nodes.ContainsKey edge.EvidenceId) then raise (LoadError "Edge evidence must identify a node in this snapshot")
    let struct (evidenceStart, evidenceEnd) = nodes.[edge.EvidenceId]
    let line = int64 edge.Line
    if not (evidenceStart <= line && line <= evidenceEnd) then raise (LoadError "Edge evidence line range must lie within its cited node")
    { Src = edge.Src; Dst = edge.Dst; Kind = edge.Kind; Basis = edge.Basis; Confidence = edge.Confidence; EvidenceId = edge.EvidenceId
      LineStart = ValueSome line; LineEnd = ValueSome line; Rule = edge.Rule; Explanation = edge.Explanation }

let private sha256Of (path: string) =
    use stream = File.OpenRead path
    Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant()

let mutable private resolvedArtifact : string option = None
let private resolverLock = obj ()
/// Point DuckDB.NET's P/Invoke at the pinned engine library instead of a bundled one.
/// The resolver can be set once per process; callers on concurrent threads (parallel test
/// classes, the gateway and indexer sharing a process in tests) serialize here.
let installEngine (engine: Engine) =
    if not (File.Exists engine.LibDuckDb) then raise (LoadError("libduckdb is not installed: " + engine.LibDuckDb))
    if sha256Of engine.LibDuckDb <> engine.LibDuckDbSha256 then raise (LoadError "libduckdb checksum differs from the reviewed lock")
    lock resolverLock (fun () ->
        match resolvedArtifact with
        | Some digest when digest <> engine.LibDuckDbSha256 ->
            raise (LoadError "A different DuckDB artifact is already bound to this process")
        | Some _ -> ()
        | None ->
            let bindings =
                typeof<DuckDBConnection>.Assembly.GetReferencedAssemblies()
                |> Array.tryFind (fun a -> a.Name = "DuckDB.NET.Bindings")
                |> Option.map Assembly.Load
                |> Option.defaultWith (fun () -> raise (LoadError "DuckDB.NET.Bindings assembly is not referenced"))
            NativeLibrary.SetDllImportResolver(bindings, fun name _ _ ->
                if name.Contains("duckdb", StringComparison.OrdinalIgnoreCase) then NativeLibrary.Load(engine.LibDuckDb) else IntPtr.Zero)
            resolvedArtifact <- Some engine.LibDuckDbSha256)

let private scalar (connection: DuckDBConnection) (sql: string) : obj =
    use command = connection.CreateCommand()
    command.CommandText <- sql
    command.ExecuteScalar()

let execute (connection: DuckDBConnection) (sql: string) =
    // Extension DDL cannot go through the binding's implicit PREPARE path.
    let mutable result = Unchecked.defaultof<DuckDBResult>
    try
        let state = NativeMethods.Query.DuckDBQuery(connection.NativeConnection, sql, &result)
        if int state <> 0 then raise (LoadError(NativeMethods.Query.DuckDBResultError(&result)))
    finally
        NativeMethods.Query.DuckDBDestroyResult(&result)

let private literal (value: string) = "'" + value.Replace("'", "''") + "'"

/// Open a connection to the verified paired artifact; activate PGQ before parsing it.
let openEngine (engine: Engine) (database: string) =
    installEngine engine
    let settings = DuckDBConnectionStringBuilder()
    settings.["Data Source"] <- database
    // Native pools start during Open, before any SQL can constrain them.
    // Bound both pools from the existing engine budget, including during startup.
    settings.["threads"] <- string engine.Threads
    settings.["async_threads"] <- string engine.Threads
    settings.["memory_limit"] <- engine.MemoryLimit
    settings.["autoinstall_known_extensions"] <- "false"
    settings.["autoload_known_extensions"] <- "false"
    let connection = new DuckDBConnection(settings.ConnectionString)
    try
        connection.Open()
        let version = string (scalar connection "SELECT version()")
        let source = string (scalar connection "SELECT source_id FROM pragma_version()")
        if version <> engine.DuckDbVersion || source <> engine.DuckDbSourceId then
            raise (LoadError "DuckDB version or source differs from the reviewed lock")
        if string (scalar connection "PRAGMA platform") <> "linux_amd64" then
            raise (LoadError "DuckPGQ artifact has not been validated for this platform")
        let builtin = string (scalar connection "SELECT install_path FROM duckdb_extensions() WHERE extension_name='duckpgq' AND loaded")
        if builtin <> "(BUILT-IN)" then raise (LoadError "DuckPGQ must be built into the verified library")
        let loaded = string (scalar connection "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='duckpgq'")
        if loaded <> engine.DuckPgqVersion then raise (LoadError "DuckPGQ version differs from the reviewed lock")
        execute connection "SET active_grammar_extensions=['duckpgq']"
        connection
    with _ ->
        connection.Dispose()
        reraise ()

let verifyGenerationEngine (engine: Engine) (metadata: JsonElement) =
    match metadata.TryGetProperty("engine") with
    | true, recorded when toText canonicalStyle (Raw recorded) = toText canonicalStyle (engineIdentity engine) -> ()
    | _ -> raise (LoadError "Generation engine identity differs from the reviewed lock")

/// Build the generation: metadata table, nodes, edges, property graph, in one transaction.
let build (engine: Engine) (database: string) (metadataText: string) (nodes: NodeRow seq) (edges: EdgeRow seq) =
    if File.Exists database then raise (LoadError "Build a new database generation; do not overwrite a live snapshot")
    use metadata = JsonDocument.Parse metadataText
    if (match metadata.RootElement.TryGetProperty("engine") with true, _ -> true | _ -> false) then
        verifyGenerationEngine engine metadata.RootElement
    let binding = engineIdentity engine
    let metadataText =
        let pairs = [ for p in metadata.RootElement.EnumerateObject() do if p.Name <> "engine" then yield p.Name, Raw p.Value ]
        toText defaultStyle (Obj(pairs @ [ "engine", binding ]))
    let connection = openEngine engine database
    try
        try
            execute connection "BEGIN"
            execute connection "CREATE TABLE metadata (value JSON)"
            execute connection ("INSERT INTO metadata VALUES (" + literal metadataText + ")")
            execute connection "CREATE TABLE nodes (id VARCHAR PRIMARY KEY, repo VARCHAR, revision VARCHAR, path VARCHAR, kind VARCHAR, title VARCHAR, line_start BIGINT, line_end BIGINT, source_url VARCHAR, corpus_layer VARCHAR, classification_rule VARCHAR, text VARCHAR)"
            (use appender = connection.CreateAppender("nodes")
             for node in nodes do
                 let row = appender.CreateRow()
                 for value in node.Values do
                     match value with
                     | null -> row.AppendNullValue() |> ignore
                     | :? string as s -> row.AppendValue(s) |> ignore
                     | :? int64 as i -> row.AppendValue(i) |> ignore
                     | other -> raise (LoadError("Unsupported node value type " + other.GetType().Name))
                 row.EndRow())
            execute connection "CREATE TABLE edges (src VARCHAR, dst VARCHAR, kind VARCHAR, basis VARCHAR, confidence DOUBLE, evidence_id VARCHAR, evidence_line_start BIGINT, evidence_line_end BIGINT, rule VARCHAR, explanation VARCHAR)"
            (use appender = connection.CreateAppender("edges")
             for edge in edges do
                 let row = appender.CreateRow()
                 row.AppendValue(edge.Src).AppendValue(edge.Dst).AppendValue(edge.Kind).AppendValue(edge.Basis).AppendValue(edge.Confidence) |> ignore
                 (if isNull edge.EvidenceId then row.AppendNullValue() else row.AppendValue(edge.EvidenceId)) |> ignore
                 (match edge.LineStart with ValueSome v -> row.AppendValue(v) | ValueNone -> row.AppendNullValue()) |> ignore
                 (match edge.LineEnd with ValueSome v -> row.AppendValue(v) | ValueNone -> row.AppendNullValue()) |> ignore
                 row.AppendValue(edge.Rule).AppendValue(edge.Explanation) |> ignore
                 row.EndRow())
            execute connection "CREATE PROPERTY GRAPH corpus VERTEX TABLES (nodes LABEL resource) EDGE TABLES (edges SOURCE KEY(src) REFERENCES nodes(id) DESTINATION KEY(dst) REFERENCES nodes(id) LABEL relation)"
            execute connection "COMMIT"
        with _ ->
            connection.Dispose()
            File.Delete database
            if File.Exists(database + ".wal") then File.Delete(database + ".wal")
            reraise ()
    finally
        connection.Dispose()
