/// One-for-one port of test_store.py: the restricted PGQ compiler (GrammarTests) and the
/// per-generation Store over a real DuckDB/DuckPGQ generation (GraphTests).
/// Graph tests need RETRIEVAL_TEST_EXTENSIONS and RETRIEVAL_TEST_LIBDUCKDB; lock files, the
/// prompt and grammars are read from RETRIEVAL_TEST_SOURCE_ROOT (default: the repo checkout).
module Retrieval.Native.Tests.QueryTests

open System
open System.IO
open System.Text
open System.Text.Json
open DuckDB.NET.Data
open Xunit
open Retrieval.Native.Canonical
open Retrieval.Native.Query
open Retrieval.Native.Tests.IndexStateTests

// ---- shared helpers (also used by GbnfTests and ExamplesTests) ----

/// Fanion checkout holding config/, docs/PQL-PROMPT.md and grammars/.
let sourceRoot =
    match Environment.GetEnvironmentVariable "RETRIEVAL_TEST_SOURCE_ROOT" with
    | null | "" -> Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../.."))
    | root -> root

let promptFile = Path.Combine(sourceRoot, "docs/PQL-PROMPT.md")

/// The pinned engine from the lock files, or None when this run has no verified extension directory.
let testEngine () : Retrieval.Native.Load.Engine option =
    match Environment.GetEnvironmentVariable "RETRIEVAL_TEST_EXTENSIONS", Environment.GetEnvironmentVariable "RETRIEVAL_TEST_LIBDUCKDB" with
    | null, _ | _, null | "", _ | _, "" -> None
    | extensions, libduckdb ->
        let lock name = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(sourceRoot, "config", name))).RootElement
        let duckpgq, lib = lock "duckpgq-lock.json", lock "libduckdb-lock.json"
        Some { LibDuckDb = libduckdb; LibDuckDbSha256 = lib.GetProperty("libduckdb_so_sha256").GetString(); DuckDbVersion = lib.GetProperty("duckdb_version").GetString(); DuckDbSourceId = lib.GetProperty("duckdb_source_id").GetString()
               DuckPgqVersion = duckpgq.GetProperty("extension_version").GetString()
               Threads = duckdbThreads; MemoryLimit = "768MB" }

/// Skip (xunit dynamic skip) unless the verified engine is configured for this run.
let requireEngine () =
    match testEngine () with
    | Some engine -> engine
    | None -> Assert.Skip "Set RETRIEVAL_TEST_EXTENSIONS and RETRIEVAL_TEST_LIBDUCKDB to the verified engine"; Unchecked.defaultof<_>

/// The ingestion budgets the Python loader enforced (ingest.MAX_*).
let defaultBudgets : Retrieval.Native.Generation.Budgets =
    { MaxOutputBytes = 384L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxTreeEntries = 100_000L; MaxFiles = 20_000
      MaxSourceBytes = 128L * 1024L * 1024L; MaxDerivedEdges = 50_000; MaxCapabilityNodes = 2048; MaxMetadataBytes = 32768 }

let get (value: J) (name: string) = field value name
let tryGet (value: J) (name: string) = match value with Obj pairs -> pairs |> List.tryPick (fun (k, v) -> if k = name then Some v else None) | _ -> None
let int' value name = match field value name with Int i -> i | _ -> failwithf "%s is not an integer" name
let float' value name = match field value name with Float f -> f | Int i -> float i | _ -> failwithf "%s is not a number" name
let items (value: J) = match value with Arr xs -> xs | _ -> failwith "not an array"
let results (response: J) = items (field response "results")
/// Set or append one member, keeping insertion order (dict assignment semantics).
let set' (value: J) (name: string) (item: J) =
    match value with
    | Obj pairs when pairs |> List.exists (fun (k, _) -> k = name) -> Obj (pairs |> List.map (fun (k, v) -> if k = name then k, item else k, v))
    | Obj pairs -> Obj (pairs @ [ name, item ])
    | _ -> failwith "not an object"
let update (value: J) (changes: (string * J) list) = changes |> List.fold (fun acc (k, v) -> set' acc k v) value
let without (value: J) (name: string) = match value with Obj pairs -> Obj (pairs |> List.filter (fun (k, _) -> k <> name)) | _ -> failwith "not an object"
let replaceAt (index: int) (item: J) (values: J list) = values |> List.mapi (fun i v -> if i = index then item else v)

/// snapshot_id = sha256(json.dumps(data without snapshot_id, sort_keys, compact, ensure_ascii=False)).
let sign (data: J) =
    let unsigned = without data "snapshot_id"
    let id, _ = digest canonicalStyle unsigned
    set' unsigned "snapshot_id" (Str id), id

let pgqParameters (seed: J) (edgeKind: string option) : PgqParameters = { Seed = seed; EdgeKind = edgeKind |> Option.map Str; Extra = false }

/// PgqParameters from a request's "parameters" object (unknown members mark Extra).
let pgqParametersOf (parameters: JsonElement) : PgqParameters =
    let seed = match parameters.TryGetProperty "seed" with | true, v -> ofElement v | _ -> Null
    let edgeKind = match parameters.TryGetProperty "edge_kind" with | true, v -> Some(ofElement v) | _ -> None
    let extra = parameters.EnumerateObject() |> Seq.exists (fun p -> p.Name <> "seed" && p.Name <> "edge_kind")
    { Seed = seed; EdgeKind = edgeKind; Extra = extra }

/// Loader rejections surface as LoadError, or wrapped by the parallel validation phase.
let loadError (action: unit -> unit) : string =
    try
        action ()
        failwith "loader accepted an invalid snapshot"
    with
    | Retrieval.Native.Load.LoadError message -> message
    | :? AggregateException as error ->
        match error.Flatten().InnerExceptions |> Seq.tryPick (function Retrieval.Native.Load.LoadError m -> Some m | _ -> None) with
        | Some message -> message
        | None -> reraise ()

let private queryError (action: unit -> unit) : string =
    try
        action ()
        failwith "expected QueryError"
    with QueryError message -> message

let seed64 = rev 'a' 64

// ---- GrammarTests (compile_pgq) ----

[<Fact>]
let ``rejects arbitrary sql and unbounded paths`` () =
    for query in [ "SELECT * FROM read_text('/etc/passwd')"
                   examples.[0] + "; DROP TABLE nodes"
                   examples.[0] + " UNION SELECT 1"
                   examples.[2].Replace("{1,3}", "+")
                   examples.[2].Replace("{1,3}", "{1,9}")
                   examples.[0].Replace("b.path", "read_text('/etc/passwd')") ] do
        queryError (fun () -> compilePgq query (pgqParameters (Str seed64) None) 20L 0L None |> ignore) |> ignore

[<Fact>]
let ``identifiers are not sql strings`` () =
    for seed in [ Str "a' OR TRUE --"; Str "abc"; Int 42L; Str (rev 'a' 63 + "G") ] do
        queryError (fun () -> compilePgq examples.[0] (pgqParameters seed None) 20L 0L None |> ignore) |> ignore

[<Fact>]
let ``provenance is only a fixed one-hop projection`` () =
    let query = examples.[0].Replace("e.kind)", "e.kind, e.confidence, e.explanation)")
    Assert.Contains("e.confidence AS confidence", compilePgq query (pgqParameters (Str seed64) None) 10L 0L None)
    for changed in [ query.Replace("e.confidence", "e.confidence + 1")
                     query.Replace("e.explanation", "e.secret")
                     examples.[2].Replace("path_length(p)", "e.confidence") ] do
        queryError (fun () -> compilePgq changed (pgqParameters (Str seed64) None) 10L 0L None |> ignore) |> ignore
    Assert.Contains("unavailable", queryError (fun () -> compilePgq query (pgqParameters (Str seed64) None) 10L 0L (Some [ "kind" ]) |> ignore))

// ---- GraphTests fixture ----

/// The three-node snapshot of test_store.GraphTests.setUp, loaded into a generation and opened as a Store.
type Graph(engine: Retrieval.Native.Load.Engine) =
    let temp = Path.Combine(Path.GetTempPath(), "query-tests-" + Guid.NewGuid().ToString("N"))
    do Directory.CreateDirectory temp |> ignore
    let ids = [ rev 'a' 64; rev 'b' 64; rev 'c' 64 ]
    let revision = rev 'e' 40
    let node number sourceId : J =
        Obj [ "id", Str sourceId; "repo", Str "FidelityFramework/test"; "revision", Str revision; "path", Str (sprintf "%d.md" number)
              "kind", Str "file"; "title", Str (sprintf "File %d" number); "line_start", Int 1L; "line_end", Int 2L
              "text", Str (sprintf "Content %d\nEvidence\n" number); "corpus_layer", Str (if number = 0 then "docs" else "implementation")
              "classification_rule", Str "test"; "source_url", Str (sprintf "https://forge.example/test/src/commit/%s/%d.md#L1-L2" revision number) ]
    let data : J =
        Obj [ "format_version", Int 1L; "extractor_version", Str "test"
              "repositories", Arr [ Obj [ "name", Str "FidelityFramework/test"; "commit", Str revision; "source_url", Str "https://forge.example/test" ] ]
              "nodes", Arr (ids |> List.mapi node)
              "edges", Arr [ Obj [ "src", Str ids.[0]; "dst", Str ids.[1]; "kind", Str "links_to" ]; Obj [ "src", Str ids.[1]; "dst", Str ids.[2]; "kind", Str "contains" ] ] ]
    let signed, initialSnapshot = sign data
    let source = Path.Combine(temp, "snapshot.json")
    do File.WriteAllText(source, toText defaultStyle signed)
    let database = Path.Combine(temp, "test.duckdb")
    do Retrieval.Native.Generation.load engine defaultBudgets source database |> ignore
    let mutable store = Store(database, engine, promptFile)
    let mutable snapshot = initialSnapshot
    member _.Engine = engine
    member _.Root = temp
    member _.Ids = ids
    member _.Revision = revision
    member _.Source = source
    /// The published snapshot document (including snapshot_id), like reading the source file back.
    member _.Data = signed
    member _.Store = store
    member _.Snapshot = snapshot
    member _.Load(data: J, name: string) =
        let signed, id = sign data
        let path = Path.Combine(temp, name + ".json")
        File.WriteAllText(path, toText defaultStyle signed)
        Retrieval.Native.Generation.load engine defaultBudgets path (Path.Combine(temp, name + ".duckdb")), id
    /// Build and open a replacement generation (GraphTests.publish).
    member this.Publish(data: J) =
        let _, id = this.Load(data, "next")
        let replacement = Store(Path.Combine(temp, "next.duckdb"), engine, promptFile)
        store.Close()
        store <- replacement
        snapshot <- id
    member _.Pgq(query: string, seed: string, edgeKind: string option, ?limit: int64, ?offset: int64) =
        store.Pgq(query, pgqParameters (Str seed) edgeKind, snapshot, defaultArg limit 25L, defaultArg offset 0L)
    member _.Find(query: string, ?repo: string, ?limit: int64, ?corpusLayer: string) = store.Find(query, snapshot, repo, defaultArg limit 10L, corpusLayer)
    member _.Sources(ids: string list, ?maxChars: int64) = store.Sources(ids, snapshot, defaultArg maxChars 8000L)
    interface IDisposable with
        member _.Dispose() =
            (try store.Close() with _ -> ())
            try Directory.Delete(temp, true) with _ -> ()

let private ids (response: J) = results response |> List.map (fun row -> str' row "id")
let private nodes (data: J) = items (get data "nodes")
let private edges (data: J) = items (get data "edges")
let private repositories (data: J) = items (get data "repositories")
let private sqlLiteral (value: string) = "'" + value.Replace("'", "''") + "'"

// ---- GraphTests ----

[<Fact>]
let ``snapshot reader uses ingestion output budget before database creation`` () =
    use g = new Graph(requireEngine ())
    let size = FileInfo(g.Source).Length
    let accepted = Path.Combine(g.Root, "within-output-budget.duckdb")
    Retrieval.Native.Generation.load g.Engine { defaultBudgets with MaxOutputBytes = size } g.Source accepted |> ignore
    let store = Store(accepted, g.Engine, promptFile)
    try Assert.Equal(g.Snapshot, str' (store.Schema()) "snapshot_id") finally store.Close()
    let refused = Path.Combine(g.Root, "outside-output-budget.duckdb")
    Assert.Equal("Snapshot exceeds ingestion output budget",
                 loadError (fun () -> Retrieval.Native.Generation.load g.Engine { defaultBudgets with MaxOutputBytes = size - 1L } g.Source refused |> ignore))
    Assert.False(File.Exists refused)

[<Fact>]
let ``exact specialized evidence beats central mentions but authority breaks relevance ties`` () =
    use g = new Graph(requireEngine ())
    let specialized =
        update (nodes g.Data).[0] [ "path", Str "src/Transport.generated.fs"; "title", Str "Transport generated API"; "text", Str "shared feature\nGenerated transport evidence\n" ]
    let central =
        update (nodes g.Data).[1] [ "repo", Str "Architecture/central"; "path", Str "docs/Overview.md"; "title", Str "Overview"
                                    "text", Str "src/Transport.generated.fs Transport generated API shared feature\nOverview only\n" ]
    let generated = set' (repositories g.Data).[0] "policy" (Obj [ "role", Str "generated"; "authority", Int 0L; "include_generated", Arr [ Str "src/*.generated.fs" ] ])
    let centralRepo = Obj [ "name", Str "Architecture/central"; "commit", Str g.Revision; "source_url", Str "https://forge.example/central"; "policy", Obj [ "role", Str "central"; "authority", Int 100L ] ]
    g.Publish(update g.Data [ "nodes", Arr (nodes g.Data |> replaceAt 0 specialized |> replaceAt 1 central); "repositories", Arr [ generated; centralRepo ] ])
    for (query, reason, score) in [ "src/Transport.generated.fs", "exact_path", 100.0; "Transport.generated.fs", "exact_name", 90.0; "Transport generated API", "exact_name", 90.0 ] do
        let rows = results (g.Find query)
        Assert.Equal<string list>(g.Ids |> List.take 2, rows |> List.map (fun row -> str' row "id"))
        Assert.Equal((reason, score, "generated"), (str' rows.[0] "reason", float' rows.[0] "score", str' rows.[0] "role"))
        Assert.Equal(("text_substring", 21.0, 100L), (str' rows.[1] "reason", float' rows.[1] "score", int' rows.[1] "authority"))
    let tied = g.Find("shared feature", limit = 1L)
    Assert.Equal(g.Ids.[1], str' (results tied).[0] "id")
    Assert.True(bool' tied "truncated")
    Assert.Equal<string list>([ g.Ids.[0] ], ids (g.Find("shared feature", repo = "FidelityFramework/test", corpusLayer = "docs")))
    Assert.Empty(results (g.Find "%"))
    Assert.Empty(results (g.Find "_"))
    let evidence = (items (get (g.Sources [ g.Ids.[0] ]) "sources")).[0]
    Assert.Contains("Generated transport evidence", str' evidence "text")
    Assert.Equal("src/Transport.generated.fs", str' evidence "path")

[<Fact>]
let ``real pgq returns inferred edge provenance and capability seeds`` () =
    use g = new Graph(requireEngine ())
    let capabilityId = rev 'f' 64
    let capability =
        update (nodes g.Data).[0] [ "id", Str capabilityId; "path", Str ""; "title", Str "native-memory"; "kind", Str "capability"
                                    "line_start", Int 0L; "line_end", Int 0L; "text", Str "Native memory capability"; "corpus_layer", Str "capability" ]
    let inferred =
        Obj [ "src", Str g.Ids.[0]; "dst", Str capabilityId; "kind", Str "suggests_capability"; "basis", Str "inferred"; "confidence", Float 0.65
              "evidence_id", Str g.Ids.[0]; "evidence_line_start", Int 2L; "evidence_line_end", Int 2L; "rule", Str "api-memory-hint"
              "explanation", Str "A source pattern suggests this capability; it does not prove support." ]
    g.Publish(update g.Data [ "nodes", Arr (nodes g.Data @ [ capability ]); "edges", Arr (edges g.Data @ [ inferred ]) ])
    let query =
        "MATCH (a:resource WHERE a.id = $seed)-[e:relation WHERE e.kind = $edge_kind]->(b:resource) COLUMNS (b.id, b.kind, e.kind, "
        + String.Join(", ", edgeProvenanceFields |> List.map (fun f -> "e." + f)) + ")"
    let rows = results (g.Pgq(query, g.Ids.[0], Some "suggests_capability"))
    Assert.Equal(1, rows.Length)
    Assert.Equal((capabilityId, "capability", "suggests_capability"), (str' rows.[0] "id", str' rows.[0] "kind", str' rows.[0] "edge_kind"))
    for name in edgeProvenanceFields do
        Assert.Equal(get inferred name, get rows.[0] name)
    let incoming = query.Replace("-[e:relation", "<-[e:relation").Replace("]->(b:", "]-(b:")
    Assert.Equal(g.Ids.[0], str' (results (g.Pgq(incoming, capabilityId, Some "suggests_capability"))).[0] "id")
    let found = results (g.Find("native-memory", corpusLayer = "capability"))
    Assert.Equal<string list>([ capabilityId ], found |> List.map (fun row -> str' row "id"))
    Assert.Equal(("exact_name", "component"), (str' found.[0] "reason", str' found.[0] "role"))
    Assert.Equal(str' capability "text", str' (items (get (g.Sources [ capabilityId ]) "sources")).[0] "text")
    let schema = g.Store.Schema()
    Assert.Equal<string list>([ "contains"; "links_to"; "suggests_capability" ], strings schema "edge_kinds")
    Assert.Contains("references_project", strings schema "supported_edge_kinds")
    Assert.True(bool' (get schema "edge_provenance") "available")
    Assert.Contains("not a calibrated probability", str' (get schema "edge_provenance") "confidence")
    Assert.Empty(results (g.Pgq(query, g.Ids.[0], Some "references_project")))
    let structural = (results (g.Pgq(query, g.Ids.[0], Some "links_to"))).[0]
    Assert.Equal("structural", str' structural "basis")
    Assert.Equal(Null, get structural "evidence_id")

[<Fact>]
let ``structural graph without provenance columns retains its engine binding`` () =
    use g = new Graph(requireEngine ())
    let database = Path.Combine(g.Root, "legacy.duckdb")
    let nodeFieldsWithText = nodeFields @ [ "text" ]
    let execute (connection: DuckDBConnection) (sql: string) = Retrieval.Native.Load.execute connection sql
    let metadata = Obj [ yield "engine", Retrieval.Native.Load.engineIdentity g.Engine
                         for (k, v) in (match g.Data with Obj pairs -> pairs | _ -> []) do if k <> "nodes" && k <> "edges" then yield k, v ]
    let value (node: J) (name: string) = match get node name with Str s -> sqlLiteral s | Int i -> string i | _ -> "NULL"
    begin
        let connection = Retrieval.Native.Load.openEngine g.Engine database
        try
            execute connection "CREATE TABLE metadata (value JSON)"
            execute connection ("INSERT INTO metadata VALUES (" + sqlLiteral (toText defaultStyle metadata) + ")")
            execute connection "CREATE TABLE nodes (id VARCHAR PRIMARY KEY, repo VARCHAR, revision VARCHAR, path VARCHAR, kind VARCHAR, title VARCHAR, line_start BIGINT, line_end BIGINT, source_url VARCHAR, corpus_layer VARCHAR, classification_rule VARCHAR, text VARCHAR)"
            for node in nodes g.Data do
                execute connection ("INSERT INTO nodes VALUES (" + String.Join(",", nodeFieldsWithText |> List.map (value node)) + ")")
            execute connection "CREATE TABLE edges (src VARCHAR, dst VARCHAR, kind VARCHAR)"
            for edge in edges g.Data do
                execute connection ("INSERT INTO edges VALUES (" + String.Join(",", [ "src"; "dst"; "kind" ] |> List.map (value edge)) + ")")
            execute connection "CREATE PROPERTY GRAPH corpus VERTEX TABLES (nodes LABEL resource) EDGE TABLES (edges SOURCE KEY(src) REFERENCES nodes(id) DESTINATION KEY(dst) REFERENCES nodes(id) LABEL relation)"
        finally
            connection.Dispose()
    end
    let legacy = Store(database, g.Engine, promptFile)
    try
        let schema = legacy.Schema()
        Assert.Equal<string list>([ "kind" ], strings schema "edge_properties")
        Assert.False(bool' (get schema "edge_provenance") "available")
        Assert.Equal(g.Ids.[1], str' (results (legacy.Pgq(examples.[0], pgqParameters (Str g.Ids.[0]) None, g.Snapshot, 25L, 0L))).[0] "id")
        Assert.Empty(results (legacy.Pgq(examples.[1], pgqParameters (Str g.Ids.[1]) (Some "generated_by"), g.Snapshot, 25L, 0L)))
        Assert.Equal("component", str' (results (legacy.Find("Evidence", g.Snapshot, None, 10L, None))).[0] "role")
        Assert.Equal(str' (nodes g.Data).[0] "text", str' (items (get (legacy.Sources([ g.Ids.[0] ], g.Snapshot, 8000L)) "sources")).[0] "text")
        for name in edgeProvenanceFields do
            let query = examples.[0].Replace("e.kind)", "e.kind, e." + name + ")")
            Assert.Contains("unavailable", queryError (fun () -> legacy.Pgq(query, pgqParameters (Str g.Ids.[0]) None, g.Snapshot, 25L, 0L) |> ignore))
    finally
        legacy.Close()

[<Fact>]
let ``parallel edge provenance has stable offset pages`` () =
    use g = new Graph(requireEngine ())
    let mentions =
        [ for number in List.rev [ 0 .. 23 ] ->
            Obj [ "src", Str g.Ids.[0]; "dst", Str g.Ids.[1]; "kind", Str "mentions"; "basis", Str "inferred"; "confidence", Float 0.5
                  "evidence_id", Str g.Ids.[0]; "evidence_line_start", Int 1L; "evidence_line_end", Int 1L
                  "rule", Str (sprintf "mention-rule-%02d" number); "explanation", Str (sprintf "Distinct supporting observation %02d." number) ] ]
    g.Publish(set' g.Data "edges" (Arr (edges g.Data @ mentions)))
    let query = "MATCH (a:resource WHERE a.id = $seed)-[e:relation WHERE e.kind = $edge_kind]->(b:resource) COLUMNS (b.id, e.rule, e.explanation, e.evidence_id)"
    let complete = results (g.Pgq(query, g.Ids.[0], Some "mentions", limit = 100L))
    Assert.Equal<string list>([ for n in 0 .. 23 -> sprintf "mention-rule-%02d" n ], complete |> List.map (fun row -> str' row "rule"))
    let mutable pages = []
    let mutable offset = Some 0L
    while offset.IsSome do
        let page = g.Pgq(query, g.Ids.[0], Some "mentions", limit = 5L, offset = offset.Value)
        let repeated = g.Pgq(query, g.Ids.[0], Some "mentions", limit = 5L, offset = offset.Value)
        Assert.Equal(page, repeated)
        pages <- pages @ results page
        offset <- (match get page "next_offset" with Int n -> Some n | _ -> None)
    Assert.Equal<J list>(complete, pages)
    Assert.Equal(24, (pages |> List.map (fun row -> str' row "rule") |> List.distinct).Length)

[<Fact>]
let ``invalid repository policy and edge provenance fail before database creation`` () =
    use g = new Graph(requireEngine ())
    let provenance =
        Obj [ "src", Str g.Ids.[0]; "dst", Str g.Ids.[1]; "kind", Str "mentions"; "basis", Str "inferred"; "confidence", Float 0.5
              "evidence_id", Str g.Ids.[0]; "evidence_line_start", Int 1L; "evidence_line_end", Int 2L; "rule", Str "literal-name"
              "explanation", Str "A name appears in this source." ]
    let badPolicies : J list =
        [ Null; Arr []; Obj [ "role", Str "truth" ]; Obj [ "authority", Bool true ]; Obj [ "authority", Int 101L ]
          Obj [ "authority", Float nan ]; Obj [ "capabilities", Arr [ Str "bad slug" ] ]
          Obj [ "include_generated", Arr [ Str "../secrets/*" ] ]; Obj [ "unexpected", Int 1L ] ]
    let badEdges : J option list =
        [ None; Some(Obj [ "confidence", Float nan ]); Some(Obj [ "confidence", Float infinity ])
          Some(Obj [ "confidence", Bool true ]); Some(Obj [ "confidence", Float -0.01 ]); Some(Obj [ "confidence", Float 1.01 ])
          Some(Obj [ "confidence", Str "0.5" ]); Some(Obj [ "basis", Str "proven" ]); Some(Obj [ "basis", Str "structural" ])
          Some(Obj [ "evidence_id", Str (rev 'f' 64) ]); Some(Obj [ "evidence_id", Arr [] ]); Some(Obj [ "evidence_line_start", Bool true ])
          Some(Obj [ "evidence_line_start", Int 0L ]); Some(Obj [ "evidence_line_start", Int 2L; "evidence_line_end", Int 1L ])
          Some(Obj [ "evidence_line_end", Int 3L ]); Some(Obj [ "rule", Str "" ]); Some(Obj [ "rule", Str (rev 'r' 121) ])
          Some(Obj [ "explanation", Str (rev 'x' 601) ]); Some(Obj [ "explanation", Str "line\nbreak" ]); Some(Obj [ "unrecognized", Str "value" ]) ]
    let cases =
        [ for policy in badPolicies -> set' g.Data "repositories" (Arr [ set' (repositories g.Data).[0] "policy" policy ]) ]
        @ [ for changed in badEdges ->
                let edge = match changed with None -> Obj [ "src", Str g.Ids.[0]; "dst", Str g.Ids.[1]; "kind", Str "mentions" ] | Some c -> update provenance (match c with Obj pairs -> pairs | _ -> [])
                set' g.Data "edges" (Arr (edges g.Data @ [ edge ])) ]
    cases |> List.iteri (fun index data ->
        let name = sprintf "invalid-%d" index
        loadError (fun () -> g.Load(data, name) |> ignore) |> ignore
        Assert.False(File.Exists(Path.Combine(g.Root, name + ".duckdb")), name))

[<Fact>]
let ``64 repository policies fit the schema response budget`` () =
    use g = new Graph(requireEngine ())
    let extra =
        [ for number in 0 .. 62 ->
            Obj [ "name", Str (sprintf "Corpus/component-%02d" number); "commit", Str g.Revision; "source_url", Str (sprintf "https://forge.example/Corpus/component-%02d" number)
                  "policy", Obj [ "role", Str "component"; "authority", Int (int64 number); "capabilities", Arr [ Str "native-windowing"; Str "native-signals" ]
                                  "include_generated", Arr [ Str "Bindings/*.clef" ]; "include_paths", Arr [ Str "src/*"; Str "docs/*" ]; "exclude_paths", Arr [ Str "docs/obsolete/*" ] ] ] ]
    let data = set' g.Data "repositories" (Arr (repositories g.Data @ extra))
    let metadata = Obj [ for (k, v) in (match data with Obj pairs -> pairs | _ -> []) do if k <> "nodes" && k <> "edges" then yield k, v ]
    Assert.True(Encoding.UTF8.GetByteCount(toText defaultStyle metadata) > 16384)
    g.Publish data
    let schema = g.Store.Schema()
    Assert.Equal(64, (items (get schema "repositories")).Length)
    Assert.True(Encoding.UTF8.GetByteCount(toText orderedStyle schema) < 65536)
    let oversized = set' data "operator_note" (Str (rev 'x' 32768))
    Assert.Contains("metadata exceeds", loadError (fun () -> g.Load(oversized, "oversized-metadata") |> ignore))

[<Fact>]
let ``build and serving connections use eight threads`` () =
    use g = new Graph(requireEngine ())
    let report = Retrieval.Native.Generation.load g.Engine defaultBudgets g.Source (Path.Combine(g.Root, "thread-check.duckdb"))
    Assert.Equal(8, report.Threads)
    Assert.Equal(g.Snapshot, report.SnapshotId)
    Assert.Equal(8L, int' (get (g.Store.Schema()) "limits") "query_threads")
    // DuckDB.NET shares one database instance per file, so a second connection observes the
    // serving connection's global settings (threads, lock_configuration, enable_external_access).
    use peer = new DuckDBConnection("Data Source=" + Path.Combine(g.Root, "test.duckdb"))
    peer.Open()
    let setting (name: string) = (use c = peer.CreateCommand() in c.CommandText <- sprintf "SELECT current_setting('%s')" name; c.ExecuteScalar())
    Assert.Equal(8L, Convert.ToInt64(setting "threads"))
    Assert.True(Convert.ToBoolean(setting "lock_configuration"))
    Assert.False(Convert.ToBoolean(setting "enable_external_access"))

[<Fact>]
let ``persisted graph schema bound paths and sources`` () =
    use g = new Graph(requireEngine ())
    Assert.Equal("corpus", str' (items (get (g.Store.Schema()) "graph_schema")).[0] "property_graph")
    let rows = results (g.Pgq(examples.[2], g.Ids.[0], None))
    Assert.Equal<(string * int64) list>([ g.Ids.[1], 1L; g.Ids.[2], 2L ], rows |> List.map (fun r -> str' r "id", int' r "hops"))
    let rows = results (g.Pgq(examples.[1], g.Ids.[1], Some "links_to"))
    Assert.Equal(g.Ids.[0], str' rows.[0] "id")
    let sources = items (get (g.Sources([ g.Ids.[2] ], maxChars = 8L)) "sources")
    Assert.Equal("Content ", str' sources.[0] "text")
    Assert.True(bool' sources.[0] "truncated")
    Assert.Equal(g.Revision, str' sources.[0] "revision")

[<Fact>]
let ``find snapshot paging and external access`` () =
    use g = new Graph(requireEngine ())
    Assert.Equal(3, (results (g.Find "Evidence")).Length)
    Assert.Equal(1, (results (g.Find("Evidence", corpusLayer = "docs"))).Length)
    let page = g.Pgq(examples.[2], g.Ids.[0], None, limit = 1L)
    Assert.Equal(1L, int' page "next_offset")
    queryError (fun () -> g.Store.Find("Evidence", rev 'f' 64, None, 10L, None) |> ignore) |> ignore
    // The serving database is locked down: external access stays off and configuration is locked.
    use peer = new DuckDBConnection("Data Source=" + Path.Combine(g.Root, "test.duckdb"))
    peer.Open()
    let run (sql: string) = (use c = peer.CreateCommand() in c.CommandText <- sql; c.ExecuteNonQuery() |> ignore)
    Assert.ThrowsAny<Exception>(fun () -> run "SELECT * FROM read_text('/etc/passwd')") |> ignore
    Assert.ThrowsAny<Exception>(fun () -> run "SET enable_external_access=true") |> ignore

[<Fact>]
let ``hop limits are respected`` () =
    use g = new Graph(requireEngine ())
    for (bounds, expected) in [ "{1,1}", [ g.Ids.[1] ]; "{1,2}", [ g.Ids.[1]; g.Ids.[2] ] ] do
        Assert.Equal<string list>(expected, ids (g.Pgq(examples.[2].Replace("{1,3}", bounds), g.Ids.[0], None)))
    queryError (fun () -> g.Pgq(examples.[2].Replace("{1,3}", "{2,2}"), g.Ids.[0], None) |> ignore) |> ignore

[<Fact>]
let ``native connections activate grammar independently without installation`` () =
    use g = new Graph(requireEngine ())
    let _, snapshot = g.Load(g.Data, "connection-matrix")
    let path = Path.Combine(g.Root, "connection-matrix.duckdb")
    use first = Retrieval.Native.Load.openEngine g.Engine path
    use peer = new DuckDBConnection("Data Source=" + path)
    peer.Open()
    let query = compilePgq examples.[0] (pgqParameters (Str g.Ids.[0]) None) 25L 0L None
    let read (c: DuckDBConnection) =
        use command = c.CreateCommand()
        command.CommandText <- query
        use rows = command.ExecuteReader()
        let mutable count = 0
        while rows.Read() do count <- count + 1
        count
    Assert.Equal(1, read first)
    let rejected = Assert.ThrowsAny<Exception>(fun () -> read peer |> ignore)
    Assert.Contains("Parser Error", rejected.Message)
    use second = Retrieval.Native.Load.openEngine g.Engine path
    Assert.Equal(1, read second)
    for setting in [ "autoinstall_known_extensions"; "autoload_known_extensions"; "allow_unsigned_extensions" ] do
        use command = second.CreateCommand()
        command.CommandText <- sprintf "SELECT current_setting('%s')" setting
        Assert.False(Convert.ToBoolean(command.ExecuteScalar()))
    Assert.NotEmpty(snapshot)

[<Fact>]
let ``native artifact source and extension identity mismatches are refused`` () =
    use g = new Graph(requireEngine ())
    let path = Path.Combine(g.Root, "wrong-engine.duckdb")
    let rejected expected engine =
        let message = loadError(fun () -> use connection = Retrieval.Native.Load.openEngine engine path in ())
        Assert.Contains(expected, message)
    rejected "checksum" { g.Engine with LibDuckDbSha256 = rev '0' 64 }
    rejected "version or source" { g.Engine with DuckDbSourceId = "wrong-source" }
    rejected "version or source" { g.Engine with DuckDbVersion = "wrong-version" }
    rejected "DuckPGQ version" { g.Engine with DuckPgqVersion = "wrong-extension" }
    let alternate = Path.Combine(g.Root, "different-artifact.so")
    File.WriteAllText(alternate, "different artifact with a matching lock digest")
    let digest = Convert.ToHexString(Security.Cryptography.SHA256.HashData(File.ReadAllBytes alternate)).ToLowerInvariant()
    rejected "already bound" { g.Engine with LibDuckDb = alternate; LibDuckDbSha256 = digest }

[<Fact>]
let ``published graph from another engine cannot be opened`` () =
    use g = new Graph(requireEngine ())
    let _, snapshot = g.Load(g.Data, "wrong-generation-engine")
    let path = Path.Combine(g.Root, "wrong-generation-engine.duckdb")
    do
        use connection = Retrieval.Native.Load.openEngine g.Engine path
        Retrieval.Native.Load.execute connection
            "UPDATE metadata SET value=json_merge_patch(value, '{\"engine\":{\"libduckdb_sha256\":\"wrong-artifact\"}}')"
    let message = loadError(fun () -> let store = Store(path, g.Engine, promptFile) in store.Close())
    Assert.Contains("Generation engine identity differs", message)
    Assert.NotEmpty(snapshot)
