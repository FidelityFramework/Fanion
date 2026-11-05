/// One-for-one port of test_gbnf.py: the request-scoped grammar builder, its static
/// artifacts under grammars/, and the acceptance corpus for the native llama.cpp validator
/// (RETRIEVAL_GBNF_VALIDATOR, skipped when unset, exactly as the Python test skipped).
module Fanion.Tests.GbnfTests

open System
open System.Diagnostics
open System.IO
open System.Reflection
open System.Text
open Fidelity.Data.JSON
open Xunit
open Fanion.Canonical
open Fanion.Gbnf
open Fanion.Tests.IndexStateTests
open Fanion.Tests.QueryTests

let SEED = rev 'a' 64
let SNAPSHOT = rev 'd' 64
let schema : SchemaContext =
    { SnapshotId = SNAPSHOT; Repositories = [ "FidelityFramework/Composer" ]; CorpusLayers = [ "docs"; "architecture" ]
      EdgeKinds = [ "contains"; "chunks_of"; "links_to" ]; NodeProperties = nodeFields; EdgeProperties = [] }
let PLAIN = "MATCH (a:resource WHERE a.id = $seed)-[e:relation]->(b:resource) COLUMNS (b.id, b.path, b.kind, e.kind)"
let FILTERED = PLAIN.Replace("[e:relation]", "[e:relation WHERE e.kind = $edge_kind]")
let SHORTEST = "MATCH p = ANY SHORTEST (a:resource WHERE a.id = $seed)-[e:relation]->{1,3}(b:resource) COLUMNS (b.id, b.path, path_length(p))"

type Case = { Name: string; Input: string; Valid: bool }
type Group = { Group: string; Grammar: string; Cases: Case list }

/// json.dumps(value, ensure_ascii=False)
let private dumps (value: J) = toText orderedStyle value

/// The acceptance corpus of test_gbnf.native_corpus(): grammar plus (input, expected validity) pairs.
let nativeCorpus () : Group list =
    let groups = ResizeArray<Group>()
    let group name grammar (build: ResizeArray<Case> -> unit) =
        let cases = ResizeArray<Case>()
        build cases
        groups.Add { Group = name; Grammar = grammar; Cases = List.ofSeq cases }
    let add (cases: ResizeArray<Case>) name (value: J) valid = cases.Add { Name = name; Input = dumps value; Valid = valid }
    let addRaw (cases: ResizeArray<Case>) name input valid = cases.Add { Name = name; Input = input; Valid = valid }
    let seedOnly = Obj [ "seed", Str SEED ]
    let parametersFor (pattern: string) = Obj ([ "seed", Str SEED ] @ (if pattern.Contains "$edge_kind" then [ "edge_kind", Str "links_to" ] else []))
    let find = Obj [ "action", Str "find"; "query", Str "docs/Architecture.md"; "snapshot_id", Str SNAPSHOT ]
    let sources = Obj [ "action", Str "sources"; "ids", Arr [ Str SEED ]; "snapshot_id", Str SNAPSHOT ]
    let pgq = Obj [ "action", Str "pgq"; "query", Str PLAIN; "parameters", seedOnly; "snapshot_id", Str SNAPSHOT ]
    let rangeCases (cases: ResizeArray<Case>) (wrap: J -> J) =
        let range first last = "MATCH (a:resource WHERE a.id = $seed)<-[e:relation WHERE e.kind = $edge_kind]-(b:resource WHERE b.line_start <= " + last + " AND b.line_end >= " + first + ") COLUMNS (b.id, b.path, b.line_start, b.line_end, e.kind)"
        let request query kind = wrap (Obj [ "query", Str query; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str kind ] ])
        for value in [ "1"; "9"; "10"; "999999999"; "1000000000"; "2000000000"; "2100000000"; "2140000000"; "2147000000"; "2147400000"; "2147480000"; "2147483000"; "2147483600"; "2147483640"; "2147483647" ] do
            add cases ("chunk-range-boundary-" + value) (request (range value value) "chunks_of") true
        add cases "chunk-range-full-int32" (request (range "1" "2147483647") "chunks_of") true
        for value in [ "0"; "-1"; "+1"; "01"; "2147483648"; "2147483650"; "2147483700"; "2147484000"; "2147490000"; "2147500000"; "2148000000"; "2150000000"; "2200000000"; "3000000000"; "99999999999"; "1.5"; "1e2"; "'1'"; "$first" ] do
            add cases ("chunk-range-last-invalid-" + value) (request (range "1" value) "chunks_of") false
            add cases ("chunk-range-first-invalid-" + value) (request (range value "2147483647") "chunks_of") false
        let query = range "181" "240"
        add cases "chunk-range-overlap" (request query "chunks_of") true
        add cases "chunk-range-wrong-edge" (request query "links_to") false
        for name, changed in [ "outgoing", query.Replace(")<-[", ")-[").Replace("]-(", "]->(")
                               "multihop", query.Replace("]-(", "]-{1,2}(")
                               "shortest", query.Replace("MATCH ", "MATCH p = ANY SHORTEST ")
                               "arbitrary-where", query.Replace(" AND b.line_end >= 181", " OR TRUE")
                               "sql-suffix", query + "; SELECT 1"
                               "operator", query.Replace("b.line_start <=", "b.line_start <")
                               "missing-bound", query.Replace(" AND b.line_end >= 181", "") ] do
            add cases ("chunk-range-" + name) (request changed "chunks_of") false
        let plain = query.Replace(" WHERE e.kind = $edge_kind", "")
        add cases "chunk-range-unfiltered" (wrap (Obj [ "query", Str plain; "parameters", seedOnly ])) false
    let asApi value =
        match value with
        | Obj fields -> Obj ([ "action", Str "pgq" ] @ fields @ [ "snapshot_id", Str SNAPSHOT ])
        | _ -> failwith "expected request object"
    group "api-static" (buildApiGrammar None None) (fun api ->
        rangeCases api asApi
        add api "schema" (Obj [ "action", Str "schema" ]) true
        add api "extra-member" (Obj [ "action", Str "schema"; "sql", Str "SELECT 1" ]) false
        add api "find" find true
        for (value, valid) in [ Int 0L, false; Int 1L, true; Int 9L, true; Int 10L, true; Int 49L, true; Int 50L, true; Int 51L, false; Int -1L, false; Float 1.5, false; Bool true, false ] do
            add api ("find-limit-" + dumps value) (set' find "limit" value) valid
        for (text, valid) in [ "", false; rev 'x' 256, true; rev 'x' 257, false; "quote\"slash\\tab\t", true; "Clef → 🐦", true ] do
            add api ("find-query-" + string text.Length) (set' find "query" (Str text)) valid
        let rawFind = dumps find
        addRaw api "invalid-json-escape" (rawFind.Replace("docs/Architecture.md", @"bad\q")) false
        addRaw api "literal-newline-in-string" (rawFind.Replace("docs/Architecture.md", "bad\nline")) false
        addRaw api "escaped-newline" (rawFind.Replace("docs/Architecture.md", @"good\nline")) true
        // Canonical generator emits non-BMP text directly instead of surrogate escapes.
        addRaw api "escaped-surrogate-pair" (rawFind.Replace("docs/Architecture.md", @"\ud83d\udc26")) false
        addRaw api "literal-nonbmp" (rawFind.Replace("docs/Architecture.md", "🐦")) true
        for escape in [ @"\ud800"; @"\udfff" ] do
            addRaw api ("lone-surrogate-" + escape) (rawFind.Replace("docs/Architecture.md", escape)) false
        add api "unknown-repo" (set' find "repo" (Str "other/repo")) false
        add api "unknown-layer" (set' find "corpus_layer" (Str "anything")) false
        add api "known-repo-layer" (update find [ "repo", Str "FidelityFramework/Composer"; "corpus_layer", Str "docs" ]) true
        for count in [ 0; 1; 10; 11 ] do
            add api ("sources-count-" + string count) (set' sources "ids" (Arr (List.replicate count (Str SEED)))) (1 <= count && count <= 10)
        for (value, valid) in [ Int 0L, false; Int 1L, true; Int 9999L, true; Int 10000L, true; Int 15999L, true; Int 16000L, true; Int 16001L, false; Int -1L, false; Float 1.5, false; Bool true, false ] do
            add api ("source-budget-" + dumps value) (set' sources "max_chars" value) valid
        for value in [ rev 'a' 63; rev 'a' 65; rev 'A' 64; rev 'g' 64 ] do
            add api ("bad-id-" + value.Substring(0, 2) + string value.Length) (set' sources "ids" (Arr [ Str value ])) false
        for (value, valid) in [ Int 0L, false; Int 1L, true; Int 99L, true; Int 100L, true; Int 101L, false; Int -1L, false; Float 1.5, false; Bool true, false ] do
            add api ("pgq-limit-" + dumps value) (set' pgq "limit" value) valid
        for (value, valid) in [ Int -1L, false; Int 0L, true; Int 1L, true; Int 9999L, true; Int 10000L, true; Int 10001L, false; Float 1.5, false; Bool true, false ] do
            add api ("pgq-offset-" + dumps value) (set' pgq "offset" value) valid
        addRaw api "leading-zero" ((dumps (set' pgq "limit" (Int 10L))).Replace("\"limit\": 10", "\"limit\": 010")) false
        addRaw api "duplicate-member" (rawFind.Substring(0, rawFind.Length - 1) + ", \"query\":\"other\"}") false
        addRaw api "code-fence" ("```json\n" + rawFind + "\n```") false)
    group "api-bound" (buildApiGrammar (Some schema) (Some [ SEED ])) (fun bound ->
        rangeCases bound asApi
        add bound "current-snapshot" find true
        add bound "stale-snapshot" (set' find "snapshot_id" (Str (rev 'e' 64))) false
        add bound "returned-id" sources true
        add bound "unknown-id" (set' sources "ids" (Arr [ Str (rev 'b' 64) ])) false
        add bound "other-static-repo" (set' find "repo" (Str "FidelityFramework/clef")) false
        add bound "other-static-layer" (set' find "corpus_layer" (Str "tests")) false
        let outgoingChunks = Obj [ "action", Str "pgq"; "query", Str FILTERED; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "chunks_of" ]; "snapshot_id", Str SNAPSHOT ]
        add bound "chunk-seed-may-traverse-outgoing" outgoingChunks true
        add bound "single-resource" (Obj [ "action", Str "pgq"; "query", Str Fanion.Query.singleResourceQuery; "parameters", seedOnly; "snapshot_id", Str SNAPSHOT ]) true)
    group "query-generation" (buildStageGrammar "query_generation" (Some schema) [ SEED ]) (fun query ->
        rangeCases query id
        [ PLAIN; FILTERED; SHORTEST ] |> List.iteri (fun index pattern ->
            add query ("valid-pattern-" + string index) (Obj [ "query", Str pattern; "parameters", parametersFor pattern ]) true)
        add query "single-resource-source" (Obj [ "query", Str Fanion.Query.singleResourceQuery; "parameters", seedOnly ]) true
        add query "single-resource-cannot-name-edge" (Obj [ "query", Str Fanion.Query.singleResourceQuery; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "links_to" ] ]) false
        let incoming = FILTERED.Replace(")-[", ")<-[").Replace("]->(", "]-(")
        let incomingChunks = incoming.Replace("b.kind, e.kind", "b.kind, b.line_start, b.line_end, e.kind")
        add query "incoming-filtered" (Obj [ "query", Str incomingChunks; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "chunks_of" ] ]) true
        add query "outgoing-chunks-from-discovered-seed" (Obj [ "query", Str (incomingChunks.Replace(")<-[", ")-[").Replace("]-(", "]->(")); "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "chunks_of" ] ]) false
        add query "chunks-without-line-spans" (Obj [ "query", Str incoming; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "chunks_of" ] ]) false
        add query "incoming-other-relation" (Obj [ "query", Str incoming; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "links_to" ] ]) true
        add query "incoming-plain" (Obj [ "query", Str (incoming.Replace(" WHERE e.kind = $edge_kind", "")); "parameters", seedOnly ]) true
        let allColumns = String.Join(", ", nodeFields |> List.map (fun f -> "b." + f))
        add query "all-columns" (Obj [ "query", Str (PLAIN.Substring(0, PLAIN.IndexOf "COLUMNS") + "COLUMNS (" + allColumns + ", e.kind)"); "parameters", seedOnly ]) true
        for high in 0 .. 5 do
            add query ("upper-bound-" + string high) (Obj [ "query", Str (SHORTEST.Replace("{1,3}", "{1," + string high + "}")); "parameters", seedOnly ]) (1 <= high && high <= 4)
        let badQueries =
            [ "wrong-vertex", PLAIN.Replace("a:resource", "a:document")
              "kind-as-label", FILTERED.Replace("e:relation", "e:links_to")
              "wrong-alias", PLAIN.Replace("b:resource", "c:resource")
              "missing-id", PLAIN.Replace("b.id, ", "")
              "duplicate-column", PLAIN.Replace("b.id, ", "b.id, b.id, ")
              "unknown-column", PLAIN.Replace("b.kind", "b.text")
              "sql-instead", "SELECT * FROM nodes"
              "sql-suffix", PLAIN + "; SELECT 1"
              "unbounded", SHORTEST.Replace("{1,3}", "+")
              "filtered-shortest", SHORTEST.Replace("[e:relation]", "[e:relation WHERE e.kind = $edge_kind]")
              "incoming-shortest", SHORTEST.Replace(")-[", ")<-[").Replace("]->{", "]-{")
              "nonone-lower-bound", SHORTEST.Replace("{1,3}", "{2,3}")
              "edge-kind-shortest", SHORTEST.Replace("path_length(p)", "e.kind")
              "path-length-onehop", PLAIN.Replace("e.kind", "path_length(p)")
              "reordered-columns", PLAIN.Replace("b.id, b.path", "b.path, b.id") ]
        for (name, pattern) in badQueries do
            add query name (Obj [ "query", Str pattern; "parameters", parametersFor pattern ]) false
        add query "unused-edge-kind" (Obj [ "query", Str PLAIN; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "links_to" ] ]) false
        add query "missing-edge-kind" (Obj [ "query", Str FILTERED; "parameters", seedOnly ]) false
        add query "invalid-edge-kind" (Obj [ "query", Str FILTERED; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "depends_on" ] ]) false
        add query "unknown-seed" (Obj [ "query", Str PLAIN; "parameters", Obj [ "seed", Str (rev 'b' 64) ] ]) false
        add query "action-not-allowed" (Obj [ "action", Str "pgq"; "query", Str PLAIN; "parameters", seedOnly ]) false
        add query "extra-parameter" (Obj [ "query", Str PLAIN; "parameters", Obj [ "seed", Str SEED; "sql", Str "SELECT 1" ] ]) false
        for name in List.tail edgeProperties do
            add query ("legacy-unavailable-" + name) (Obj [ "query", Str (PLAIN.Replace("e.kind)", "e.kind, e." + name + ")")); "parameters", seedOnly ]) false)
    let provenanceSchema = { schema with EdgeKinds = edgeKinds; EdgeProperties = edgeProperties }
    let edgeColumns = String.Join(", ", edgeProperties |> List.map (fun f -> "e." + f))
    group "query-provenance" (buildStageGrammar "query_generation" (Some provenanceSchema) [ SEED ]) (fun provenance ->
        for name in edgeProperties do
            add provenance ("single-property-" + name) (Obj [ "query", Str (PLAIN.Replace("e.kind)", "e." + name + ")")); "parameters", seedOnly ]) true
            add provenance ("shortest-forbids-" + name) (Obj [ "query", Str (SHORTEST.Replace("path_length(p)", "e." + name)); "parameters", seedOnly ]) false
        let allProvenance = PLAIN.Replace("e.kind)", edgeColumns + ")")
        add provenance "all-provenance" (Obj [ "query", Str allProvenance; "parameters", seedOnly ]) true
        add provenance "incoming-provenance" (Obj [ "query", Str (allProvenance.Replace(")-[", ")<-[").Replace("]->(", "]-(")); "parameters", seedOnly ]) true
        for kind in edgeKinds do
            let query = FILTERED.Replace("e.kind)", edgeColumns + ")")
            let query =
                if kind = "chunks_of" then query.Replace(")-[", ")<-[").Replace("]->(", "]-(").Replace("b.kind, e.kind", "b.kind, b.line_start, b.line_end, e.kind")
                else query
            add provenance ("filtered-" + kind) (Obj [ "query", Str query; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str kind ] ]) true
        for (name, pattern) in [ "reordered-properties", PLAIN.Replace("e.kind)", "e.confidence, e.basis)")
                                 "duplicate-property", PLAIN.Replace("e.kind)", "e.kind, e.basis, e.basis)")
                                 "unknown-property", PLAIN.Replace("e.kind)", "e.password)")
                                 "expression-property", PLAIN.Replace("e.kind)", "e.confidence + 1)")
                                 "node-after-edge", PLAIN.Replace("b.path, b.kind, e.kind)", "e.basis, b.path)") ] do
            add provenance name (Obj [ "query", Str pattern; "parameters", seedOnly ]) false)
    let partialSchema = { provenanceSchema with EdgeProperties = [ "confidence"; "kind"; "basis" ]; EdgeKinds = [ "mentions" ] }
    group "query-partial-provenance" (buildStageGrammar "query_generation" (Some partialSchema) [ SEED ]) (fun partial ->
        add partial "advertised-columns" (Obj [ "query", Str (PLAIN.Replace("e.kind)", "e.kind, e.basis, e.confidence)")); "parameters", seedOnly ]) true
        add partial "unadvertised-column" (Obj [ "query", Str (PLAIN.Replace("e.kind)", "e.evidence_id)")); "parameters", seedOnly ]) false
        add partial "advertised-edge-kind" (Obj [ "query", Str FILTERED; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "mentions" ] ]) true
        add partial "unadvertised-edge-kind" (Obj [ "query", Str FILTERED; "parameters", Obj [ "seed", Str SEED; "edge_kind", Str "links_to" ] ]) false)
    group "seed-discovery" (buildStageGrammar "seed_discovery" (Some schema) []) (fun plan ->
        let lookup = Obj [ "query", Str "docs/Architecture.md"; "repo", Null; "corpus_layer", Null ]
        for count in [ 0; 1; 3; 4 ] do
            add plan ("lookup-count-" + string count) (Obj [ "lookups", Arr (List.replicate count lookup) ]) (1 <= count && count <= 3)
        add plan "bound-vocabulary" (Obj [ "lookups", Arr [ update lookup [ "repo", Str "FidelityFramework/Composer"; "corpus_layer", Str "docs" ] ] ]) true
        add plan "wrong-repo" (Obj [ "lookups", Arr [ set' lookup "repo" (Str "other/repo") ] ]) false
        add plan "missing-null-field" (Obj [ "lookups", Arr [ Obj [ "query", Str "docs" ] ] ]) false)
    List.ofSeq groups

/// Run the native llama.cpp grammar validator (exit code, stdout, stderr).
let runValidator (validator: string) (grammar: string) (input: string) =
    let info = ProcessStartInfo(validator)
    info.ArgumentList.Add grammar
    info.ArgumentList.Add input
    info.RedirectStandardOutput <- true
    info.RedirectStandardError <- true
    info.UseShellExecute <- false
    use p = Process.Start info
    let stdout = p.StandardOutput.ReadToEndAsync()
    let stderr = p.StandardError.ReadToEndAsync()
    if not (p.WaitForExit 10000) then (p.Kill true; failwithf "validator timed out on %s" input)
    p.ExitCode, stdout.Result, stderr.Result

/// The validator accepts when its stdout carries the acceptance line; it exits zero either way.
let validatorAccepts (validator: string) (grammar: string) (input: string) (context: string) =
    let code, stdout, stderr = runValidator validator grammar input
    Assert.True((code = 0), context + ": " + stdout + stderr)
    let accepted = stdout.Contains "Input string is valid according to the grammar."
    let rejected = stdout.Contains "Input string is invalid according to the grammar."
    Assert.True(accepted <> rejected, context + ": " + stdout + stderr)
    accepted, stdout + stderr

/// gbnf._grammar(mode) with no schema: the static reference artifacts are written from the
/// private builder, which the F# module does not export; reach it by reflection.
let private staticGrammar (mode: string) =
    let moduleType = typeof<SchemaContext>.DeclaringType
    let builder = moduleType.GetMethod("grammar", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Static)
    Assert.NotNull builder
    builder.Invoke(null, [| box mode; null; null |]) :?> string

[<Fact>]
let ``static artifact matches generator`` () =
    let artifact name = File.ReadAllText(Path.Combine(sourceRoot, "grammars", name))
    Assert.Equal(artifact "retrieval-api.gbnf", buildApiGrammar None None)
    Assert.Equal(artifact "pgq-query.gbnf", staticGrammar "query_generation")

[<Fact>]
let ``model grammar binds chunks to incoming traversal and source ranges`` () =
    let grammar = buildStageGrammar "query_generation" (Some schema) [ SEED ]
    let line name = grammar.Split('\n') |> Array.find (fun s -> s.StartsWith(name + " ::="))
    Assert.Contains("fields-chunks", line "pgq-fields")
    Assert.Contains("query-chunks", line "fields-chunks")
    Assert.Contains("parameters-chunks", line "fields-chunks")
    Assert.Contains("<-[e:relation WHERE e.kind = $edge_kind]-", line "query-chunks")
    Assert.Contains("b.path", line "chunk-columns")
    Assert.Contains("b.line_start", line "chunk-columns")
    Assert.Contains("b.line_end", line "chunk-columns")
    Assert.DoesNotContain("chunks_of", line "edge-kind-other")
    Assert.Contains("links_to", line "edge-kind-other")
    let chunksOnly = buildStageGrammar "query_generation" (Some { schema with EdgeKinds = [ "chunks_of" ] }) [ SEED ]
    Assert.DoesNotContain("fields-filtered-other", chunksOnly)

[<Fact>]
let ``range grammar binds numeric overlap to the advertised chunk relation`` () =
    for grammar in [ buildApiGrammar None None; buildApiGrammar (Some schema) (Some [ SEED ])
                     buildStageGrammar "query_generation" (Some schema) [ SEED ] ] do
        let line name = grammar.Split('\n') |> Array.find (fun s -> s.StartsWith(name + " ::="))
        Assert.Contains("fields-chunks-range", line "pgq-fields")
        Assert.Contains("query-chunks-range", line "fields-chunks-range")
        Assert.Contains("parameters-chunks", line "fields-chunks-range")
        Assert.Contains("chunks_of", line "parameters-chunks")
        Assert.Contains("<-[e:relation WHERE e.kind = $edge_kind]-(b:resource WHERE b.line_start <=", line "query-chunks-range")
        Assert.Contains(" AND b.line_end >=", line "query-chunks-range")
        Assert.Contains("line-number", line "query-chunks-range")
    for context in [ { schema with EdgeKinds = [ "links_to" ] }
                     { schema with NodeProperties = List.filter ((<>) "line_start") nodeFields }
                     { schema with NodeProperties = List.filter ((<>) "line_end") nodeFields } ] do
        Assert.DoesNotContain("fields-chunks-range", buildStageGrammar "query_generation" (Some context) [ SEED ])

[<Fact>]
let ``every emitted Unicode escape denotes a scalar accepted by GBNF engines`` () =
    for grammar in [ buildApiGrammar None None; buildApiGrammar (Some schema) (Some [ SEED ])
                     buildStageGrammar "seed_discovery" (Some schema) []; buildStageGrammar "query_generation" (Some schema) [ SEED ] ] do
        for escape in System.Text.RegularExpressions.Regex.Matches(grammar, @"\\u([0-9a-fA-F]{4})") do
            let scalar = Convert.ToInt32(escape.Groups.[1].Value, 16)
            Assert.True(Rune.IsValid scalar, "GBNF contains a non-scalar escape: " + escape.Value)

[<Fact>]
let ``generated positive pgq also passes server compiler`` () =
    let mutable compiled = 0
    for group in nativeCorpus () do
        for case in group.Cases do
            if case.Valid then
                let root = JsonInput.parse case.Input
                match Json.prop "query" root with
                | Some(JsonValue.String query) when query.StartsWith("MATCH", StringComparison.Ordinal) ->
                    let number name fallback = Json.prop name root |> Option.map JsonInput.int64 |> Option.defaultValue fallback
                    Fanion.Query.compilePgq query (pgqParametersOf (JsonInput.property "parameters" root)) (number "limit" 20L) (number "offset" 0L) None |> ignore
                    compiled <- compiled + 1
                | _ -> ()
    Assert.True(compiled > 10, sprintf "compiled %d" compiled)

[<Fact>]
let ``context rejects invalid snapshots unknown graph and fabricated seeds`` () =
    // Python also rejected graph != 'corpus' and an explicit empty edge_properties list; the
    // F# context has no graph member and an empty list means "not advertised" (legacy = kind only).
    let rejected : (string * SchemaContext) list =
        [ "snapshot_id", { schema with SnapshotId = "stale" }
          "edge_kinds", { schema with EdgeKinds = [ "depends_on" ] }
          "node_properties", { schema with NodeProperties = [ "id"; "password" ] }
          "edge_properties", { schema with EdgeProperties = [ "kind"; "password" ] }
          "edge_properties-without-kind", { schema with EdgeProperties = [ "confidence" ] }
          "edge_properties-overflow", { schema with EdgeProperties = List.replicate 9 "kind" } ]
    for (name, invalid) in rejected do
        let error = Assert.Throws<ArgumentException>(fun () -> buildApiGrammar (Some invalid) None |> ignore)
        Assert.False(isNull error.Message, name)
    for seeds in [ []; [ rev 'A' 64 ]; [ "short" ] ] do
        Assert.Throws<ArgumentException>(fun () -> buildStageGrammar "query_generation" (Some schema) seeds |> ignore) |> ignore

[<Fact>]
let ``provenance matches store whitelist and only advertised columns`` () =
    Assert.Equal<Set<string>>(Fanion.Query.edgeKinds, set edgeKinds)
    Assert.Equal<string list>("kind" :: Fanion.Query.edgeProvenanceFields, edgeProperties)
    let legacy = buildStageGrammar "query_generation" (Some schema) [ SEED ]
    Assert.Equal(legacy, buildStageGrammar "query_generation" (Some { schema with EdgeProperties = [ "kind" ] }) [ SEED ])
    Assert.DoesNotContain("e.basis", legacy)
    let current = buildStageGrammar "query_generation" (Some { schema with EdgeProperties = [ "confidence"; "kind"; "basis" ] }) [ SEED ]
    Assert.Contains("edge-columns ::= (\", e.kind\")? (\", e.basis\")? (\", e.confidence\")?", current)
    Assert.DoesNotContain("e.evidence_id", current)
    Assert.True(Encoding.UTF8.GetByteCount current < 8192)

[<Fact>]
let ``grammar is request scoped`` () =
    let other = { schema with SnapshotId = rev 'f' 64; Repositories = [ "Different/Repository" ]; CorpusLayers = [ "manuals" ] }
    let first = buildApiGrammar (Some schema) (Some [ SEED ])
    let second = buildApiGrammar (Some other) (Some [ rev 'b' 64 ])
    Assert.Contains(SNAPSHOT, first)
    Assert.DoesNotContain(SNAPSHOT, second)
    Assert.Contains("Different/Repository", second)
    Assert.DoesNotContain("FidelityFramework/Composer", second)
    Assert.DoesNotContain(SEED, second)

[<Fact>]
let ``native llama.cpp acceptance and rejection`` () =
    match Environment.GetEnvironmentVariable "RETRIEVAL_GBNF_VALIDATOR" with
    | null | "" -> Assert.Skip "Set RETRIEVAL_GBNF_VALIDATOR to the native llama.cpp validator"
    | validator ->
        let temp = Path.Combine(Path.GetTempPath(), "gbnf-tests-" + Guid.NewGuid().ToString("N"))
        Directory.CreateDirectory temp |> ignore
        try
            let grammar, input = Path.Combine(temp, "grammar.gbnf"), Path.Combine(temp, "input.json")
            for group in nativeCorpus () do
                File.WriteAllText(grammar, group.Grammar)
                for case in group.Cases do
                    File.WriteAllText(input, case.Input)
                    let accepted, output = validatorAccepts validator grammar input (group.Group + "/" + case.Name)
                    Assert.True((accepted = case.Valid), group.Group + "/" + case.Name + ": " + output)
        finally
            try Directory.Delete(temp, true) with _ -> ()
