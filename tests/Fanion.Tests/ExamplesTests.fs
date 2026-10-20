/// One-for-one port of test_prompt_examples.py: the worked examples shipped with each model
/// request must compile, obey the stage grammars and retrieve the intended evidence on an
/// actual DuckPGQ graph. The examples are read back from the shipped prompt text itself.
module Fanion.Tests.ExamplesTests

open System
open System.IO
open Fidelity.Data.JSON
open Xunit
open Fanion.Canonical
open Fanion.Examples
open Fanion.Tests.IndexStateTests
open Fanion.Tests.QueryTests
open Fanion.Tests.GbnfTests

/// The JSON example records of one stage, as sent in the prompt (one object per line).
let private worked (stage: string) : JsonValue list =
    [ for line in (workedExamples stage).Split('\n') do
        if line.StartsWith("{", StringComparison.Ordinal) then
            yield JsonInput.parse line ]

let private candidateIds (example: JsonValue) =
    match Json.prop "candidates" example with
    | Some candidates -> [ for candidate in JsonInput.items candidates -> candidate |> JsonInput.property "id" |> JsonInput.string ]
    | None -> []

let private request (example: JsonValue) = example |> JsonInput.property "request" |> JsonInput.string

[<Fact>]
let ``seed examples search literal passages rather than concatenated concepts`` () =
    for example in worked "seed_discovery" do
        for lookup in example |> JsonInput.property "output" |> JsonInput.property "lookups" |> JsonInput.items do
            let term = lookup |> JsonInput.property "query" |> JsonInput.string
            Assert.True((request example).Contains(term, StringComparison.Ordinal), "Seed query is not a literal phrase in its request: " + term)

[<Fact>]
let ``all query examples compile with the server`` () =
    let examples = worked "query_generation"
    Assert.Equal(6, examples.Length)
    for example in examples do
        let output = JsonInput.property "output" example
        let parameters = JsonInput.property "parameters" output
        Assert.Contains(parameters |> JsonInput.property "seed" |> JsonInput.string, candidateIds example)
        Fanion.Query.compilePgq (output |> JsonInput.property "query" |> JsonInput.string) (pgqParametersOf parameters) 20L 0L None |> ignore

[<Fact>]
let ``worked outputs obey actual stage grammars`` () =
    match Environment.GetEnvironmentVariable "RETRIEVAL_GBNF_VALIDATOR" with
    | null | "" -> Assert.Skip "Native llama.cpp validator required"
    | validator ->
        let schema : Fanion.Gbnf.SchemaContext =
            { SnapshotId = rev 'd' 64; Repositories = [ exampleRepo; otherRepo ]; CorpusLayers = [ "docs" ]
              EdgeKinds = [ "contains"; "chunks_of"; "links_to" ]; NodeProperties = Fanion.Gbnf.nodeFields; EdgeProperties = [] }
        let temp = Path.Combine(Path.GetTempPath(), "examples-tests-" + Guid.NewGuid().ToString("N"))
        Directory.CreateDirectory temp |> ignore
        try
            let grammar, response = Path.Combine(temp, "grammar.gbnf"), Path.Combine(temp, "response.json")
            for stage in [ "seed_discovery"; "query_generation" ] do
                for example in worked stage do
                    File.WriteAllText(grammar, Fanion.Gbnf.buildStageGrammar stage (Some schema) (candidateIds example))
                    File.WriteAllText(response, example |> JsonInput.property "output" |> ofJson |> toText orderedStyle)
                    let accepted, output = validatorAccepts validator grammar response (stage + ": " + request example)
                    Assert.True(accepted, stage + ": " + request example + ": " + output)
        finally
            try Directory.Delete(temp, true) with _ -> ()

[<Fact>]
let ``queries retrieve the intended relationships not just valid syntax`` () =
    let engine = requireEngine ()
    let records =
        [ "compiler", exampleRepo, "", "repository"
          "handbook", otherRepo, "", "repository"
          "architecture", exampleRepo, "docs/Architecture.md", "file"
          "mention", exampleRepo, "docs/00-overview.md", "file"
          "referring", exampleRepo, "docs/References.md", "file"
          "linked", exampleRepo, "docs/Closures.md", "file"
          "indirect", exampleRepo, "docs/Regions.md", "file"
          "chunk", exampleRepo, "docs/Architecture.md", "chunk"
          "other-architecture", otherRepo, "docs/Architecture.md", "file"
          "other-linked", otherRepo, "docs/Guide.md", "file" ]
    let revision = rev 'e' 40
    let nodes =
        [ for (name, repo, path, kind) in records ->
            Obj [ "id", Str (exampleId name); "repo", Str repo; "revision", Str revision; "path", Str path; "kind", Str kind
                  "title", Str name; "line_start", Int 1L; "line_end", Int 2L; "source_url", Str ("https://example.invalid/" + name)
                  "corpus_layer", Str "docs"; "classification_rule", Str "synthetic"; "text", Str "docs/Architecture.md\n" ] ]
    let relations =
        [ for (name, repo, _, kind) in records do if kind = "file" then yield (if repo = exampleRepo then "compiler" else "handbook"), name, "contains" ]
        @ [ "architecture", "linked", "links_to"; "linked", "indirect", "links_to"; "referring", "architecture", "links_to"
            "chunk", "architecture", "chunks_of"; "other-architecture", "other-linked", "links_to" ]
    let data =
        Obj [ "format_version", Int 1L; "extractor_version", Str "teaching-fixture"
              "repositories", Arr [ for repo in [ exampleRepo; otherRepo ] -> Obj [ "name", Str repo; "commit", Str revision; "source_url", Str ("https://example.invalid/" + repo) ] ]
              "nodes", Arr nodes
              "edges", Arr [ for (src, dst, kind) in relations -> Obj [ "src", Str (exampleId src); "dst", Str (exampleId dst); "kind", Str kind ] ] ]
    let signed, snapshotId = sign data
    let expected = [ [ "linked" ]; [ "referring" ]; [ "chunk" ]; [ "compiler" ]; [ "linked"; "indirect" ]; [ "other-linked" ] ]
    let temp = Path.Combine(Path.GetTempPath(), "examples-graph-" + Guid.NewGuid().ToString("N"))
    Directory.CreateDirectory temp |> ignore
    try
        let source, database = Path.Combine(temp, "snapshot.json"), Path.Combine(temp, "fixture.duckdb")
        File.WriteAllText(source, toText defaultStyle signed)
        Fanion.Generation.load engine defaultBudgets source database |> ignore
        let store = Fanion.Query.Store(database, engine, promptFile)
        try
            let examples = worked "query_generation"
            Assert.Equal(expected.Length, examples.Length)
            for (example, names) in List.zip examples expected do
                let output = JsonInput.property "output" example
                let query = output |> JsonInput.property "query" |> JsonInput.string
                let parameters = JsonInput.property "parameters" output
                let rows = results (store.Pgq(query, pgqParametersOf parameters, snapshotId, 25L, 0L))
                Assert.Equal<Set<string>>(set [ for name in names -> exampleId name ], set [ for row in rows -> str' row "id" ])
                if query.Contains "path_length(p)" then
                    Assert.Equal<Map<string, int64>>(Map.ofList [ exampleId "linked", 1L; exampleId "indirect", 2L ], Map.ofList [ for row in rows -> str' row "id", int' row "hops" ])
                let edgeKind = Json.prop "edge_kind" parameters |> Option.map JsonInput.string |> Option.defaultValue ""
                if edgeKind = "chunks_of" then
                    Assert.Equal<(int64 * int64) list>([ 1L, 2L ], [ for row in rows -> int' row "line_start", int' row "line_end" ])
        finally
            store.Close()
    finally
        try Directory.Delete(temp, true) with _ -> ()
