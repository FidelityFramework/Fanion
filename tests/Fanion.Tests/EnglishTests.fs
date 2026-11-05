/// One-for-one port of test_english.py: English API contracts against a real
/// published generation (local git fixtures, real DuckPGQ) and a scripted model.
/// No network: transport tests substitute an HttpMessageHandler explicitly.
/// Graph tests require RETRIEVAL_TEST_EXTENSIONS and RETRIEVAL_TEST_LIBDUCKDB.
module Fanion.Tests.EnglishTests

open System
open System.Diagnostics
open System.IO
open System.Net
open System.Net.Http
open System.Security.Cryptography
open System.Text
open System.Threading
open System.Threading.Tasks
open Xunit
open Fanion
open Fanion.Canonical
open Fanion.Query
open Fanion.English
open Fanion.Tests.IndexStateTests

// ---------------------------------------------------------------- J helpers

let rec path (value: J) (keys: string list) = match keys with [] -> value | k :: rest -> path (field value k) rest
let items (value: J) = match value with Arr i -> i | _ -> failwith "not an array"
let text (value: J) = match value with Str s -> s | _ -> failwithf "not a string: %A" value
let int' (value: J) = match value with Int i -> i | _ -> failwith "not an integer"
let pairs (value: J) = match value with Obj p -> p | _ -> failwith "not an object"
let keys (value: J) = pairs value |> List.map fst
let has (value: J) (name: string) = match value with Obj p -> p |> List.exists (fun (k, _) -> k = name) | _ -> false
let stringsOf (value: J) = items value |> List.map text
let parse (json: string) = JsonInput.parse json |> ofJson
let jsonOf (value: J) = toText orderedStyle value
let canon (value: J) = toText canonicalStyle value
let sha256Hex (value: string) = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes value)).ToLowerInvariant()

/// The outcome of a service call as the HTTP layer would report it.
type Outcome =
    | Succeeded of J
    | Failed of string * int * bool

let attempt (action: unit -> J) : Outcome =
    try Succeeded(action ())
    with
    | EnglishError(code, status, retryable) -> Failed(code, status, retryable)
    | QueryError _ | :? ArgumentException -> Failed("invalid_request", 400, false)

/// The code, status and retryability carried by a raised EnglishError.
let englishError (error: exn) = match error with EnglishError(code, status, retryable) -> code, status, retryable | other -> raise other
let success (outcome: Outcome) = match outcome with Succeeded v -> v | Failed(code, status, _) -> failwithf "expected success, got %s (%d)" code status
let failure (outcome: Outcome) = match outcome with Failed(code, status, retryable) -> code, status, retryable | Succeeded v -> failwithf "expected failure, got %s" (jsonOf v)

// ----------------------------------------------------- engine and corpus fixture

/// Engine locks and the served prompt are source-controlled Fanion inputs.
let sourceRoot =
    match Environment.GetEnvironmentVariable "RETRIEVAL_TEST_SOURCE_ROOT" with
    | null | "" -> Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../.."))
    | value -> value
let promptFile = Path.Combine(sourceRoot, "docs/PQL-PROMPT.md")
let lockFile (name: string) = Path.Combine(sourceRoot, "config", name)

let environment () =
    match Environment.GetEnvironmentVariable "RETRIEVAL_TEST_EXTENSIONS", Environment.GetEnvironmentVariable "RETRIEVAL_TEST_LIBDUCKDB" with
    | null, _ | _, null -> None
    | extensions, libduckdb -> Some(extensions, libduckdb)

let engineOf (extensions: string) (libduckdb: string) : Load.Engine =
    let duckpgq = File.ReadAllBytes(lockFile "duckpgq-lock.json") |> JsonInput.parseBytes
    let lib = File.ReadAllBytes(lockFile "libduckdb-lock.json") |> JsonInput.parseBytes
    { LibDuckDb = libduckdb; LibDuckDbSha256 = lib |> JsonInput.property "libduckdb_so_sha256" |> JsonInput.string
      DuckDbVersion = lib |> JsonInput.property "duckdb_version" |> JsonInput.string
      DuckDbSourceId = lib |> JsonInput.property "duckdb_source_id" |> JsonInput.string
      DuckPgqVersion = duckpgq |> JsonInput.property "extension_version" |> JsonInput.string
      Threads = 8; MemoryLimit = "1GB" }

let private git (repo: string) (args: string list) =
    let info = ProcessStartInfo("git")
    for a in [ "-C"; repo ] @ args do info.ArgumentList.Add a
    info.RedirectStandardOutput <- true; info.RedirectStandardError <- true; info.UseShellExecute <- false
    info.Environment.["GIT_CONFIG_NOSYSTEM"] <- "1"; info.Environment.["GIT_CONFIG_GLOBAL"] <- "/dev/null"
    use p = Process.Start info
    let out = p.StandardOutput.ReadToEnd()
    p.WaitForExit()
    if p.ExitCode <> 0 then failwithf "git %A failed: %s" args (p.StandardError.ReadToEnd())
    out.Trim()

/// The committed Python fixture graph, rebuilt from real repositories: 0.md links to 1.md,
/// every Markdown file carries "Evidence", and one source file supplies the implementation layer.
let standardFiles =
    [ "0.md", "Content 0 [one](1.md)\nEvidence\n"
      "1.md", "Content 1\nEvidence\n"
      "2.md", "Content 2\nEvidence\n"
      "src/Main.fs", "let marker = \"unrelated implementation\"\n" ]
let standard = [ "test", standardFiles ]

/// _indexed_mentions_before_target: the target file plus `count` files that only mention it.
let mentionFiles (target: string) (count: int) =
    let link = if target.Contains '/' then "../1.md" else "1.md"
    [ target, sprintf "Content 0 [one](%s)\nEvidence\n" link; "1.md", "Content 1\nEvidence\n"; "2.md", "Content 2\nEvidence\n"
      "src/Main.fs", "let marker = \"unrelated implementation\"\n" ]
    @ [ for number in 0 .. count - 1 -> sprintf "a-mention-%02d.md" number, sprintf "Discusses %s\nMention only.\n" target ]

/// Local FidelityFramework/<name> repositories, a corpus configuration tracking them, and a
/// published index root built by the real coordinator (the same route serve uses).
type Corpus(repos: (string * (string * string) list) list, engine: Load.Engine, extensions: string) =
    let temp = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../.scratch/tests", "english-" + Guid.NewGuid().ToString("N")))
    let fixtures = Path.Combine(temp, "fixtures")
    let owner = Path.Combine(fixtures, "FidelityFramework")
    let repoDir (name: string) = Path.Combine(owner, name)
    let commit (name: string) (files: (string * string) list) (message: string) =
        let directory = repoDir name
        for (relative, content) in files do
            let target = Path.Combine(directory, relative)
            Directory.CreateDirectory(Path.GetDirectoryName target) |> ignore
            File.WriteAllText(target, content)
        git directory [ "add"; "." ] |> ignore
        git directory [ "commit"; "--quiet"; "--no-gpg-sign"; "-m"; message ] |> ignore
        git directory [ "rev-parse"; "HEAD" ]
    do
        Directory.CreateDirectory owner |> ignore
        for (name, files) in repos do
            let directory = repoDir name
            Directory.CreateDirectory directory |> ignore
            git directory [ "init"; "--quiet"; "--initial-branch=main" ] |> ignore
            git directory [ "config"; "user.name"; "Retrieval Test" ] |> ignore
            git directory [ "config"; "user.email"; "retrieval@example.invalid" ] |> ignore
            commit name files "Initial evidence" |> ignore
    let config = Path.Combine(temp, "corpus.json")
    do
        let sources =
            [ for (name, _) in repos ->
                Obj ([ "id", Str (name.ToLowerInvariant()); "repo", Str ("FidelityFramework/" + name); "ref", Str "refs/heads/main"; "depends_on", Arr [] ]
                     @ (if name = "test" then [ "layer_rules", Arr [ Obj [ "name", Str "docs"; "patterns", Arr [ Str "planning/*" ] ] ] ] else [])) ]
        File.WriteAllText(config, toText orderedCompactStyle (Obj [ "schema_version", Int 1L; "sources", Arr sources ]))
    let root = Path.Combine(temp, "state")
    let options : Fanion.Coordinator.Options =
        { Root = root; Config = config; Engine = engine
          Budgets = { MaxOutputBytes = 384L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxTreeEntries = 100_000L; MaxFiles = 20_000; MaxSourceBytes = 128L * 1024L * 1024L; MaxDerivedEdges = 50_000; MaxCapabilityNodes = 2048; MaxMetadataBytes = 32768 }
          ExtractLimits = { MaxRepositories = 64; MaxTreeEntries = 100_000L; MaxTreeBytes = 16 * 1024 * 1024; MaxFiles = 20_000; MaxFileBytes = 1024 * 1024; MaxSourceBytes = 128L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxOutputBytes = 384L * 1024L * 1024L; MaxLayerRules = 64; MaxRulePatterns = 32; MaxTotalRulePatterns = 256; MaxLinksPerFile = 1024; MaxBuildSeconds = 600.0; GitTimeoutSeconds = 15.0 }
          RemoteBase = fixtures; LeaseSeconds = 900.0; SettleSeconds = Some 0.0; MaxSettleSeconds = Some 0.0
          LockFiles = [ lockFile "duckpgq-lock.json"; lockFile "libduckdb-lock.json" ] }
    let coordinator = lazy (Fanion.Coordinator.Coordinator(options, CancellationToken.None))
    member _.Root = root
    member _.Config = config
    member _.Engine = engine
    member _.Coordinator = coordinator.Force()
    member _.Head(name: string) = git (repoDir name) [ "rev-parse"; "HEAD" ]
    member _.Commit(name: string, relative: string, content: string) = commit name [ relative, content ] "Update"
    member this.Observe() = this.Coordinator.Observe()
    member this.RunOnce() = this.Coordinator.RunOnce true
    /// Observe the fixtures and publish a generation; the result must report publication.
    member this.Publish() =
        this.Observe() |> ignore
        let result = this.RunOnce()
        Assert.True(bool' result "published", jsonOf result)
        result
    member _.Open() = ManagedStore(root, config, engine, promptFile)
    interface IDisposable with
        member _.Dispose() = try Directory.Delete(temp, true) with _ -> ()

/// Build and open a published corpus; silently a no-op when the engine is not configured for this run.
let withCorpus (repos: (string * (string * string) list) list) (body: Corpus -> ManagedStore -> string -> unit) =
    match environment () with
    | None -> ()
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(repos, engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        let store = corpus.Open()
        try body corpus store (text (field (store.Schema()) "snapshot_id"))
        finally store.Close()

/// The evidence ID of a file node by path (find ranks the exact path first).
let idOfPath (store: ManagedStore) (snapshot: string) (relative: string) =
    items (field (store.Find(relative, snapshot, None, 50L, None)) "results")
    |> List.pick (fun row -> if text (field row "path") = relative && text (field row "kind") = "file" then Some(text (field row "id")) else None)

let schemaContext (schema: J) : Gbnf.SchemaContext =
    { SnapshotId = text (field schema "snapshot_id"); Repositories = [ for r in items (field schema "repositories") -> text (field r "name") ]
      CorpusLayers = stringsOf (field schema "corpus_layers"); EdgeKinds = stringsOf (field schema "edge_kinds")
      NodeProperties = stringsOf (field schema "node_properties"); EdgeProperties = stringsOf (field schema "edge_properties") }

// --------------------------------------------------------------- scripted model

let fixtureSettings : ModelSettings =
    { BaseUrl = "http://internal-model.invalid:8000/v1"; Model = "fixture-model"; TokenFile = Some "/private/model-token"; Timeout = 30.0
      Temperature = 0.0; TopP = None; EnableThinking = None; MaxTokens = 1024L; ResponseFormat = "json_object" }
let settingsWith (responseFormat: string) = { fixtureSettings with BaseUrl = "http://internal-model.invalid/v1"; Model = "fixture"; TokenFile = None; ResponseFormat = responseFormat }

/// RecordingModel: scripted responses, recorded messages and the exact non-secret requests.
type RecordingModel(settings: ModelSettings, responses: Choice<J, exn> list, store: ManagedStore option) =
    inherit ModelClient(settings)
    member val Responses = ResizeArray<Choice<J, exn>>(responses) with get
    member val Calls = ResizeArray<J>() with get
    member val Requests = ResizeArray<J>() with get
    member val OnCall : int -> unit = ignore with get, set
    override this.CompleteAsync(messages, deadline, responseSchema, grammar, cancellation) = task {
        // Inference must not hold the database lock: a concurrent schema call must succeed.
        store |> Option.iter (fun s -> s.Schema() |> ignore)
        Assert.True(deadline > DateTime.UtcNow)
        this.Calls.Add messages
        this.Requests.Add(this.PrepareRequest(messages, responseSchema, grammar))
        let index = this.Calls.Count - 1
        this.OnCall index
        if index >= this.Responses.Count then failwith "Unexpected model call"
        match this.Responses.[index] with
        | Choice1Of2 response -> return response
        | Choice2Of2 error -> return raise error
    }

let scripted (responses: J list) = [ for r in responses -> Choice1Of2 r ]
let question = "Which files does 0.md link to?"
let plan = Obj [ "lookups", Arr [ Obj [ "query", Str "0.md"; "repo", Str "FidelityFramework/test"; "corpus_layer", Str "docs" ] ] ]
let generatedFor (seed: string) = Obj [ "query", Str examples.[0]; "parameters", Obj [ "seed", Str seed ] ]
let service (store: ManagedStore) (model: RecordingModel option) = EnglishService(store, model |> Option.map (fun m -> m :> ModelClient))

let askWith (english: EnglishService) (text: string) (includeContext: bool) (includeQuery: bool) (includeResults: bool) =
    attempt (fun () -> english.Ask(text, None, 8L, 2400L, includeContext, includeQuery, includeResults))
let ask english text = askWith english text false true true
let askContext english text = askWith english text true true true

let private userContext (stage: J) = parse (text (field (items (field stage "messages")).[1] "content"))
let private resultPaths (context: J) = [ for lookup in items (field context "seed_lookups") do for row in items (field lookup "results") -> text (field row "path") ]

// ------------------------------------------------------------ EnglishGraphTests

[<Fact>]
let ``empty graph result is returned without reversing or retrying`` () =
    withCorpus standard (fun _ store snapshot ->
        let seed = idOfPath store snapshot "0.md"
        let query = "MATCH (a:resource WHERE a.id = $seed)<-[e:relation WHERE e.kind = $edge_kind]-(b:resource) COLUMNS (b.id, b.path, b.kind, e.kind)"
        let generated = Obj [ "query", Str query; "parameters", Obj [ "seed", Str seed; "edge_kind", Str "links_to" ] ]
        let model = RecordingModel(fixtureSettings, scripted [ plan; generated ], Some store)
        let result = success (ask (service store (Some model)) "Which files link to 0.md?")
        Assert.Empty(items (field result "results"))
        Assert.Empty(items (field result "sources"))
        Assert.Equal(2, model.Calls.Count))

[<Theory>]
[<InlineData(1, 100)>]
[<InlineData(450, 460)>]
[<InlineData(160, 165)>]
[<InlineData(1000, 1100)>]
let ``requested lines are selected by the graph query before pagination and hydration`` (first: int) (last: int) =
    let content = String.concat "\n" [ for n in 1 .. 600 -> sprintf "line %03d" n ] + "\n"
    withCorpus [ "test", [ "src/Main.fs", content ] ] (fun _ store snapshot ->
        let seed = idOfPath store snapshot "src/Main.fs"
        let query = sprintf "MATCH (a:resource WHERE a.id = $seed)<-[e:relation WHERE e.kind = $edge_kind]-(b:resource WHERE b.line_start <= %d AND b.line_end >= %d) COLUMNS (b.id, b.path, b.line_start, b.line_end, b.source_url, e.kind)" last first
        let generated = Obj [ "query", Str query; "parameters", Obj [ "seed", Str seed; "edge_kind", Str "chunks_of" ] ]
        let lookup = Obj [ "lookups", Arr [ Obj [ "query", Str "src/Main.fs"; "repo", Str "FidelityFramework/test"; "corpus_layer", Null ] ] ]
        let model = RecordingModel(fixtureSettings, scripted [ lookup; generated ], Some store)
        let result = success (ask (service store (Some model)) (sprintf "Show lines %d through %d of src/Main.fs in FidelityFramework/test" first last))
        Assert.Equal(query, text (field result "query"))
        let rows = items (field result "results")
        let sources = items (field result "sources")
        if first > 600 then
            Assert.Empty rows
            Assert.Empty sources
        else
            Assert.NotEmpty rows
            Assert.NotEmpty sources
            for row in rows @ sources do
                Assert.Equal("src/Main.fs", text (field row "path"))
                Assert.True(int' (field row "line_start") <= int64 last)
                Assert.True(int' (field row "line_end") >= int64 first)
            for source in sources do
                Assert.True(int' (field source "excerpt_line_start") >= int64 first)
                Assert.True(int' (field source "excerpt_line_end") <= int64 last)
                Assert.NotEmpty(text (field source "text"))
            Assert.True(sources |> List.exists (fun source -> (text (field source "text")).Contains(sprintf "line %03d" first)))
        Assert.Equal(2, model.Calls.Count))

[<Fact>]
let ``graph engine can return a discovered file without traversing an edge`` () =
    match environment () with
    | None -> Assert.Skip "Set the verified graph engine paths"
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        let snapshot = text (field (corpus.Publish()) "snapshot_id")
        let database = Path.Combine(corpus.Root, "generations", snapshot + ".duckdb")
        use connection = Fanion.Load.openEngine corpus.Engine database
        use lookup = connection.CreateCommand()
        lookup.CommandText <- "SELECT id FROM nodes WHERE path = 'src/Main.fs' AND kind = 'file'"
        let seed = string (lookup.ExecuteScalar())
        use command = connection.CreateCommand()
        command.CommandText <- sprintf "SELECT * FROM GRAPH_TABLE(corpus MATCH (a:resource WHERE a.id = '%s') COLUMNS (a.id AS id))" seed
        use reader = command.ExecuteReader()
        Assert.True(reader.Read())
        Assert.Equal(seed, reader.GetString 0)
        Assert.False(reader.Read())

[<Fact>]
let ``a model-selected file is returned as source without inventing a relationship`` () =
    withCorpus standard (fun corpus store snapshot ->
        let seed = idOfPath store snapshot "src/Main.fs"
        let lookup = Obj [ "lookups", Arr [ Obj [ "query", Str "src/Main.fs"; "repo", Str "FidelityFramework/test"; "corpus_layer", Null ] ] ]
        let generated = Obj [ "query", Str singleResourceQuery; "parameters", Obj [ "seed", Str seed ] ]
        let model = RecordingModel(fixtureSettings, scripted [ lookup; generated ], Some store)
        let result = success (ask (service store (Some model)) "What does src/Main.fs say in FidelityFramework/test?")
        Assert.Equal(seed, text (field (items (field result "results")).[0] "id"))
        let source = (items (field result "sources")).[0]
        Assert.Equal("src/Main.fs", text (field source "path"))
        Assert.Equal(corpus.Head "test", text (field source "revision"))
        Assert.Contains("unrelated implementation", text (field source "text"))
        Assert.Equal(0L, int' (field result "sources_omitted")))

[<Fact>]
let ``generated query runs real graph and returns exact context and citations`` () =
    withCorpus standard (fun corpus store snapshot ->
        let id0, id1 = idOfPath store snapshot "0.md", idOfPath store snapshot "1.md"
        let model = RecordingModel(fixtureSettings, scripted [ plan; generatedFor id0 ], Some store)
        let body = success (askContext (service store (Some model)) question)
        Assert.Equal(snapshot, text (field body "snapshot_id"))
        Assert.Equal(examples.[0], text (field body "query"))
        Assert.Equal(canon (Obj [ "seed", Str id0 ]), canon (field body "parameters"))
        Assert.Equal(id1, text (field (items (field body "results")).[0] "id"))
        let source = (items (field body "sources")).[0]
        Assert.Equal("Content 1\nEvidence\n", text (field source "text"))
        Assert.Equal(corpus.Head "test", text (field source "revision"))
        Assert.Contains("/src/commit/", text (field source "source_url"))
        let context = items (field body "context")
        Assert.Equal(2, context.Length)
        Assert.True(Encoding.UTF8.GetByteCount(jsonOf body) < 65536)
        for (sent, request, trace) in List.zip3 (List.ofSeq model.Calls) (List.ofSeq model.Requests) context do
            Assert.DoesNotContain(snapshot, jsonOf sent)
            Assert.False(has (parse (text (field (items sent).[1] "content"))) "snapshot_id")
            Assert.Equal(canon sent, canon (field trace "messages"))
            Assert.Equal(sha256Hex (toText orderedCompactStyle sent), text (field trace "sha256"))
            Assert.Equal(canon (Obj (("messages", field trace "messages") :: pairs (field trace "request_options"))), canon request)
            Assert.Equal(sha256Hex (canon request), text (field trace "request_sha256"))
        Assert.Contains("sample_evidence", text (field (items model.Calls.[1]).[1] "content")))

[<Fact>]
let ``prompt does not call model or disclose internal configuration`` () =
    withCorpus standard (fun _ store _ ->
        let model = RecordingModel(fixtureSettings, [], Some store)
        let body = (service store (Some model)).Prompt(question, None)
        Assert.Empty(model.Calls)
        Assert.Equal("fixture-model", text (path body [ "english"; "model" ]))
        let context = items (field body "context")
        Assert.Equal(1, context.Length)
        let reference = text (path (store.Schema()) [ "query_prompt"; "text" ])
        Assert.Equal(reference, text (path body [ "query_prompt"; "text" ]))
        let messages = items (field context.[0] "messages")
        Assert.DoesNotContain(reference, text (field messages.[0] "content"))
        Assert.True(int' (field context.[0] "bytes") < int64 contextBytes)
        let user = parse (text (field messages.[1] "content"))
        Assert.False(has user "classification_rules")
        Assert.False(has user "repository_layer_overrides")
        Assert.Contains("docs", stringsOf (field user "corpus_layers"))
        let rendered = jsonOf body
        for secret in [ fixtureSettings.BaseUrl; fixtureSettings.TokenFile.Value ] do Assert.DoesNotContain(secret, rendered))

[<Fact>]
let ``relationship source proof is hydrated alongside target`` () =
    // A real inferred edge: 0.md mentions the Handbook repository, so the derived mentions
    // edge cites 0.md itself as evidence, hydrated beside the targets.
    let repos = [ "test", ("0.md", "Content 0 mentions Handbook [one](1.md)\nEvidence\n") :: List.tail standardFiles; "Handbook", [ "README.md", "# Handbook\nGuidance.\n" ] ]
    withCorpus repos (fun _ store snapshot ->
        let id0, id1 = idOfPath store snapshot "0.md", idOfPath store snapshot "1.md"
        let generated = Obj [ "query", Str (examples.[0].Replace("e.kind)", "e.kind, e.basis, e.evidence_id)")); "parameters", Obj [ "seed", Str id0 ] ]
        let model = RecordingModel(fixtureSettings, scripted [ plan; generated ], Some store)
        let body = success (ask (service store (Some model)) question)
        let rows = items (field body "results")
        let mention = rows |> List.find (fun row -> text (field row "edge_kind") = "mentions")
        Assert.Equal("inferred", text (field mention "basis"))
        Assert.Equal(id0, text (field mention "evidence_id"))
        let sources = items (field body "sources") |> List.map (fun s -> text (field s "id"))
        Assert.Equal<Set<string>>(set [ id1; text (field mention "id"); id0 ], set sources)
        let evidence = items (field body "sources") |> List.find (fun s -> text (field s "id") = id0)
        Assert.StartsWith("Content 0 mentions Handbook", text (field evidence "text"))
        Assert.Equal(0L, int' (field body "sources_omitted"))
        Assert.Contains("research leads", text (field body "note")))

[<Fact>]
let ``stage constraints follow live schema and discovered ids`` () =
    withCorpus standard (fun _ store snapshot ->
        let id0 = idOfPath store snapshot "0.md"
        let settings = { settingsWith "json_schema" with Temperature = 1.0; TopP = Some 0.95; EnableThinking = Some false; MaxTokens = 2048L }
        let model = RecordingModel(settings, scripted [ plan; generatedFor id0 ], Some store)
        success (askContext (service store (Some model)) question) |> ignore
        let first, second = model.Requests.[0], model.Requests.[1]
        let lookup = path first [ "response_format"; "json_schema"; "schema"; "properties"; "lookups"; "items"; "properties" ]
        Assert.Equal(canon (Arr [ Null; Str "FidelityFramework/test" ]), canon (path lookup [ "repo"; "enum" ]))
        Assert.Equal(canon (Arr [ Null; Str "docs"; Str "implementation"; Str "repository" ]), canon (path lookup [ "corpus_layer"; "enum" ]))
        let parameters = path second [ "response_format"; "json_schema"; "schema"; "properties"; "parameters" ]
        for alternative in items (field parameters "anyOf") do
            Assert.Equal(canon (Arr [ Str id0 ]), canon (path alternative [ "properties"; "seed"; "enum" ]))
        Assert.Equal(canon (Obj [ "enable_thinking", Bool false ]), canon (field second "chat_template_kwargs"))
        Assert.Equal(Float 1.0, field second "temperature")
        Assert.Equal(Float 0.95, field second "top_p")
        Assert.Equal(Int 2048L, field second "max_tokens")
        // Unsupported backend output remains rejected despite declaring schema mode.
        let model = RecordingModel(settingsWith "json_schema", scripted [ plan; generatedFor (String('f', 64)) ], Some store)
        let code, _, _ = failure (ask (service store (Some model)) question)
        Assert.Equal("model_query_rejected", code))

[<Fact>]
let ``gbnf stages are bound to live schema and record exact sent constraints`` () =
    withCorpus standard (fun _ store snapshot ->
        let id0, id1 = idOfPath store snapshot "0.md", idOfPath store snapshot "1.md"
        let model = RecordingModel(settingsWith "gbnf", scripted [ plan; generatedFor id0 ], Some store)
        let english = service store (Some model)
        let prompt = english.Prompt(question, None)
        let context = schemaContext (store.Schema())
        let expectedPlan = Gbnf.buildStageGrammar "seed_discovery" (Some context) []
        Assert.Equal(expectedPlan, text (field (items (field prompt "context")).[0] "grammar"))
        Assert.Empty(model.Calls)
        let body = success (askContext english question)
        let contexts = items (field body "context")
        Assert.Equal(expectedPlan, text (field contexts.[0] "grammar"))
        Assert.Equal(Gbnf.buildStageGrammar "query_generation" (Some context) [ id0 ], text (field contexts.[1] "grammar"))
        for (stage, request) in List.zip contexts (List.ofSeq model.Requests) do
            let grammar = text (field request "grammar")
            Assert.Equal(grammar, text (field stage "grammar"))
            Assert.Equal(sha256Hex grammar, text (field stage "grammar_sha256"))
            Assert.Equal(grammar, text (path stage [ "request_options"; "grammar" ]))
            Assert.False(has request "response_format")
            Assert.Equal(canon (Obj (("messages", field stage "messages") :: pairs (field stage "request_options"))), canon request)
            Assert.Equal(sha256Hex (canon request), text (field stage "request_sha256"))
        Assert.Equal(id1, text (field (items (field body "results")).[0] "id"))
        Assert.True(Encoding.UTF8.GetByteCount(jsonOf body) < 65536))

[<Fact>]
let ``json modes do not build or send grammars`` () =
    withCorpus standard (fun _ store snapshot ->
        let id0 = idOfPath store snapshot "0.md"
        for mode in [ "json_object"; "json_schema" ] do
            let model = RecordingModel(settingsWith mode, scripted [ plan; generatedFor id0 ], Some store)
            let body = success (askContext (service store (Some model)) question)
            for request in model.Requests do Assert.False(has request "grammar")
            for stage in items (field body "context") do
                Assert.False(has stage "grammar")
                Assert.False(has stage "grammar_sha256"))

[<Fact>]
let ``no model is explicit retryable and prompt remains inspectable`` () =
    withCorpus standard (fun _ store _ ->
        let english = service store None
        Assert.Equal(("model_not_configured", 503, true), failure (ask english question))
        Assert.Equal(1, (items (field (english.Prompt(question, None)) "context")).Length))

[<Fact>]
let ``magnitude stages preserve native grammar binding with request last for cache reuse`` () =
    withCorpus standard (fun _ store snapshot ->
        let id0 = idOfPath store snapshot "0.md"
        let settings = { settingsWith "magnitude_gbnf" with EnableThinking = Some false }
        let model = RecordingModel(settings, scripted [ plan; generatedFor id0 ], Some store)
        let body = success (askContext (service store (Some model)) question)
        let contexts = items (field body "context")
        Assert.Equal(2, model.Requests.Count)
        for (stage, request) in List.zip contexts (List.ofSeq model.Requests) do
            let grammar = text (field stage "grammar")
            Assert.Equal(grammar, text (path request [ "response_format"; "grammar" ]))
            Assert.Equal("grammar", text (path request [ "response_format"; "type" ]))
            Assert.Equal("none", text (field request "reasoning_effort"))
            Assert.Equal(Bool true, field request "cache_prompt")
            Assert.False(has request "grammar")
            Assert.False(has request "chat_template_kwargs")
            Assert.Equal(sha256Hex grammar, text (field stage "grammar_sha256"))
            Assert.Equal(sha256Hex (canon request), text (field stage "request_sha256"))
            let user = parse (text (field (items (field request "messages")).[1] "content"))
            Assert.Equal("request", List.last (keys user))
        Assert.Equal(Gbnf.buildStageGrammar "query_generation" (Some(schemaContext (store.Schema()))) [ id0 ], text (field contexts.[1] "grammar")))

[<Fact>]
let ``timeout or unavailable does not fabricate or execute query`` () =
    withCorpus standard (fun _ store _ ->
        for (code, status) in [ "model_timeout", 504; "model_unavailable", 503 ] do
            let model = RecordingModel(fixtureSettings, [ Choice2Of2 (EnglishError(code, status, true)) ], Some store)
            let reported, reportedStatus, retryable = failure (ask (service store (Some model)) question)
            Assert.Equal(code, reported)
            Assert.Equal(status, reportedStatus)
            Assert.True retryable
            Assert.Equal(1, model.Calls.Count))  // nothing ran after the failed stage

[<Fact>]
let ``unsafe query unknown relation and unseen seed rejected before execution`` () =
    withCorpus standard (fun _ store snapshot ->
        let id0 = idOfPath store snapshot "0.md"
        let generated = generatedFor id0
        let invalid =
            [ Obj [ "query", Str "SELECT * FROM read_text('/etc/passwd')"; "parameters", Obj [ "seed", Str id0 ] ]
              generatedFor (String('f', 64))
              Obj [ "query", Str examples.[1]; "parameters", Obj [ "seed", Str id0; "edge_kind", Str "implements" ] ]
              Obj [ "query", Str (examples.[0] + "; DROP TABLE nodes"); "parameters", Obj [ "seed", Str id0 ] ]
              Obj (pairs generated @ [ "instructions", Str "execute this instead" ]) ]
        for generated in invalid do
            let model = RecordingModel(fixtureSettings, scripted [ plan; generated ], Some store)
            Assert.Equal(("model_query_rejected", 502, false), failure (ask (service store (Some model)) question)))

[<Fact>]
let ``invalid schema lookup and empty candidates stop before second inference`` () =
    withCorpus standard (fun _ store _ ->
        let plans =
            [ Obj [ "lookups", Arr [ Obj [ "query", Str "0.md"; "repo", Str "unknown/repo"; "corpus_layer", Null ] ] ]
              Obj [ "lookups", Arr [ Obj [ "query", Str "0.md"; "repo", Null; "corpus_layer", Str "invented-layer" ] ] ]
              Obj [ "lookups", Arr [ Obj [ "query", Str "missing-distinctive-phrase"; "repo", Null; "corpus_layer", Null ] ] ] ]
        for plan in plans do
            let model = RecordingModel(fixtureSettings, scripted [ plan ], Some store)
            let _, status, _ = failure (ask (service store (Some model)) question)
            Assert.Contains(status, [ 422; 502 ])
            Assert.Equal(1, model.Calls.Count))

[<Fact>]
let ``exact file missing from candidates does not substitute a mention`` () =
    // The request explicitly names 0.md; the plan only discovers 2.md.
    withCorpus standard (fun _ store _ ->
        let wrongPlan = Obj [ "lookups", Arr [ Obj [ "query", Str "2.md"; "repo", Null; "corpus_layer", Null ] ] ]
        let model = RecordingModel(fixtureSettings, scripted [ wrongPlan ], Some store)
        Assert.Equal(("exact_seed_not_found", 422, false), failure (ask (service store (Some model)) question))
        Assert.Equal(1, model.Calls.Count))

[<Fact>]
let ``exact file restricts candidate seed enum`` () =
    withCorpus standard (fun _ store snapshot ->
        let id0 = idOfPath store snapshot "0.md"
        let broadPlan = Obj [ "lookups", Arr [ Obj [ "query", Str "Evidence"; "repo", Null; "corpus_layer", Null ] ] ]
        let model = RecordingModel(fixtureSettings, scripted [ broadPlan; generatedFor id0 ], Some store)
        let body = success (askContext (service store (Some model)) question)
        let stage = (items (field body "context")).[1]
        for alternative in items (path stage [ "response_schema"; "properties"; "parameters"; "anyOf" ]) do
            Assert.Equal(canon (Arr [ Str id0 ]), canon (path alternative [ "properties"; "seed"; "enum" ]))
        Assert.Equal<string list>([ "0.md" ], resultPaths (userContext stage)))

[<Fact>]
let ``ranked exact file needs no expansion after truncated mentions`` () =
    withCorpus [ "test", mentionFiles "z-target.md" 8 ] (fun _ store snapshot ->
        let target = idOfPath store snapshot "z-target.md"
        let plan = Obj [ "lookups", Arr [ Obj [ "query", Str "z-target.md"; "repo", Str "FidelityFramework/test"; "corpus_layer", Null ] ] ]
        let model = RecordingModel(fixtureSettings, scripted [ plan; generatedFor target ], Some store)
        let body = success (askContext (service store (Some model)) "Find files linked from z-target.md.")
        let context = userContext (items (field body "context")).[1]
        Assert.Equal<string list>([ "z-target.md" ], resultPaths context)
        let lookup = (items (field context "seed_lookups")).[0]
        Assert.Equal(Int 4L, field lookup "lookup_limit")
        Assert.True(bool' lookup "truncated")  // the mentions overflow the ranked lookup
        Assert.False(has lookup "expanded_lookup")
        Assert.Equal(target, text (path body [ "parameters"; "seed" ])))

[<Fact>]
let ``ranked unique basename resolves requested path without expansion`` () =
    withCorpus [ "test", mentionFiles "docs/z-target.md" 8 ] (fun _ store snapshot ->
        let target = idOfPath store snapshot "docs/z-target.md"
        let plan = Obj [ "lookups", Arr [ Obj [ "query", Str "z-target.md"; "repo", Str "FidelityFramework/test"; "corpus_layer", Null ] ] ]
        let model = RecordingModel(fixtureSettings, scripted [ plan; generatedFor target ], Some store)
        let body = success (askContext (service store (Some model)) "Find files linked from docs/z-target.md.")
        let lookup = (items (field (userContext (items (field body "context")).[1]) "seed_lookups")).[0]
        Assert.Equal("z-target.md", text (path lookup [ "lookup"; "query" ]))
        Assert.Equal(Int 4L, field lookup "lookup_limit")
        Assert.False(has lookup "expanded_lookup")
        Assert.Equal<string list>([ "docs/z-target.md" ], [ for row in items (field lookup "results") -> text (field row "path") ]))

[<Fact>]
let ``truncated basename lookup fails without retrying the full path`` () =
    // Four same-name files put the requested path outside the bounded first lookup.
    let decoys = [ "z-target.md", "Root decoy\n"; "a/z-target.md", "Decoy a\n"; "b/z-target.md", "Decoy b\n"; "c/z-target.md", "Decoy c\n" ]
    withCorpus [ "test", mentionFiles "docs/z-target.md" 8 @ decoys ] (fun _ store _ ->
        let plan = Obj [ "lookups", Arr [ Obj [ "query", Str "z-target.md"; "repo", Str "FidelityFramework/test"; "corpus_layer", Null ] ] ]
        let model = RecordingModel(fixtureSettings, scripted [ plan ], Some store)
        let english = service store (Some model)
        Assert.Equal(("exact_seed_not_found", 422, false), failure (ask english "Find files linked from docs/z-target.md."))
        Assert.Equal(1, model.Calls.Count)
        Assert.Equal(0L, int' (path (english.Contract()) [ "limits"; "lookup_expansions" ])))

[<Fact>]
let ``ambiguous basename never guesses an expansion target`` () =
    withCorpus [ "test", mentionFiles "docs/z-target.md" 8 ] (fun _ store _ ->
        let plan = Obj [ "lookups", Arr [ Obj [ "query", Str "z-target.md"; "repo", Str "FidelityFramework/test"; "corpus_layer", Null ] ] ]
        let model = RecordingModel(fixtureSettings, scripted [ plan ], Some store)
        Assert.Equal(("exact_seed_not_found", 422, false), failure (ask (service store (Some model)) "Compare docs/z-target.md and spec/z-target.md."))
        Assert.Equal(1, model.Calls.Count))

[<Fact>]
let ``snapshot change is retryable and does not mix evidence`` () =
    withCorpus standard (fun corpus store _ ->
        let model = RecordingModel(fixtureSettings, scripted [ plan ], Some store)
        // A new generation is published while the planner stage is in flight.
        model.OnCall <- (fun _ ->
            corpus.Commit("test", "2.md", "Content 2 changed\nEvidence\n") |> ignore
            corpus.Publish() |> ignore)
        Assert.Equal(("snapshot_changed", 409, true), failure (ask (service store (Some model)) question))
        Assert.Equal(1, model.Calls.Count))

[<Fact>]
let ``optional query results and context obey flags`` () =
    withCorpus standard (fun _ store snapshot ->
        let id0 = idOfPath store snapshot "0.md"
        let model = RecordingModel(fixtureSettings, scripted [ plan; generatedFor id0 ], Some store)
        let body = success (askWith (service store (Some model)) question false false false)
        for name in [ "query"; "parameters"; "results" ] do Assert.False(has body name)
        let stage = (items (field body "context")).[0]
        Assert.False(has stage "messages")
        Assert.True(has stage "sha256")
        Assert.NotEmpty(items (field body "sources")))

[<Fact>]
let ``response budgets are bounded before model execution`` () =
    withCorpus standard (fun _ store snapshot ->
        let model = RecordingModel(fixtureSettings, [], Some store)
        let english = service store (Some model)
        for (limit, maxChars, text) in [ 21L, 2400L, question; 8L, 4001L, question; 8L, 2400L, String('x', 2001) ] do
            let _, status, _ = failure (attempt (fun () -> english.Ask(text, None, limit, maxChars, false, true, true)))
            Assert.Equal(400, status)
        Assert.Empty(model.Calls))

// ------------------------------------------------------------ ModelTransportTests

let private stagePrefix (stage: J) =
    let content = text (field (items (field stage "messages")).[1] "content")
    let position = content.IndexOf("\"request\":", StringComparison.Ordinal)
    Assert.True(position > 0)
    content, content.Substring(0, position)

[<Fact>]
let ``stable stage schema precedes question and candidates`` () =
    withCorpus standard (fun _ store snapshot ->
        let id0 = idOfPath store snapshot "0.md"
        // Both stages: the schema material is identical before the request, then request, then candidates.
        for (stage, questions) in [ 0, ("First question?", "Second question?"); 1, ("First question about 0.md?", "Second question about 0.md?") ] do
            let prepared (text: string) =
                if stage = 0 then (items (field ((service store None).Prompt(text, None)) "context")).[0]
                else
                    let model = RecordingModel(fixtureSettings, scripted [ plan; generatedFor id0 ], Some store)
                    (items (field (success (askContext (service store (Some model)) text)) "context")).[1]
            let first, second = prepared (fst questions), prepared (snd questions)
            let content, prefix = stagePrefix first
            let _, other = stagePrefix second
            Assert.Equal(prefix, other)
            let context = parse content
            let index name = keys context |> List.findIndex ((=) name)
            Assert.True(index "request" > 0)
            Assert.False(has context "snapshot_id")
            Assert.DoesNotContain(snapshot, content)
            Assert.Equal(fst questions, text (field context "request"))
            if stage = 1 then Assert.True(index "seed_lookups" > index "request")
            Assert.NotEqual<string>(text (field first "sha256"), text (field second "sha256")))

let private environmentLock = obj ()
let private modelVariables =
    [ "RETRIEVAL_MODEL_BASE_URL"; "RETRIEVAL_MODEL"; "RETRIEVAL_MODEL_TOKEN_FILE"; "RETRIEVAL_MODEL_TIMEOUT_SECONDS"; "RETRIEVAL_MODEL_TEMPERATURE"
      "RETRIEVAL_MODEL_TOP_P"; "RETRIEVAL_MODEL_ENABLE_THINKING"; "RETRIEVAL_MODEL_MAX_TOKENS"; "RETRIEVAL_MODEL_RESPONSE_FORMAT" ]

/// patch.dict(os.environ, variables, clear=True) for the model variables.
let withEnvironment (variables: (string * string) list) (body: unit -> unit) =
    lock environmentLock (fun () ->
        let saved = [ for name in modelVariables -> name, Environment.GetEnvironmentVariable name ]
        try
            for name in modelVariables do Environment.SetEnvironmentVariable(name, null)
            for (name, value) in variables do Environment.SetEnvironmentVariable(name, value)
            body ()
        finally
            for (name, value) in saved do Environment.SetEnvironmentVariable(name, value))

/// ValueError in the reference: an invalid configuration is refused, whichever argument check trips.
let rejectsConfiguration (variables: (string * string) list) =
    withEnvironment variables (fun () ->
        let rejected = (try settingsFromEnvironment () |> ignore; false with :? ArgumentException | :? FormatException | :? OverflowException -> true)
        Assert.True(rejected, sprintf "accepted %A" variables))

/// Recording HttpMessageHandler: the transport's exact requests, scripted responses.
type FakeHandler(respond: HttpRequestMessage -> CancellationToken -> HttpResponseMessage) =
    inherit HttpMessageHandler()
    member val Requests = ResizeArray<string * string * string * string>() with get  // method, url, authorization, body
    override this.Send(request: HttpRequestMessage, cancellation: CancellationToken) : HttpResponseMessage =
        let body = if isNull request.Content then "" else request.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        let authorization = match request.Headers.Authorization with null -> "" | value -> value.ToString()
        this.Requests.Add((request.Method.Method, request.RequestUri.ToString(), authorization, body))
        respond request cancellation
    override this.SendAsync(request, cancellation) = Task.FromResult(this.Send(request, cancellation))

let private respondWith (status: int ref) (body: string ref) =
    fun (_: HttpRequestMessage) (_: CancellationToken) ->
        new HttpResponseMessage(enum<HttpStatusCode> status.Value, Content = new StringContent(body.Value, Encoding.UTF8, "application/json"))
let private clientWith (settings: ModelSettings) (handler: FakeHandler) = ModelClient(settings, new HttpClient(handler, Timeout = Timeout.InfiniteTimeSpan))
let private soon () = DateTime.UtcNow.AddSeconds 5.0
let private completion (content: string) = sprintf """{"choices":[{"finish_reason":"stop","message":{"content":%s}}]}""" (toText defaultUnicodeStyle (Str content))

[<Fact>]
let ``gbnf mode is explicit bounded and does not add response format`` () =
    let grammar = "root ::= \"alpha\" | \"beta\"\n"
    let client = ModelClient(settingsWith "gbnf")
    let request = client.PrepareRequest(Arr [ Obj [ "role", Str "user"; "content", Str "Choose a route." ] ], None, Some grammar)
    Assert.Equal(grammar, text (field request "grammar"))
    Assert.False(has request "response_format")
    Assert.DoesNotContain("PGQ", jsonOf request)
    Assert.Equal(Float 0.0, field request "temperature")
    Assert.Equal(Int 1024L, field request "max_tokens")
    for invalid in [ None; Some ""; Some "  \n"; Some "root ::= \"x\"\000"; Some(String('x', 8193)); Some(String('é', 4097)) ] do
        Assert.Throws<ArgumentException>(fun () -> client.PrepareRequest(Arr [], None, invalid) |> ignore) |> ignore
    for mode in [ "json_object"; "json_schema" ] do
        Assert.Throws<ArgumentException>(fun () -> ModelClient(settingsWith mode).PrepareRequest(Arr [], Some(Obj []), Some grammar) |> ignore) |> ignore
    withEnvironment [ "RETRIEVAL_MODEL_BASE_URL", "http://model.invalid/v1"; "RETRIEVAL_MODEL", "fixture"; "RETRIEVAL_MODEL_RESPONSE_FORMAT", "gbnf" ] (fun () ->
        Assert.Equal("gbnf", (settingsFromEnvironment ()).Value.ResponseFormat))

[<Fact>]
let ``complete sends supplied grammar without implicit fallback`` () =
    let grammar = "root ::= \"{\" [ \\t]* \"}\"\n"
    let status, body = ref 200, ref (completion "{}")
    let handler = new FakeHandler(respondWith status body)
    let client = clientWith (settingsWith "gbnf") handler
    Assert.Equal(canon (Obj []), canon (client.Complete(Arr [], soon (), None, Some grammar)))
    let (_, _, _, sent) = handler.Requests.[0]
    let posted = parse sent
    Assert.Equal(grammar, text (field posted "grammar"))
    Assert.False(has posted "response_format")
    status.Value <- 400
    let code, _, _ = englishError (Assert.Throws<EnglishError>(fun () -> client.Complete(Arr [], soon (), None, Some grammar) |> ignore))
    Assert.Equal("model_request_rejected", code)
    Assert.Equal(2, handler.Requests.Count)  // One call for each explicit attempt.

[<Fact>]
let ``magnitude grammar is sent directly and reasoning output is rejected`` () =
    let settings = { settingsWith "magnitude_gbnf" with EnableThinking = Some false }
    let grammar = "root ::= \"{\" [ \\t]* \"}\"\n"
    let status, body = ref 200, ref (completion "{}")
    let handler = new FakeHandler(respondWith status body)
    let client = clientWith settings handler
    Assert.Equal(canon (Obj []), canon (client.Complete(Arr [], soon (), None, Some grammar)))
    let (_, _, _, sent) = handler.Requests.[0]
    let posted = parse sent
    Assert.Equal(grammar, text (path posted [ "response_format"; "grammar" ]))
    Assert.Equal("grammar", text (path posted [ "response_format"; "type" ]))
    Assert.Equal("none", text (field posted "reasoning_effort"))
    Assert.False(has posted "grammar")
    Assert.False(has posted "chat_template_kwargs")
    for response in [ """{"choices":[{"finish_reason":"stop","message":{"content":"{}","reasoning_content":"think"}}]}"""
                      """{"choices":[{"finish_reason":"stop","message":{"content":"{}"}}],"usage":{"completion_tokens_details":{"reasoning_tokens":1}}}""" ] do
        body.Value <- response
        let code, _, _ = englishError (Assert.Throws<EnglishError>(fun () -> client.Complete(Arr [], soon (), None, Some grammar) |> ignore))
        Assert.Equal("model_response_invalid", code)
    for invalid in [ None; Some ""; Some " \n"; Some "root ::= \"x\"\000"; Some(String('é', 4097)) ] do
        Assert.Throws<ArgumentException>(fun () -> client.PrepareRequest(Arr [], None, invalid) |> ignore) |> ignore
    for thinking in [ None; Some true ] do
        Assert.Throws<ArgumentException>(fun () -> ModelClient({settings with EnableThinking=thinking}) |> ignore) |> ignore
    withEnvironment [ "RETRIEVAL_MODEL_BASE_URL", "http://model.invalid/v1"; "RETRIEVAL_MODEL", "fixture"
                      "RETRIEVAL_MODEL_RESPONSE_FORMAT", "magnitude_gbnf"; "RETRIEVAL_MODEL_ENABLE_THINKING", "false" ] (fun () ->
        Assert.Equal("magnitude_gbnf", (settingsFromEnvironment ()).Value.ResponseFormat))

[<Fact>]
let ``explicit path recognition handles punctuation without parsing urls`` () =
    Assert.Equal<string list>([ "docs/PSG_Nanopass_Architecture.md" ], explicitPaths "Find links to `docs/PSG_Nanopass_Architecture.md`.")
    Assert.Equal<string list>([ "README.md"; "spec/expressions.md" ], explicitPaths "Read (spec/expressions.md), then README.md.")
    Assert.Equal<string list>([ "src/Foo.fs"; "tests/Bar.Tests.fs" ], explicitPaths "Read \"src/Foo.fs\" and tests/Bar.Tests.fs?")
    Assert.Empty(explicitPaths "See https://example.org/docs/readme.md or inspect object.property.")

[<Fact>]
let ``configuration rejects credentials redirect style paths and bad budgets`` () =
    // "http://@model/v1": System.Uri drops the empty userinfo, so validateSettings rejects any '@' itself
    // (the Python reference refused it through urlsplit's empty username).
    for endpoint in [ "http://user:pass@model/v1"; "http://@model/v1"; "http://model\n/v1"; "http://model/v1?key=secret"; "http://model/v1#fragment"; "file:///tmp/model" ] do
        rejectsConfiguration [ "RETRIEVAL_MODEL_BASE_URL", endpoint; "RETRIEVAL_MODEL", "fixture" ]
    for timeout in [ "0"; "61"; "nan" ] do
        rejectsConfiguration [ "RETRIEVAL_MODEL_BASE_URL", "http://model/v1"; "RETRIEVAL_MODEL", "fixture"; "RETRIEVAL_MODEL_TIMEOUT_SECONDS", timeout ]

[<Fact>]
let ``model profile is explicit and independent of model name`` () =
    let environment =
        [ "RETRIEVAL_MODEL_BASE_URL", "http://model.invalid/v1"; "RETRIEVAL_MODEL", "any-gateway-model"; "RETRIEVAL_MODEL_TEMPERATURE", "1"
          "RETRIEVAL_MODEL_TOP_P", "0.95"; "RETRIEVAL_MODEL_ENABLE_THINKING", "false"; "RETRIEVAL_MODEL_MAX_TOKENS", "2048"; "RETRIEVAL_MODEL_RESPONSE_FORMAT", "json_schema" ]
    let settings = ref fixtureSettings
    withEnvironment environment (fun () -> settings.Value <- (settingsFromEnvironment ()).Value)
    let client = ModelClient(settings.Value)
    let unrelatedSchema = Obj [ "type", Str "object"; "properties", Obj [ "route", Obj [ "enum", Arr [ Str "alpha"; Str "beta" ] ] ]; "required", Arr [ Str "route" ]; "additionalProperties", Bool false ]
    let request = client.PrepareRequest(Arr [ Obj [ "role", Str "user"; "content", Str "Route this work item." ] ], Some unrelatedSchema, None)
    Assert.Equal(canon unrelatedSchema, canon (path request [ "response_format"; "json_schema"; "schema" ]))
    Assert.DoesNotContain("PGQ", jsonOf request)
    Assert.Equal(canon (Obj [ "enable_thinking", Bool false ]), canon (field request "chat_template_kwargs"))
    Assert.Throws<ArgumentException>(fun () -> client.PrepareRequest(Arr [], None, None) |> ignore) |> ignore
    for (name, value) in [ "RETRIEVAL_MODEL_TEMPERATURE", "nan"; "RETRIEVAL_MODEL_TOP_P", "0"; "RETRIEVAL_MODEL_TOP_P", "True"
                           "RETRIEVAL_MODEL_ENABLE_THINKING", "yes"; "RETRIEVAL_MODEL_MAX_TOKENS", "4096"; "RETRIEVAL_MODEL_RESPONSE_FORMAT", "auto" ] do
        rejectsConfiguration [ "RETRIEVAL_MODEL_BASE_URL", "http://model.invalid/v1"; "RETRIEVAL_MODEL", "fixture"; name, value ]

[<Fact>]
let ``transport only posts bounded json and does not follow redirects`` () =
    let directory = Path.Combine(Path.GetTempPath(), "english-token-" + Guid.NewGuid().ToString("N"))
    Directory.CreateDirectory directory |> ignore
    try
        let tokenFile = Path.Combine(directory, "token")
        File.WriteAllText(tokenFile, "private-model-token")
        let status, body = ref 200, ref (completion """{"lookups":[]}""")
        let handler = new FakeHandler(respondWith status body)
        let client = clientWith { settingsWith "json_object" with BaseUrl = "http://model.invalid/v1"; TokenFile = Some tokenFile } handler
        Assert.Equal(canon (Obj [ "lookups", Arr [] ]), canon (client.Complete(Arr [ Obj [ "role", Str "user"; "content", Str "test" ] ], soon (), None, None)))
        let (meth, url, authorization, sent) = handler.Requests.[0]
        Assert.Equal(("POST", "http://model.invalid/v1/chat/completions"), (meth, url))
        Assert.Equal("Bearer private-model-token", authorization)
        Assert.DoesNotContain("private-model-token", sent)
        Assert.Equal(Int 1024L, field (parse sent) "max_tokens")
        status.Value <- 302
        let code, _, _ = englishError (Assert.Throws<EnglishError>(fun () -> client.Complete(Arr [], soon (), None, None) |> ignore))
        Assert.Equal("model_unavailable", code)
        status.Value <- 400
        let code, _, retryable = englishError (Assert.Throws<EnglishError>(fun () -> client.Complete(Arr [], soon (), None, None) |> ignore))
        Assert.Equal("model_request_rejected", code)
        Assert.False retryable
    finally
        try Directory.Delete(directory, true) with _ -> ()

[<Fact>]
let ``malformed duplicate truncated and oversized model output rejected`` () =
    let cases =
        [ String('x', 32769)
          """{"choices": []}"""
          """{"choices":[{"finish_reason":"length","message":{"content":"{}"}}]}"""
          completion """{"query":"a","query":"b"}"""
          """{"choices":[{"finish_reason":"stop","message":{"content":"{}","tool_calls":[{}]}}]}""" ]
    for raw in cases do
        let handler = new FakeHandler(respondWith (ref 200) (ref raw))
        let client = clientWith (settingsWith "json_object") handler
        let code, _, _ = englishError (Assert.Throws<EnglishError>(fun () -> client.Complete(Arr [], soon (), None, None) |> ignore))
        Assert.Equal("model_response_invalid", code)

[<Fact>]
let ``socket timeout is retryable and response body is not exposed`` () =
    // The upstream never answers within the deadline; the transport's own cancellation fires.
    let handler = new FakeHandler(fun _ cancellation -> Task.Delay(Timeout.Infinite, cancellation).GetAwaiter().GetResult(); failwith "upstream private detail")
    let client = clientWith (settingsWith "json_object") handler
    let caught = Assert.Throws<EnglishError>(fun () -> client.Complete(Arr [], DateTime.UtcNow.AddSeconds 1.0, None, None) |> ignore)
    let code, _, retryable = englishError caught
    Assert.Equal("model_timeout", code)
    Assert.True retryable
    Assert.DoesNotContain("private detail", string caught)

[<Fact>]
let ``qualified literal paths use one constrained completion and retain evidence`` () =
    withCorpus standard (fun corpus store snapshot ->
        let seed = idOfPath store snapshot "0.md"
        let settings = { settingsWith "magnitude_gbnf" with EnableThinking = Some false }
        let model = RecordingModel(settings, scripted [ generatedFor seed ], Some store)
        let body = success (askContext (service store (Some model)) "Which files does 0.md link to in FidelityFramework/test?")
        Assert.Single(model.Calls) |> ignore
        let stages = items (field body "context")
        Assert.Single(stages) |> ignore
        Assert.Equal("query_generation", text (field stages.[0] "stage"))
        Assert.Equal("1.md", text (field (items (field body "results")).[0] "path"))
        Assert.Equal(corpus.Head "test", text (field (items (field body "sources")).[0] "revision"))
        Assert.Equal("none", text (field model.Requests.[0] "reasoning_effort"))
        Assert.Equal("grammar", text (path model.Requests.[0] [ "response_format"; "type" ])))

[<Fact>]
let ``qualified literal lookup cannot substitute a mentioning file`` () =
    withCorpus standard (fun _ store _ ->
        let settings = { settingsWith "magnitude_gbnf" with EnableThinking = Some false }
        let model = RecordingModel(settings, [], Some store)
        let result = ask (service store (Some model)) "Show chunks of missing.md in FidelityFramework/test."
        Assert.Equal(("exact_seed_not_found", 422, false), failure result)
        Assert.Empty(model.Calls))
