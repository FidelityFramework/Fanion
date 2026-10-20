/// Relationship semantics and provenance against the production DuckDB pipeline.
/// The fixture builds in-memory snapshots (mutable node/edge documents, as the Python
/// dicts were), runs Enrich.run over their JSON and folds the result back into the
/// fixture the way the Python enrich_snapshot did: surviving nodes and edges keep
/// their identity, capability nodes and derived edges are fresh documents, and the
/// published node/edge order is the one Program.writeRelationships emits.
module Fanion.Tests.EnrichTests

open System
open System.Collections.Generic
open System.IO
open System.Security.Cryptography
open Fidelity.Data.JSON
open Xunit
open Fanion.Canonical
open Fanion.Snapshot

module Enrich = Fanion.Enrich
module Text = Fanion.Text

/// A mutable JSON object, standing in for the Python dict.
type Doc = Dictionary<string, J>

let private sha256Hex (value: string) =
    Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes value)).ToLowerInvariant()

let private dictOf (pairs: (string * J) list) : Doc =
    let d = Doc()
    for (k, v) in pairs do d.[k] <- v
    d

let private toJ (d: Doc) = Obj [ for kv in d -> kv.Key, kv.Value ]
let private ofJ (value: J) : Doc = match value with Obj pairs -> dictOf pairs | _ -> failwith "not an object"
let private render (d: Doc) = toText canonicalStyle (toJ d)

let str (d: Doc) (key: string) = match d.[key] with Str s -> s | _ -> failwithf "%s is not a string" key
let int' (d: Doc) (key: string) = match d.[key] with Int i -> int i | _ -> failwithf "%s is not an integer" key
let kind (d: Doc) = str d "kind"

/// The coordinator's production budgets (ingest.MAX_FILES / MAX_SOURCE_BYTES and the Snapshot constants).
let defaultLimits : Enrich.Limits =
    { MaxFiles = 20_000; MaxSourceBytes = 128L * 1024L * 1024L; MaxDerivedEdges = maxDerivedEdges; MaxCapabilityNodes = maxCapabilityNodes }

/// The same explicitly configured, hash-pinned engine used by the graph tests.
let private requireEngine () : Fanion.Load.Engine =
    match Environment.GetEnvironmentVariable "RETRIEVAL_TEST_EXTENSIONS", Environment.GetEnvironmentVariable "RETRIEVAL_TEST_LIBDUCKDB" with
    | null, _ | _, null | "", _ | _, "" ->
        Assert.Skip "Set RETRIEVAL_TEST_EXTENSIONS and RETRIEVAL_TEST_LIBDUCKDB to the verified engine"
        Unchecked.defaultof<_>
    | _, libduckdb ->
        let sourceRoot =
            match Environment.GetEnvironmentVariable "RETRIEVAL_TEST_SOURCE_ROOT" with
            | null | "" -> Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../.."))
            | root -> root
        let read name = File.ReadAllBytes(Path.Combine(sourceRoot, "config", name)) |> JsonInput.parseBytes
        let duckpgq, lib = read "duckpgq-lock.json", read "libduckdb-lock.json"
        let field name value = JsonInput.property name value |> JsonInput.string
        { LibDuckDb = libduckdb; LibDuckDbSha256 = field "libduckdb_so_sha256" lib
          DuckDbVersion = field "duckdb_version" lib; DuckDbSourceId = field "duckdb_source_id" lib
          DuckPgqVersion = field "extension_version" duckpgq
          Threads = Fanion.Query.duckdbThreads; MemoryLimit = "768MB" }

type Fixture() =
    member val Repositories = ResizeArray<Doc>() with get, set
    member val Nodes = ResizeArray<Doc>() with get, set
    member val Edges = ResizeArray<Doc>() with get, set
    member val Relationships : J option = None with get, set
    member this.Snapshot : J =
        Obj ([ "repositories", Arr [ for r in this.Repositories -> toJ r ]
               "nodes", Arr [ for n in this.Nodes -> toJ n ]
               "edges", Arr [ for e in this.Edges -> toJ e ] ]
             @ (match this.Relationships with Some r -> [ "relationships", r ] | None -> []))
    /// Canonical serialization; equality of two fixtures is equality of this text (copy.deepcopy + assertEqual).
    member this.Text = toText canonicalStyle this.Snapshot
    /// Deep copy through the JSON text, like copy.deepcopy of the snapshot dict.
    member this.Clone() =
        let copy = Fixture()
        let root = JsonInput.parse this.Text |> ofJson
        let items name =
            match root with
            | Obj pairs ->
                (match pairs |> List.tryPick (fun (k, v) -> if k = name then Some v else None) with
                 | Some(Arr values) -> [ for v in values -> ofJ v ]
                 | _ -> [])
            | _ -> []
        copy.Repositories.AddRange(items "repositories")
        copy.Nodes.AddRange(items "nodes")
        copy.Edges.AddRange(items "edges")
        copy.Relationships <- this.Relationships
        copy
    member this.Repo(short: string, ?policy: J, ?gitRef: string) : Doc =
        let name = "FidelityFramework/" + short
        let revision = (sha256Hex short).Substring(0, 40)
        let sourceUrl = "https://forge.spkez.dev/" + name
        this.Repositories.Add(dictOf ([ "name", Str name; "commit", Str revision; "source_url", Str sourceUrl
                                        "ref", Str (defaultArg gitRef "refs/heads/main") ]
                                      @ (match policy with Some p -> [ "policy", p ] | None -> [])))
        let node =
            dictOf [ "id", Str (sha256Hex (name + revision)); "repo", Str name; "revision", Str revision; "path", Str ""
                     "kind", Str "repository"; "title", Str name; "text", Str ""; "line_start", Int 0L; "line_end", Int 0L
                     "corpus_layer", Str "repository"; "classification_rule", Str "repository"
                     "source_url", Str (sourceUrl + "/src/commit/" + revision) ]
        this.Nodes.Add node
        node
    member this.File(repo: Doc, path: string, text: string) : Doc =
        let node = Doc(repo :> IDictionary<string, J>)
        node.["id"] <- Str (sha256Hex (str repo "id" + path))
        node.["path"] <- Str path
        node.["kind"] <- Str "file"
        node.["title"] <- Str path
        node.["text"] <- Str text
        node.["line_start"] <- Int 1L
        node.["line_end"] <- Int (int64 (max 1 (Text.splitLines text).Length))
        node.["corpus_layer"] <- Str "docs"
        node.["classification_rule"] <- Str "test-path-rule"
        node.["source_url"] <- Str (str repo "source_url" + "/" + path)
        this.Nodes.Add node
        this.Edges.Add(dictOf [ "src", Str (str repo "id"); "dst", Str (str node "id"); "kind", Str "contains" ])
        node

let private edgeKey (e: Doc) =
    let get k = match e.TryGetValue k with | true, Str s -> s | _ -> ""
    (get "src", get "dst", get "kind", get "evidence_id", get "rule")

/// Serialize one derived object exactly as the relationships subcommand writes it, then read it back.
let private written (value: JsonValue) : Doc =
    Json.serialize value |> JsonInput.parse |> ofJson |> ofJ

/// Run the pass and fold its result back into the fixture; nothing is applied when it raises.
let enrichWith (limits: Enrich.Limits) (fixture: Fixture) : Fixture =
    let result = Enrich.run (requireEngine ()) (JsonInput.parse fixture.Text) limits
    let originals = Dictionary<string, Doc>()
    for n in fixture.Nodes do originals.[str n "id"] <- n
    let existing = Dictionary<string * string * string * string * string, Doc>()
    for e in fixture.Edges do existing.[edgeKey e] <- e
    let nodes =
        Array.append (result.Nodes |> Array.map (fun n -> n.Id, originals.[n.Id]))
                     (result.Capabilities |> Array.map (fun c -> c.Id, written (capabilityJson c)))
        |> Array.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b))
    let edges =
        Array.append (result.PassthroughEdges |> Array.map (fun e -> let d = ofJ (ofJson e) in (match existing.TryGetValue(edgeKey d) with | true, o -> o | _ -> d)))
                     (result.Derived |> Array.map (edgeJson >> written))
        |> Array.sortBy edgeKey
    fixture.Nodes <- ResizeArray(nodes |> Array.map snd)
    fixture.Edges <- ResizeArray edges
    fixture.Relationships <-
        Some(Obj [ "version", Str relationshipVersion; "derived_edges", Int (int64 result.Derived.Length)
                   "capability_nodes", Int (int64 result.Capabilities.Length)
                   "confidence_note", Str "Fixed heuristic strengths, not calibrated probabilities or implementation proofs." ])
    fixture

let enrich (fixture: Fixture) = enrichWith defaultLimits fixture

/// assertRaisesRegex(RelationshipError, fragment); parallel phases may wrap the error in an AggregateException.
let private expectRelationshipError (fragment: string) (action: unit -> unit) =
    let rec unwrap (e: exn) =
        match e with
        | RelationshipError m -> Some m
        | :? AggregateException as a -> a.Flatten().InnerExceptions |> Seq.tryPick unwrap
        | _ -> None
    let message =
        try
            action ()
            None
        with e ->
            match unwrap e with
            | Some m -> Some m
            | None -> raise e
    match message with
    | Some m -> Assert.Contains(fragment, m)
    | None -> failwith "expected a RelationshipError"

let private ofKind (fixture: Fixture) (k: string) = [ for e in fixture.Edges do if kind e = k then yield e ]
let private capabilities (fixture: Fixture) = [ for n in fixture.Nodes do if kind n = "capability" then yield n ]

/// The Fidelity.UI -> WrenHello -> WebKit -> Gtk3 -> Farscape chain shared by several tests.
let private chain () =
    let f = Fixture()
    let ui, wren, web, gtk, generator = f.Repo("Fidelity.UI"), f.Repo("WrenHello"), f.Repo("Fidelity.WebKit"), f.Repo("Fidelity.Gtk3"), f.Repo("Farscape")
    let uiDoc = f.File(ui, "docs/architecture.md", "Experimental host: [WrenHello](/home/hhh/repos/WrenHello/WrenHello.fidproj:1).\n")
    let wrenProject = f.File(wren, "WrenHello.fidproj", "[dependencies]\nWebKit = { path = \"../Fidelity.WebKit/Fidelity.WebKit.fidproj\" }\n")
    let webProject = f.File(web, "Fidelity.WebKit.fidproj", "[dependencies]\nGtk3 = { path = \"../Fidelity.Gtk3/Fidelity.Gtk3.Native.fidproj\" }\n")
    f.File(gtk, "Fidelity.Gtk3.Native.fidproj", "[package]\nname = \"Fidelity.Gtk3.Native\"\n") |> ignore
    let binding = f.File(gtk, "Bindings/Gtk3Native/Native.clef", "module Fidelity.Gtk3.Native\n// Generated by Farscape\n[<FidelityExtern(\"gtk-3\", \"gtk_window_new\")>]\nlet gtk_window_new () = NativeDefault.zeroed ()\n")
    let profile = f.File(gtk, "pilot/gtk-3.native.pilot.toml", "[library]\nname = \"gtk-3\"\n[output]\ndirectory = \"../Bindings/Gtk3Native\"\n[[namespace]]\nname = \"Fidelity.Gtk3.Native\"\n")
    f, (ui, wren, web, gtk, generator), (uiDoc, wrenProject, webProject, binding, profile)

[<Fact>]
let ``realistic UI binding generation chain is traversable and cited`` () =
    let f, (ui, _, _, _, generator), (_, _, webProject, binding, profile) = chain ()
    let original = [ for e in f.Edges -> render e ]
    enrich f |> ignore
    let edges = List.ofSeq f.Edges
    let byId = dict [ for n in f.Nodes -> str n "id", n ]
    let rendered = HashSet<string>(edges |> List.map render)
    for e in original do Assert.True(rendered.Contains e, "structural edge dropped: " + e)
    let adjacency = Dictionary<string, ResizeArray<string>>()
    for e in edges do
        match adjacency.TryGetValue(str e "src") with
        | true, targets -> targets.Add(str e "dst")
        | _ -> adjacency.[str e "src"] <- ResizeArray [ str e "dst" ]
    let visited = HashSet<string>()
    let frontier = Stack<string>([ str ui "id" ])
    while frontier.Count > 0 do
        let node = frontier.Pop()
        if visited.Add node then
            match adjacency.TryGetValue node with
            | true, targets -> for t in targets do frontier.Push t
            | _ -> ()
    // Exact file links and their repository ownership connect the whole chain.
    Assert.True(visited.Contains(str webProject "id"))
    Assert.True(visited.Contains(str generator "id"))
    Assert.True(edges |> List.exists (fun e -> str e "src" = str binding "id" && str e "dst" = str generator "id" && kind e = "generated_by"))
    let profileEdges = ofKind f "generated_from"
    Assert.Equal(1, profileEdges.Length)
    Assert.Equal(str profile "id", str profileEdges.[0] "dst")
    Assert.Equal("inferred", str profileEdges.[0] "basis")
    for e in edges do
        let rule = match e.TryGetValue "rule" with | true, Str r -> r | _ -> ""
        if rule.StartsWith(relationshipVersion, StringComparison.Ordinal) then
            let evidence = byId.[str e "evidence_id"]
            Assert.True(int' e "evidence_line_start" >= int' evidence "line_start")
            Assert.True(int' e "evidence_line_end" <= int' evidence "line_end")
            Assert.False(String.IsNullOrEmpty(str e "explanation"))
    let generatedBy = edges |> List.find (fun e -> kind e = "generated_by")
    Assert.Contains("not established", str generatedBy "explanation")
    // Confidence keeps Python's float repr when the edge is published: 1.0, never 1.
    Assert.Contains("\"confidence\":1.0", render generatedBy)
    Assert.Contains("\"confidence\":0.9", render profileEdges.[0])

[<Fact>]
let ``enrichment is idempotent and order independent`` () =
    let f, _, _ = chain ()
    let reversedInput = f.Clone()
    reversedInput.Nodes.Reverse()
    reversedInput.Edges.Reverse()
    enrich f |> ignore
    let once = f.Text
    Assert.Same(f, enrich f)
    Assert.Equal(once, f.Text)
    enrich reversedInput |> ignore
    Assert.Equal(once, reversedInput.Text)

[<Fact>]
let ``ref revision and renamed path do not retarget links`` () =
    let f = Fixture()
    let source = f.Repo("Source")
    let target = f.Repo("Target", gitRef = "refs/heads/release/next")
    let current = f.File(target, "current.md", "current\n")
    let base' = "https://forge.spkez.dev/FidelityFramework/Target"
    let revision = str target "revision"
    let readme =
        f.File(source, "README.md",
               String.Join("\n", [ sprintf "[valid](%s/src/commit/%s/current.md)" base' revision
                                   sprintf "[wrong commit](%s/src/commit/%s/current.md)" base' (String('f', 40))
                                   sprintf "[wrong ref](%s/src/branch/main/current.md)" base'
                                   sprintf "[valid ref](%s/src/branch/release/next/current.md)" base'
                                   sprintf "[missing](%s/src/commit/%s/renamed.md)" base' revision
                                   "[outside](https://evil.example/FidelityFramework/Target/current.md)" ]) + "\n")
    enrich f |> ignore
    let refs = ofKind f "links_to" |> List.filter (fun e -> str e "dst" = str current "id")
    Assert.Equal(1, refs.Length)  // One cited relation per endpoint/rule.
    Assert.Equal<string * string * int>((str readme "id", str current "id", 1), (str refs.[0] "src", str refs.[0] "dst", int' refs.[0] "evidence_line_start"))
    Assert.Empty(ofKind f "references_project")
    let once = f.Text
    enrich f |> ignore
    Assert.Equal(once, f.Text)
    f.Nodes.RemoveAll(fun n -> str n "id" = str current "id") |> ignore
    f.Edges.RemoveAll(fun e -> kind e = "contains" && str e "dst" = str current "id") |> ignore
    enrich f |> ignore
    Assert.Empty(ofKind f "links_to")

[<Fact>]
let ``mentions preserve uncertainty and exclude fenced examples`` () =
    let f = Fixture()
    let source, target, unrelated = f.Repo("Source"), f.Repo("Fidelity.UI"), f.Repo("Farscape")
    f.File(source, "README.md", "Fidelity.UI is proposed, not implemented.\n```text\nFarscape\n```\nFidelity.UI.Other is not the exact repository name.\n") |> ignore
    enrich f |> ignore
    let mentions = ofKind f "mentions"
    Assert.Equal(1, mentions.Length)
    Assert.Equal(str target "id", str mentions.[0] "dst")
    Assert.Equal("inferred", str mentions.[0] "basis")
    Assert.Contains("proposals or negative statements", str mentions.[0] "explanation")
    Assert.False(mentions |> List.exists (fun e -> str e "dst" = str unrelated "id"))

[<Fact>]
let ``curated and api capabilities are distinct and removable`` () =
    let f = Fixture()
    f.Repo("Central", policy = Obj [ "capabilities", Arr [ Str "native-windowing" ] ]) |> ignore
    let target = f.Repo("Binding")
    let binding = f.File(target, "Bindings.clef", "// FidelityExtern(\"gtk-3\", \"gtk_widget_destroy\") is only a comment.\n[<FidelityExtern(\"gtk-3\", \"gtk_window_new\")>]\nlet gtk_window_new () = 0\n")
    enrich f |> ignore
    let caps = capabilities f
    Assert.Equal(3, caps.Length)
    Assert.Equal<Set<string>>(set [ "native-windowing"; "native-library-gtk-3" ], set [ for n in caps -> str n "title" ])
    Assert.Equal(2, (ofKind f "shares_capability").Length)
    Assert.Equal("curated", str (ofKind f "declares_capability" |> List.head) "basis")
    Assert.Equal("inferred", str (ofKind f "suggests_capability" |> List.head) "basis")
    f.Repositories.[0].["policy"] <- Obj [ "capabilities", Arr [] ]
    binding.["text"] <- Str "// no bindings remain\n"
    enrich f |> ignore
    Assert.Empty(capabilities f)
    Assert.False(f.Edges |> Seq.exists (fun e -> derivedKinds.Contains(kind e)))

[<Fact>]
let ``profile requires matching namespace and generated header`` () =
    let f, _, (_, _, _, binding, _) = chain ()
    binding.["text"] <- Str ((str binding "text").Replace("module Fidelity.Gtk3.Native", "module Unrelated"))
    enrich f |> ignore
    Assert.Empty(ofKind f "generated_from")
    binding.["text"] <- Str "module Fidelity.Gtk3.Native\n// handwritten replacement\n"
    enrich f |> ignore
    Assert.Empty(ofKind f "generated_from")

[<Fact>]
let ``module imports are cited and removed after rename`` () =
    let f = Fixture()
    let source, target = f.Repo("Composer"), f.Repo("Adaptive")
    let declaration = f.File(target, "Api.fs", "namespace FSharp.Data.Adaptive\n")
    let use' = f.File(source, "Use.fs", "module Consumer\nopen FSharp.Data.Adaptive\nlet x = FSharp.Data.Adaptive.AVal.force value\n")
    enrich f |> ignore
    let edges = ofKind f "references_project"
    Assert.Equal(4, edges.Length)
    Assert.Equal<Set<string>>(set [ str target "id"; str declaration "id" ], set [ for e in edges -> str e "dst" ])
    Assert.Equal<Set<int>>(set [ 2; 3 ], set [ for e in edges -> int' e "evidence_line_start" ])
    Assert.True(edges |> List.forall (fun e -> str e "basis" = "inferred" && str e "evidence_id" = str use' "id"))
    Assert.True(edges |> List.forall (fun e -> (render e).Contains "\"confidence\":0.75"))
    let once = f.Text
    enrich f |> ignore
    Assert.Equal(once, f.Text)
    declaration.["text"] <- Str "namespace Renamed\n"
    enrich f |> ignore
    Assert.Empty(ofKind f "references_project")

[<Fact>]
let ``module imports ignore comments literals and ambiguous owners`` () =
    let f = Fixture()
    let source, target, other = f.Repo("Composer"), f.Repo("Adaptive"), f.Repo("Other")
    f.File(target, "Api.fs", "namespace Adaptive.Api\n") |> ignore
    let use' = f.File(source, "Use.fs", "// open Adaptive.Api\n(* nested (* comment *)\nopen Adaptive.Api\n*)\nlet s = \"\"\"\nopen Adaptive.Api\n\"\"\"\nopen Missing.Api\nopen Adaptive.ApiExtra\n")
    enrich f |> ignore
    Assert.Empty(ofKind f "references_project")
    use'.["text"] <- Str "open Adaptive.Api\n"
    f.File(other, "Api.fs", "namespace Adaptive.Api\n") |> ignore
    enrich f |> ignore
    Assert.Empty(ofKind f "references_project")

[<Fact>]
let ``msbuild project reference resolves indexed target`` () =
    let f = Fixture()
    let source, target = f.Repo("Composer"), f.Repo("Adaptive")
    let project = f.File(target, "Adaptive.fsproj", "<Project />\n")
    let manifest = f.File(source, "Composer.fsproj", "<Project>\n<ProjectReference Include=\"../Adaptive/Adaptive.fsproj\" Condition=\"false\" />\n</Project>\n")
    enrich f |> ignore
    let edges = ofKind f "references_project"
    Assert.Equal<Set<string>>(set [ str target "id"; str project "id" ], set [ for e in edges -> str e "dst" ])
    Assert.True(edges |> List.forall (fun e -> str e "basis" = "extracted" && str e "evidence_id" = str manifest "id" && int' e "evidence_line_start" = 2))

[<Fact>]
let ``project references match XML local names but only unqualified Include attributes`` () =
    let f = Fixture()
    let source, target = f.Repo("Consumer"), f.Repo("Target")
    let project = f.File(target, "Target.fsproj", "<Project />\n")
    f.File(target, "Ignored.fsproj", "<Project />\n") |> ignore
    let manifest =
        f.File(source, "Consumer.fsproj",
               "<p:Project xmlns:p=\"urn:msbuild\" xmlns:x=\"urn:other\">\n<p:ItemGroup>\n"
               + "<p:ProjectReference Include=\"../Target/Target.fsproj\" Condition=\"false\" />\n"
               + "<p:ProjectReference x:Include=\"../Target/Ignored.fsproj\" />\n</p:ItemGroup>\n</p:Project>\n")
    f.File(source, "Malformed.fsproj", "<Project><ProjectReference Include=\"../Target/Ignored.fsproj\" /></Mismatch>\n") |> ignore
    enrich f |> ignore
    let edges = ofKind f "references_project"
    Assert.Equal(2, edges.Length)
    Assert.Equal<Set<string>>(set [ str target "id"; str project "id" ], set [ for e in edges -> str e "dst" ])
    Assert.True(edges |> List.forall (fun e ->
        str e "src" = str manifest "id" && str e "evidence_id" = str manifest "id" &&
        int' e "evidence_line_start" = 3 && str e "basis" = "extracted"))

[<Fact>]
let ``TOML dependency tables preserve first source evidence rather than map key order`` () =
    let forms =
        [ "[dependencies]\nZulu = { path = \"../Target/Z.fidproj\" }\nAlpha = { path = \"../Target/A.fidproj\" }\n", 2
          "[dependencies.Zulu]\npath = \"../Target/Z.fidproj\"\n[dependencies.Alpha]\npath = \"../Target/A.fidproj\"\n", 2
          "# ../Target/A.fidproj appears before the first dependency\n[dependencies]\nZulu = { path = '../Target/Z.fidproj' }\nAlpha = { path = '../Target/A.fidproj' }\n", 3
          "description = \"\"\"\n../Target/A.fidproj\n\"\"\"\n[dependencies]\nZulu = { path = '../Target/Z.fidproj' }\nAlpha = { path = '../Target/A.fidproj' }\n", 5
          "dependencies = { Zulu = { path = '../Target/Z.fidproj' }, Alpha = { path = '../Target/A.fidproj' } }\n", 1 ]
    for text, firstLine in forms do
        let f = Fixture()
        let source, target = f.Repo("Consumer"), f.Repo("Target")
        let first = f.File(target, "Z.fidproj", "[package]\nname = \"Z\"\n")
        let second = f.File(target, "A.fidproj", "[package]\nname = \"A\"\n")
        let manifest = f.File(source, "Consumer.fidproj", text)
        enrich f |> ignore
        let edges = ofKind f "references_project"
        Assert.Equal(3, edges.Length)
        Assert.Equal<Set<string>>(set [ str target "id"; str first "id"; str second "id" ], set [ for e in edges -> str e "dst" ])
        let repositoryEdge = edges |> List.find (fun e -> str e "dst" = str target "id")
        Assert.Equal(firstLine, int' repositoryEdge "evidence_line_start")
        Assert.Equal(firstLine, int' repositoryEdge "evidence_line_end")
        Assert.True(edges |> List.forall (fun e -> str e "evidence_id" = str manifest "id" && str e "basis" = "extracted"))

[<Fact>]
let ``generation profiles accept inline tables and arrays of namespace tables`` () =
    let f = Fixture()
    let repo = f.Repo("Bindings")
    let inlineProfile =
        f.File(repo, "inline.pilot.toml", "output = { directory = 'Bindings' }\nnamespace = [{ name = 'Binding.Native' }]\n")
    let sectionProfile =
        f.File(repo, "section.pilot.toml", "[output]\ndirectory = 'Bindings'\n[[namespace]]\nname = 'Binding.Native'\n")
    let binding = f.File(repo, "Bindings/Api.fs", "namespace Binding.Native\n// Generated by Tool\n")
    enrich f |> ignore
    let edges = ofKind f "generated_from"
    Assert.Equal(2, edges.Length)
    Assert.Equal<Set<string>>(set [ str inlineProfile "id"; str sectionProfile "id" ], set [ for e in edges -> str e "dst" ])
    Assert.True(edges |> List.forall (fun e -> str e "src" = str binding "id" && str e "evidence_id" = str e "dst"))
    Assert.Equal(1, edges |> List.find (fun e -> str e "dst" = str inlineProfile "id") |> fun e -> int' e "evidence_line_start")
    Assert.Equal(2, edges |> List.find (fun e -> str e "dst" = str sectionProfile "id") |> fun e -> int' e "evidence_line_start")

[<Fact>]
let ``budget failure does not publish partial results`` () =
    let f, _, _ = chain ()
    let before = f.Text
    expectRelationshipError "budget" (fun () -> enrichWith { defaultLimits with MaxDerivedEdges = 1 } f |> ignore)
    Assert.Equal(before, f.Text)

[<Fact>]
let ``many generated files are bounded and corroborated`` () =
    let f = Fixture()
    let repo = f.Repo("Bindings")
    f.File(repo, "pilot/test.pilot.toml", "[output]\ndirectory = \"../Bindings\"\n[[namespace]]\nname = \"Binding.Native\"\n") |> ignore
    for number in 0 .. 29 do
        f.File(repo, sprintf "Bindings/Module%d.clef" number, sprintf "module Binding.Native.Module%d\n// Generated by Farscape\n" number) |> ignore
    enrich f |> ignore
    Assert.Equal(30, (ofKind f "generated_from").Length)
    let before = f.Text
    expectRelationshipError "budget" (fun () -> enrichWith { defaultLimits with MaxDerivedEdges = 10 } f |> ignore)
    Assert.Equal(before, f.Text)
    expectRelationshipError "relationship source scan exceeded file/byte budget" (fun () -> enrichWith { defaultLimits with MaxSourceBytes = 1L } f |> ignore)
    Assert.Equal(before, f.Text)
    expectRelationshipError "relationship source scan exceeded file/byte budget" (fun () -> enrichWith { defaultLimits with MaxFiles = 1 } f |> ignore)
    Assert.Equal(before, f.Text)

[<Fact>]
let ``mismatched snapshot revision is rejected`` () =
    let f, _, (uiDoc, _, _, _, _) = chain ()
    uiDoc.["revision"] <- Str (String('0', 40))
    expectRelationshipError "revision" (fun () -> enrich f |> ignore)

[<Fact>]
let ``unseen native libraries grow capability graph without new rules`` () =
    let f = Fixture()
    let first, second = f.Repo("First"), f.Repo("Second")
    for repo in [ first; second ] do
        f.File(repo, "Bindings.clef", "[<FidelityExtern(\"future_accel.42\", \"new_accelerator_run\")>]\nlet new_accelerator_run () = 0\n") |> ignore
    enrich f |> ignore
    let caps = capabilities f
    Assert.Equal(2, caps.Length)
    Assert.Equal(str caps.[0] "title", str caps.[1] "title")
    Assert.StartsWith("native-library-future-accel-42-", str caps.[0] "title")
    Assert.Equal(2, (ofKind f "shares_capability").Length)
    // _library_capability is private to the pass; its identity and bound are observed through the labels it emits:
    // lossy normalization keeps distinct libraries distinct, and every label fits the 64-character bound.
    let g = Fixture()
    let repo = g.Repo("Labels")
    g.File(repo, "Bindings.clef",
           "[<FidelityExtern(\"future.accel\", \"a\")>]\n[<FidelityExtern(\"future_accel\", \"b\")>]\n[<FidelityExtern(\""
           + String('A', 128) + "\", \"c\")>]\nlet f () = 0\n") |> ignore
    enrich g |> ignore
    let labels = [ for n in capabilities g -> str n "title" ]
    Assert.Equal(3, labels.Length)
    Assert.Equal(3, (set labels).Count)
    Assert.True(labels |> List.forall (fun label -> label.Length <= 64))

[<Fact>]
let ``qualified access selects longest declaration before rejecting ambiguous owners`` () =
    let f = Fixture()
    let source, shorter, longer, ambiguous = f.Repo("Consumer"), f.Repo("Short"), f.Repo("Long"), f.Repo("Other")
    let shortFile = f.File(shorter, "Api.fs", "namespace Layer.Api\n")
    let longFile = f.File(longer, "Detail.fs", "namespace Layer.Api.Detail\n")
    let duplicate = f.File(ambiguous, "Detail.fs", "namespace Layer.Api.Detail\n")
    let use' =
        f.File(source, "Use.fs",
               "let a = Layer.Api.Detail.call value\nlet b = Layer.Api.call value\nlet c = Layer.Api.Detail.other value\n")
    let references () =
        ofKind f "references_project" |> List.filter (fun e -> str e "src" = str use' "id")
    enrich f |> ignore
    let edges = references ()
    Assert.Equal(2, edges.Length)
    Assert.Equal<Set<string>>(set [ str shorter "id"; str shortFile "id" ], set [ for e in edges -> str e "dst" ])
    Assert.True(edges |> List.forall (fun e -> int' e "evidence_line_start" = 2))
    // The longest declaration is ambiguous: choosing a shorter unambiguous prefix
    // would invent a relationship for line 1. Removing that ambiguity admits it.
    f.Nodes.Remove duplicate |> ignore
    f.Edges.RemoveAll(fun e -> str e "dst" = str duplicate "id") |> ignore
    enrich f |> ignore
    let resolved =
        references ()
        |> List.filter (fun e -> str e "dst" = str longer "id" || str e "dst" = str longFile "id")
    Assert.Equal(2, resolved.Length)
    Assert.True(resolved |> List.forall (fun e ->
        int' e "evidence_line_start" = 1 && int' e "evidence_line_end" = 1 &&
        str e "evidence_id" = str use' "id" && str e "basis" = "inferred"))
    Assert.Equal(4, (references ()).Length)

[<Fact>]
let ``module declaration fanout preserves the first evidence separately for each usage rule`` () =
    let f = Fixture()
    let source, owner = f.Repo("Consumer"), f.Repo("Owner")
    let first = f.File(owner, "First.fs", "namespace Shared.Api\n")
    let second = f.File(owner, "Second.fs", "namespace Shared.Api\n")
    let use' =
        f.File(source, "Use.fs",
               "open Shared.Api\nopen global.Shared.Api\nlet x = Shared.Api.first value\nlet y = Shared.Api.second value\n")
    let reversed = f.Clone()
    reversed.Nodes.Reverse()
    reversed.Edges.Reverse()
    enrich f |> ignore
    enrich reversed |> ignore
    Assert.Equal(f.Text, reversed.Text)
    let edges = ofKind f "references_project"
    Assert.Equal(6, edges.Length)
    Assert.Equal<Set<string>>(set [ str owner "id"; str first "id"; str second "id" ], set [ for e in edges -> str e "dst" ])
    for e in edges do
        let expectedLine = if (str e "rule").Contains("qualified-access", StringComparison.Ordinal) then 3 else 1
        Assert.Equal(expectedLine, int' e "evidence_line_start")
        Assert.Equal(expectedLine, int' e "evidence_line_end")
        Assert.Equal(str use' "id", str e "evidence_id")
        Assert.Equal("inferred", str e "basis")
        Assert.Contains("\"confidence\":0.75", render e)
    Assert.Equal(3, edges |> List.filter (fun e -> (str e "rule").Contains("module-open", StringComparison.Ordinal)) |> List.length)
    Assert.Equal(3, edges |> List.filter (fun e -> (str e "rule").Contains("module-qualified-access", StringComparison.Ordinal)) |> List.length)

[<Fact>]
let ``curated capability wins over inferred evidence and shared labels form an ordinal star`` () =
    let f = Fixture()
    let policy = Obj [ "capabilities", Arr [ Str "native-windowing"; Str "native-windowing" ] ]
    let last, middle, anchor = f.Repo("Zulu", policy = policy), f.Repo("Middle", policy = policy), f.Repo("Alpha", policy = policy)
    let binding = f.File(last, "Bindings.fs", "[<FidelityExtern(\"gtk-3\", \"gtk_window_new\")>]\nlet gtk_window_new () = 0\n")
    let reversed = f.Clone()
    reversed.Nodes.Reverse()
    reversed.Edges.Reverse()
    reversed.Repositories.Reverse()
    enrich f |> ignore
    enrich reversed |> ignore
    // Repository metadata retains input order, while the derived graph must not.
    Assert.Equal<string list>([ for n in f.Nodes -> render n ], [ for n in reversed.Nodes -> render n ])
    Assert.Equal<string list>([ for e in f.Edges -> render e ], [ for e in reversed.Edges -> render e ])
    let windowCaps = capabilities f |> List.filter (fun n -> str n "title" = "native-windowing")
    Assert.Equal(3, windowCaps.Length)
    Assert.True(windowCaps |> List.forall (fun n -> str n "classification_rule" = relationshipVersion + ":curated"))
    let capFor repo = windowCaps |> List.find (fun n -> str n "repo" = str repo "repo") |> fun n -> str n "id"
    let a, b, c = capFor anchor, capFor middle, capFor last
    let shares = ofKind f "shares_capability"
    Assert.Equal<Set<string * string>>(set [ a, b; b, a; a, c; c, a ], set [ for e in shares -> str e "src", str e "dst" ])
    Assert.Equal(4, shares.Length)
    Assert.True(shares |> List.forall (fun e -> str e "evidence_id" = str e "src" && int' e "evidence_line_start" = 0))
    Assert.Equal(3, (ofKind f "declares_capability").Length)
    let inferred = ofKind f "suggests_capability" |> List.filter (fun e -> str e "dst" = c)
    Assert.Equal(1, inferred.Length)
    Assert.Equal(str binding "id", str inferred.Head "evidence_id")
    Assert.Equal("inferred", str inferred.Head "basis")
    Assert.Contains("\"confidence\":0.8", render inferred.Head)

[<Fact>]
let ``generation profile joins respect directory namespace and header boundaries`` () =
    let f = Fixture()
    let repo = f.Repo("Bindings")
    let parent = f.File(repo, "pilot/parent.pilot.toml", "[output]\ndirectory = \"../Bindings\"\n[[namespace]]\nname = \"Binding.Native\"\n")
    let nested = f.File(repo, "pilot/nested.pilot.toml", "[output]\ndirectory = \"../Bindings/Sub\"\n[[namespace]]\nname = \"Binding.Native.Sub\"\n")
    let direct = f.File(repo, "Bindings/Api.fs", "namespace Binding.Native\n// Generated by Tool\n")
    let child = f.File(repo, "Bindings/Sub/Api.fs", "module Binding.Native.Sub.Api\n// Generated by Tool\n")
    f.File(repo, "BindingsExtra/Api.fs", "namespace Binding.Native\n// Generated by Tool\n") |> ignore
    f.File(repo, "Bindings/Other.fs", "namespace Binding.NativeExtra\n// Generated by Tool\n") |> ignore
    f.File(repo, "Bindings/Handwritten.fs", "namespace Binding.Native\n// handwritten\n") |> ignore
    f.File(repo, "Bindings/LateHeader.fs",
           "namespace Binding.Native\n" + String.replicate 19 "// filler\n" + "// Generated by Tool\n") |> ignore
    enrich f |> ignore
    let edges = ofKind f "generated_from"
    Assert.Equal<Set<string * string>>(
        set [ str direct "id", str parent "id"; str child "id", str parent "id"; str child "id", str nested "id" ],
        set [ for e in edges -> str e "src", str e "dst" ])
    Assert.Equal(3, edges.Length)
    Assert.True(edges |> List.forall (fun e ->
        str e "evidence_id" = str e "dst" && int' e "evidence_line_start" = 2 &&
        int' e "evidence_line_end" = 2 && str e "basis" = "inferred"))
    Assert.True(edges |> List.forall (fun e -> (render e).Contains("\"confidence\":0.9", StringComparison.Ordinal)))

[<Fact>]
let ``publication budgets count unique edges and reject excess capability nodes atomically`` () =
    let f = Fixture()
    let source, target = f.Repo("Consumer"), f.Repo("Owner")
    f.File(target, "Api.fs", "namespace Shared.Api\n") |> ignore
    f.File(source, "Use.fs", String.replicate 30 "open Shared.Api\n") |> ignore
    enrichWith { defaultLimits with MaxDerivedEdges = 2 } f |> ignore
    Assert.Equal(2, (ofKind f "references_project").Length)
    Assert.True(ofKind f "references_project" |> List.forall (fun e -> int' e "evidence_line_start" = 1))
    let g = Fixture()
    g.Repo("Capabilities", policy = Obj [ "capabilities", Arr [ Str "first"; Str "second" ] ]) |> ignore
    let before = g.Text
    expectRelationshipError "capability node budget exceeded" (fun () ->
        enrichWith { defaultLimits with MaxCapabilityNodes = 1 } g |> ignore)
    Assert.Equal(before, g.Text)
    enrichWith { defaultLimits with MaxCapabilityNodes = 2 } g |> ignore
    Assert.Equal(2, (capabilities g).Length)
