/// One-for-one port of test_relationships.py against the F# relationship pass.
/// The fixture builds in-memory snapshots (mutable node/edge documents, as the Python
/// dicts were), runs Enrich.run over their JSON and folds the result back into the
/// fixture the way the Python enrich_snapshot did: surviving nodes and edges keep
/// their identity, capability nodes and derived edges are fresh documents, and the
/// published node/edge order is the one Program.writeRelationships emits.
module Retrieval.Native.Tests.EnrichTests

open System
open System.Collections.Generic
open System.IO
open System.Security.Cryptography
open System.Text.Json
open Xunit
open Retrieval.Native.Canonical
open Retrieval.Native.Snapshot

module Enrich = Retrieval.Native.Enrich
module Text = Retrieval.Native.Text

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
        use document = JsonDocument.Parse(this.Text)
        let root = ofElement document.RootElement
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
let private written (write: Utf8JsonWriter -> unit) : Doc =
    use stream = new MemoryStream()
    (use writer = new Utf8JsonWriter(stream)
     write writer)
    use parsed = JsonDocument.Parse(stream.ToArray())
    ofJ (ofElement parsed.RootElement)

/// Run the pass and fold its result back into the fixture; nothing is applied when it raises.
let enrichWith (limits: Enrich.Limits) (fixture: Fixture) : Fixture =
    use document = JsonDocument.Parse(fixture.Text)
    let result = Enrich.run document.RootElement limits
    let originals = Dictionary<string, Doc>()
    for n in fixture.Nodes do originals.[str n "id"] <- n
    let existing = Dictionary<string * string * string * string * string, Doc>()
    for e in fixture.Edges do existing.[edgeKey e] <- e
    let nodes =
        Array.append (result.Nodes |> Array.map (fun n -> n.Id, originals.[n.Id]))
                     (result.Capabilities |> Array.map (fun c -> c.Id, written (fun w -> writeCapability w c)))
        |> Array.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b))
    let edges =
        Array.append (result.PassthroughEdges |> Array.map (fun e -> let d = ofJ (ofElement e) in (match existing.TryGetValue(edgeKey d) with | true, o -> o | _ -> d)))
                     (result.Derived |> Array.map (fun e -> written (fun w -> writeEdge w e)))
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
