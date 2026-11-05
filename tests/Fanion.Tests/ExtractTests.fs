/// One-for-one port of test_ingest.py against Extract.build over local fixture repositories.
/// The Python tests patched `_git` to prove excluded blobs were never read; here the
/// `candidate_files` counter (blobs that reached the batch reader) carries that evidence.
module Fanion.Tests.ExtractTests

open System
open System.Diagnostics
open System.IO
open System.Text
open Fidelity.Data.JSON
open System.Text.RegularExpressions
open Xunit
open Fanion.Canonical
open Fanion.Git
open Fanion

/// The CLI defaults (Program.extractLimits); tests tighten individual caps from here.
let limits : Extract.Limits =
    { MaxRepositories = 64; MaxTreeEntries = 100_000L; MaxTreeBytes = 16 * 1024 * 1024; MaxFiles = 20_000; MaxFileBytes = 1024 * 1024
      MaxSourceBytes = 128L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxOutputBytes = 384L * 1024L * 1024L
      MaxLayerRules = 64; MaxRulePatterns = 32; MaxTotalRulePatterns = 256; MaxLinksPerFile = 1024; MaxBuildSeconds = 600.0; GitTimeoutSeconds = 15.0 }

let private git (directory: string) (args: string list) =
    let info = ProcessStartInfo("git")
    for a in [ "-C"; directory ] @ args do info.ArgumentList.Add a
    info.RedirectStandardOutput <- true; info.RedirectStandardError <- true; info.UseShellExecute <- false
    for key in [ for k in info.Environment.Keys -> k ] do
        if key.StartsWith("GIT_", StringComparison.Ordinal) then info.Environment.Remove key |> ignore
    info.Environment.["GIT_CONFIG_NOSYSTEM"] <- "1"; info.Environment.["GIT_CONFIG_GLOBAL"] <- "/dev/null"
    use p = Process.Start info
    let out = p.StandardOutput.ReadToEnd()
    let err = p.StandardError.ReadToEnd()
    p.WaitForExit()
    if p.ExitCode <> 0 then failwithf "git %A failed: %s" args err
    out.Trim()

let private str (node: JsonValue) (name: string) = JsonInput.property name node |> JsonInput.string
let private int' (node: JsonValue) (name: string) = JsonInput.property name node |> JsonInput.int32

/// A built snapshot: the canonical bytes plus a parsed view for assertions.
type Built(bytes: byte[]) =
    let root = JsonInput.parseBytes bytes
    member _.Bytes = bytes
    member _.Text = Encoding.UTF8.GetString bytes
    member _.Root = root
    member this.Id = str this.Root "snapshot_id"
    member this.Nodes = JsonInput.property "nodes" this.Root |> JsonInput.items
    member this.Edges = [ for e in JsonInput.property "edges" this.Root |> JsonInput.items -> str e "src", str e "dst", str e "kind" ]
    member this.Files = Map.ofList [ for n in this.Nodes do if str n "kind" = "file" then yield str n "path", n ]
    member this.Paths = this.Files |> Map.toList |> List.map fst |> set
    member this.Stats = JsonInput.property "stats" this.Root
    member this.Repositories = JsonInput.property "repositories" this.Root |> JsonInput.items
    member this.Counter(name: string) = JsonInput.property name this.Stats |> JsonInput.int64
    member this.Reasons = Map.ofList [ for name, value in JsonInput.property "excluded_by_reason" this.Stats |> JsonInput.fields -> name, JsonInput.int64 value ]

let private canonical (value: JsonValue) = toText canonicalStyle (Raw value)
let private sameSnapshot (a: Built) (b: Built) =
    Assert.Equal(a.Id, b.Id)
    Assert.True((a.Bytes = b.Bytes), "snapshot bytes differ")
let private layers (b: Built) = b.Files |> Map.map (fun _ n -> str n "corpus_layer")

let private toJson (value: J) = toText canonicalStyle value |> JsonInput.parse
let private raw (json: string) = Raw (JsonInput.parse json)
let build (manifest: J) = Built(snd (Extract.build (toJson manifest) limits))
let private buildWith (limits: Extract.Limits) (manifest: J) = Built(snd (Extract.build (toJson manifest) limits))

[<Fact>]
let ``JSON input rejects duplicate members at every nesting level`` () =
    for text in
        [ """{"name":"first","name":"second"}"""
          """{"repositories":[{"name":"first","name":"second"}]}"""
          """{"policy":{"capabilities":[],"capabilities":["other"]}}"""
          """[{"escaped":1,"\u0065scaped":2}]""" ] do
        Assert.Throws<JsonInputError>(fun () -> JsonInput.parse text |> ignore) |> ignore
    let separateObjects = JsonInput.parse """[{"name":"first"},{"name":"second"}]"""
    Assert.Equal(2, (JsonInput.items separateObjects).Length)

[<Fact>]
let ``JSON input rejects malformed UTF8 without replacement characters`` () =
    for bytes in
        [ [| 0x22uy; 0xC3uy; 0x28uy; 0x22uy |]
          [| 0x22uy; 0xEDuy; 0xA0uy; 0x80uy; 0x22uy |]
          [| 0x22uy; 0xF0uy; 0x9Fuy; 0x22uy |] ] do
        Assert.Throws<JsonInputError>(fun () -> JsonInput.parseBytes bytes |> ignore) |> ignore
    Assert.Equal("\u03BB", JsonInput.parseBytes [| 0x22uy; 0xCEuy; 0xBBuy; 0x22uy |] |> JsonInput.string)

[<Fact>]
let ``JSON numeric literals retain exact spelling before conversion`` () =
    for text in [ "9007199254740993"; "-9223372036854775808"; "1.2300"; "1e+30"; "-0" ] do
        match JsonInput.parse text with
        | JsonValue.NumberLiteral literal -> Assert.Equal(text, JsonNumberLiteral.text literal)
        | _ -> failwith "Parsed numeric token did not retain its literal"
        Assert.Equal(text, JsonInput.parse text |> Raw |> toText canonicalStyle)
    Assert.Equal(9007199254740993L, JsonInput.parse "9007199254740993" |> JsonInput.int64)
    for text in [ "9223372036854775808"; "1.0"; "1e0" ] do
        Assert.Throws<JsonInputError>(fun () -> JsonInput.parse text |> JsonInput.int64 |> ignore) |> ignore

/// Set (or replace) one field of the first manifest repository.
let private withField (name: string) (value: J) (manifest: J) =
    match manifest with
    | Obj [ "repositories", Arr (Obj first :: rest) ] ->
        Obj [ "repositories", Arr (Obj ((first |> List.filter (fun (k, _) -> k <> name)) @ [ name, value ]) :: rest) ]
    | _ -> failwith "unexpected manifest shape"

/// Parallel phases wrap IngestError in AggregateException exactly as the CLI unwraps it.
let private ingestError (action: unit -> unit) : string =
    try
        action ()
        failwith "expected IngestError"
    with
    | IngestError message -> message
    | :? AggregateException as error ->
        match error.Flatten().InnerExceptions |> Seq.tryPick (function IngestError m -> Some m | _ -> None) with
        | Some message -> message
        | None -> reraise ()

let private rejects (label: string) (manifest: J) =
    let raised = try build manifest |> ignore; false with IngestError _ -> true | :? AggregateException as e when e.Flatten().InnerExceptions |> Seq.exists (function IngestError _ -> true | _ -> false) -> true
    Assert.True(raised, sprintf "expected IngestError for %s" label)

type Fixture() =
    let temp = Path.Combine(Path.GetTempPath(), "extract-" + Guid.NewGuid().ToString("N"))
    let repo = Path.Combine(temp, "repository")
    do
        Directory.CreateDirectory repo |> ignore
        git repo [ "init"; "--quiet" ] |> ignore
        git repo [ "config"; "user.email"; "test@example.invalid" ] |> ignore
        git repo [ "config"; "user.name"; "Snapshot Test" ] |> ignore
    member _.Root = temp
    member _.Repo = repo
    member _.Git(args: string list) = git repo args
    member _.WriteBytes(path: string, value: byte[]) =
        let target = Path.Combine(repo, path)
        Directory.CreateDirectory(Path.GetDirectoryName target) |> ignore
        File.WriteAllBytes(target, value)
    member this.Write(path: string, value: string) = this.WriteBytes(path, Encoding.UTF8.GetBytes value)
    member this.Commit() =
        this.Git [ "add"; "." ] |> ignore
        this.Git [ "commit"; "--no-gpg-sign"; "--quiet"; "-m"; "fixture" ] |> ignore
        this.Git [ "rev-parse"; "HEAD" ]
    member _.Manifest(revision: string) : J =
        Obj [ "repositories", Arr [ Obj [ "path", Str repo; "name", Str "FidelityFramework/fixture"; "commit", Str revision
                                          "source_url", Str "https://forge.example/FidelityFramework/fixture" ] ] ]
    interface IDisposable with
        member _.Dispose() = try Directory.Delete(temp, true) with _ -> ()

let private bytes (text: string) = Encoding.UTF8.GetBytes text

[<Fact>]
let ``document titles preserve declared metadata and headings instead of duplicating paths`` () =
    use f = new Fixture()
    let documents =
        [ "blog/yaml.md", "---\ntitle: \"Retrieving Fidelity\"\nauthors:\n  - Houston Haynes\n---\nBody\n", "Retrieving Fidelity"
          "blog/toml.md", "+++\ntitle = 'Compiler Evidence'\n+++\nBody\n", "Compiler Evidence"
          "guide.md", "```\n# Example code\n```\n# Actual Guide #\nBody\n", "Actual Guide"
          "untitled.md", "A document without a declared title.\n", "untitled.md"
          "code.fs", "let title = \"Code is not document metadata\"\n", "code.fs" ]
    for path, content, _ in documents do f.Write(path, content)
    let snapshot = build (f.Manifest(f.Commit()))
    for path, content, title in documents do
        Assert.Equal(title, str snapshot.Files.[path] "title")
        Assert.Equal(content, str snapshot.Files.[path] "text")
        Assert.Equal(path, str snapshot.Files.[path] "path")

[<Fact>]
let ``malformed declared document metadata reports its source path`` () =
    use f = new Fixture()
    f.Write("broken.md", "+++\ntitle = [\n+++\nBody\n")
    let manifest = f.Manifest(f.Commit())
    Assert.Contains("broken.md", ingestError (fun () -> build manifest |> ignore))

[<Fact>]
let ``exact commit excludes dirty staged untracked and later commit`` () =
    use f = new Fixture()
    f.Write("code.fs", "let committed = 1\n")
    let revision = f.Commit()
    f.Write("code.fs", "let staged = 2\n")
    f.Git [ "add"; "code.fs" ] |> ignore
    f.Write("code.fs", "let dirty = 3\n")
    f.Write("untracked.fs", "let untracked = 4\n")
    let first = build (f.Manifest revision)
    Assert.Equal<Set<string>>(set [ "code.fs" ], first.Paths)
    Assert.Equal("let committed = 1\n", str first.Files.["code.fs"] "text")
    f.Commit() |> ignore
    sameSnapshot first (build (f.Manifest revision))

[<Fact>]
let ``symlinks submodules binary generated large and secrets excluded`` () =
    use f = new Fixture()
    f.Write("keep.md", "retained\n")
    let first = f.Commit()
    let outside = Path.Combine(f.Root, "outside.txt")
    File.WriteAllText(outside, "outside sentinel")
    File.CreateSymbolicLink(Path.Combine(f.Repo, "link.md"), outside) |> ignore
    f.WriteBytes("binary.dat", Array.concat [ bytes "binary"; [| 0uy |]; bytes "data" ])
    f.WriteBytes("nonutf8.txt", [| 0xFFuy; 0xFEuy |])
    f.WriteBytes("big.txt", Array.create (limits.MaxFileBytes + 1) (byte 'x'))
    f.Write("vendor/third.md", "excluded")
    f.Write("obj/Generated.fs", "excluded")
    f.Write("minified.min.js", "excluded")
    f.Write(".env", "SECRET=value")
    f.Write(".env.example", "SECRET=example")
    f.Write("key.pem", "private")
    f.Write("id_ed25519", "private")
    f.Write("innocent.txt", "-----BEGIN OPENSSH PRIVATE KEY-----\nprivate\n")
    f.Write("lfs.txt", "version https://git-lfs.github.com/spec/v1\noid sha256:abc\n")
    f.Git [ "update-index"; "--add"; "--cacheinfo"; "160000"; first; "submodule" ] |> ignore
    let revision = f.Commit()
    let snapshot = build (f.Manifest revision)
    Assert.Equal<Set<string>>(set [ "keep.md" ], snapshot.Paths)
    Assert.DoesNotContain("outside sentinel", snapshot.Text)
    Assert.DoesNotContain("SECRET=value", snapshot.Text)

[<Fact>]
let ``tracked build definitions retain policy without admitting build artifacts`` () =
    use f = new Fixture()
    let definitions = set [ "build/Boundary.targets"; "build/Contract.props"; "tools/build/Rules.targets" ]
    let excluded =
        set [ "build/Generated.fs"; "build/result.dll"; "build/notes.md"
              "build/obj/Generated.targets"; "bin/build/Generated.props"; "vendor/build/ThirdParty.targets"
              ".codex/build/Instructions.targets"; "build/.env.targets"; "build/id_rsa.props" ]
    for path in Set.union definitions excluded do f.Write(path, "<Project />\n")
    f.Write("build/Secret.targets", "-----BEGIN OPENSSH PRIVATE KEY-----\nprivate\n")
    let manifest = f.Manifest(f.Commit())
    let snapshot = build manifest
    Assert.Equal<Set<string>>(definitions, snapshot.Paths)
    Assert.Equal(4L, snapshot.Counter "candidate_files")
    Assert.Equal(6L, snapshot.Reasons.["excluded_directory"])
    Assert.Equal(1L, snapshot.Reasons.["operational_config"])
    Assert.Equal(2L, snapshot.Reasons.["secret_filename"])
    for path in definitions do
        Assert.Equal("<Project />\n", str snapshot.Files.[path] "text")
        Assert.Equal("config", str snapshot.Files.[path] "corpus_layer")
    let scoped = build (manifest |> withField "policy" (raw """{"exclude_paths":["build/Boundary.targets"]}"""))
    Assert.Equal<Set<string>>(Set.remove "build/Boundary.targets" definitions, scoped.Paths)
    Assert.Equal(1L, scoped.Reasons.["policy_scope"])

[<Fact>]
let ``line chunks preserve crlf overlap and anchors`` () =
    use f = new Fixture()
    let text = String.Join("", [ for n in 1 .. 204 -> sprintf "line %d\r\n" n ]) + "line 205"
    f.Write("docs/a b.md", text)
    let revision = f.Commit()
    let snapshot = build (f.Manifest revision)
    let chunks = snapshot.Nodes |> List.filter (fun n -> str n "kind" = "chunk") |> List.sortBy (fun n -> int' n "line_start")
    Assert.Equal<(int * int) list>([ 1, 100; 91, 190; 181, 205 ], [ for n in chunks -> int' n "line_start", int' n "line_end" ])
    Assert.StartsWith("line 1\r\n", str chunks.Head "text")
    Assert.EndsWith("line 205", str (List.last chunks) "text")
    Assert.Contains(sprintf "/src/commit/%s/docs/a%%20b.md#L91-L190" revision, str chunks.[1] "source_url")
    let fileId = str snapshot.Files.["docs/a b.md"] "id"
    for chunk in chunks do Assert.Contains((str chunk "id", fileId, "chunks_of"), snapshot.Edges)

[<Fact>]
let ``selected generated sources keep other exclusions and committed reads`` () =
    use f = new Fixture()
    let selected =
        [ "bindings/UI.generated.fs", "let committed = 1\n"; "bindings/UI.generated.cs", "class UI {}\n"
          "bindings/Window.g.fs", "let window = 1\n"; "bindings/Window.g.cs", "class Window {}\n" ]
    let ordinary = [ "bindings/UI.clef", "module UI\n"; "README.md", "library\n" ]
    let excludedWithoutRead =
        [ "build/Build.generated.fs", "excluded-build"; "vendor/Vendor.g.cs", "excluded-vendor"; ".codex/Instructions.g.fs", "excluded-operational"
          ".ssh/Key.g.fs", "excluded-credentials"; ".env.generated.fs", "excluded-env"; "bindings/id_ed25519.generated.fs", "excluded-key-name"
          "bindings/key.pem", "excluded-pem"; "bindings/code.min.js", "excluded-artifact"; "bindings/lock.lock", "excluded-lockfile"
          "bindings/Large.generated.fs", String('x', limits.MaxFileBytes + 1) ]
    let rejectedContent =
        [ "bindings/Binary.generated.fs", Array.concat [ bytes "bad"; [| 0uy |]; bytes "binary" ]; "bindings/Invalid.g.cs", [| 0xFFuy |]
          "bindings/Private.generated.cs", bytes "-----BEGIN OPENSSH PRIVATE KEY-----\nprivate\n"
          "bindings/Lfs.g.fs", bytes "version https://git-lfs.github.com/spec/v1\noid sha256:abc\n" ]
    for (path, text) in selected @ ordinary @ excludedWithoutRead do f.Write(path, text)
    for (path, content) in rejectedContent do f.WriteBytes(path, content)
    File.CreateSymbolicLink(Path.Combine(f.Repo, "bindings/Link.g.fs"), Path.Combine(f.Repo, "README.md")) |> ignore
    let revision = f.Commit()
    let fallback = build (f.Manifest revision)
    Assert.Equal<Set<string>>(set (List.map fst ordinary), fallback.Paths)
    f.Write("bindings/UI.generated.fs", "let dirty = 2\n")
    f.Git [ "add"; "bindings/UI.generated.fs" ] |> ignore
    f.Write("bindings/Untracked.g.cs", "untracked")
    // Even the broadest selection only waives the four source suffixes.
    let manifest = f.Manifest revision |> withField "policy" (raw """{"role": "generated", "include_generated": ["*"]}""")
    let snapshot = build manifest
    Assert.Equal<Set<string>>(set (List.map fst (selected @ ordinary)), snapshot.Paths)
    Assert.Equal("let committed = 1\n", str snapshot.Files.["bindings/UI.generated.fs"] "text")
    // Only selected, ordinary and content-rejected blobs reach the reader; nothing excluded by path was read.
    Assert.Equal(int64 (selected.Length + ordinary.Length + rejectedContent.Length), snapshot.Counter "candidate_files")
    Assert.Equal(int64 selected.Length, snapshot.Counter "accepted_selected_generated_files")
    Assert.Equal(selected |> List.sumBy (fun (_, text) -> int64 (bytes text).Length), snapshot.Counter "accepted_selected_generated_bytes")
    Assert.Equal(snapshot.Counter "tree_entries", snapshot.Counter "accepted_files" + snapshot.Counter "excluded_entries")
    Assert.Equal(canonical snapshot.Stats, canonical (JsonInput.property "stats" snapshot.Repositories.Head))
    for text in [ "private\n"; "excluded-build"; "let dirty"; "untracked" ] do Assert.DoesNotContain(text, snapshot.Text)

[<Fact>]
let ``generated selection is nominated and policy changes snapshot not source ids`` () =
    use f = new Fixture()
    f.Write("bindings/UI.generated.fs", "let ui = 1\n")
    f.Write("other/Other.generated.fs", "let other = 1\n")
    f.Write("README.md", "library")
    let manifest = f.Manifest(f.Commit())
    let before = build manifest
    Assert.Equal("""{"authority":0,"capabilities":[],"exclude_paths":[],"include_generated":[],"include_paths":[],"role":"component"}""",
                 canonical (JsonInput.property "policy" before.Repositories.Head))
    sameSnapshot before (build (manifest |> withField "policy" (Obj [])))
    let policy includeGenerated capabilities authority =
        Obj [ "role", Str "generated"; "authority", Int authority; "capabilities", Arr (List.map Str capabilities); "include_generated", Arr (List.map Str includeGenerated) ]
    let after = build (manifest |> withField "policy" (policy [ "BINDINGS/*.GENERATED.FS"; "absent/*" ] [ "ui"; "native-bindings" ] 30L))
    Assert.Equal<Set<string>>(set [ "README.md"; "bindings/UI.generated.fs" ], after.Paths)
    Assert.NotEqual<string>(before.Id, after.Id)
    Assert.Equal(str before.Files.["README.md"] "id", str after.Files.["README.md"] "id")
    Assert.Equal("repository-policy-v1", str after.Root "policy_version")
    sameSnapshot after (build (manifest |> withField "policy" (policy [ "absent/*"; "BINDINGS/*.GENERATED.FS" ] [ "native-bindings"; "ui" ] 30L)))
    let reprioritized = build (manifest |> withField "policy" (policy [ "absent/*"; "BINDINGS/*.GENERATED.FS" ] [ "native-bindings"; "ui" ] 31L))
    Assert.NotEqual<string>(after.Id, reprioritized.Id)
    Assert.Equal(canonical (JsonInput.property "nodes" after.Root), canonical (JsonInput.property "nodes" reprioritized.Root))

[<Fact>]
let ``manifest rejects invalid repository policy before git reads`` () =
    use f = new Fixture()
    f.Write("README.md", "library")
    let manifest = f.Manifest(f.Commit())
    for policy in [ "null"; "[]"; """{"unknown": 1}"""; """{"role": "official"}"""; """{"authority": true}"""; """{"capabilities": ["Fidelity.UI"]}"""; """{"include_generated": ["../*"]}""" ] do
        let invalid = manifest |> withField "policy" (raw policy)
        // parseManifest runs no git command at all; build must fail with that same validation message.
        let message = ingestError (fun () -> Extract.parseManifest (toJson invalid) limits |> ignore)
        Assert.Equal(message, ingestError (fun () -> build invalid |> ignore))
        Assert.DoesNotContain("git", message)

[<Fact>]
let ``curated paths limit blob reads and cannot admit hard exclusions`` () =
    use f = new Fixture()
    let content =
        [ "README.md", "platform overview"; "docs/architecture.md", "platform contracts"; "docs/old/draft.md", "old draft"
          "src/Platform.clef", "module Platform"; "Hardware/Microcontroller.clef", "large hardware body"; "Hardware/Board.md", "board docs"
          "bindings/UI.g.fs", "let generated = 1"; "build/Build.clef", "build output"; ".env", "env secret" ]
    for (path, body) in content do f.Write(path, body)
    let manifest = f.Manifest(f.Commit())
    let original = build manifest
    let policy includePaths =
        Obj [ "include_paths", Arr (List.map Str includePaths); "exclude_paths", Arr [ Str "docs/old/*" ]; "include_generated", Arr [ Str "bindings/*" ] ]
    let includePaths = [ "README.md"; "DOCS/*"; "src/*"; "bindings/*"; "build/*"; ".env" ]
    let excluded = set [ "Hardware/Microcontroller.clef"; "Hardware/Board.md"; "docs/old/draft.md"; "build/Build.clef"; ".env" ]
    let curated = build (manifest |> withField "policy" (policy includePaths))
    Assert.Equal<Set<string>>(Set.difference (set (List.map fst content)) excluded, curated.Paths)
    Assert.Equal<Map<string, int64>>(Map.ofList [ "policy_scope", 3L; "excluded_directory", 1L; "secret_filename", 1L ], curated.Reasons)
    Assert.Equal(4L, curated.Counter "candidate_files")  // out-of-scope blobs were never read
    Assert.Equal(1L, curated.Counter "accepted_selected_generated_files")
    Assert.True(curated.Counter "candidate_bytes" < original.Counter "candidate_bytes")
    sameSnapshot curated (build (manifest |> withField "policy" (policy (List.rev includePaths))))
    // Empty includes retain ordinary scope; exclusions still apply and do
    // not opt generated suffixes in implicitly.
    let excludedOnly = build (manifest |> withField "policy" (raw """{"include_paths": [], "exclude_paths": ["HARDWARE/*"]}"""))
    Assert.Equal<Set<string>>(Set.difference original.Paths (set [ "Hardware/Microcontroller.clef"; "Hardware/Board.md" ]), excludedOnly.Paths)

[<Fact>]
let ``relative links stay in committed manifest repo and skip code`` () =
    use f = new Fixture()
    f.Write("README.md", "readme")
    f.Write("docs/target space.md", "target")
    f.Write("docs/code-only.md", "not a real link")
    let start = """[up](../README.md#heading)
[local](<target space.md> "title")
[repeat](target%20space.md)
[escaped](../../outside.md)
[encoded](%2e%2e/%2e%2e/outside.md)
[absolute](/README.md)
[external](https://outside.example/README.md)
[network](//outside.example/README.md)
[missing](missing.md)
[secret](../.env)
[reference][ref]
[ref]: ../README.md
`[inline](code-only.md)`
```markdown
[fenced](code-only.md)
```
"""
    f.Write("docs/start.md", start)
    f.Write(".env", "secret")
    let revision = f.Commit()
    f.Write("docs/missing.md", "untracked")
    let snapshot = build (f.Manifest revision)
    let files = snapshot.Files
    let links = snapshot.Edges |> List.filter (fun (_, _, kind) -> kind = "links_to")
    let startId = str files.["docs/start.md"] "id"
    Assert.Equal<Set<string * string>>(set [ startId, str files.["README.md"] "id"; startId, str files.["docs/target space.md"] "id" ],
                                       set [ for (src, dst, _) in links -> src, dst ])
    Assert.Equal(2, links.Length)

[<Fact>]
let ``revision manifest and source ids are deterministic`` () =
    use f = new Fixture()
    f.Write("empty.txt", "")
    f.Write("source.fs", "let x = 1\n")
    let revision = f.Commit()
    let manifest = f.Manifest revision
    let first = build manifest
    sameSnapshot first (build (manifest |> withField "commit" (Str (revision.ToUpperInvariant()))))
    for node in first.Nodes do
        Assert.Equal(64, (str node "id").Length)
        Assert.Equal(revision, str node "revision")
    let withoutId = match ofJson first.Root with Obj pairs -> Obj (pairs |> List.filter (fun (k, _) -> k <> "snapshot_id")) | _ -> failwith "snapshot is not an object"
    Assert.Equal(first.Id, fst (digest canonicalStyle withoutId))
    Assert.Equal(0, int' first.Files.["empty.txt"] "line_start")
    Assert.Equal(0, int' first.Files.["empty.txt"] "line_end")
    f.Write("source.fs", "let x = 2\n")
    let newer = build (f.Manifest(f.Commit()))
    Assert.NotEqual<string>(first.Id, newer.Id)
    Assert.NotEqual<string>(str first.Files.["source.fs"] "id", str newer.Files.["source.fs"] "id")

[<Fact>]
let ``clone location does not change snapshot`` () =
    use f = new Fixture()
    f.Write("readme.md", "versioned")
    let revision = f.Commit()
    let first = build (f.Manifest revision)
    let clone = Path.Combine(f.Root, "bare.git")
    git f.Root [ "clone"; "--quiet"; "--bare"; f.Repo; clone ] |> ignore
    sameSnapshot first (build (f.Manifest revision |> withField "path" (Str clone)))

[<Fact>]
let ``docs and code have equal eligibility and explicit syntactic layers`` () =
    use f = new Fixture()
    let expected =
        Map.ofList
            [ "README.md", "docs"; "AGENTS.md", "docs"; "CLAUDE.md", "docs"; "docs/overview.txt", "docs"; "spec/language.md", "spec"
              "docs/CCS_Architecture.md", "architecture"; "docs/decisions/001.md", "decisions"; "docs/guides/start.md", "guides"
              "research/experiment.md", "research"; "src/Main.fs", "implementation"; "tests/MainTests.fs", "tests"; "config/app.yaml", "config"
              "samples/hello.fs", "examples"; "scripts/check.py", "tooling"; "unknown.data", "other" ]
    for path in expected.Keys do f.Write(path, "untrusted committed evidence\n")
    let snapshot = build (f.Manifest(f.Commit()))
    let files = snapshot.Files
    Assert.Equal<Set<string>>(set expected.Keys, snapshot.Paths)
    Assert.Equal<Map<string, string>>(expected, layers snapshot)
    for node in snapshot.Nodes do
        if str node "kind" = "repository" then
            Assert.Equal("repository", str node "corpus_layer")
            Assert.Equal("repository", str node "classification_rule")
        else
            let file = files.[str node "path"]
            Assert.Equal(str file "corpus_layer", str node "corpus_layer")
            Assert.Equal(str file "classification_rule", str node "classification_rule")
            Assert.Matches(Regex "^default:[0-9]+:[a-z]+$", str node "classification_rule")
    Assert.Equal(int64 expected.Count, snapshot.Counter "accepted_files")
    Assert.Equal(canonical snapshot.Stats, canonical (JsonInput.property "stats" snapshot.Repositories.Head))
    Assert.Equal(int64 expected.Count, JsonInput.property "accepted_by_layer" snapshot.Stats |> JsonInput.fields |> List.sumBy (fun (_, value) -> JsonInput.property "files" value |> JsonInput.int64))

[<Fact>]
let ``ordered layer rules are deterministic versioned and cannot admit secrets`` () =
    use f = new Fixture()
    f.Write("docs/plan.md", "plan\n")
    f.Write("src/Main.fs", "let x = 1\n")
    f.Write(".mcp.json", "hidden\n")
    let manifest = f.Manifest(f.Commit())
    let rule name patterns = Obj [ "name", Str name; "patterns", Arr (List.map Str patterns) ]
    let rules = [ rule "research" [ "DOCS/*" ]; rule "decisions" [ "docs/plan.md" ] ]
    let first = build (manifest |> withField "layer_rules" (Arr rules))
    sameSnapshot first (build (manifest |> withField "layer_rules" (Arr rules)))
    Assert.Equal("manifest:0:research", str first.Files.["docs/plan.md"] "classification_rule")
    Assert.Equal("implementation", str first.Files.["src/Main.fs"] "corpus_layer")
    Assert.Equal("[\"docs/*\"]", JsonInput.property "layer_rules" first.Repositories.Head |> JsonInput.items |> List.head |> JsonInput.property "patterns" |> canonical)
    Assert.Equal(Extract.classificationVersion, str first.Root "classification_version")
    Assert.NotEmpty(JsonInput.property "classification_defaults" first.Root |> JsonInput.items)
    Assert.False(first.Files.ContainsKey ".mcp.json")
    let second = build (manifest |> withField "layer_rules" (Arr (List.rev rules)))
    Assert.Equal("decisions", str second.Files.["docs/plan.md"] "corpus_layer")
    Assert.NotEqual<string>(first.Id, second.Id)
    // Even a currently nonmatching rule changes provenance and snapshot hash.
    let third = build (manifest |> withField "layer_rules" (Arr (List.rev rules @ [ rule "guides" [ "absent/*" ] ])))
    Assert.NotEqual<string>(second.Id, third.Id)

[<Fact>]
let ``rules and explicit branch ref validation`` () =
    use f = new Fixture()
    f.Write("README.md", "committed")
    let manifest = f.Manifest(f.Commit())
    let docs patterns = sprintf """[{"name": "docs", "patterns": %s}]""" patterns
    let badRules =
        [ "null"; "{}"; """[{"name": "repository", "patterns": ["*"]}]"""; """[{"name": "semantic-proof", "patterns": ["*"]}]"""
          docs "[]"; docs "\"*\""; docs """["../*"]"""; docs """["/absolute/*"]"""; docs """["bad\\path"]"""
          docs (sprintf "[\"%s\"]" (String('a', 257))); docs "[true]"; """[{"name": "docs", "patterns": ["*"], "extra": 1}]"""
          "[" + String.Join(",", Array.create (limits.MaxLayerRules + 1) """{"name": "docs", "patterns": ["*"]}""") + "]" ]
    for rules in badRules do rejects (sprintf "layer_rules %s" rules) (manifest |> withField "layer_rules" (raw rules))
    let absent = build manifest
    Assert.True(Json.prop "ref" absent.Repositories.Head |> Option.isNone)
    for reference in [ Str "main"; Str "HEAD"; Str "refs/tags/v1"; Str "refs/heads/"; Str "refs/heads/a..b"; Str "refs/heads/a.lock"; Str "refs/heads/a/"; Str "refs/heads/a\nb"; Str "refs/heads/a//b"; Bool true ] do
        rejects (sprintf "ref %A" reference) (manifest |> withField "ref" reference)
    let explicit = build (manifest |> withField "ref" (Str "refs/heads/main"))
    Assert.Equal("refs/heads/main", str explicit.Repositories.Head "ref")
    Assert.NotEqual<string>(absent.Id, explicit.Id)

[<Fact>]
let ``parser sized source and docs are included up to one mib`` () =
    use f = new Fixture()
    Assert.Equal(1024 * 1024, limits.MaxFileBytes)
    Assert.Equal(128L * 1024L * 1024L, limits.MaxSourceBytes)
    f.Write("src/Compiler/pars.fsy", String('x', 298376))
    f.Write("docs/parser.md", String('x', 298376))
    f.Write("too-large.md", String('x', limits.MaxFileBytes + 1))
    let snapshot = build (f.Manifest(f.Commit()))
    Assert.Equal<Set<string>>(set [ "src/Compiler/pars.fsy"; "docs/parser.md" ], snapshot.Paths)
    Assert.Equal(596752L, snapshot.Counter "accepted_bytes")
    Assert.Equal<Map<string, int64>>(Map.ofList [ "file_too_large", 1L ], snapshot.Reasons)

/// The owning corpus configuration in this primary checkout.
let private frameworkSource (sourceId: string) =
    let config = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../config/corpus.framework.json"))
    let root = File.ReadAllBytes config |> JsonInput.parseBytes
    JsonInput.property "sources" root |> JsonInput.items |> List.find (fun source -> str source "id" = sourceId)

[<Fact>]
let ``coordinator full scope keeps project and test layers with hard exclusions`` () =
    use f = new Fixture()
    let admitted =
        set [ "Bozzetto.Composer/FormatterSession.fs"; "Bozzetto.Composer.Protocol/Protocol.fs"; "Bozzetto/ComposerTools.fs"; "Bozzetto/ComposerRoutes.fs"
              "Bozzetto/ComposerSupervisor.fs"; "Bozzetto.Composer.Tests/FormatPreviewTests.fs"; "docs/Incremental_Provider_Checkpoint_2026-10-01.md"; "AGENTS.md"
              "Bozzetto.Core/Cohort.fs"; "Bozzetto.Gui/Legacy.fs"; "bozzetto-vs/Legacy.cs"; "Bozzetto.Tests/UnrelatedTests.fs"; "samples/example.fs"
              "samples/demos/Bozzetto.Samples.ConsoleTicker.Tests/Tests.fs" ]
    let excluded = set [ "Bozzetto/bin/build.fs"; "node_modules/package/index.js"; "private.pem"; ".codex/notes.md"; ".mcp.json"; "Bozzetto/WebAssets.fs" ]
    for path in Set.union admitted excluded do f.Write(path, "selection witness\n")
    let source = frameworkSource "bozzetto"
    let manifest = f.Manifest(f.Commit()) |> withField "policy" (Raw (JsonInput.property "policy" source)) |> withField "layer_rules" (Raw (JsonInput.property "layer_rules" source))
    let snapshot = build manifest
    Assert.Equal<Set<string>>(admitted, snapshot.Paths)
    Assert.Equal(int64 admitted.Count, snapshot.Counter "candidate_files")
    Assert.Equal<Map<string, int64>>(Map.ofList [ "excluded_directory", 2L; "excluded_suffix", 1L; "operational_config", 2L; "policy_scope", 1L ], snapshot.Reasons)
    let layer path = str snapshot.Files.[path] "corpus_layer"
    Assert.Equal("tests", layer "Bozzetto.Tests/UnrelatedTests.fs")
    Assert.Equal("tests", layer "Bozzetto.Composer.Tests/FormatPreviewTests.fs")
    Assert.Equal("tests", layer "samples/demos/Bozzetto.Samples.ConsoleTicker.Tests/Tests.fs")
    Assert.Equal("examples", layer "samples/example.fs")
    Assert.Equal("implementation", layer "Bozzetto.Core/Cohort.fs")

[<Fact>]
let ``formatter scope keeps owned sources and excludes pinned and archived material`` () =
    use f = new Fixture()
    let admitted = set [ "src/Calque.Incremental/DocumentFormatter.fsi"; "src/Calque.Core/CodeFormatterImpl.fs"; "src/Calque.Syntax/Parser.fs"; "src/Calque.Tests/LifecycleTests.fs"; "README.md" ]
    let excluded = set [ ".deps/fcs/Compiler.fs"; "UPSTREAM_CHANGELOG.md"; "UPSTREAM_CODE_OF_CONDUCT.md" ]
    for path in Set.union admitted excluded do f.Write(path, "selection witness\n")
    let manifest = f.Manifest(f.Commit()) |> withField "policy" (Raw (JsonInput.property "policy" (frameworkSource "calque")))
    let snapshot = build manifest
    Assert.Equal<Set<string>>(admitted, snapshot.Paths)
    Assert.Equal(int64 admitted.Count, snapshot.Counter "candidate_files")
    Assert.Equal<Map<string, int64>>(Map.ofList [ "policy_scope", int64 excluded.Count ], snapshot.Reasons)

[<Fact>]
let ``operational exclusions are counted without blob reads or path disclosure`` () =
    use f = new Fixture()
    let paths = [ ".mcp.json"; ".clef-deploy-state.json"; ".serena/memories/private.md"; ".hermes/config.yaml"; ".codex/notes.md"; "nested/.mcp.json" ]
    paths |> List.iteri (fun i path -> f.Write(path, sprintf "private-sentinel-%d" i))
    f.Write("AGENTS.md", "untrusted agent guidance")
    f.Write("CLAUDE.md", "untrusted agent guidance")
    f.WriteBytes("binary.dat", [| byte 'n'; byte 'o'; 0uy |])
    f.WriteBytes("invalid.txt", [| 0xFFuy |])
    let revision = f.Commit()
    let snapshot = build (f.Manifest revision)
    Assert.Equal<Set<string>>(set [ "AGENTS.md"; "CLAUDE.md" ], snapshot.Paths)
    Assert.Equal<Map<string, int64>>(Map.ofList [ "operational_config", 6L; "binary_content", 1L; "non_utf8_content", 1L ], snapshot.Reasons)
    Assert.Equal(10L, snapshot.Counter "tree_entries")
    Assert.Equal(4L, snapshot.Counter "candidate_files")  // the six operational blobs were never read
    Assert.Equal(snapshot.Counter "tree_entries", snapshot.Counter "accepted_files" + snapshot.Counter "excluded_entries")
    for path in paths do Assert.DoesNotContain(path, snapshot.Text)
    Assert.DoesNotContain("private-sentinel", snapshot.Text)

[<Fact>]
let ``manifest rejects noncommit invalid revision and implicit parent repo`` () =
    use f = new Fixture()
    f.Write("docs/readme.md", "x")
    let revision = f.Commit()
    for value in [ "HEAD"; revision.Substring(0, 12); String('0', 40); f.Git [ "rev-parse"; "HEAD:docs/readme.md" ]; f.Git [ "rev-parse"; "HEAD^{tree}" ] ] do
        rejects (sprintf "revision %s" value) (f.Manifest value)
    rejects "nested path" (f.Manifest revision |> withField "path" (Str (Path.Combine(f.Repo, "docs"))))
    rejects "credentialed source_url" (f.Manifest revision |> withField "source_url" (Str "https://user:password@forge.example/repo"))

[<Fact>]
let ``only manifest repositories are indexed and order is canonical`` () =
    use f = new Fixture()
    f.Write("first.md", "first")
    let revision = f.Commit()
    let first = f.Manifest revision
    let other = Path.Combine(f.Root, "other")
    git f.Root [ "clone"; "--quiet"; f.Repo; other ] |> ignore
    File.WriteAllText(Path.Combine(other, "outside.md"), "never indexed")
    let snapshot = build first
    Assert.Equal(1, snapshot.Repositories.Length)
    let entry = Obj [ "path", Str other; "name", Str "other"; "commit", Str revision; "source_url", Str "https://forge.example/other" ]
    let entries = match first with Obj [ "repositories", Arr items ] -> items | _ -> failwith "unexpected manifest shape"
    let before = build (Obj [ "repositories", Arr (entries @ [ entry ]) ])
    sameSnapshot before (build (Obj [ "repositories", Arr (entry :: entries) ]))
    Assert.DoesNotContain("never indexed", before.Text)

[<Fact>]
let ``caps abort instead of returning partial snapshot`` () =
    use f = new Fixture()
    f.Write("a.md", "[a](b.md)\n[a](b.md)\n")
    f.Write("b.md", "payload")
    let revision = f.Commit()
    let capped =
        [ "MaxFiles", { limits with MaxFiles = 1 }; "MaxSourceBytes", { limits with MaxSourceBytes = 1L }; "MaxNodes", { limits with MaxNodes = 1 }
          "MaxEdges", { limits with MaxEdges = 1 }; "MaxTreeEntries", { limits with MaxTreeEntries = 1L }; "MaxTreeBytes", { limits with MaxTreeBytes = 1 }
          "MaxOutputBytes", { limits with MaxOutputBytes = 1L }; "MaxLinksPerFile", { limits with MaxLinksPerFile = 1 }; "MaxBuildSeconds", { limits with MaxBuildSeconds = 0.0 } ]
    for (name, tightened) in capped do
        let raised =
            try buildWith tightened (f.Manifest revision) |> ignore; false
            with IngestError _ -> true | :? AggregateException as e when e.Flatten().InnerExceptions |> Seq.exists (function IngestError _ -> true | _ -> false) -> true
        Assert.True(raised, sprintf "expected IngestError under %s" name)

[<Fact>]
let ``cli atomic output and failure preserves previous snapshot`` () =
    use f = new Fixture()
    f.Write("readme.md", "committed")
    let revision = f.Commit()
    let manifest = Path.Combine(f.Root, "manifest.json")
    let output = Path.Combine(f.Root, "snapshot.json")
    // The extractor binary is built beside the tests by the project reference.
    let binary = Path.Combine(AppContext.BaseDirectory, "retrieval-native")
    let run () =
        let info = ProcessStartInfo(binary)
        for a in [ "extract"; manifest; output ] do info.ArgumentList.Add a
        info.RedirectStandardOutput <- true; info.RedirectStandardError <- true; info.UseShellExecute <- false
        use p = Process.Start info
        let stdout = p.StandardOutput.ReadToEnd()
        let stderr = p.StandardError.ReadToEnd()
        p.WaitForExit()
        p.ExitCode, stdout, stderr
    File.WriteAllText(manifest, toText canonicalStyle (f.Manifest revision))
    let code, _, stderr = run ()
    Assert.True((code = 0), sprintf "extract failed (%d): %s" code stderr)
    let before = File.ReadAllBytes output
    Assert.Equal<byte[]>(Array.append (build (f.Manifest revision)).Bytes [| byte '\n' |], before)
    Assert.Equal(UnixFileMode.UserRead ||| UnixFileMode.UserWrite, File.GetUnixFileMode output)
    File.WriteAllText(manifest, toText canonicalStyle (f.Manifest "HEAD"))
    let code, _, _ = run ()
    Assert.Equal(2, code)  // validation rejection: the native CLI's exit status (Python's wrapper used 1)
    Assert.Equal<byte[]>(before, File.ReadAllBytes output)
    Assert.Empty(Directory.GetFileSystemEntries(f.Root, ".snapshot-*"))
