/// Deterministic, bounded component snapshots of explicitly manifested Git commits:
/// files and line chunks, never a working tree. Output bytes match the reference
/// extractor (committed-git-lines-v3) so component identities are unchanged.
module Fanion.Extract

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.IO
open System.Text
open Fidelity.Data.JSON
open System.Text.RegularExpressions
open System.Threading.Tasks
open Fanion.Canonical
open Fanion.Git

let extractorVersion = "committed-git-lines-v3"
let classificationVersion = "path-layers-v1"
let chunkLines, chunkOverlapLines = 100, 10

type Limits =
    { MaxRepositories: int; MaxTreeEntries: int64; MaxTreeBytes: int; MaxFiles: int; MaxFileBytes: int; MaxSourceBytes: int64
      MaxNodes: int; MaxEdges: int; MaxOutputBytes: int64; MaxLayerRules: int; MaxRulePatterns: int; MaxTotalRulePatterns: int
      MaxLinksPerFile: int; MaxBuildSeconds: float; GitTimeoutSeconds: float }

let corpusLayers = set [ "docs"; "spec"; "architecture"; "decisions"; "guides"; "research"; "implementation"; "tests"; "config"; "examples"; "tooling"; "other" ]

let defaultLayerRules : (string * string list) list =
    [ "decisions", [ "decisions/*"; "*/decisions/*"; "adr/*"; "*/adr/*"; "*supersession*.md" ]
      "research", [ "research/*"; "*/research/*"; "*research*.md" ]
      "spec", [ "spec/*"; "*/spec/*"; "specification/*"; "*/specification/*" ]
      "architecture", [ "architecture/*"; "*/architecture/*"; "*architecture*.md"; "*architecture*.rst" ]
      "guides", [ "guides/*"; "*/guides/*"; "tutorials/*"; "*/tutorials/*"; "contributing*"; "*/contributing*" ]
      "docs", [ "docs/*"; "*/docs/*"; "documentation/*"; "*/documentation/*"; "hugo/content/*"; "site-design/*"; "rationale/*" ]
      "tests", [ "tests/*"; "*/tests/*"; "test/*"; "*/test/*"; "test_*.py"; "*/test_*.py"; "*_test.*"; "*.test.*"; "*.spec.*" ]
      "examples", [ "samples/*"; "*/samples/*"; "examples/*"; "*/examples/*" ]
      "docs", [ "*.md"; "*.markdown"; "*.rst"; "*.adoc"; "readme*"; "*/readme*"; "license*"; "*/license*"; "notice*"; "*/notice*" ]
      "config", [ "*.json"; "*.toml"; "*.yaml"; "*.yml"; "*.xml"; "*.ini"; "*.cfg"; "*.config"; "*.fsproj"; "*.csproj"; "*.sln"; "*.props"; "*.targets"; ".gitignore"; "*/.gitignore"; ".gitattributes"; "*/.gitattributes"; "go.mod"; "*/go.mod"; "go.sum"; "*/go.sum" ]
      "tooling", [ "scripts/*"; "*/scripts/*"; "buildtools/*"; "*/buildtools/*"; "tools/*"; "*/tools/*"; "makefile"; "*/makefile"; "dockerfile*"; "*/dockerfile*"; "*.sh" ]
      "implementation", [ "src/*"; "*/src/*"; "lib/*"; "*/lib/*"; "*.fs"; "*.fsi"; "*.fsy"; "*.fsl"; "*.clef"; "*.cs"; "*.py"; "*.rs"; "*.go"; "*.c"; "*.h"; "*.cpp"; "*.hpp"; "*.js"; "*.mjs"; "*.ts"; "*.tsx"; "*.jsx"; "*.html"; "*.css"; "*.scss"; "*.sql" ]
      "other", [ "*" ] ]

let private operationalDirs = set [ ".serena"; ".hermes"; ".codex" ]
let private operationalFiles = set [ ".mcp.json"; ".clef-deploy-state.json" ]
let private excludedDirs = set [ ".git"; ".svn"; ".hg"; ".ssh"; ".gnupg"; ".aws"; ".azure"; ".venv"; "venv"; "env"; "node_modules"; "vendor"; "vendors"; "third_party"; "third-party"; "__pycache__"; ".cache"; ".next"; ".nuxt"; ".output"; "dist"; "build"; "out"; "target"; "coverage"; "htmlcov"; "bin"; "obj"; "packages"; ".pytest_cache"; ".mypy_cache" ]
let private generatedSuffixes = [ ".generated.fs"; ".generated.cs"; ".g.fs"; ".g.cs" ]
let private excludedSuffixes = [ ".min.js"; ".min.css"; ".map"; ".pyc"; ".pyo"; ".dll"; ".exe"; ".so"; ".dylib"; ".o"; ".a"; ".class"; ".jar"; ".zip"; ".gz"; ".tar"; ".7z"; ".pdf"; ".png"; ".jpg"; ".jpeg"; ".gif"; ".webp"; ".ico"; ".woff"; ".woff2"; ".ttf"; ".lock"; ".generated.fs"; ".generated.cs"; ".g.cs"; ".g.fs"; ".pem"; ".key"; ".p12"; ".pfx"; ".keystore"; ".jks" ]
let private secretNames = set [ ".netrc"; ".npmrc"; ".pypirc"; "credentials"; "credentials.json"; "credentials.yaml"; "credentials.yml"; "secrets.json"; "secrets.yaml"; "secrets.yml"; "secrets.toml"; "secrets.env" ]
let private privateKey = Regex(@"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----", RegexOptions.Compiled)
let private inlineLink = Regex(@"(?<!!)\[[^\[\]\n]{0,2048}\]\(\s*(<[^>\n]{1,4096}>|[^\s()]{1,4096})(?:\s+""[^""\n]{0,2048}""|\s+'[^'\n]{0,2048}')?\s*\)", RegexOptions.Compiled)
let private referenceDef = Regex(@"^ {0,3}\[([^\[\]\n]{1,2048})\]:\s*(<[^>\n]{1,4096}>|\S{1,4096})", RegexOptions.Compiled ||| RegexOptions.Multiline)
let private referenceLink = Regex(@"(?<!!)\[([^\[\]\n]{1,2048})\]\[([^\[\]\n]{0,2048})\]", RegexOptions.Compiled)
let private fenceMarker = Regex(@"^ {0,3}(`{3,}|~{3,})", RegexOptions.Compiled)
let private inlineCode = Regex(@"(`+).*?\1", RegexOptions.Compiled)
let private whitespace = Regex(@"\s+", RegexOptions.Compiled)
let private shaPattern = Regex(@"\A[0-9a-fA-F]{40}\z", RegexOptions.Compiled)
let private namePattern = Regex(@"\A[A-Za-z0-9_.\-]+(?:/[A-Za-z0-9_.\-]+)*\z", RegexOptions.Compiled)
let private refPattern = Regex(@"\Arefs/heads/[A-Za-z0-9_][A-Za-z0-9_./\-]{0,239}\z", RegexOptions.Compiled)

type LayerRule = { Origin: string; Name: string; Patterns: string list }

type Entry =
    { Path: string; Name: string; Commit: string; SourceUrl: string; LayerRules: LayerRule list; Policy: Policy.Normalized; Ref: string option }

let private identity (parts: J list) = fst (digest canonicalStyle (Arr parts))

/// urllib.parse.quote(path, safe="/")
let private quotePath (path: string) =
    let sb = StringBuilder()
    for b in Encoding.UTF8.GetBytes path do
        let c = char b
        if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c = '_' || c = '.' || c = '-' || c = '~' || c = '/' then sb.Append(c) |> ignore
        else sb.Append('%').Append(b.ToString("X2")) |> ignore
    sb.ToString()

let private sourceUrl (baseUrl: string) (commit: string) (path: string) (start: int) (finish: int) =
    let mutable url = baseUrl + "/src/commit/" + commit
    if path <> "" then url <- url + "/" + quotePath path
    if start <> 0 then url <- url + sprintf "#L%d" start + (if finish <> start then sprintf "-L%d" finish else "")
    url

/// Only LF is a line boundary; CRLF and a final unterminated line are preserved.
let lines (text: string) : string[] =
    let newlineCount = text |> Seq.filter ((=) '\n') |> Seq.length
    let parts = text.Split('\n')
    let parts = if parts.Length > 0 && parts.[parts.Length - 1] = "" then parts.[.. parts.Length - 2] else parts
    parts |> Array.mapi (fun i part -> if i < newlineCount then part + "\n" else part)

let layerRules (raw: JsonValue option) (limits: Limits) : LayerRule list =
    let manifestRules =
        match raw with
        | None -> []
        | Some value ->
            let rules =
                match value with
                | JsonValue.Array rules when rules.Length <= limits.MaxLayerRules -> rules
                | _ -> raise (IngestError "layer_rules must be a bounded ordered list")
            let mutable count = 0
            [ for rule in rules do
                match rule with
                | JsonValue.Object fields when (fields |> List.map fst |> set) = set [ "name"; "patterns" ] -> ()
                | _ -> raise (IngestError "layer rules require exactly name and patterns")
                let name =
                    match JsonInput.property "name" rule with
                    | JsonValue.String name when corpusLayers.Contains name -> name
                    | _ -> raise (IngestError "unknown corpus layer")
                let patterns =
                    match JsonInput.property "patterns" rule with
                    | JsonValue.Array patterns when not patterns.IsEmpty && patterns.Length <= limits.MaxRulePatterns -> patterns
                    | _ -> raise (IngestError "layer patterns must be a nonempty bounded list")
                count <- count + patterns.Length
                if count > limits.MaxTotalRulePatterns then raise (IngestError "too many layer patterns")
                let values =
                    [ for pattern in patterns ->
                        let text =
                            match pattern with
                            | JsonValue.String text -> text
                            | _ -> raise (IngestError "invalid repository-relative layer glob")
                        if text = "" || Encoding.UTF8.GetByteCount text > 256 || text.StartsWith("/", StringComparison.Ordinal) || text.Contains '\\'
                           || text.Split('/') |> Array.exists (fun part -> part = "" || part = "." || part = "..")
                           || text |> Seq.exists (fun c -> int c < 32 || int c = 127) then
                            raise (IngestError "invalid repository-relative layer glob")
                        text.ToLowerInvariant() ]
                yield { Origin = "manifest"; Name = name; Patterns = values } ]
    manifestRules @ [ for (name, patterns) in defaultLayerRules -> { Origin = "default"; Name = name; Patterns = patterns } ]

let private realpath (path: string) =
    let full = Path.GetFullPath path
    let info = DirectoryInfo(full)
    match info.LinkTarget with
    | null -> full.TrimEnd('/')
    | _ -> (match info.ResolveLinkTarget(true) with null -> full.TrimEnd('/') | target -> target.FullName.TrimEnd('/'))

let parseManifest (manifest: JsonValue) (limits: Limits) : Entry list =
    let entries =
        match manifest with
        | JsonValue.Object [ "repositories", entries ] -> entries
        | _ -> raise (IngestError "manifest must contain only a repositories list")
    let entries =
        match entries with
        | JsonValue.Array entries when not entries.IsEmpty && entries.Length <= limits.MaxRepositories -> entries
        | _ -> raise (IngestError(sprintf "manifest requires 1\u2013%d repositories" limits.MaxRepositories))
    let names, paths = HashSet<string>(), HashSet<string>()
    let required = set [ "path"; "name"; "commit"; "source_url" ]
    let optional = set [ "layer_rules"; "ref"; "policy" ]
    let validated =
        [ for entry in entries do
            let keys = match entry with JsonValue.Object fields -> fields |> List.map fst |> set | _ -> Set.empty
            if not (Set.isSubset required keys) || not (Set.isSubset keys (Set.union required optional)) then
                raise (IngestError "repository entries require path, name, commit, source_url and optional layer_rules/ref/policy")
            let str name =
                match JsonInput.property name entry with
                | JsonValue.String text when text <> "" -> text
                | _ -> raise (IngestError "repository fields must be nonempty strings")
            let path, name, commit, source = str "path", str "name", str "commit", str "source_url"
            if name.Length > 256 || not (namePattern.IsMatch name) || name.Split('/') |> Array.exists (fun p -> p = "." || p = "..") then raise (IngestError "invalid logical repository name")
            if not (shaPattern.IsMatch commit) then raise (IngestError "commit must be a full 40-hex SHA-1 commit ID")
            if not (Path.IsPathRooted path) || not (Directory.Exists path) then raise (IngestError "repository path must be an existing absolute directory")
            let resolved = realpath path
            let url = Resolve.splitUrl source
            let fragment = source.IndexOf '#'
            if (url.Scheme <> "https" && url.Scheme <> "http") || url.Netloc = "" || url.Netloc.Contains '@' || url.Query <> "" || fragment >= 0
               || source |> Seq.exists (fun c -> int c < 33) then
                raise (IngestError "source_url must be an HTTP(S) repository URL without credentials/query/fragment")
            if names.Contains name || paths.Contains resolved then raise (IngestError "duplicate repository name or path")
            names.Add name |> ignore; paths.Add resolved |> ignore
            let policy =
                try
                    match Json.prop "policy" entry with
                    | Some(JsonValue.Object _ as policy) -> Policy.normalize (Some policy)
                    | Some _ -> raise (Snapshot.RelationshipError "repository policy must be an object")
                    | None -> Policy.normalize None
                with Snapshot.RelationshipError message -> raise (IngestError message)
            let rules = layerRules (Json.prop "layer_rules" entry) limits
            let reference =
                match Json.prop "ref" entry with
                | Some r ->
                    let text = match r with JsonValue.String text -> text | _ -> null
                    if isNull text || not (refPattern.IsMatch text) || text.Contains ".." || text.Contains "//"
                       || text.Split('/') |> Array.exists (fun part -> part = "" || part.StartsWith(".", StringComparison.Ordinal) || part.EndsWith(".", StringComparison.Ordinal) || part.EndsWith(".lock", StringComparison.Ordinal)) then
                        raise (IngestError "ref must be a bounded explicit refs/heads/... name")
                    Some text
                | _ -> None
            yield { Path = resolved; Name = name; Commit = commit.ToLowerInvariant(); SourceUrl = source.TrimEnd('/'); LayerRules = rules; Policy = policy; Ref = reference } ]
    validated |> List.sortWith (fun a b -> String.CompareOrdinal(a.Name, b.Name))

let private endsWithAny (value: string) (suffixes: string list) = suffixes |> List.exists (fun s -> value.EndsWith(s, StringComparison.Ordinal))
let private selectedGenerated (path: string) (patterns: string list) =
    let lower = path.ToLowerInvariant()
    endsWithAny lower generatedSuffixes && patterns |> List.exists (fun p -> Glob.matches lower p)

let private excludedReason (path: string) (includeGenerated: string list) : string option =
    let parts = path.ToLowerInvariant().Split('/')
    let name = parts.[parts.Length - 1]
    let dirs = parts.[.. parts.Length - 2]
    // Tracked MSBuild definitions are source policy even beneath a build directory.
    // Other excluded ancestors and all filename/content/policy checks still apply.
    let msbuildDefinition = endsWithAny name [ ".props"; ".targets" ]
    if dirs |> Array.exists operationalDirs.Contains || operationalFiles.Contains name then Some "operational_config"
    elif dirs |> Array.exists (fun dir -> excludedDirs.Contains dir && not (dir = "build" && msbuildDefinition)) then Some "excluded_directory"
    elif name = ".env" || name.StartsWith(".env.", StringComparison.Ordinal) || name.StartsWith("id_rsa", StringComparison.Ordinal) || name.StartsWith("id_ed25519", StringComparison.Ordinal) || name.StartsWith("id_ecdsa", StringComparison.Ordinal) || name.StartsWith("id_dsa", StringComparison.Ordinal) then Some "secret_filename"
    elif secretNames.Contains name then Some "secret_filename"
    elif endsWithAny name excludedSuffixes && not (selectedGenerated path includeGenerated) then Some "excluded_suffix"
    else None

let private inPolicyScope (path: string) (policy: Policy.Normalized) =
    let lower = path.ToLowerInvariant()
    (policy.IncludePaths.IsEmpty || policy.IncludePaths |> List.exists (fun p -> Glob.matches lower p))
    && not (policy.ExcludePaths |> List.exists (fun p -> Glob.matches lower p))

let private classify (path: string) (rules: LayerRule list) =
    let lower = path.ToLowerInvariant()
    let ordinals = Dictionary<string, int>(dict [ "manifest", 0; "default", 0 ])
    let mutable result = None
    for rule in rules do
        let index = ordinals.[rule.Origin]
        ordinals.[rule.Origin] <- index + 1
        if result.IsNone && rule.Patterns |> List.exists (fun p -> Glob.matches lower p) then
            result <- Some(rule.Name, sprintf "%s:%d:%s" rule.Origin index rule.Name)
    match result with Some r -> r | None -> raise (IngestError "classification rules have no fallback")

/// Mutable per-scope statistics (global and per repository).
type Stats() =
    member val Counters = Dictionary<string, int64>(dict [ "tree_entries", 0L; "candidate_files", 0L; "candidate_bytes", 0L; "accepted_files", 0L; "accepted_bytes", 0L; "excluded_entries", 0L; "accepted_selected_generated_files", 0L; "accepted_selected_generated_bytes", 0L ])
    member val Reasons = Dictionary<string, int64>()
    member val Layers = Dictionary<string, int64 * int64>()
    member this.Count(key: string, amount: int64) = this.Counters.[key] <- this.Counters.[key] + amount
    member this.Exclude(reason: string) =
        this.Count("excluded_entries", 1L)
        this.Reasons.[reason] <- (match this.Reasons.TryGetValue reason with | true, v -> v | _ -> 0L) + 1L
    member this.Layer(layer: string, bytes: int64) =
        let files, total = match this.Layers.TryGetValue layer with | true, (f, b) -> f, b | _ -> 0L, 0L
        this.Layers.[layer] <- (files + 1L, total + bytes)
    member this.Merge(other: Stats) =
        for kv in other.Counters do this.Counters.[kv.Key] <- this.Counters.[kv.Key] + kv.Value
        for kv in other.Reasons do this.Reasons.[kv.Key] <- (match this.Reasons.TryGetValue kv.Key with | true, v -> v | _ -> 0L) + kv.Value
        for kv in other.Layers do
            let files, total = match this.Layers.TryGetValue kv.Key with | true, (f, b) -> f, b | _ -> 0L, 0L
            this.Layers.[kv.Key] <- (files + fst kv.Value, total + snd kv.Value)
    member this.ToJ() : J =
        Obj ([ for kv in this.Counters -> kv.Key, Int kv.Value ]
             @ [ "excluded_by_reason", Obj [ for kv in this.Reasons -> kv.Key, Int kv.Value ]
                 "accepted_by_layer", Obj [ for kv in this.Layers -> kv.Key, Obj [ "files", Int (fst kv.Value); "bytes", Int (snd kv.Value) ] ] ])

type NodeOut = { Id: string; Repo: string; Revision: string; Path: string; Kind: string; Title: string; LineStart: int; LineEnd: int; Text: string; SourceUrl: string; Layer: string; Rule: string }
let private nodeJ (n: NodeOut) : J =
    Obj [ "id", Str n.Id; "repo", Str n.Repo; "revision", Str n.Revision; "path", Str n.Path; "kind", Str n.Kind; "title", Str n.Title
          "line_start", Int (int64 n.LineStart); "line_end", Int (int64 n.LineEnd); "text", Str n.Text; "source_url", Str n.SourceUrl
          "corpus_layer", Str n.Layer; "classification_rule", Str n.Rule ]

/// Conservative inline/reference Markdown link targets, excluding fenced/inline code.
let markdownTargets (text: string) (maxLinks: int) : string list =
    let prose = ResizeArray<string>()
    let mutable fence : char option = None
    let mutable fenceLength = 0
    for line in Text.splitLines text do
        let marker = fenceMarker.Match(line)
        if marker.Success then
            let run = marker.Groups.[1].Value
            match fence with
            | None -> fence <- Some run.[0]; fenceLength <- run.Length
            | Some f when run.[0] = f && run.Length >= fenceLength -> fence <- None
            | _ -> ()
        elif fence.IsNone then prose.Add(inlineCode.Replace(line, ""))
    let body = String.Join("\n", prose)
    let normalize (label: string) = String.Join(" ", whitespace.Split(label.ToLowerInvariant().Trim()) |> Array.filter (fun s -> s <> ""))
    let references = Dictionary<string, string>()
    for m in referenceDef.Matches(body) do references.[normalize m.Groups.[1].Value] <- m.Groups.[2].Value.Trim([| '<'; '>' |])
    let results = ResizeArray<string>()
    let mutable count = 0
    for m in inlineLink.Matches(body) do
        count <- count + 1
        if count > maxLinks then raise (IngestError "Markdown file exceeded link limit")
        results.Add(m.Groups.[1].Value.Trim([| '<'; '>' |]))
    for m in referenceLink.Matches(body) do
        count <- count + 1
        if count > maxLinks then raise (IngestError "Markdown file exceeded link limit")
        let label = if m.Groups.[2].Value <> "" then m.Groups.[2].Value else m.Groups.[1].Value
        match references.TryGetValue(normalize label) with
        | true, target -> results.Add target
        | _ -> ()
    List.ofSeq results

let resolveLink (source: string) (target: string) (files: HashSet<string>) : string option =
    let url = Resolve.splitUrl target
    match Resolve.unquoteStrict url.Path with
    | None -> None
    | Some path when url.Scheme <> "" || url.Netloc <> "" || path = "" || path.StartsWith("/", StringComparison.Ordinal) || path.Contains '\\' || path |> Seq.exists (fun c -> int c < 32) -> None
    | Some path ->
        let candidate = Resolve.posixNormpath (Resolve.posixJoin [ Resolve.posixDirname source; path ])
        if candidate = ".." || candidate.StartsWith("../", StringComparison.Ordinal) then None
        elif files.Contains candidate then Some candidate else None

type private Candidate = { Path: string; ObjectId: string; Size: int }

type RepoResult = { Metadata: J; Nodes: NodeOut list; Edges: (string * string * string) list; Stats: Stats; TreeEntries: int64; CandidateFiles: int64; CandidateBytes: int64 }

let private processRepository (entry: Entry) (limits: Limits) (deadline: DateTime) : RepoResult =
    let stats = Stats()
    let git args cap = run entry.Path args cap deadline limits.GitTimeoutSeconds
    let bare = Encoding.UTF8.GetString(git [ "rev-parse"; "--is-bare-repository" ] 4096).Trim() = "true"
    if not bare then
        let root = Encoding.UTF8.GetString(git [ "rev-parse"; "--show-toplevel" ] 4096).Trim()
        if realpath root <> entry.Path then raise (IngestError "manifest path must name the repository root, not a nested directory")
    if Encoding.UTF8.GetString(git [ "cat-file"; "-t"; entry.Commit ] 4096).Trim() <> "commit" then raise (IngestError "manifest revision does not name a commit object")
    let repoId = identity [ Str entry.Name; Str entry.Commit; Str "repository" ]
    let repoNode = { Id = repoId; Repo = entry.Name; Revision = entry.Commit; Path = ""; Kind = "repository"; Title = entry.Name; LineStart = 0; LineEnd = 0; Text = ""; SourceUrl = sourceUrl entry.SourceUrl entry.Commit "" 0 0; Layer = "repository"; Rule = "repository" }
    let tree = git [ "ls-tree"; "-r"; "-l"; "-z"; "--full-tree"; entry.Commit ] limits.MaxTreeBytes
    // Tree walk: counting and exclusion in tree order, exactly as the reference.
    let candidates = ResizeArray<Candidate>()
    let mutable treeEntries = 0L
    let mutable offset = 0
    while offset < tree.Length do
        let stop = Array.IndexOf(tree, 0uy, offset)
        let stop = if stop < 0 then tree.Length else stop
        if stop > offset then
            treeEntries <- treeEntries + 1L
            stats.Count("tree_entries", 1L)
            let record = tree.[offset .. stop - 1]
            let tab = Array.IndexOf(record, byte '\t')
            let header = Encoding.ASCII.GetString(record, 0, tab).Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)
            let rawPath = record.[tab + 1 ..]
            let mode, kind, objectId, size = header.[0], header.[1], header.[2], header.[3]
            if (mode <> "100644" && mode <> "100755") || kind <> "blob" then stats.Exclude "non_regular_entry"
            else
                let path = try Some(UTF8Encoding(false, true).GetString rawPath) with _ -> None
                match path with
                | None -> stats.Exclude "invalid_path"
                | Some path when rawPath.Length > 4096 || path.StartsWith("/", StringComparison.Ordinal) || path.Contains '\\'
                                 || path.Split('/') |> Array.exists (fun p -> p = "" || p = "." || p = "..")
                                 || path |> Seq.exists (fun c -> int c < 32 || int c = 127) -> stats.Exclude "invalid_path"
                | Some path ->
                    match excludedReason path entry.Policy.IncludeGenerated with
                    | Some reason -> stats.Exclude reason
                    | None ->
                        if not (inPolicyScope path entry.Policy) then stats.Exclude "policy_scope"
                        else
                            let size = Int32.Parse(size, Globalization.CultureInfo.InvariantCulture)
                            if size > limits.MaxFileBytes then stats.Exclude "file_too_large"
                            else candidates.Add { Path = path; ObjectId = objectId; Size = size }
        offset <- stop + 1
    // Blob reads through one streaming batch process, in tree order.
    let blobs = ResizeArray<Candidate * byte[]>()
    let mutable candidateBytes = 0L
    (use reader = new BatchReader(entry.Path, deadline, limits.GitTimeoutSeconds)
     for candidate in candidates do
         if int64 blobs.Count >= int64 limits.MaxFiles || candidateBytes + int64 candidate.Size > limits.MaxSourceBytes then
             raise (IngestError "snapshot exceeded source file/byte limit")
         let raw = reader.Blob(candidate.ObjectId, candidate.Size, limits.MaxFileBytes)
         blobs.Add((candidate, raw))
         candidateBytes <- candidateBytes + int64 candidate.Size)
    // Per-blob work in parallel; results merged in tree order so counts are deterministic.
    let perFile : (string * NodeOut list * string * string) option[] = Array.zeroCreate blobs.Count // (reject reason | nodes), layer, text
    let results : Choice<string, NodeOut list * string * int * string> [] = Array.zeroCreate blobs.Count
    let strict = UTF8Encoding(false, true)
    Parallel.For(0, blobs.Count, fun i ->
        let candidate, raw = blobs.[i]
        if raw |> Array.exists (fun b -> b < 32uy && b <> 9uy && b <> 10uy && b <> 12uy && b <> 13uy) then results.[i] <- Choice1Of2 "binary_content"
        else
            match (try Some(strict.GetString raw) with _ -> None) with
            | None -> results.[i] <- Choice1Of2 "non_utf8_content"
            | Some text when privateKey.IsMatch text -> results.[i] <- Choice1Of2 "private_key_header"
            | Some text when text.StartsWith("version https://git-lfs.github.com/spec/v1\n", StringComparison.Ordinal) -> results.[i] <- Choice1Of2 "lfs_pointer"
            | Some text ->
                let layer, rule = classify candidate.Path entry.LayerRules
                let fileLines = lines text
                let count = fileLines.Length
                let fileId = identity [ Str entry.Name; Str entry.Commit; Str "file"; Str candidate.Path ]
                let fileNode = { Id = fileId; Repo = entry.Name; Revision = entry.Commit; Path = candidate.Path; Kind = "file"; Title = candidate.Path
                                 LineStart = (if count > 0 then 1 else 0); LineEnd = count; Text = text; Layer = layer; Rule = rule
                                 SourceUrl = sourceUrl entry.SourceUrl entry.Commit candidate.Path (if count > 0 then 1 else 0) count }
                let chunks = ResizeArray<NodeOut>()
                let mutable start = 0
                let mutable finished = count = 0
                while not finished do
                    let stop = min (start + chunkLines) count
                    let chunkId = identity [ Str entry.Name; Str entry.Commit; Str "chunk"; Str candidate.Path; Int (int64 (start + 1)); Int (int64 stop) ]
                    chunks.Add { Id = chunkId; Repo = entry.Name; Revision = entry.Commit; Path = candidate.Path; Kind = "chunk"; Title = sprintf "%s:%d-%d" candidate.Path (start + 1) stop
                                 LineStart = start + 1; LineEnd = stop; Text = String.Concat(fileLines.[start .. stop - 1]); Layer = layer; Rule = rule
                                 SourceUrl = sourceUrl entry.SourceUrl entry.Commit candidate.Path (start + 1) stop }
                    if stop = count then finished <- true else start <- start + (chunkLines - chunkOverlapLines)
                results.[i] <- Choice2Of2 (fileNode :: List.ofSeq chunks, layer, candidate.Size, text)) |> ignore
    let nodes = ResizeArray<NodeOut>([ repoNode ])
    let edges = ResizeArray<string * string * string>()
    let files = Dictionary<string, string * string>() // path -> fileId, text
    for i in 0 .. blobs.Count - 1 do
        let candidate, _ = blobs.[i]
        stats.Count("candidate_files", 1L); stats.Count("candidate_bytes", int64 candidate.Size)
        match results.[i] with
        | Choice1Of2 reason -> stats.Exclude reason
        | Choice2Of2 (fileNodes, layer, size, text) ->
            stats.Count("accepted_files", 1L); stats.Count("accepted_bytes", int64 size)
            if selectedGenerated candidate.Path entry.Policy.IncludeGenerated then
                stats.Count("accepted_selected_generated_files", 1L); stats.Count("accepted_selected_generated_bytes", int64 size)
            stats.Layer(layer, int64 size)
            let fileNode = fileNodes.Head
            nodes.AddRange fileNodes
            edges.Add((repoId, fileNode.Id, "contains"))
            for chunk in fileNodes.Tail do edges.Add((chunk.Id, fileNode.Id, "chunks_of"))
            files.[candidate.Path] <- (fileNode.Id, text)
    // Same-repository Markdown links, in parallel per document.
    let known = HashSet<string>(files.Keys)
    let markdown = [| for kv in files do if endsWithAny (kv.Key.ToLowerInvariant()) [ ".md"; ".markdown" ] then yield kv.Key, kv.Value |]
    let linkEdges : (string * string * string) list [] = Array.zeroCreate markdown.Length
    Parallel.For(0, markdown.Length, fun i ->
        let path, (fileId, text) = markdown.[i]
        linkEdges.[i] <- [ for target in markdownTargets text limits.MaxLinksPerFile do
                             match resolveLink path target known with
                             | Some resolved -> yield (fileId, fst files.[resolved], "links_to")
                             | None -> () ]) |> ignore
    for list in linkEdges do edges.AddRange list
    let policyJ = Obj [ "role", Str entry.Policy.Role; "authority", Int entry.Policy.Authority; "capabilities", Arr (entry.Policy.Capabilities |> List.map Str)
                        "include_generated", Arr (entry.Policy.IncludeGenerated |> List.map Str); "include_paths", Arr (entry.Policy.IncludePaths |> List.map Str)
                        "exclude_paths", Arr (entry.Policy.ExcludePaths |> List.map Str) ]
    let metadata =
        Obj ([ "name", Str entry.Name; "commit", Str entry.Commit; "source_url", Str entry.SourceUrl; "policy", policyJ
               "layer_rules", Arr [ for rule in entry.LayerRules do if rule.Origin = "manifest" then yield Obj [ "name", Str rule.Name; "patterns", Arr (rule.Patterns |> List.map Str) ] ]
               "stats", stats.ToJ() ] @ (match entry.Ref with Some r -> [ "ref", Str r ] | None -> []))
    { Metadata = metadata; Nodes = List.ofSeq nodes; Edges = List.ofSeq edges; Stats = stats; TreeEntries = treeEntries; CandidateFiles = int64 blobs.Count; CandidateBytes = candidateBytes }

/// Build the component snapshot: canonical bytes (with snapshot_id) ready to write.
let build (manifest: JsonValue) (limits: Limits) : string * byte[] =
    let deadline = DateTime.UtcNow.AddSeconds limits.MaxBuildSeconds
    let entries = parseManifest manifest limits
    if DateTime.UtcNow >= deadline then raise (IngestError "snapshot build exceeded its time limit")
    let results : RepoResult[] = Array.zeroCreate entries.Length
    Parallel.For(0, entries.Length, fun i -> results.[i] <- processRepository entries.[i] limits deadline) |> ignore
    if results |> Array.sumBy (fun r -> r.TreeEntries) > limits.MaxTreeEntries then raise (IngestError "snapshot exceeded tree-entry limit")
    if results |> Array.sumBy (fun r -> r.CandidateFiles) > int64 limits.MaxFiles || results |> Array.sumBy (fun r -> r.CandidateBytes) > limits.MaxSourceBytes then
        raise (IngestError "snapshot exceeded source file/byte limit")
    let stats = Stats()
    for r in results do stats.Merge r.Stats
    let nodes = results |> Array.collect (fun r -> Array.ofList r.Nodes)
    if nodes.Length > limits.MaxNodes then raise (IngestError "snapshot exceeded node limit")
    let edges = results |> Array.collect (fun r -> Array.ofList r.Edges) |> Array.distinct
    if edges.Length > limits.MaxEdges then raise (IngestError "snapshot exceeded edge limit")
    if DateTime.UtcNow >= deadline then raise (IngestError "snapshot build exceeded its time limit")
    let sortedNodes = nodes |> Array.sortWith (fun a b -> String.CompareOrdinal(a.Id, b.Id))
    let sortedEdges = edges |> Array.sortWith (fun (a1, a2, a3) (b1, b2, b3) ->
        let c1 = String.CompareOrdinal(a1, b1)
        if c1 <> 0 then c1 else
        let c2 = String.CompareOrdinal(a2, b2)
        if c2 <> 0 then c2 else String.CompareOrdinal(a3, b3))
    let body =
        Obj [ "format_version", Int 1L; "extractor_version", Str extractorVersion; "policy_version", Str Policy.policyVersion
              "classification_version", Str classificationVersion
              "classification_defaults", Arr [ for (name, patterns) in defaultLayerRules -> Obj [ "name", Str name; "patterns", Arr (patterns |> List.map Str) ] ]
              "stats", stats.ToJ()
              "repositories", Arr [ for r in results -> r.Metadata ]
              "nodes", Arr [ for n in sortedNodes -> nodeJ n ]
              "edges", Arr [ for (src, dst, kind) in sortedEdges -> Obj [ "src", Str src; "dst", Str dst; "kind", Str kind ] ] ]
    let snapshotId, length = digest canonicalStyle body
    if length + 100L > limits.MaxOutputBytes then raise (IngestError "snapshot exceeded serialized output limit")
    let signed = match body with Obj pairs -> Obj (pairs @ [ "snapshot_id", Str snapshotId ]) | other -> other
    snapshotId, Encoding.UTF8.GetBytes(toText canonicalStyle signed)

/// Atomic 0600 write beside the destination, like the reference CLI.
let writeAtomic (output: string) (content: byte[]) =
    let directory = Path.GetDirectoryName(Path.GetFullPath output)
    let temporary = Path.Combine(directory, ".snapshot-" + Guid.NewGuid().ToString("N"))
    try
        (use stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None)
         File.SetUnixFileMode(temporary, UnixFileMode.UserRead ||| UnixFileMode.UserWrite)
         stream.Write(content, 0, content.Length)
         stream.Write([| byte '\n' |], 0, 1)
         stream.Flush(true))
        File.Move(temporary, output, true)
    finally
        if File.Exists temporary then File.Delete temporary
