/// The relationship rules. Per-file scanning runs on all cores; results merge in
/// the same order the single-threaded reference pass used, so first-wins
/// de-duplication and every evidence line are identical.
module Retrieval.Native.Enrich

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Text.Json
open System.Text.RegularExpressions
open System.Threading.Tasks
open System.Xml.Linq
open Tomlyn
open Tomlyn.Model
open Retrieval.Native.Snapshot

let private slug = Regex(@"\A[a-z0-9]+(?:-[a-z0-9]+)*\z", RegexOptions.Compiled)
let private link = Regex(@"(?<!!)\[[^\]\n]{0,2048}\]\(\s*(<[^>\n]{1,4096}>|[^\s()]{1,4096})(?:\s+[""'][^\n]{0,2048}[""'])?\s*\)", RegexOptions.Compiled)
let private referenceDefinition = Regex(@"\A {0,3}\[[^\]\n]+\]:\s*(<[^>\n]{1,4096}>|\S{1,4096})", RegexOptions.Compiled)
let private extern' = Regex(@"^[ \t]*\[<?FidelityExtern\(\s*""([^""\n]{1,128})""\s*,\s*""([^""\n]{1,128})""\s*\)\s*>?\]", RegexOptions.Compiled ||| RegexOptions.Multiline)
let private modulePattern = Regex(@"^\s*(?:module|namespace)\s+([A-Za-z_][A-Za-z0-9_.]*)", RegexOptions.Compiled ||| RegexOptions.Multiline)
let private declaration = Regex(@"\A(?:namespace|module)\s+(?:rec\s+)?([A-Za-z_][A-Za-z0-9_.]*)(?:\s|$)", RegexOptions.Compiled)
let private opened = Regex(@"\A\s*open\s+(?:type\s+)?(?:global\.)?([A-Za-z_][A-Za-z0-9_.]*)(?:\s|$)", RegexOptions.Compiled)
let private generatedBy = Regex(@"\b[Gg]enerated by\s+\[?([A-Za-z][A-Za-z0-9_.\-]*)", RegexOptions.Compiled)
let private generatedHeader = Regex(@"\b[Gg]enerated by\b", RegexOptions.Compiled)
let private identifierToken = Regex(@"[A-Za-z0-9_.]+", RegexOptions.Compiled)
let private mentionToken = Regex(@"[A-Za-z0-9_.\-]+", RegexOptions.Compiled)
let private nonSlug = Regex(@"[^a-z0-9]+", RegexOptions.Compiled)

let private apiCapabilities =
    dict [ "gtk_window_new", "native-windowing"; "gtk_widget_destroy", "widget-lifecycle"
           "g_signal_connect_data", "native-signals"; "webkit_web_view_new", "web-content-hosting"
           "webkit_web_view_load_html", "web-content-hosting"; "wl_display_connect", "wayland-client" ]

let private libraryCapability (library: string) =
    let normalized = nonSlug.Replace(library.ToLowerInvariant(), "-").Trim('-')
    let normalized = if normalized = "" then "library" else normalized
    let prefix = "native-library-"
    if normalized <> library || (prefix + normalized).Length > 64 then
        let head = (if normalized.Length > 40 then normalized.Substring(0, 40) else normalized).TrimEnd('-')
        let hash = Convert.ToHexString(Security.Cryptography.SHA256.HashData(Text.Encoding.UTF8.GetBytes(library))).ToLowerInvariant()
        prefix + head + "-" + hash.Substring(0, 8)
    else prefix + normalized

let private endsWithAny (value: string) (suffixes: string list) =
    suffixes |> List.exists (fun s -> value.EndsWith(s, StringComparison.Ordinal))

let private isIdentStart (c: char) = c = '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')

type Result =
    { Nodes: Node[]; Capabilities: CapabilityNode[]; PassthroughEdges: JsonElement[]; Derived: DerivedEdge[] }

/// Publication budgets; the coordinator passes its own constants so one source of truth holds.
type Limits = { MaxFiles: int; MaxSourceBytes: int64; MaxDerivedEdges: int; MaxCapabilityNodes: int }

/// Run over already-parsed inputs (repositories, every node, every edge); previous
/// derived output is dropped before the pass, as the reference did.
let runParsed (repositories: Repository list) (allNodes: Node seq) (allEdges: JsonElement seq) (limits: Limits) : Result =
    let maxFiles, maxSourceBytes = limits.MaxFiles, limits.MaxSourceBytes
    if repositories.Length > 64 then raise (RelationshipError "relationship pass supports at most 64 repositories")
    let repos = repositories |> List.map (fun r -> r.Name, r) |> Map.ofList
    let nodes = [| for node in allNodes do if node.Kind <> "capability" then yield node |]
    let passthroughEdges =
        [| for e in allEdges do
             let kind = match e.TryGetProperty("kind") with | true, k when k.ValueKind = JsonValueKind.String -> k.GetString() | _ -> ""
             let rule = match e.TryGetProperty("rule") with | true, r when r.ValueKind = JsonValueKind.String -> r.GetString() | _ -> ""
             if not (derivedKinds.Contains kind) && not (rule.StartsWith("relationships-v", StringComparison.Ordinal)) then yield e |]
    let repoNodes = Dictionary<string, Node>()
    for n in nodes do if n.Kind = "repository" then repoNodes.[n.Repo] <- n
    let files =
        nodes |> Array.filter (fun n -> n.Kind = "file")
        |> Array.sortWith (fun a b -> match String.CompareOrdinal(a.Repo, b.Repo) with 0 -> String.CompareOrdinal(a.Path, b.Path) | c -> c)
    let totalBytes = files |> Array.sumBy (fun n -> int64 (Text.Encoding.UTF8.GetByteCount n.Text))
    if files.Length > maxFiles || totalBytes > maxSourceBytes then raise (RelationshipError "relationship source scan exceeded file/byte budget")
    let byPath = Dictionary<struct (string * string), Node>()
    let filePaths = Dictionary<string, ResizeArray<string>>()
    for name in repos.Keys do filePaths.[name] <- ResizeArray()
    for node in files do
        match Map.tryFind node.Repo repos with
        | Some meta when node.Revision = meta.Commit ->
            byPath.[struct (node.Repo, node.Path)] <- node
            filePaths.[node.Repo].Add(node.Path)
        | _ -> raise (RelationshipError "file revision does not match repository metadata")
    let aliases = Dictionary<string, string>()
    for name in repos.Keys |> Seq.sortWith (fun a b -> String.CompareOrdinal(a, b)) do
        let short = name.Substring(name.LastIndexOf('/') + 1)
        if aliases.ContainsKey short then raise (RelationshipError "repository short names must be unambiguous")
        aliases.[short] <- name
        match repoNodes.TryGetValue name with
        | true, node when node.Revision = repos.[name].Commit -> ()
        | _ -> raise (RelationshipError "repository node is absent or at another revision")
    let lookup : Resolve.Lookup =
        { Repositories = repos
          Aliases = aliases |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq
          RepoNode = fun name -> match repoNodes.TryGetValue name with | true, n -> Some n | _ -> None
          FileNode = fun name path -> match byPath.TryGetValue(struct (name, path)) with | true, n -> Some n | _ -> None }

    let capabilities = ConcurrentDictionary<struct (string * string), CapabilityNode>()
    let capability (repo: string) (label: string) (curated: bool) =
        capabilities.GetOrAdd(struct (repo, label), fun _ ->
            { Id = identity [ relationshipVersion; repo; repos.[repo].Commit; "capability"; label ]
              Repo = repo; Revision = repos.[repo].Commit; Label = label; Curated = curated
              SourceUrl = repoNodes.[repo].SourceUrl })
    let capId (cap: CapabilityNode) = cap.Id

    let edge (src: string) (dst: string) kind basis confidence (evidence: string) line rule (explanation: string) : DerivedEdge option =
        if src = dst then None
        else Some { Src = src; Dst = dst; Kind = kind; Basis = basis; Confidence = confidence; EvidenceId = evidence
                    Line = line; Rule = relationshipVersion + ":" + rule
                    Explanation = (if explanation.Length > maxExplanation then explanation.Substring(0, maxExplanation) else explanation) }
    let emit (sink: ResizeArray<DerivedEdge>) e = match e with Some v -> sink.Add v | None -> ()
    let reference (sink: ResizeArray<DerivedEdge>) (source: Node) (destination: Node) line rule (explanation: string) kind =
        emit sink (edge source.Id destination.Id kind "extracted" 1.0 source.Id line rule explanation)
        if destination.Kind <> "repository" then
            emit sink (edge source.Id repoNodes.[destination.Repo].Id kind "extracted" 1.0 source.Id line (rule + "-repository")
                        (explanation + " Repository endpoint is the indexed owner of the referenced file."))

    // 1. Curated policy capabilities (sequential, sorted by repository name).
    let policyEdges = ResizeArray<DerivedEdge>()
    for name in repos.Keys |> Seq.sortWith (fun a b -> String.CompareOrdinal(a, b)) do
        let labels = repos.[name].Capabilities
        if labels.Length > 32 || labels |> List.exists (fun x -> x.Length > 80 || not (slug.IsMatch x)) then
            raise (RelationshipError "invalid curated capability labels")
        for label in labels |> List.distinct |> List.sortWith (fun a b -> String.CompareOrdinal(a, b)) do
            let cap = capability name label true
            emit policyEdges (edge repoNodes.[name].Id (capId cap) "declares_capability" "curated" 1.0 repoNodes.[name].Id 0 "policy-capability"
                                ("Corpus policy associates this repository with " + label + "; this is a curated navigation label."))

    // 2. Top-level declarations (parallel masking; merge in file order).
    let isCode (n: Node) = endsWithAny (n.Path.ToLowerInvariant()) [ ".fs"; ".fsi"; ".clef" ]
    let codeLines : string[][] = Array.zeroCreate files.Length
    let perFileDeclarations : (string * int)[][] = Array.zeroCreate files.Length
    Parallel.For(0, files.Length, fun i ->
        if isCode files.[i] then
            let lines = Text.codeLines files.[i].Text
            codeLines.[i] <- lines
            let found = ResizeArray<string * int>()
            let mutable ns = ""
            for line in lines do
                let m = declaration.Match(line)
                if m.Success then
                    let mutable name = m.Groups.[1].Value
                    if line.StartsWith("namespace ", StringComparison.Ordinal) then ns <- name
                    elif ns <> "" then name <- ns + "." + name
                    found.Add((name, i))
            perFileDeclarations.[i] <- found.ToArray()
        else
            codeLines.[i] <- [||]
            perFileDeclarations.[i] <- [||]) |> ignore
    let declarations = Dictionary<string, ResizeArray<Node>>()
    for i in 0 .. files.Length - 1 do
        for (name, index) in perFileDeclarations.[i] do
            match declarations.TryGetValue name with
            | true, list -> list.Add files.[index]
            | _ -> declarations.[name] <- ResizeArray [ files.[index] ]
    let qualifiedNames = HashSet<string>(declarations.Keys |> Seq.filter (fun k -> k.Contains '.'))

    // 3. Module uses (parallel per file, line order preserved within a file).
    let moduleEdges : ResizeArray<DerivedEdge>[] = Array.init files.Length (fun _ -> ResizeArray())
    Parallel.For(0, files.Length, fun i ->
        let node = files.[i]
        let sink = moduleEdges.[i]
        codeLines.[i] |> Array.iteri (fun lineIndex value ->
            let line = lineIndex + 1
            let openMatch = opened.Match(value)
            let names = ResizeArray<string * string>()
            if openMatch.Success then names.Add((openMatch.Groups.[1].Value, "open"))
            elif qualifiedNames.Count > 0 then
                // Equivalent to the longest-first alternation regex with identifier lookarounds:
                // within each maximal identifier token, the longest declared dotted prefix that is
                // followed by "." and a letter is a qualified access.
                for token in identifierToken.Matches(value) do
                    let text = token.Value
                    if isIdentStart text.[0] then
                        let mutable k = text.LastIndexOf('.')
                        let mutable matched = false
                        while not matched && k > 0 do
                            if k + 1 < text.Length && isIdentStart text.[k + 1] && qualifiedNames.Contains(text.Substring(0, k)) then
                                names.Add((text.Substring(0, k), "qualified-access")); matched <- true
                            else k <- text.LastIndexOf('.', k - 1)
            for (name, usage) in names do
                match declarations.TryGetValue name with
                | true, candidates ->
                    let owners = candidates |> Seq.map (fun c -> c.Repo) |> Seq.distinct |> List.ofSeq
                    if owners.Length = 1 && not (owners.Head = node.Repo) then
                        for destination in candidates do
                            emit sink (edge node.Id destination.Id "references_project" "inferred" 0.75 node.Id line ("module-" + usage)
                                        ("Textual " + usage + " of " + name + " matches an indexed top-level declaration; compiler name resolution and successful compilation are not established."))
                        emit sink (edge node.Id repoNodes.[owners.Head].Id "references_project" "inferred" 0.75 node.Id line ("module-" + usage + "-repository")
                                    ("Indexed repository owns the matching declaration for " + name + "; textual inference only."))
                | _ -> ())) |> ignore

    // 4. Documents, generator headers, externs, project files (parallel per file).
    let fileEdges : ResizeArray<DerivedEdge>[] = Array.init files.Length (fun _ -> ResizeArray())
    Parallel.For(0, files.Length, fun i ->
        let node = files.[i]
        let sink = fileEdges.[i]
        let text = node.Text
        let lower = node.Path.ToLowerInvariant()
        if endsWithAny lower [ ".md"; ".markdown"; ".rst"; ".adoc" ] then
            for (line, value) in Text.proseLines text do
                let targets = ResizeArray<string>()
                for m in link.Matches(value) do targets.Add(m.Groups.[1].Value)
                let definition = referenceDefinition.Match(value)
                if definition.Success then targets.Add(definition.Groups.[1].Value)
                for target in targets do
                    match Resolve.resolve lookup node target with
                    | Some dest when dest.Repo <> node.Repo ->
                        reference sink node dest line "markdown-reference"
                            "Explicit cross-repository document link; a reference does not imply dependency or implementation." "links_to"
                    | _ -> ()
                if aliases.Count > 0 then
                    for m in mentionToken.Matches(value) do
                        match aliases.TryGetValue m.Value with
                        | true, name when name <> node.Repo ->
                            emit sink (edge node.Id repoNodes.[name].Id "mentions" "inferred" 0.55 node.Id line "exact-repository-name"
                                        "Exact allowlisted repository name occurs in prose; topical relationship only, including possible proposals or negative statements.")
                        | _ -> ()
        let header = Text.splitLines text
        for lineIndex in 0 .. (min 20 header.Length) - 1 do
            let m = generatedBy.Match(header.[lineIndex])
            if m.Success then
                match aliases.TryGetValue m.Groups.[1].Value with
                | true, name ->
                    emit sink (edge node.Id repoNodes.[name].Id "generated_by" "extracted" 1.0 node.Id (lineIndex + 1) "generator-declaration"
                                ("Source declares generator " + m.Groups.[1].Value + "; its historical generating commit is not established."))
                | _ -> ()
        if endsWithAny lower [ ".clef"; ".fs"; ".cs" ] then
            for m in extern'.Matches(text) do
                let library, symbol = m.Groups.[1].Value, m.Groups.[2].Value
                let nativeLabel = libraryCapability library
                let nativeCap = capability node.Repo nativeLabel false
                let line = Text.lineAt text m.Index
                emit sink (edge node.Id (capId nativeCap) "suggests_capability" "inferred" 0.95 node.Id line "binding-library-capability"
                            (sprintf "Explicit binding library %s is normalized to %s; this identifies native-library reach, not validated runtime support." library nativeLabel))
                match apiCapabilities.TryGetValue symbol with
                | true, label ->
                    let cap = capability node.Repo label false
                    emit sink (edge node.Id (capId cap) "suggests_capability" "inferred" 0.8 node.Id line "binding-api-capability"
                                (sprintf "Binding declaration %s::%s suggests %s; declaration is not runtime validation." library symbol label))
                | _ -> ()
        if endsWithAny lower [ ".fsproj"; ".csproj" ] then
            let project = try Some(XDocument.Parse(text)) with _ -> None
            match project with
            | Some document when not (isNull document.Root) ->
                for element in document.Root.DescendantsAndSelf() do
                    if element.Name.LocalName = "ProjectReference" then
                        let attribute = element.Attribute(XName.Get "Include")
                        let target = if isNull attribute then "" else attribute.Value
                        if target <> "" && not (target.Contains '$') && not (target.Contains '%') then
                            match Resolve.resolve lookup node (target.Replace('\\', '/')) with
                            | Some dest when dest.Repo <> node.Repo ->
                                let at = Text.firstLineContaining text target
                                if at > 0 then
                                    reference sink node dest at "msbuild-project-reference"
                                        "ProjectReference declares this path; conditions and successful compilation are not evaluated." "references_project"
                            | _ -> ()
            | _ -> ()
        if node.Path.EndsWith(".fidproj", StringComparison.Ordinal) then
            let parsed = try Some(Toml.ToModel(text)) with _ -> None
            match parsed with
            | Some model ->
                match model.TryGetValue("dependencies") with
                | true, (:? TomlTable as dependencies) ->
                    for declaration in dependencies.Values do
                        match declaration with
                        | :? TomlTable as table ->
                            match table.TryGetValue("path") with
                            | true, (:? string as target) ->
                                match Resolve.resolve lookup node target with
                                | Some dest when dest.Repo <> node.Repo ->
                                    let at = Text.firstLineContaining text target
                                    if at > 0 then
                                        reference sink node dest at "fidproj-dependency"
                                            "Project manifest declares this cross-repository path dependency; successful compilation is not asserted." "references_project"
                                | _ -> ()
                            | _ -> ()
                        | _ -> ()
                | _ -> ()
            | None -> ()) |> ignore

    // 5. Generation profiles (sequential; sorted prefix lookup).
    let profileEdges = ResizeArray<DerivedEdge>()
    for profile in files |> Array.filter (fun n -> n.Path.EndsWith(".pilot.toml", StringComparison.Ordinal)) do
        match (try Some(Toml.ToModel(profile.Text)) with _ -> None) with
        | None -> ()
        | Some model ->
            let directory =
                match model.TryGetValue("output") with
                | true, (:? TomlTable as output) -> (match output.TryGetValue("directory") with | true, (:? string as d) -> Some d | _ -> None)
                | _ -> None
            let namespaces : TomlObject seq option =
                match model.TryGetValue("namespace") with
                | true, (:? TomlTableArray as items) -> Some(items |> Seq.cast<TomlObject>)
                | true, (:? TomlArray as items) -> Some(items |> Seq.cast<TomlObject>)
                | false, _ -> Some Seq.empty
                | _ -> None
            match directory, namespaces with
            | Some directory, Some namespaces when not (directory.StartsWith("/", StringComparison.Ordinal)) && not (directory.Contains '\\') ->
                let names =
                    [ for item in namespaces do
                        match item with
                        | :? TomlTable as t ->
                            match t.TryGetValue("name") with
                            | true, (:? string as n) -> yield n
                            | _ -> ()
                        | _ -> () ]
                let prefix = Resolve.posixNormpath (Resolve.posixJoin [ Resolve.posixDirname profile.Path; directory ])
                if not (prefix = "." || prefix = ".." || prefix.StartsWith("../", StringComparison.Ordinal)) then
                    let prefix = prefix.TrimEnd('/') + "/"
                    let paths = filePaths.[profile.Repo]
                    let at = Text.firstLineContaining profile.Text directory
                    if at > 0 then
                        let mutable start = paths.BinarySearch(prefix, StringComparer.Ordinal)
                        if start < 0 then start <- ~~~start
                        let mutable index = start
                        while index < paths.Count && paths.[index].StartsWith(prefix, StringComparison.Ordinal) do
                            let node = byPath.[struct (profile.Repo, paths.[index])]
                            let m = modulePattern.Match(node.Text)
                            let headerText = String.Join("\n", Text.splitLines node.Text |> Array.truncate 20)
                            if m.Success && names |> List.exists (fun n -> m.Groups.[1].Value = n || m.Groups.[1].Value.StartsWith(n + ".", StringComparison.Ordinal))
                               && generatedHeader.IsMatch(headerText) then
                                emit profileEdges (edge node.Id profile.Id "generated_from" "inferred" 0.9 profile.Id at "pilot-output-namespace"
                                                    "Profile output directory and namespace match this generated declaration; a reproducible generation receipt is not available.")
                            index <- index + 1
            | _ -> ()

    // 6. Shared-capability star per label (sequential, sorted).
    let starEdges = ResizeArray<DerivedEdge>()
    let groups = Dictionary<string, ResizeArray<CapabilityNode>>()
    for cap in capabilities.Values |> Seq.sortWith (fun a b -> match String.CompareOrdinal(a.Repo, b.Repo) with 0 -> String.CompareOrdinal(a.Label, b.Label) | c -> c) do
        match groups.TryGetValue cap.Label with
        | true, list -> list.Add cap
        | _ -> groups.[cap.Label] <- ResizeArray [ cap ]
    for label in groups.Keys |> Seq.sortWith (fun a b -> String.CompareOrdinal(a, b)) do
        let group = groups.[label]
        let anchor = group.[0]
        for other in group |> Seq.skip 1 do
            for (source, target) in [ (anchor, other); (other, anchor) ] do
                emit starEdges (edge source.Id target.Id "shares_capability" "inferred" 0.6 source.Id 0 "shared-capability-label"
                                 ("Both repositories carry capability label " + label + "; similarity is not equivalence, dependency or interoperability."))

    // Merge in reference order with first-wins de-duplication and the publication budget.
    let derived = Dictionary<struct (string * string * string * string * string), DerivedEdge>()
    let addAll (edges: ResizeArray<DerivedEdge>) =
        for e in edges do
            let key = struct (e.Src, e.Dst, e.Kind, e.EvidenceId, e.Rule)
            if not (derived.ContainsKey key) then
                if derived.Count >= limits.MaxDerivedEdges then raise (RelationshipError "relationship edge budget exceeded")
                derived.[key] <- e
    addAll policyEdges
    for sink in moduleEdges do addAll sink
    for sink in fileEdges do addAll sink
    addAll profileEdges
    addAll starEdges
    if capabilities.Count > limits.MaxCapabilityNodes then raise (RelationshipError "capability node budget exceeded")
    { Nodes = nodes; Capabilities = capabilities.Values |> Array.ofSeq; PassthroughEdges = passthroughEdges; Derived = derived.Values |> Array.ofSeq }

/// Run over a whole snapshot document.
let run (root: JsonElement) (limits: Limits) : Result =
    runParsed [ for r in root.GetProperty("repositories").EnumerateArray() -> parseRepository r ]
              (root.GetProperty("nodes").EnumerateArray() |> Seq.map parseNode)
              (root.GetProperty("edges").EnumerateArray())
              limits
