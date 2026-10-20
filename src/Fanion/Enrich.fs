/// Parallel lexical extraction feeds a connection-local DuckDB relationship
/// pipeline. Ordered facts preserve first-wins provenance across set-based joins.
module Fanion.Enrich

open System
open System.Collections.Generic
open System.Text.RegularExpressions
open System.Threading.Tasks
open System.Xml.Linq
open Fidelity.Data.TOML
open Fidelity.Data.JSON
open Fanion.Canonical
open Fanion.Snapshot

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
    { Nodes: Node[]; Capabilities: CapabilityNode[]; PassthroughEdges: JsonValue[]; Derived: DerivedEdge[]
      Phases: (string * float) list }

/// Publication budgets; the coordinator passes its own constants so one source of truth holds.
type Limits = { MaxFiles: int; MaxSourceBytes: int64; MaxDerivedEdges: int; MaxCapabilityNodes: int }

/// Lexical facts carry source order; all cross-file matching belongs to DuckDB.
/// Temporary tables live only in the candidate connection and never enter the published schema.
type private Fact =
    | Declaration of string
    | Use of int * string * string * int
    | Reference of int * Resolve.Address * string * string * string
    | Alias of int * string * string * string * float * string * string
    | Mentions of int * string
    | Capability of int * string * float * string * string
    | Profile of string * string * int

let private scan (node: Node) =
    let facts = ResizeArray<Fact>()
    let text, lower = node.Text, node.Path.ToLowerInvariant()
    if endsWithAny lower [ ".fs"; ".fsi"; ".clef" ] then
        let lines = Text.codeLines text
        let mutable ns = ""
        for value in lines do
            let m = declaration.Match(value)
            if m.Success then
                let name = m.Groups.[1].Value
                if value.StartsWith("namespace ", StringComparison.Ordinal) then ns <- name
                facts.Add(Declaration(if not (value.StartsWith("namespace ", StringComparison.Ordinal)) && ns <> "" then ns + "." + name else name))
        for index in 0 .. lines.Length - 1 do
            let line, value = index + 1, lines.[index]
            let m = opened.Match(value)
            if m.Success then facts.Add(Use(line, m.Groups.[1].Value, "open", 0))
            else
                for token in identifierToken.Matches(value) do
                    if isIdentStart token.Value.[0] && token.Value.IndexOf('.') <> token.Value.LastIndexOf('.') then
                        facts.Add(Use(line, token.Value, "qualified-access", token.Index))
    let reference line target rule explanation kind =
        match Resolve.address node target with
        | Some address -> facts.Add(Reference(line, address, kind, rule, explanation))
        | None -> ()
    if endsWithAny lower [ ".md"; ".markdown"; ".rst"; ".adoc" ] then
        for line, value in Text.proseLines text do
            let targets = ResizeArray<string>()
            for m in link.Matches(value) do targets.Add(m.Groups.[1].Value)
            let definition = referenceDefinition.Match(value)
            if definition.Success then targets.Add(definition.Groups.[1].Value)
            for target in targets do
                reference line target "markdown-reference"
                    "Explicit cross-repository document link; a reference does not imply dependency or implementation." "links_to"
            facts.Add(Mentions(line, value))
    let header = Text.firstLines 20 text
    for i in 0 .. min 20 header.Length - 1 do
        let m = generatedBy.Match(header.[i])
        if m.Success then
            facts.Add(Alias(i + 1, m.Groups.[1].Value, "generated_by", "extracted", 1.0, "generator-declaration",
                "Source declares generator " + m.Groups.[1].Value + "; its historical generating commit is not established."))
    if endsWithAny lower [ ".clef"; ".fs"; ".cs" ] then
        for m in extern'.Matches(text) do
            let library, symbol = m.Groups.[1].Value, m.Groups.[2].Value
            let label, line = libraryCapability library, Text.lineAt text m.Index
            facts.Add(Capability(line, label, 0.95, "binding-library-capability",
                sprintf "Explicit binding library %s is normalized to %s; this identifies native-library reach, not validated runtime support." library label))
            match apiCapabilities.TryGetValue symbol with
            | true, label ->
                facts.Add(Capability(line, label, 0.8, "binding-api-capability",
                    sprintf "Binding declaration %s::%s suggests %s; declaration is not runtime validation." library symbol label))
            | _ -> ()
    if endsWithAny lower [ ".fsproj"; ".csproj" ] then
        match (try Some(XDocument.Parse text) with _ -> None) with
        | Some document when not (isNull document.Root) ->
            for element in document.Root.DescendantsAndSelf() do
                if element.Name.LocalName = "ProjectReference" then
                    let attribute = element.Attribute(XName.Get "Include")
                    let target = if isNull attribute then "" else attribute.Value
                    if target <> "" && not (target.Contains '$') && not (target.Contains '%') then
                        let line = Text.firstLineContaining text target
                        if line > 0 then
                            reference line (target.Replace('\\', '/')) "msbuild-project-reference"
                                "ProjectReference declares this path; conditions and successful compilation are not evaluated." "references_project"
        | _ -> ()
    if node.Path.EndsWith(".fidproj", StringComparison.Ordinal) then
        match (try Some(Toml.parseWithOrder text) with _ -> None) with
        | Some(Ok parsed) ->
            match Toml.getTable "dependencies" parsed.Document with
            | Some dependencies ->
                let names =
                    parsed.DeclarationPaths
                    |> List.choose (function "dependencies" :: name :: _ -> Some name | _ -> None)
                    |> List.distinct
                for name in names do
                    match Map.tryFind name dependencies with
                    | Some(TomlValue.Table table | TomlValue.InlineTable table) ->
                        match Toml.getString "path" table with
                        | Some target ->
                            let line = Text.firstLineContaining text target
                            if line > 0 then
                                reference line target "fidproj-dependency"
                                    "Project manifest declares this cross-repository path dependency; successful compilation is not asserted." "references_project"
                        | None -> ()
                    | _ -> ()
            | _ -> ()
        | _ -> ()
    if node.Path.EndsWith(".pilot.toml", StringComparison.Ordinal) then
        match (try Some(Toml.parse text) with _ -> None) with
        | Some(Ok model) ->
            let directory = Toml.getString "output.directory" model
            let namespaces =
                match Toml.getValue "namespace" model with
                | Some(TomlValue.Array items) -> Some items
                | None -> Some []
                | _ -> None
            match directory, namespaces with
            | Some directory, Some namespaces when not (directory.StartsWith("/", StringComparison.Ordinal)) && not (directory.Contains '\\') ->
                let prefix = Resolve.posixNormpath (Resolve.posixJoin [ Resolve.posixDirname node.Path; directory ])
                let line = Text.firstLineContaining text directory
                if line > 0 && prefix <> "." && prefix <> ".." && not (prefix.StartsWith("../", StringComparison.Ordinal)) then
                    for item in namespaces do
                        match item with
                        | TomlValue.Table table | TomlValue.InlineTable table ->
                            match Toml.getString "name" table with
                            | Some name -> facts.Add(Profile(prefix.TrimEnd('/') + "/", name, line))
                            | _ -> ()
                        | _ -> ()
            | _ -> ()
        | _ -> ()
    let generated = generatedHeader.IsMatch(String.Join("\n", header))
    let moduleMatch = if generated then modulePattern.Match text else Match.Empty
    facts.ToArray(),
    (if moduleMatch.Success then moduleMatch.Groups.[1].Value else ""),
    generated

let private tables =
    [ "repository", "ordinal BIGINT, id VARCHAR, repo VARCHAR, commit VARCHAR, ref VARCHAR, alias VARCHAR, source_url VARCHAR, scheme VARCHAR, netloc VARCHAR, path VARCHAR"
      "file", "ordinal BIGINT, id VARCHAR, repo VARCHAR, path VARCHAR, module VARCHAR, generated BOOLEAN"
      "declaration", "file_ord BIGINT, occurrence BIGINT, name VARCHAR"
      "use", "file_ord BIGINT, occurrence BIGINT, line BIGINT, name VARCHAR, usage VARCHAR, token BIGINT"
      "address", "event BIGINT, scheme VARCHAR, netloc VARCHAR, path VARCHAR, alias VARCHAR"
      "reference", "event BIGINT, file_ord BIGINT, occurrence BIGINT, line BIGINT, kind VARCHAR, rule VARCHAR, explanation VARCHAR"
      "alias", "file_ord BIGINT, occurrence BIGINT, line BIGINT, alias VARCHAR, kind VARCHAR, basis VARCHAR, confidence DOUBLE, rule VARCHAR, explanation VARCHAR"
      "mention", "file_ord BIGINT, occurrence BIGINT, line BIGINT, text VARCHAR"
      "capability", "repo VARCHAR, label VARCHAR, id VARCHAR, curated BOOLEAN, revision VARCHAR, source_url VARCHAR"
      "policy", "repo VARCHAR, label VARCHAR, occurrence BIGINT"
      "binding", "file_ord BIGINT, occurrence BIGINT, line BIGINT, label VARCHAR, confidence DOUBLE, rule VARCHAR, explanation VARCHAR"
      "profile", "file_ord BIGINT, prefix VARCHAR, namespace VARCHAR, line BIGINT" ]

let private append (connection: DuckDB.NET.Data.DuckDBConnection) name (rows: obj[] seq) =
    use appender = connection.CreateAppender("temp", "main", "enrich_" + name)
    for values in rows do
        let row = appender.CreateRow()
        for value in values do
            match value with
            | null -> row.AppendNullValue() |> ignore
            | :? string as s -> row.AppendValue(s) |> ignore
            | :? int as i -> row.AppendValue(int64 i) |> ignore
            | :? int64 as i -> row.AppendValue(i) |> ignore
            | :? bool as b -> row.AppendValue(b) |> ignore
            | :? float as f -> row.AppendValue(f) |> ignore
            | _ -> raise (RelationshipError "Invalid lexical fact")
        row.EndRow()

/// Each rule emits the same ordered evidence tuples. SQL owns joins, ambiguity,
/// grouping, capability stars and first-evidence deduplication across the corpus.
let private relationshipsSql = """
CREATE TEMP TABLE enrich_caps AS
 SELECT repo,label,id,bool_or(curated) curated,revision,source_url
 FROM enrich_capability GROUP BY ALL;
CREATE TEMP TABLE enrich_modules AS
 WITH names AS (SELECT DISTINCT name FROM enrich_declaration),
 expanded AS (
  SELECT * FROM enrich_use WHERE usage='open'
  UNION ALL
  SELECT u.file_ord,u.occurrence,u.line,array_to_string(list_slice(parts,1,p.n),'.'),u.usage,u.token
  FROM (SELECT *,string_split(name,'.') parts FROM enrich_use WHERE usage='qualified-access') u,
       LATERAL range(2,len(parts)) p(n)
  WHERE regexp_matches(parts[p.n+1],'^[A-Za-z_]')
 ),
 longest AS (
  SELECT u.* FROM expanded u JOIN names n USING(name)
  QUALIFY row_number() OVER(PARTITION BY file_ord,line,token,usage ORDER BY length(name) DESC,occurrence)=1
 ), owners AS (
  SELECT d.name,min(f.repo) repo FROM enrich_declaration d JOIN enrich_file f ON f.ordinal=d.file_ord
  GROUP BY d.name HAVING count(DISTINCT f.repo)=1
 )
 SELECT u.*,o.repo target_repo FROM longest u JOIN owners o USING(name)
 JOIN enrich_file f ON f.ordinal=u.file_ord WHERE f.repo<>o.repo;
CREATE TEMP TABLE enrich_candidates AS
 SELECT 0 phase,r.ordinal file_ord,p.occurrence,0::BIGINT target_ord,0 emission,
 r.id src,c.id dst,'declares_capability' kind,'curated' basis,1.0::DOUBLE confidence,r.id evidence_id,0::BIGINT line,
 'policy-capability' rule,'Corpus policy associates this repository with '||c.label||'; this is a curated navigation label.' explanation
 FROM enrich_policy p JOIN enrich_repository r USING(repo) JOIN enrich_caps c USING(repo,label)
 UNION ALL
 SELECT 1,u.file_ord,u.occurrence,d.file_ord,0,f.id,t.id,'references_project','inferred',0.75,f.id,u.line,
 'module-'||u.usage,'Textual '||u.usage||' of '||u.name||' matches an indexed top-level declaration; compiler name resolution and successful compilation are not established.'
 FROM enrich_modules u JOIN enrich_file f ON f.ordinal=u.file_ord
 JOIN enrich_declaration d ON d.name=u.name JOIN enrich_file t ON t.ordinal=d.file_ord
 UNION ALL
 SELECT 1,u.file_ord,u.occurrence,9223372036854775807,0,f.id,r.id,'references_project','inferred',0.75,f.id,u.line,
 'module-'||u.usage||'-repository','Indexed repository owns the matching declaration for '||u.name||'; textual inference only.'
 FROM enrich_modules u JOIN enrich_file f ON f.ordinal=u.file_ord JOIN enrich_repository r ON r.repo=u.target_repo
 UNION ALL
 SELECT 2,a.file_ord,a.occurrence,0,0,f.id,r.id,a.kind,a.basis,a.confidence,f.id,a.line,a.rule,a.explanation
 FROM enrich_alias a JOIN enrich_file f ON f.ordinal=a.file_ord JOIN enrich_repository r ON r.alias=a.alias
 WHERE a.kind='generated_by' OR f.repo<>r.repo
 UNION ALL
 SELECT 2,a.file_ord,a.occurrence,t.ordinality,0,f.id,r.id,'mentions','inferred',0.55,f.id,a.line,
 'exact-repository-name','Exact allowlisted repository name occurs in prose; topical relationship only, including possible proposals or negative statements.'
 FROM enrich_mention a CROSS JOIN unnest(regexp_extract_all(a.text,'[A-Za-z0-9_.\-]+')) WITH ORDINALITY t(alias,ordinality)
 JOIN enrich_file f ON f.ordinal=a.file_ord JOIN enrich_repository r ON r.alias=t.alias AND f.repo<>r.repo
 UNION ALL
 SELECT 2,b.file_ord,b.occurrence,0,0,f.id,c.id,'suggests_capability','inferred',b.confidence,f.id,b.line,b.rule,b.explanation
 FROM enrich_binding b JOIN enrich_file f ON f.ordinal=b.file_ord JOIN enrich_caps c ON c.repo=f.repo AND c.label=b.label
 UNION ALL
 SELECT 2,a.file_ord,a.occurrence,0,0,f.id,t.dst,a.kind,'extracted',1.0,f.id,a.line,a.rule,a.explanation
 FROM enrich_reference a JOIN enrich_file f ON f.ordinal=a.file_ord JOIN enrich_resolved t USING(event) WHERE f.repo<>t.target_repo
 UNION ALL
 SELECT 2,a.file_ord,a.occurrence,0,1,f.id,t.repo_id,a.kind,'extracted',1.0,f.id,a.line,a.rule||'-repository',
 a.explanation||' Repository endpoint is the indexed owner of the referenced file.'
 FROM enrich_reference a JOIN enrich_file f ON f.ordinal=a.file_ord JOIN enrich_resolved t USING(event)
 WHERE f.repo<>t.target_repo AND t.target_kind<>'repository'
 UNION ALL
 SELECT DISTINCT 3,p.file_ord,f.ordinal,0,0,f.id,s.id,'generated_from','inferred',0.9,s.id,p.line,'pilot-output-namespace',
 'Profile output directory and namespace match this generated declaration; a reproducible generation receipt is not available.'
 FROM enrich_profile p JOIN enrich_file s ON s.ordinal=p.file_ord
 JOIN enrich_file f ON f.repo=s.repo AND starts_with(f.path,p.prefix) AND f.generated
 AND f.module<>'' AND (f.module=p.namespace OR starts_with(f.module,p.namespace||'.'))
 UNION ALL
 SELECT 4,dense_rank() OVER(ORDER BY c.label),r.ordinal,0,d.direction,
 CASE d.direction WHEN 0 THEN a.id ELSE c.id END,CASE d.direction WHEN 0 THEN c.id ELSE a.id END,
 'shares_capability','inferred',0.6,CASE d.direction WHEN 0 THEN a.id ELSE c.id END,0,'shared-capability-label',
 'Both repositories carry capability label '||c.label||'; similarity is not equivalence, dependency or interoperability.'
 FROM enrich_caps c JOIN enrich_repository r ON r.repo=c.repo
 JOIN (SELECT c.* FROM enrich_caps c JOIN enrich_repository r USING(repo)
       QUALIFY row_number() OVER(PARTITION BY label ORDER BY r.ordinal)=1) a ON a.label=c.label AND a.repo<>c.repo
 CROSS JOIN (VALUES(0),(1)) d(direction);
"""

/// Enrich inside the candidate generation transaction. Canonical serialization and
/// field validation remain at the host boundary; relational matching stays in-engine.
let runParsed (connection: DuckDB.NET.Data.DuckDBConnection) (repositories: Repository list) (allNodes: Node seq) (allEdges: JsonValue seq) (limits: Limits) : Result =
    let phases = ResizeArray<string * float>()
    let timer = Diagnostics.Stopwatch.StartNew()
    let phase name = phases.Add(name, timer.Elapsed.TotalSeconds); timer.Restart()
    if repositories.Length > 64 then raise (RelationshipError "relationship pass supports at most 64 repositories")
    let repos = repositories |> List.sortWith (fun a b -> String.CompareOrdinal(a.Name,b.Name))
    let nodes = allNodes |> Seq.filter (fun n -> n.Kind <> "capability") |> Seq.toArray
    let passthrough =
        allEdges |> Seq.filter (fun e ->
            let field name = match Json.prop name e with Some(JsonValue.String value) -> value | _ -> ""
            not (derivedKinds.Contains(field "kind")) && not ((field "rule").StartsWith("relationships-v",StringComparison.Ordinal))) |> Seq.toArray
    let files = nodes |> Array.filter (fun n -> n.Kind="file") |> Array.sortWith (fun a b ->
        match String.CompareOrdinal(a.Repo,b.Repo) with 0 -> String.CompareOrdinal(a.Path,b.Path) | c -> c)
    if files.Length > limits.MaxFiles || (files |> Array.sumBy (fun n -> int64(Text.Encoding.UTF8.GetByteCount n.Text))) > limits.MaxSourceBytes then
        raise (RelationshipError "relationship source scan exceeded file/byte budget")
    let metadata = repos |> Seq.map (fun r -> r.Name,r) |> dict
    let repoNodes = nodes |> Seq.filter (fun n -> n.Kind="repository") |> Seq.map (fun n -> n.Repo,n) |> dict
    let aliases = HashSet<string>(StringComparer.Ordinal)
    for r in repos do
        if not (aliases.Add(r.Name.Substring(r.Name.LastIndexOf('/')+1))) then raise (RelationshipError "repository short names must be unambiguous")
        if not (repoNodes.ContainsKey r.Name) || repoNodes.[r.Name].Revision<>r.Commit then raise (RelationshipError "repository revision does not match repository metadata")
        if r.Capabilities.Length>32 || r.Capabilities |> List.exists (fun s -> s.Length>80 || not(slug.IsMatch s)) then
            raise (RelationshipError "invalid curated capability labels")
    for f in files do
        if not (metadata.ContainsKey f.Repo) || f.Revision<>metadata.[f.Repo].Commit then raise (RelationshipError "file revision does not match repository metadata")
    let scanned : (Fact[] * string * bool)[] = Array.zeroCreate files.Length
    Parallel.For(0,files.Length,fun i -> scanned.[i] <- scan files.[i]) |> ignore
    phase "relationship_scan"
    for name,columns in tables do Load.execute connection ("CREATE TEMP TABLE enrich_"+name+" ("+columns+")")
    append connection "repository" (repos |> Seq.mapi (fun i r ->
        let u=Resolve.splitUrl(r.SourceUrl.TrimEnd('/'))
        [|box i;box repoNodes.[r.Name].Id;box r.Name;box r.Commit;box r.Ref;box(r.Name.Substring(r.Name.LastIndexOf('/')+1));box repoNodes.[r.Name].SourceUrl;box u.Scheme;box u.Netloc;box(u.Path.TrimEnd('/'))|]))
    append connection "file" (files |> Seq.mapi (fun i f ->
        let _,name,generated = scanned.[i]
        [|box i;box f.Id;box f.Repo;box f.Path;box name;box generated|]))
    let rows = tables |> Seq.map(fun (name,_) -> name,ResizeArray<obj[]>()) |> dict
    let add name values = rows.[name].Add values
    let cap repo label curated =
        let r=metadata.[repo]
        // Canonical identity is shared with snapshot hashing, not reimplemented in SQL JSON.
        add "capability" [|box repo;box label;box(identity [relationshipVersion;repo;r.Commit;"capability";label]);box curated;box r.Commit;box repoNodes.[repo].SourceUrl|]
    for r in repos do
        r.Capabilities |> List.distinct |> List.sortWith (fun a b -> String.CompareOrdinal(a,b)) |> List.iteri (fun i label ->
            cap r.Name label true
            add "policy" [|box r.Name;box label;box i|])
    let mutable event=0L
    for i in 0..scanned.Length-1 do
        let facts,_,_ = scanned.[i]
        for j in 0..facts.Length-1 do
            match facts.[j] with
            | Declaration name -> add "declaration" [|box i;box j;box name|]
            | Use(line,name,usage,token) -> add "use" [|box i;box j;box line;box name;box usage;box token|]
            | Reference(line,address,kind,rule,explanation) ->
                event<-event+1L
                add "address" [|box event;box address.Scheme;box address.Netloc;box address.Path;box address.Alias|]
                add "reference" [|box event;box i;box j;box line;box kind;box rule;box explanation|]
            | Alias(line,name,kind,basis,confidence,rule,explanation) ->
                add "alias" [|box i;box j;box line;box name;box kind;box basis;box confidence;box rule;box explanation|]
            | Mentions(line,text) -> add "mention" [|box i;box j;box line;box text|]
            | Capability(line,label,confidence,rule,explanation) ->
                cap files.[i].Repo label false
                add "binding" [|box i;box j;box line;box label;box confidence;box rule;box explanation|]
            | Profile(prefix,ns,line) -> add "profile" [|box i;box prefix;box ns;box line|]
    for name,_ in tables do if name<>"repository" && name<>"file" then append connection name rows.[name]
    phase "relationship_stage"
    Load.execute connection ("CREATE TEMP TABLE enrich_resolved AS " + Resolve.sqlResolve)
    Load.execute connection relationshipsSql
    Load.execute connection (sprintf """
CREATE TEMP TABLE enrich_derived AS
 SELECT * EXCLUDE(phase,file_ord,occurrence,target_ord,emission,rule,explanation),
 '%s:'||rule rule,left(explanation,%d) explanation
 FROM enrich_candidates WHERE src<>dst
 QUALIFY row_number() OVER(PARTITION BY src,dst,kind,evidence_id,rule ORDER BY phase,file_ord,occurrence,target_ord,emission)=1
 ORDER BY phase,file_ord,occurrence,target_ord,emission;
""" relationshipVersion maxExplanation)
    phase "relationship_join"
    let read sql action =
        use command=connection.CreateCommand()
        command.CommandText<-sql
        use reader=command.ExecuteReader()
        action reader
    let count table = read ("SELECT count(*) FROM " + table) (fun r -> r.Read() |> ignore; r.GetInt64 0)
    if count "enrich_derived" > int64 limits.MaxDerivedEdges then raise (RelationshipError "relationship edge budget exceeded")
    if count "enrich_caps" > int64 limits.MaxCapabilityNodes then raise (RelationshipError "capability node budget exceeded")
    let capabilities = read "SELECT id,repo,revision,label,curated,source_url FROM enrich_caps" (fun r ->
        [|while r.Read() do yield {Id=r.GetString 0;Repo=r.GetString 1;Revision=r.GetString 2;Label=r.GetString 3;Curated=r.GetBoolean 4;SourceUrl=r.GetString 5}|])
    let derived = read "SELECT src,dst,kind,basis,confidence,evidence_id,line,rule,explanation FROM enrich_derived" (fun r ->
        [|while r.Read() do yield {Src=r.GetString 0;Dst=r.GetString 1;Kind=r.GetString 2;Basis=r.GetString 3;Confidence=r.GetDouble 4;EvidenceId=r.GetString 5;Line=int(r.GetInt64 6);Rule=r.GetString 7;Explanation=r.GetString 8}|])
    // Retain only the validated output for Load's in-engine transfer.
    Load.execute connection
        (String.concat "\n" [ for name in (tables |> List.map fst) @ ["caps";"modules";"resolved";"candidates"] ->
                                "DROP TABLE temp.main.enrich_" + name + ";" ])
    phase "relationship_read"
    {Nodes=nodes;Capabilities=capabilities;PassthroughEdges=passthrough;Derived=derived;Phases=List.ofSeq phases}

/// Standalone enrichment uses the same SQL pass as candidate generation.
let run (engine: Load.Engine) (root: JsonValue) (limits: Limits) : Result =
    use connection=Load.openEngine engine ":memory:"
    runParsed connection (JsonInput.property "repositories" root |> JsonInput.items |> List.map parseRepository)
        (JsonInput.property "nodes" root |> JsonInput.items |> Seq.map parseNode)
        (JsonInput.property "edges" root |> JsonInput.items) limits
