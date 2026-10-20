/// Request-scoped llama.cpp grammars (gbnf.py), byte-identical output.
module Fanion.Gbnf

open System
open System.Text.RegularExpressions
open Fanion.Canonical

let version = "fidelity-gbnf-v2"
let private hex = Regex(@"\A[0-9a-f]{64}\z", RegexOptions.Compiled)
let nodeFields = [ "id"; "repo"; "revision"; "path"; "kind"; "title"; "line_start"; "line_end"; "source_url"; "corpus_layer"; "classification_rule" ]
let edgeKinds = [ "chunks_of"; "contains"; "declares_capability"; "generated_by"; "generated_from"; "links_to"; "mentions"; "references_project"; "shares_capability"; "suggests_capability" ]
let edgeProperties = [ "kind"; "basis"; "confidence"; "evidence_id"; "evidence_line_start"; "evidence_line_end"; "rule"; "explanation" ]
let defaultRepositories = [ "FidelityFramework/Composer"; "FidelityFramework/clef"; "FidelityFramework/clef-lang-site"; "FidelityFramework/clef-lang-spec" ]
let defaultLayers = [ "architecture"; "config"; "decisions"; "docs"; "examples"; "guides"; "implementation"; "other"; "repository"; "research"; "spec"; "tests"; "tooling" ]

/// json.dumps(text, ensure_ascii=False)
let private literal (text: string) = toText defaultUnicodeStyle (Str text)
let private jsonLiteral (value: J) = literal (toText orderedCompactStyle value)
let private enum' (values: string list) = String.Join(" | ", values |> List.map (fun v -> jsonLiteral (Str v)))

let private strings (values: string list) (label: string) (maxCount: int) =
    if values.Length < 1 || values.Length > maxCount then raise (ArgumentException(sprintf "%s must contain 1–%d values" label maxCount))
    if values |> List.exists (fun v -> isNull v || v = "" || v.Length > 256) then raise (ArgumentException("Invalid " + label))
    values |> List.distinct |> List.sortWith (fun a b -> String.CompareOrdinal(a, b))

/// The live schema subset the grammars bind to.
type SchemaContext = { SnapshotId: string; Repositories: string list; CorpusLayers: string list; EdgeKinds: string list; NodeProperties: string list; EdgeProperties: string list }

let private context (schema: SchemaContext option) (evidenceIds: string list option) =
    let snapshot, repos, layers, kinds, fields, edgeFields =
        match schema with
        | None -> None, defaultRepositories, defaultLayers, edgeKinds, nodeFields, edgeProperties
        | Some s ->
            if isNull s.SnapshotId || not (hex.IsMatch s.SnapshotId) then raise (ArgumentException "A current 64-hex snapshot_id is required")
            let repos = strings s.Repositories "repositories" 512
            let layers = strings s.CorpusLayers "corpus layers" 512
            let kinds = strings s.EdgeKinds "edge kinds" 512
            if not (kinds |> List.forall (fun k -> List.contains k edgeKinds)) then raise (ArgumentException "Unsupported edge kind for this gateway")
            let supplied = strings s.NodeProperties "node properties" 512
            if not (List.contains "id" supplied) || not (supplied |> List.forall (fun f -> List.contains f nodeFields)) then raise (ArgumentException "Unsupported node properties for this gateway")
            let fields = nodeFields |> List.filter (fun f -> List.contains f supplied)
            let suppliedEdges = strings (if s.EdgeProperties.IsEmpty then [ "kind" ] else s.EdgeProperties) "edge properties" edgeProperties.Length
            if not (List.contains "kind" suppliedEdges) || not (suppliedEdges |> List.forall (fun f -> List.contains f edgeProperties)) then raise (ArgumentException "Unsupported edge properties for this gateway")
            Some s.SnapshotId, repos, layers, kinds, fields, edgeProperties |> List.filter (fun f -> List.contains f suppliedEdges)
    let evidence =
        match evidenceIds with
        | None -> None
        | Some ids ->
            let ids = strings ids "evidence IDs" 512
            if ids |> List.exists (fun v -> not (hex.IsMatch v)) then raise (ArgumentException "Evidence IDs must be lowercase 64-hex values")
            Some ids
    snapshot, repos, layers, kinds, fields, edgeFields, evidence

let private grammar (mode: string) (schema: SchemaContext option) (evidenceIds: string list option) =
    let snapshot, repos, layers, kinds, fields, edgeFields, evidence = context schema evidenceIds
    let rules = ResizeArray<string>()
    let rule (name: string) (expression: string) = rules.Add(name + " ::= " + expression)
    let memberOf (name: string) (value: string) = jsonLiteral (Str name) + " ws \":\" ws " + value
    let objectRule (name: string) (expression: string) = rule name ("\"{\" ws " + expression + " ws \"}\"")
    let comma = "ws \",\" ws "
    match mode with
    | "api" ->
        rule "root" "ws (action-schema | action-find | action-pgq | action-sources) ws"
        objectRule "action-schema" (memberOf "action" (jsonLiteral (Str "schema")))
        objectRule "action-find" (memberOf "action" (jsonLiteral (Str "find")) + " " + comma + memberOf "query" "query-string" + " " + comma
                                  + memberOf "snapshot_id" "snapshot" + " (" + comma + memberOf "repo" "repo" + ")?" + " (" + comma + memberOf "corpus_layer" "layer" + ")?"
                                  + " (" + comma + memberOf "limit" "find-limit" + ")?")
        objectRule "action-pgq" (memberOf "action" (jsonLiteral (Str "pgq")) + " " + comma + "pgq-fields " + comma + memberOf "snapshot_id" "snapshot"
                                 + " (" + comma + memberOf "limit" "pgq-limit" + ")?" + " (" + comma + memberOf "offset" "offset" + ")?")
        objectRule "action-sources" (memberOf "action" (jsonLiteral (Str "sources")) + " " + comma + memberOf "ids" "seed-array" + " " + comma + memberOf "snapshot_id" "snapshot"
                                     + " (" + comma + memberOf "max_chars" "max-chars" + ")?")
    | "seed_discovery" ->
        rule "root" "ws discovery ws"
        objectRule "discovery" (memberOf "lookups" "lookups")
        rule "lookups" "\"[\" ws lookup (ws \",\" ws lookup){0,2} ws \"]\""
        objectRule "lookup" (memberOf "query" "query-string" + " " + comma + memberOf "repo" "(repo | \"null\")" + " " + comma + memberOf "corpus_layer" "(layer | \"null\")")
    | "query_generation" ->
        rule "root" "ws query-response ws"
        objectRule "query-response" "pgq-fields"
    | _ -> raise (ArgumentException "Unknown grammar mode")
    if mode <> "seed_discovery" then
        rule "pgq-fields" "(fields-filtered | fields-plain | fields-shortest)"
        for (variant, parameters) in [ "filtered", "parameters-filtered"; "plain", "parameters-plain"; "shortest", "parameters-plain" ] do
            rule ("fields-" + variant) (memberOf "query" ("query-" + variant) + " " + comma + memberOf "parameters" parameters)
        objectRule "parameters-plain" (memberOf "seed" "seed")
        objectRule "parameters-filtered" (memberOf "seed" "seed" + " " + comma + memberOf "edge_kind" "edge-kind")
        let seedVertex = "(a:resource WHERE a.id = $seed)"
        let resultVertex = "(b:resource) COLUMNS ("
        for (variant, edge) in [ "filtered", "[e:relation WHERE e.kind = $edge_kind]"; "plain", "[e:relation]" ] do
            let patterns = [ sprintf "MATCH %s-%s->%s" seedVertex edge resultVertex; sprintf "MATCH %s<-%s-%s" seedVertex edge resultVertex ]
            rule ("query-" + variant) (literal "\"" + " (" + String.Join(" | ", patterns |> List.map literal) + ") node-columns edge-columns " + literal ")\"")
        rule "query-shortest" (literal ("\"MATCH p = ANY SHORTEST " + seedVertex + "-[e:relation]->{1,") + " [1-4] " + literal ("}" + resultVertex) + " node-columns (\", path_length(p)\")? " + literal ")\"")
        rule "node-columns" (literal "b.id" + String.Join("", fields |> List.filter (fun f -> f <> "id") |> List.map (fun f -> " (" + literal (", b." + f) + ")?")))
        rule "edge-columns" (String.Join(" ", edgeFields |> List.map (fun f -> "(" + literal (", e." + f) + ")?")))
        rule "edge-kind" (enum' kinds)
        rule "seed" (match evidence with Some ids -> enum' ids | None -> "hex64-string")
    if mode = "api" || mode = "seed_discovery" then
        rule "repo" (enum' repos)
        rule "layer" (enum' layers)
        rule "query-string" "\"\\\"\" json-char{1,256} \"\\\"\""
        // GBNF operates on Unicode scalars; surrogate codepoints cannot occur
        // directly and must not appear as escapes in a character class.
        // The JSON escape branch still excludes escaped lone surrogates.
        rule "json-char" "[^\"\\\\\\x00-\\x1F] | \"\\\\\" ([\"\\\\/bfnrt] | \"u\" ([0-9a-cA-Ce-fE-F] [0-9a-fA-F]{3} | [dD] [0-7] [0-9a-fA-F]{2}))"
    if mode = "api" then
        rule "snapshot" (match snapshot with Some s -> jsonLiteral (Str s) | None -> "hex64-string")
        rule "seed-array" "\"[\" ws seed (ws \",\" ws seed){0,9} ws \"]\""
        rule "find-limit" "[1-9] | [1-4] [0-9] | \"50\""
        rule "pgq-limit" "[1-9] | [1-9] [0-9] | \"100\""
        rule "offset" "\"0\" | [1-9] [0-9]{0,3} | \"10000\""
        rule "max-chars" "[1-9] [0-9]{0,3} | \"1\" [0-5] [0-9]{3} | \"16000\""
    if mode <> "seed_discovery" && (evidence.IsNone || (mode = "api" && snapshot.IsNone)) then
        rule "hex64-string" "\"\\\"\" [0-9a-f]{64} \"\\\"\""
    rule "ws" "[ \\t\\n\\r]{0,4}"
    "# Generated by retrieval.gbnf; gateway-owned, sent per request.\n# Canonical member/column order; server validation remains required.\n" + String.Join("\n", rules) + "\n"

let buildApiGrammar schema evidenceIds = grammar "api" schema evidenceIds

let buildStageGrammar (stage: string) (schema: SchemaContext option) (seedIds: string list) =
    if stage <> "seed_discovery" && stage <> "query_generation" then raise (ArgumentException "Unknown model stage")
    if schema.IsNone then raise (ArgumentException "A live schema is required for model stages")
    if stage = "query_generation" && seedIds.IsEmpty then raise (ArgumentException "Query generation requires discovered evidence IDs")
    grammar stage schema (if stage = "query_generation" then Some seedIds else None)
