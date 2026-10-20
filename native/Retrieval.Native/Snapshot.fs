/// Snapshot model. Untouched nodes and edges round-trip as their original JSON
/// so the Python coordinator's canonical hash is unaffected by this process.
module Retrieval.Native.Snapshot

open System
open System.Collections.Generic
open System.Text.Json
open System.Text.Json.Nodes

exception RelationshipError of string

let relationshipVersion = "relationships-v2"
let derivedKinds = HashSet<string>([ "references_project"; "generated_by"; "generated_from"; "mentions"; "declares_capability"; "suggests_capability"; "shares_capability" ])
let maxDerivedEdges = 50_000
let maxCapabilityNodes = 2048
let maxExplanation = 512

type Node =
    { Id: string; Repo: string; Revision: string; Path: string; Kind: string; Text: string; SourceUrl: string; Raw: JsonElement }

type Repository =
    { Name: string; Commit: string; Ref: string; SourceUrl: string; Capabilities: string list; Raw: JsonElement }

/// A derived edge or capability node produced by this pass.
type DerivedEdge =
    { Src: string; Dst: string; Kind: string; Basis: string; Confidence: float
      EvidenceId: string; Line: int; Rule: string; Explanation: string }

type CapabilityNode =
    { Id: string; Repo: string; Revision: string; Label: string; Curated: bool; SourceUrl: string }

let private str (element: JsonElement) (name: string) =
    match element.TryGetProperty(name) with
    | true, value when value.ValueKind = JsonValueKind.String -> value.GetString()
    | _ -> null

let private requireStr element name =
    match str element name with
    | null -> raise (RelationshipError(sprintf "snapshot field %s is missing or not a string" name))
    | value -> value

let parseNode (element: JsonElement) : Node =
    { Id = requireStr element "id"; Repo = requireStr element "repo"; Revision = str element "revision"
      Path = (match str element "path" with null -> "" | p -> p); Kind = requireStr element "kind"
      Text = (match str element "text" with null -> "" | t -> t); SourceUrl = str element "source_url"; Raw = element }

let parseRepository (element: JsonElement) : Repository =
    let capabilities =
        match element.TryGetProperty("policy") with
        | true, policy when policy.ValueKind = JsonValueKind.Object ->
            match policy.TryGetProperty("capabilities") with
            | true, labels when labels.ValueKind = JsonValueKind.Array ->
                [ for label in labels.EnumerateArray() ->
                    if label.ValueKind <> JsonValueKind.String then raise (RelationshipError "invalid curated capability labels")
                    label.GetString() ]
            | true, _ -> raise (RelationshipError "invalid curated capability labels")
            | _ -> []
        | _ -> []
    { Name = requireStr element "name"; Commit = requireStr element "commit"
      Ref = (match str element "ref" with null -> "" | r -> r)
      SourceUrl = (match str element "source_url" with null -> "" | u -> u)
      Capabilities = capabilities; Raw = element }

/// Python repr-style float: integral values keep a trailing ".0".
let formatConfidence (value: float) =
    let text = value.ToString("R", Globalization.CultureInfo.InvariantCulture)
    if text.Contains '.' || text.Contains 'E' then text else text + ".0"

/// sha256 over json.dumps(parts, sort_keys=True, separators=(",",":"), ensure_ascii=False)
let identity (parts: string list) =
    use stream = new IO.MemoryStream()
    let options = JsonWriterOptions(Encoder = Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping, Indented = false)
    (use writer = new Utf8JsonWriter(stream, options)
     writer.WriteStartArray()
     for part in parts do writer.WriteStringValue(part)
     writer.WriteEndArray())
    let hash = Security.Cryptography.SHA256.HashData(stream.ToArray())
    Convert.ToHexString(hash).ToLowerInvariant()

let capabilityText (label: string) (curated: bool) =
    sprintf "%s: %s; not proof of implemented or validated behavior." label
        (if curated then "curated repository label" else "capability inferred from binding API declarations")

let writeCapability (writer: Utf8JsonWriter) (cap: CapabilityNode) =
    writer.WriteStartObject()
    writer.WriteString("id", cap.Id)
    writer.WriteString("repo", cap.Repo)
    writer.WriteString("revision", cap.Revision)
    writer.WriteString("path", "")
    writer.WriteString("kind", "capability")
    writer.WriteString("title", cap.Label)
    writer.WriteNumber("line_start", 0)
    writer.WriteNumber("line_end", 0)
    writer.WriteString("text", capabilityText cap.Label cap.Curated)
    writer.WriteString("source_url", cap.SourceUrl)
    writer.WriteString("corpus_layer", "capability")
    writer.WriteString("classification_rule", relationshipVersion + ":" + (if cap.Curated then "curated" else "api"))
    writer.WriteEndObject()

let writeEdge (writer: Utf8JsonWriter) (edge: DerivedEdge) =
    writer.WriteStartObject()
    writer.WriteString("src", edge.Src)
    writer.WriteString("dst", edge.Dst)
    writer.WriteString("kind", edge.Kind)
    writer.WriteString("basis", edge.Basis)
    writer.WritePropertyName("confidence")
    writer.WriteRawValue(formatConfidence edge.Confidence, skipInputValidation = true)
    writer.WriteString("evidence_id", edge.EvidenceId)
    writer.WriteNumber("evidence_line_start", edge.Line)
    writer.WriteNumber("evidence_line_end", edge.Line)
    writer.WriteString("rule", edge.Rule)
    writer.WriteString("explanation", edge.Explanation)
    writer.WriteEndObject()
