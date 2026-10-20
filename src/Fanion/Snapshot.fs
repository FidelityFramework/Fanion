/// Graph values and their persisted representation. Fidelity.Data owns JSON;
/// snapshot identities retain the established canonical byte contract.
module Fanion.Snapshot

open System
open System.Collections.Generic
open Fidelity.Data.JSON
open Fanion.Canonical

exception RelationshipError of string

let relationshipVersion = "relationships-v2"
let derivedKinds = HashSet<string>([ "references_project"; "generated_by"; "generated_from"; "mentions"; "declares_capability"; "suggests_capability"; "shares_capability" ])
let maxDerivedEdges = 50_000
let maxCapabilityNodes = 2048
let maxExplanation = 512

type Node =
    { Id: string; Repo: string; Revision: string; Path: string; Kind: string; Text: string; SourceUrl: string; Raw: JsonValue }

type Repository =
    { Name: string; Commit: string; Ref: string; SourceUrl: string; Capabilities: string list; Raw: JsonValue }

type DerivedEdge =
    { Src: string; Dst: string; Kind: string; Basis: string; Confidence: float
      EvidenceId: string; Line: int; Rule: string; Explanation: string }

type CapabilityNode =
    { Id: string; Repo: string; Revision: string; Label: string; Curated: bool; SourceUrl: string }

let private str element name = Json.prop name element |> Option.bind Json.asString |> Option.defaultValue null
let private requireStr element name =
    match str element name with
    | null -> raise (RelationshipError(sprintf "snapshot field %s is missing or not a string" name))
    | value -> value

let parseNode element : Node =
    { Id = requireStr element "id"; Repo = requireStr element "repo"; Revision = str element "revision"
      Path = (match str element "path" with null -> "" | p -> p); Kind = requireStr element "kind"
      Text = (match str element "text" with null -> "" | t -> t); SourceUrl = str element "source_url"; Raw = element }

let parseRepository element : Repository =
    let capabilities =
        match Json.prop "policy" element with
        | Some (JsonValue.Object _ as policy) ->
            match Json.prop "capabilities" policy with
            | Some (JsonValue.Array labels) ->
                labels |> List.map (function
                    | JsonValue.String label -> label
                    | _ -> raise (RelationshipError "invalid curated capability labels"))
            | Some _ -> raise (RelationshipError "invalid curated capability labels")
            | None -> []
        | _ -> []
    { Name = requireStr element "name"; Commit = requireStr element "commit"
      Ref = (match str element "ref" with null -> "" | r -> r)
      SourceUrl = (match str element "source_url" with null -> "" | u -> u)
      Capabilities = capabilities; Raw = element }

/// Integral confidence values retain the persisted trailing decimal point.
let formatConfidence (value: float) =
    let text = value.ToString("R", Globalization.CultureInfo.InvariantCulture)
    if text.Contains '.' || text.Contains 'E' then text else text + ".0"

let identity parts = digest canonicalStyle (Arr(List.map Str parts)) |> fst

let capabilityText label curated =
    sprintf "%s: %s; not proof of implemented or validated behavior." label
        (if curated then "curated repository label" else "capability inferred from binding API declarations")

let capabilityJson (cap: CapabilityNode) =
    JsonValue.Object [
        "id", JsonValue.String cap.Id; "repo", JsonValue.String cap.Repo
        "revision", JsonValue.String cap.Revision; "path", JsonValue.String ""
        "kind", JsonValue.String "capability"; "title", JsonValue.String cap.Label
        "line_start", Json.ofInt64 0L; "line_end", Json.ofInt64 0L
        "text", JsonValue.String(capabilityText cap.Label cap.Curated)
        "source_url", JsonValue.String cap.SourceUrl; "corpus_layer", JsonValue.String "capability"
        "classification_rule", JsonValue.String(relationshipVersion + ":" + (if cap.Curated then "curated" else "api")) ]

let edgeJson (edge: DerivedEdge) =
    JsonValue.Object [
        "src", JsonValue.String edge.Src; "dst", JsonValue.String edge.Dst
        "kind", JsonValue.String edge.Kind; "basis", JsonValue.String edge.Basis
        "confidence", JsonInput.parse (formatConfidence edge.Confidence)
        "evidence_id", JsonValue.String edge.EvidenceId
        "evidence_line_start", Json.ofInt64(int64 edge.Line); "evidence_line_end", Json.ofInt64(int64 edge.Line)
        "rule", JsonValue.String edge.Rule; "explanation", JsonValue.String edge.Explanation ]
