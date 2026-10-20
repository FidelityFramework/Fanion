/// Worked retrieval examples sent with each model request (prompt_examples.py), byte-identical.
module Fanion.Examples

open System
open System.Security.Cryptography
open System.Text
open Fanion.Canonical

let exampleId (name: string) = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("fidelity-pgq-teaching-v1:" + name))).ToLowerInvariant()
let exampleRepo, otherRepo = "Example/Compiler", "Example/Handbook"

let private candidate name path repo : J = Obj [ "id", Str (exampleId name); "repo", Str repo; "path", Str path; "kind", Str "file" ]
let private main = candidate "architecture" "docs/Architecture.md" exampleRepo
let private mention = candidate "mention" "docs/00-overview.md" exampleRepo
let private other = candidate "other-architecture" "docs/Architecture.md" otherRepo
let private idOf (c: J) = match c with Obj pairs -> (pairs |> List.pick (fun (k, v) -> if k = "id" then (match v with Str s -> Some s | _ -> None) else None)) | _ -> ""

let private planExamples : J list =
    [ Obj [ "request", Str "Which files link to docs/Architecture.md in Example/Compiler?"
            "decision", Str "Discover the named file first. Preserve the whole path and repository; no guessed layer filter."
            "output", Obj [ "lookups", Arr [ Obj [ "query", Str "docs/Architecture.md"; "repo", Str exampleRepo; "corpus_layer", Null ] ] ] ]
      Obj [ "request", Str "Find the design discussion of region ownership in Example/Compiler."
            "decision", Str "Search a distinctive literal phrase. The request does not establish a corpus layer. This is substring discovery, not semantic search."
            "output", Obj [ "lookups", Arr [ Obj [ "query", Str "region ownership"; "repo", Str exampleRepo; "corpus_layer", Null ] ] ] ]
      Obj [ "request", Str "What prevents foreign-call ownership details from leaking between the parser and emitter?"
            "decision", Str "Search separate literal technical terms. A combined string such as foreign-call ownership parser emitter is not a literal passage. No repository or layer is established by this question."
            "output", Obj [ "lookups", Arr [ Obj [ "query", Str "foreign-call"; "repo", Null; "corpus_layer", Null ]
                                             Obj [ "query", Str "ownership"; "repo", Null; "corpus_layer", Null ] ] ] ] ]

let private oneHop incoming columns =
    let edge = if incoming then "<-[e:relation WHERE e.kind = $edge_kind]-" else "-[e:relation WHERE e.kind = $edge_kind]->"
    "MATCH (a:resource WHERE a.id = $seed)" + edge + "(b:resource) COLUMNS (" + columns + ")"

let private queryExample request decision query (edgeKind: string option) (seed: J) (candidates: J list) : J =
    let parameters = Obj ([ "seed", Str (idOf seed) ] @ (match edgeKind with Some k -> [ "edge_kind", Str k ] | None -> []))
    Obj [ "request", Str request; "candidates", Arr candidates; "decision", Str decision; "output", Obj [ "query", Str query; "parameters", parameters ] ]

let private queryExamples : J list =
    [ queryExample "Which files are linked FROM docs/Architecture.md in Example/Compiler?"
        "Choose the exact path, not the first candidate or a file mentioning it. The seed links to each result: a -> b."
        (oneHop false "b.id, b.repo, b.path, b.kind, b.source_url, e.kind") (Some "links_to") main [ mention; main ]
      queryExample "Which files link TO docs/Architecture.md in Example/Compiler?"
        "Use the same exact file as the preceding example, but reverse the traversal. Each result links to the seed: b -> a."
        (oneHop true "b.id, b.repo, b.path, b.kind, b.source_url, e.kind") (Some "links_to") main [ mention; main ]
      queryExample "Return the source chunks belonging to docs/Architecture.md in Example/Compiler, including their line ranges."
        "The discovered seed is a file. chunks_of points chunk -> file, so traverse incoming to return chunks with source spans."
        (oneHop true "b.id, b.path, b.kind, b.line_start, b.line_end, b.source_url, e.kind") (Some "chunks_of") main [ mention; main ]
      queryExample "Which repository contains docs/Architecture.md in Example/Compiler?"
        "contains points repository -> file. From a file seed use incoming contains; b is the owning repository."
        (oneHop true "b.id, b.repo, b.path, b.kind, b.title, e.kind") (Some "contains") main [ mention; main ]
      queryExample "What resources are reachable from docs/Architecture.md in Example/Compiler within three outgoing hops, allowing any relation kind?"
        "Use outgoing ANY SHORTEST with bounds 1..3. This is explicitly unfiltered: parameters contains only seed. hops is a shortest distance, not an enumeration of every path."
        "MATCH p = ANY SHORTEST (a:resource WHERE a.id = $seed)-[e:relation]->{1,3}(b:resource) COLUMNS (b.id, b.repo, b.path, b.kind, b.source_url, path_length(p))" None main [ mention; main ]
      queryExample "Which files are linked from docs/Architecture.md in Example/Handbook?"
        "Both files have the same path. Select the requested repository as well as the path; do not use candidate order. Then follow outgoing links_to."
        (oneHop false "b.id, b.repo, b.path, b.kind, e.kind") (Some "links_to") other [ main; other ] ]

let workedExamples (stage: string) =
    let examples =
        match stage with
        | "seed_discovery" -> planExamples
        | "query_generation" -> queryExamples
        | _ -> raise (ArgumentException "Unknown example stage")
    "\n\nWORKED EXAMPLES (synthetic teaching fixtures, not current evidence).\n"
    + "The example repositories, paths and IDs are fictional. For the actual request,\n"
    + "use only its supplied repository/layer values and discovered evidence IDs.\n"
    + "Apply the example pattern to the actual question; never copy a teaching ID.\n"
    + "The decision notes explain the examples. Your output contains only the JSON\n"
    + "shown under output, with actual request values; omit all decision notes.\n"
    + String.Join("\n", examples |> List.map (toText orderedCompactStyle))
