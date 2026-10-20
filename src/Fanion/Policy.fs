/// Repository policy validation (corpus_policy.normalize_policy semantics): rejects
/// unsupported or unbounded curation before a generation is built.
module Fanion.Policy

open System
open System.Text.RegularExpressions
open Fidelity.Data.JSON
open Fanion.Canonical
open Fanion.Snapshot

let private roles = set [ "central"; "component"; "generator"; "generated"; "example" ]
let private slug = Regex(@"\A[a-z0-9]+(?:-[a-z0-9]+)*\z", RegexOptions.Compiled)

let private glob (value: JsonValue) (field: string) =
    let v =
        match value with
        | JsonValue.String text when text <> "" -> text.ToLowerInvariant()
        | _ -> raise (RelationshipError(field + " entries must be repository-relative globs"))
    let size = Text.Encoding.UTF8.GetByteCount v
    if size > 256 || v.StartsWith("/", StringComparison.Ordinal) || v.Contains '\\'
       || v.Split('/') |> Array.exists (fun part -> part = "" || part = "." || part = "..")
       || v |> Seq.exists (fun c -> int c < 32 || int c = 127) then
        raise (RelationshipError("invalid repository-relative " + field + " glob"))
    let mutable i = 0
    while i < v.Length do
        if v.[i] = ']' then raise (RelationshipError("unmatched character-class delimiter in " + field + " glob"))
        if v.[i] <> '[' then i <- i + 1
        else
            let mutable start = i + 1
            if start < v.Length && v.[start] = '!' then start <- start + 1
            let mutable finish = start + (if start < v.Length && v.[start] = ']' then 1 else 0)
            finish <- v.IndexOf(']', finish)
            if finish < 0 || finish = start || v.Substring(start, finish - start).Contains '/' then
                raise (RelationshipError("invalid character class in " + field + " glob"))
            let body = v.Substring(start, finish - start)
            for offset in 1 .. body.Length - 2 do
                if body.[offset] = '-' && Char.ToLowerInvariant body.[offset - 1] > Char.ToLowerInvariant body.[offset + 1] then
                    raise (RelationshipError("reversed character range in " + field + " glob"))
            i <- finish + 1

/// corpus_policy.normalize_policy output: a fresh canonical policy.
type Normalized =
    { Role: string; Authority: int64; Capabilities: string list
      IncludeGenerated: string list; IncludePaths: string list; ExcludePaths: string list }

let policyVersion = "repository-policy-v1"

/// Validate one policy object (not the repository wrapper); raises RelationshipError with PolicyError's messages.
let validatePolicy (policy: JsonValue) =
    let fields =
        match policy with
        | JsonValue.Object fields -> fields
        | _ -> raise (RelationshipError "policy accepts only role, authority, capabilities, include_generated, include_paths and exclude_paths")
    let allowed = set [ "role"; "authority"; "capabilities"; "include_generated"; "include_paths"; "exclude_paths" ]
    for name, _ in fields do
        if not (allowed.Contains name) then raise (RelationshipError "policy accepts only role, authority, capabilities, include_generated, include_paths and exclude_paths")
    match Json.prop "role" policy with
    | Some(JsonValue.String role) when roles.Contains role -> ()
    | Some _ -> raise (RelationshipError "unknown repository policy role")
    | None -> ()
    match Json.prop "authority" policy with
    | Some authority ->
        match Json.tryAsInt64 authority with
        | Some value when value >= 0L && value <= 100L -> ()
        | _ -> raise (RelationshipError "policy authority must be an integer from 0 to 100")
    | None -> ()
    match Json.prop "capabilities" policy with
    | Some(JsonValue.Array capabilities) when capabilities.Length <= 32 ->
        for item in capabilities do
            match item with
            | JsonValue.String label when label.Length <= 64 && slug.IsMatch label -> ()
            | _ -> raise (RelationshipError "policy capabilities must be lowercase slugs of at most 64 characters")
    | Some _ -> raise (RelationshipError "policy capabilities must be a list of at most 32 slugs")
    | None -> ()
    for field in [ "include_generated"; "include_paths"; "exclude_paths" ] do
        match Json.prop field policy with
        | Some(JsonValue.Array patterns) when patterns.Length <= 32 -> for item in patterns do glob item field
        | Some _ -> raise (RelationshipError("policy " + field + " must be a list of at most 32 globs"))
        | None -> ()

/// Validate a repository's optional policy object.
let validate (repository: JsonValue) =
    match Json.prop "policy" repository with
    | None -> ()
    | Some(JsonValue.Object _ as policy) -> validatePolicy policy
    | Some _ -> raise (RelationshipError "Repository policy must be an object")

/// normalize_policy: defaults, sorted distinct capabilities, sorted distinct lowercased globs.
let normalize (policy: JsonValue option) : Normalized =
    match policy with
    | None ->
        { Role = "component"; Authority = 0L; Capabilities = []; IncludeGenerated = []; IncludePaths = []; ExcludePaths = [] }
    | Some p ->
        validatePolicy p
        let strings name =
            match Json.prop name p with
            | Some(JsonValue.Array values) -> values |> List.map JsonInput.string
            | _ -> []
        let sortedDistinct (items: string list) = items |> List.distinct |> List.sortWith (fun a b -> String.CompareOrdinal(a, b))
        { Role = (Json.prop "role" p |> Option.map JsonInput.string |> Option.defaultValue "component")
          Authority = (Json.prop "authority" p |> Option.map JsonInput.int64 |> Option.defaultValue 0L)
          Capabilities = sortedDistinct (strings "capabilities")
          IncludeGenerated = sortedDistinct (strings "include_generated" |> List.map (fun g -> g.ToLowerInvariant()))
          IncludePaths = sortedDistinct (strings "include_paths" |> List.map (fun g -> g.ToLowerInvariant()))
          ExcludePaths = sortedDistinct (strings "exclude_paths" |> List.map (fun g -> g.ToLowerInvariant())) }
