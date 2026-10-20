/// Repository policy validation (corpus_policy.normalize_policy semantics): rejects
/// unsupported or unbounded curation before a generation is built.
module Retrieval.Native.Policy

open System
open System.Text.Json
open System.Text.RegularExpressions
open Retrieval.Native.Snapshot

let private roles = set [ "central"; "component"; "generator"; "generated"; "example" ]
let private slug = Regex(@"\A[a-z0-9]+(?:-[a-z0-9]+)*\z", RegexOptions.Compiled)

let private glob (value: JsonElement) (field: string) =
    if value.ValueKind <> JsonValueKind.String || value.GetString() = "" then raise (RelationshipError(field + " entries must be repository-relative globs"))
    let v = value.GetString().ToLowerInvariant()
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
let validatePolicy (policy: JsonElement) =
    if policy.ValueKind <> JsonValueKind.Object then raise (RelationshipError "policy accepts only role, authority, capabilities, include_generated, include_paths and exclude_paths")
    begin
        let allowed = set [ "role"; "authority"; "capabilities"; "include_generated"; "include_paths"; "exclude_paths" ]
        for p in policy.EnumerateObject() do
            if not (allowed.Contains p.Name) then raise (RelationshipError "policy accepts only role, authority, capabilities, include_generated, include_paths and exclude_paths")
        match policy.TryGetProperty("role") with
        | true, role when role.ValueKind <> JsonValueKind.String || not (roles.Contains(role.GetString())) -> raise (RelationshipError "unknown repository policy role")
        | _ -> ()
        match policy.TryGetProperty("authority") with
        | true, authority ->
            let ok = authority.ValueKind = JsonValueKind.Number && not (authority.GetRawText().Contains '.') && not (authority.GetRawText().Contains 'e') && not (authority.GetRawText().Contains 'E')
                     && (match authority.TryGetInt64() with | true, v -> v >= 0L && v <= 100L | _ -> false)
            if not ok then raise (RelationshipError "policy authority must be an integer from 0 to 100")
        | _ -> ()
        match policy.TryGetProperty("capabilities") with
        | true, capabilities ->
            if capabilities.ValueKind <> JsonValueKind.Array || capabilities.GetArrayLength() > 32 then raise (RelationshipError "policy capabilities must be a list of at most 32 slugs")
            for item in capabilities.EnumerateArray() do
                if item.ValueKind <> JsonValueKind.String || item.GetString().Length > 64 || not (slug.IsMatch(item.GetString())) then
                    raise (RelationshipError "policy capabilities must be lowercase slugs of at most 64 characters")
        | _ -> ()
        for field in [ "include_generated"; "include_paths"; "exclude_paths" ] do
            match policy.TryGetProperty(field) with
            | true, patterns ->
                if patterns.ValueKind <> JsonValueKind.Array || patterns.GetArrayLength() > 32 then raise (RelationshipError("policy " + field + " must be a list of at most 32 globs"))
                for item in patterns.EnumerateArray() do glob item field
            | _ -> ()
    end

/// Validate a repository's optional policy object.
let validate (repository: JsonElement) =
    match repository.TryGetProperty("policy") with
    | false, _ -> ()
    | true, policy when policy.ValueKind <> JsonValueKind.Object -> raise (RelationshipError "Repository policy must be an object")
    | true, policy -> validatePolicy policy

/// normalize_policy: defaults, sorted distinct capabilities, sorted distinct lowercased globs.
let normalize (policy: JsonElement option) : Normalized =
    match policy with
    | None ->
        { Role = "component"; Authority = 0L; Capabilities = []; IncludeGenerated = []; IncludePaths = []; ExcludePaths = [] }
    | Some p ->
        validatePolicy p
        let strings name =
            match p.TryGetProperty(name: string) with
            | true, v when v.ValueKind = JsonValueKind.Array -> [ for item in v.EnumerateArray() -> item.GetString() ]
            | _ -> []
        let sortedDistinct (items: string list) = items |> List.distinct |> List.sortWith (fun a b -> String.CompareOrdinal(a, b))
        { Role = (match p.TryGetProperty("role") with | true, v -> v.GetString() | _ -> "component")
          Authority = (match p.TryGetProperty("authority") with | true, v -> v.GetInt64() | _ -> 0L)
          Capabilities = sortedDistinct (strings "capabilities")
          IncludeGenerated = sortedDistinct (strings "include_generated" |> List.map (fun g -> g.ToLowerInvariant()))
          IncludePaths = sortedDistinct (strings "include_paths" |> List.map (fun g -> g.ToLowerInvariant()))
          ExcludePaths = sortedDistinct (strings "exclude_paths" |> List.map (fun g -> g.ToLowerInvariant())) }
