/// Exact allowlisted link resolution: never a network or filesystem lookup.
module Fanion.Resolve

open System
open System.Text
open System.Text.RegularExpressions

/// Minimal urllib.parse.urlsplit: scheme, netloc, path, query (fragment dropped).
type SplitUrl = { Scheme: string; Netloc: string; Path: string; Query: string }

let private schemePattern = Regex(@"\A([A-Za-z][A-Za-z0-9+.\-]*):", RegexOptions.Compiled)

let splitUrl (value: string) : SplitUrl =
    let mutable rest = value
    let mutable scheme = ""
    let m = schemePattern.Match(rest)
    if m.Success then
        scheme <- m.Groups.[1].Value.ToLowerInvariant()
        rest <- rest.Substring(m.Length)
    let mutable netloc = ""
    if rest.StartsWith("//", StringComparison.Ordinal) then
        let stop = rest.IndexOfAny([| '/'; '?'; '#' |], 2)
        let endIndex = if stop < 0 then rest.Length else stop
        netloc <- rest.Substring(2, endIndex - 2)
        rest <- rest.Substring(endIndex)
    let hash = rest.IndexOf('#')
    if hash >= 0 then rest <- rest.Substring(0, hash)
    let q = rest.IndexOf('?')
    let path, query = if q >= 0 then rest.Substring(0, q), rest.Substring(q + 1) else rest, ""
    { Scheme = scheme; Netloc = netloc; Path = path; Query = query }

/// urllib.parse.unquote(errors="strict"): None when percent-decoding is not valid UTF-8.
let unquoteStrict (value: string) : string option =
    if not (value.Contains '%') then Some value else
    let bytes = ResizeArray<byte>()
    let decoder = UTF8Encoding(false, true)
    let output = StringBuilder()
    let flush () =
        if bytes.Count > 0 then
            output.Append(decoder.GetString(bytes.ToArray())) |> ignore
            bytes.Clear()
    try
        let mutable i = 0
        while i < value.Length do
            if value.[i] = '%' && i + 2 < value.Length + 0 && i + 2 <= value.Length - 1
               && Uri.IsHexDigit value.[i + 1] && Uri.IsHexDigit value.[i + 2] then
                bytes.Add(Convert.ToByte(value.Substring(i + 1, 2), 16)); i <- i + 3
            else
                flush (); output.Append(value.[i]) |> ignore; i <- i + 1
        flush ()
        Some(output.ToString())
    with :? DecoderFallbackException -> None

let private hasCredentials (netloc: string) =
    let at = netloc.LastIndexOf('@')
    at > 0 // nonempty userinfo; Python's .username/.password would be truthy

/// posixpath helpers (pure string semantics, no filesystem).
let posixDirname (path: string) =
    let i = path.LastIndexOf('/')
    if i < 0 then "" else
    let head = path.Substring(0, i + 1)
    if head <> "" && head.Trim('/') = "" then head else head.TrimEnd('/')

let posixJoin (parts: string list) =
    let mutable result = ""
    for part in parts do
        if part.StartsWith("/", StringComparison.Ordinal) then result <- part
        elif result = "" || result.EndsWith("/", StringComparison.Ordinal) then result <- result + part
        else result <- result + "/" + part
    result

let posixNormpath (path: string) =
    if path = "" then "." else
    let initialSlashes =
        if path.StartsWith("//", StringComparison.Ordinal) && not (path.StartsWith("///", StringComparison.Ordinal)) then 2
        elif path.StartsWith("/", StringComparison.Ordinal) then 1 else 0
    let components = ResizeArray<string>()
    for comp in path.Split('/') do
        if comp = "" || comp = "." then ()
        elif comp <> ".." || (initialSlashes = 0 && components.Count = 0) || (components.Count > 0 && components.[components.Count - 1] = "..") then components.Add(comp)
        elif components.Count > 0 then components.RemoveAt(components.Count - 1)
    let joined = String.Join("/", components)
    let prefix = String('/', initialSlashes)
    let result = prefix + joined
    if result = "" then "." else result

let private localCitation = Regex(@"\A/home/[^/]+/repos/([^/]+)(?:/(.*))?\z", RegexOptions.Compiled)
let private lineSuffix = Regex(@":\d+(?:-\d+)?$", RegexOptions.Compiled)

/// A lexical address, before any corpus lookup. Absolute paths retain their exact
/// spelling; only relative references have POSIX dot-segment normalization.
type Address = { Scheme: string; Netloc: string; Path: string; Alias: string }

let address (source: Snapshot.Node) (target: string) : Address option =
    let parsed = splitUrl (target.Trim([| '<'; '>' |]))
    match unquoteStrict parsed.Path with
    | None -> None
    | Some path when parsed.Query <> "" || hasCredentials parsed.Netloc || path.Contains '\\' || path |> Seq.exists (fun c -> int c < 32) -> None
    | Some path when parsed.Scheme <> "" || parsed.Netloc <> "" ->
        if parsed.Scheme <> "https" && parsed.Scheme <> "http" then None
        else Some { Scheme = parsed.Scheme; Netloc = parsed.Netloc; Path = path; Alias = "" }
    | Some path ->
        let local = localCitation.Match(path)
        if local.Success then
            let rest = lineSuffix.Replace((if local.Groups.[2].Success then local.Groups.[2].Value else ""), "")
            Some { Scheme = ""; Netloc = ""; Path = rest; Alias = local.Groups.[1].Value }
        elif path.StartsWith("/", StringComparison.Ordinal) then None
        else
            let short = source.Repo.Substring(source.Repo.LastIndexOf('/') + 1)
            let joined = posixNormpath (posixJoin [ short; posixDirname source.Path; path ])
            let slash = joined.IndexOf('/')
            let first, rest = if slash >= 0 then joined.Substring(0, slash), joined.Substring(slash + 1) else joined, ""
            Some { Scheme = ""; Netloc = ""; Path = rest; Alias = first }

/// Corpus resolution belongs to these joins. Repository ordinal is the existing
/// ordinal name order; ranking happens after missing destinations are removed.
/// The three input tables are connection-local staging tables owned by Enrich.
let sqlResolve = """
WITH local_paths AS (
    SELECT a.event, r.ordinal, r.repo, r.id AS repo_id, a.path AS target_path
    FROM enrich_address a
    JOIN enrich_repository r ON a."alias" = r."alias"
    WHERE a.scheme = '' AND a.netloc = ''
), remote_candidates AS (
    SELECT a.event, r.ordinal, r.repo, r.id AS repo_id, r."commit", r."ref",
           rtrim(a.path, '/') IN (rtrim(r.path, '/'), rtrim(r.path, '/') || '.git') AS is_root,
           CASE WHEN starts_with(a.path, rtrim(r.path, '/') || '/')
                THEN substr(a.path, length(rtrim(r.path, '/')) + 2)
                ELSE NULL END AS tail
    FROM enrich_address a
    JOIN enrich_repository r ON a.scheme = r.scheme AND a.netloc = r.netloc
    WHERE a.scheme IN ('http', 'https') AND a."alias" = ''
), remote_paths AS (
    SELECT event, ordinal, repo, repo_id,
           CASE
               WHEN is_root THEN ''
               WHEN tail = 'src/commit/' || "commit" THEN ''
               WHEN starts_with(tail, 'src/commit/' || "commit" || '/')
                   THEN substr(tail, length('src/commit/' || "commit" || '/') + 1)
               WHEN starts_with("ref", 'refs/heads/')
                    AND starts_with(tail, 'src/branch/' || substr("ref", 12) || '/')
                   THEN substr(tail, length('src/branch/' || substr("ref", 12) || '/') + 1)
               ELSE NULL
           END AS target_path
    FROM remote_candidates
), paths AS (
    SELECT * FROM local_paths
    UNION ALL
    SELECT * FROM remote_paths WHERE target_path IS NOT NULL
), resolved AS (
    SELECT p.event, p.ordinal,
           CASE WHEN p.target_path = '' THEN p.repo_id ELSE f.id END AS dst,
           p.repo AS target_repo,
           CASE WHEN p.target_path = '' THEN 'repository' ELSE 'file' END AS target_kind,
           p.repo_id
    FROM paths p
    LEFT JOIN enrich_file f ON f.repo = p.repo AND f.path = p.target_path
    WHERE p.target_path = '' OR f.id IS NOT NULL
)
SELECT event, dst, target_repo, target_kind, repo_id
FROM resolved
WHERE dst IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY event ORDER BY ordinal) = 1
"""
