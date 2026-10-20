/// Exact allowlisted link resolution: never a network or filesystem lookup.
module Retrieval.Native.Resolve

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

type Lookup =
    { Repositories: Map<string, Snapshot.Repository>
      Aliases: Map<string, string>
      RepoNode: string -> Snapshot.Node option
      FileNode: string -> string -> Snapshot.Node option }

let private fileOrRepo (lookup: Lookup) (name: string) (path: string) =
    if path = "" then lookup.RepoNode name else lookup.FileNode name path

/// Resolve a link target written in `source` to an indexed node of this snapshot.
let resolve (lookup: Lookup) (source: Snapshot.Node) (target: string) : Snapshot.Node option =
    let parsed = splitUrl (target.Trim([| '<'; '>' |]))
    match unquoteStrict parsed.Path with
    | None -> None
    | Some path when parsed.Query <> "" || hasCredentials parsed.Netloc || path.Contains '\\' || path |> Seq.exists (fun c -> int c < 32) -> None
    | Some path when parsed.Scheme <> "" || parsed.Netloc <> "" ->
        if parsed.Scheme <> "https" && parsed.Scheme <> "http" then None else
        lookup.Repositories
        |> Map.toSeq
        |> Seq.tryPick (fun (name, meta) ->
            let baseUrl = splitUrl (meta.SourceUrl.TrimEnd('/'))
            let prefix = baseUrl.Path.TrimEnd('/')
            if parsed.Scheme <> baseUrl.Scheme || parsed.Netloc <> baseUrl.Netloc then None
            elif (let p = path.TrimEnd('/') in p = prefix || p = prefix + ".git") then lookup.RepoNode name
            else
                let tail = if path.StartsWith(prefix + "/", StringComparison.Ordinal) then path.Substring(prefix.Length + 1) else ""
                let commitHit =
                    if tail.StartsWith("src/commit/", StringComparison.Ordinal) then
                        let remainder = tail.Substring("src/commit/".Length)
                        let slash = remainder.IndexOf('/')
                        let revision, hasSep, rest = if slash >= 0 then remainder.Substring(0, slash), true, remainder.Substring(slash + 1) else remainder, false, ""
                        if revision = meta.Commit then Some(fileOrRepo lookup name (if hasSep then rest else "")) else None
                    else None
                match commitHit with
                | Some result -> result
                | None ->
                    if meta.Ref.StartsWith("refs/heads/", StringComparison.Ordinal) then
                        let marker = "src/branch/" + meta.Ref.Substring("refs/heads/".Length) + "/"
                        if tail.StartsWith(marker, StringComparison.Ordinal) then fileOrRepo lookup name (tail.Substring(marker.Length)) else None
                    else None)
    | Some path ->
        let local = localCitation.Match(path)
        if local.Success then
            match Map.tryFind local.Groups.[1].Value lookup.Aliases with
            | Some name ->
                let rest = lineSuffix.Replace((if local.Groups.[2].Success then local.Groups.[2].Value else ""), "")
                fileOrRepo lookup name rest
            | None -> None
        elif path.StartsWith("/", StringComparison.Ordinal) then None
        else
            let short = source.Repo.Substring(source.Repo.LastIndexOf('/') + 1)
            let joined = posixNormpath (posixJoin [ short; posixDirname source.Path; path ])
            let slash = joined.IndexOf('/')
            let first, hasSep, rest = if slash >= 0 then joined.Substring(0, slash), true, joined.Substring(slash + 1) else joined, false, ""
            match Map.tryFind first lookup.Aliases with
            | Some name -> fileOrRepo lookup name (if hasSep then rest else "")
            | None -> None
