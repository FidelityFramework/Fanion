/// Python fnmatch.fnmatchcase semantics: `*` spans any characters including `/`,
/// `?` one character, `[seq]`/`[!seq]` classes; everything else literal.
module Retrieval.Native.Glob

open System
open System.Collections.Concurrent
open System.Text
open System.Text.RegularExpressions

let private cache = ConcurrentDictionary<string, Regex>()

/// Translate exactly as fnmatch.translate does (a `[` with no closing `]` is literal).
let private translate (pattern: string) =
    let sb = StringBuilder("\\A")
    let mutable i = 0
    let n = pattern.Length
    while i < n do
        let c = pattern.[i]
        i <- i + 1
        match c with
        | '*' -> sb.Append(".*") |> ignore
        | '?' -> sb.Append('.') |> ignore
        | '[' ->
            let mutable j = i
            if j < n && pattern.[j] = '!' then j <- j + 1
            if j < n && pattern.[j] = ']' then j <- j + 1
            while j < n && pattern.[j] <> ']' do j <- j + 1
            if j >= n then sb.Append("\\[") |> ignore
            else
                let mutable body = pattern.Substring(i, j - i).Replace("\\", "\\\\")
                i <- j + 1
                if body.StartsWith("!", StringComparison.Ordinal) then body <- "^" + body.Substring(1)
                elif body.StartsWith("^", StringComparison.Ordinal) then body <- "\\" + body
                sb.Append('[').Append(body).Append(']') |> ignore
        | c -> sb.Append(Regex.Escape(string c)) |> ignore
    sb.Append("\\z").ToString()

let matches (name: string) (pattern: string) =
    let regex = cache.GetOrAdd(pattern, fun p -> Regex(translate p, RegexOptions.Compiled ||| RegexOptions.Singleline ||| RegexOptions.CultureInvariant))
    regex.IsMatch(name)
