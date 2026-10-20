/// Line-preserving text helpers shared by the relationship rules.
/// Semantics mirror the accepted Python pass exactly (see test parity receipt);
/// line numbers are 1-based and refer to the original text.
module Fanion.Text

open System
open System.Text
open System.Text.RegularExpressions

let private fenceMarker = Regex(@"^ {0,3}(`{3,}|~{3,})", RegexOptions.Compiled)

/// Python str.splitlines() semantics for the line terminators that occur in
/// source: \n, \r\n and \r. A trailing terminator does not yield an empty line.
let firstLines (limit: int) (text: string) : string[] =
    if text.Length = 0 then [||] else
    let lines = ResizeArray<string>()
    let sb = StringBuilder()
    let mutable i = 0
    while i < text.Length && lines.Count < limit do
        let c = text.[i]
        if c = '\n' then
            lines.Add(sb.ToString()); sb.Clear() |> ignore; i <- i + 1
        elif c = '\r' then
            lines.Add(sb.ToString()); sb.Clear() |> ignore
            i <- (if i + 1 < text.Length && text.[i + 1] = '\n' then i + 2 else i + 1)
        elif c = '\u000b' || c = '\u000c' || c = '\u001c' || c = '\u001d' || c = '\u001e' || c = '\u0085' || c = '\u2028' || c = '\u2029' then
            lines.Add(sb.ToString()); sb.Clear() |> ignore; i <- i + 1
        else
            sb.Append(c) |> ignore; i <- i + 1
    if lines.Count < limit && sb.Length > 0 then lines.Add(sb.ToString())
    lines.ToArray()

let splitLines text = firstLines Int32.MaxValue text

/// Numbered Markdown lines outside fenced code blocks.
let proseLines (text: string) : (int * string) list =
    let mutable fence : string = null
    let result = ResizeArray<int * string>()
    splitLines text
    |> Array.iteri (fun index line ->
        let marker = fenceMarker.Match(line)
        if marker.Success then
            let value = marker.Groups.[1].Value
            if isNull fence then fence <- value
            elif value.[0] = fence.[0] && value.Length >= fence.Length then fence <- null
        elif isNull fence then
            result.Add((index + 1, line)))
    List.ofSeq result

let private codeTokens = Regex("//[^\\n]*|\\(\\*|\\*\\)|\"\"\"|@\"|\"|\\n", RegexOptions.Compiled)

/// Mask F#/Clef comments and string literals with spaces, preserving offsets
/// and newlines so heuristic citations keep their line numbers.
let codeLines (text: string) : string[] =
    let masked = text.ToCharArray()
    let blank (fromIndex: int) (toIndex: int) =
        for i in fromIndex .. (min toIndex masked.Length) - 1 do
            if masked.[i] <> '\n' then masked.[i] <- ' '
    let mutable depth = 0
    let mutable literal : string = null
    let mutable start = 0
    let mutable offset = 0
    let mutable finished = false
    let startsWith (value: string) (at: int) =
        at + value.Length <= text.Length && String.CompareOrdinal(text, at, value, 0, value.Length) = 0
    while not finished && offset < text.Length do
        if depth > 0 then
            if startsWith "(*" offset then depth <- depth + 1; offset <- offset + 2
            elif startsWith "*)" offset then
                depth <- depth - 1; offset <- offset + 2
                if depth = 0 then blank start offset
            else offset <- offset + 1
        elif not (isNull literal) then
            if literal = "\"" && text.[offset] = '\\' then offset <- offset + 2
            elif literal = "@\"" && startsWith "\"\"" offset then offset <- offset + 2
            elif startsWith (if literal = "@\"" then "\"" else literal) offset then
                offset <- offset + (if literal = "@\"" then 1 else literal.Length)
                blank start offset
                literal <- null
            else offset <- offset + 1
        else
            let m = codeTokens.Match(text, offset)
            if not m.Success then finished <- true
            else
                offset <- m.Index + m.Length
                let value = m.Value
                if value.StartsWith("//", StringComparison.Ordinal) then
                    for i in m.Index .. offset - 1 do masked.[i] <- ' '
                elif value = "(*" then start <- m.Index; depth <- 1
                elif value = "\"" || value = "@\"" || value = "\"\"\"" then start <- m.Index; literal <- value
    if depth > 0 || not (isNull literal) then blank start text.Length
    splitLines (String(masked))

/// 1-based line number of a character offset (Python: text.count("\n", 0, i) + 1).
let lineAt (text: string) (index: int) =
    let mutable count = 0
    for i in 0 .. index - 1 do
        if text.[i] = '\n' then count <- count + 1
    count + 1

/// First 1-based line containing the value, or 0 (Python next(...) default).
let firstLineContaining (text: string) (value: string) =
    let lines = splitLines text
    let mutable found = 0
    let mutable i = 0
    while found = 0 && i < lines.Length do
        if lines.[i].Contains(value, StringComparison.Ordinal) then found <- i + 1
        i <- i + 1
    found
