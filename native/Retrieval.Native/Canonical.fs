/// Python-compatible JSON serialization (json.dumps semantics) over a mixed tree of
/// raw JsonElements and typed values. Output streams to a SHA-256 plus byte counter
/// or to a buffer, so a 125 MB snapshot is never materialized just to hash it.
module Retrieval.Native.Canonical

open System
open System.IO
open System.Security.Cryptography
open System.Text
open System.Text.Json

/// A JSON value: raw input preserved verbatim, or values produced by this toolchain.
type J =
    | Raw of JsonElement
    | Str of string
    | Int of int64
    | Float of float
    | Bool of bool
    | Null
    | Arr of J list
    | Obj of (string * J) list

type Style = { Compact: bool; EnsureAscii: bool; SortKeys: bool }
/// json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)
let canonicalStyle = { Compact = true; EnsureAscii = false; SortKeys = true }
/// json.dumps(value) defaults: ", " and ": " separators, ensure_ascii=True
let defaultStyle = { Compact = false; EnsureAscii = true; SortKeys = true }
/// json.dumps(value, ensure_ascii=False)
let defaultUnicodeStyle = { Compact = false; EnsureAscii = false; SortKeys = true }
/// json.dumps(value, ensure_ascii=False, separators=(",", ":")) with insertion order kept
let orderedCompactStyle = { Compact = true; EnsureAscii = false; SortKeys = false }
/// json.dumps(value, ensure_ascii=False) with insertion order kept (API responses)
let orderedStyle = { Compact = false; EnsureAscii = false; SortKeys = false }

/// Python repr of a float as json.dumps emits it.
let pythonFloat (value: float) =
    if Double.IsNaN value then "NaN"
    elif Double.IsPositiveInfinity value then "Infinity"
    elif Double.IsNegativeInfinity value then "-Infinity"
    else
        let r = value.ToString("R", Globalization.CultureInfo.InvariantCulture)
        let r =
            if r.Contains 'E' then
                let i = r.IndexOf 'E'
                let mantissa, exponent = r.Substring(0, i), int (r.Substring(i + 1))
                sprintf "%se%s%02d" mantissa (if exponent < 0 then "-" else "+") (abs exponent)
            else r
        if r.Contains '.' || r.Contains 'e' then r else r + ".0"

let private hexLower = "0123456789abcdef"

/// Writer over an emit function; all Python escaping rules live here once.
type private Writer(style: Style, emit: ReadOnlyMemory<byte> -> unit) =
    let buffer = new MemoryStream(1 <<< 16)
    let ascii (text: string) =
        for c in text do buffer.WriteByte(byte c)
    let flushIfLarge () =
        if buffer.Length > (1L <<< 20) then
            emit (ReadOnlyMemory(buffer.GetBuffer(), 0, int buffer.Length))
            buffer.SetLength 0L
    member _.Style = style
    member _.Finish() =
        if buffer.Length > 0L then emit (ReadOnlyMemory(buffer.GetBuffer(), 0, int buffer.Length))
        buffer.SetLength 0L
    member _.Separator() = ascii (if style.Compact then "," else ", ")
    member _.Colon() = ascii (if style.Compact then ":" else ": ")
    member _.Ascii(text: string) = ascii text; flushIfLarge ()
    member _.String(value: string) =
        let sb = StringBuilder(value.Length + 8)
        sb.Append('"') |> ignore
        for c in value do
            match c with
            | '"' -> sb.Append("\\\"") |> ignore
            | '\\' -> sb.Append("\\\\") |> ignore
            | '\n' -> sb.Append("\\n") |> ignore
            | '\r' -> sb.Append("\\r") |> ignore
            | '\t' -> sb.Append("\\t") |> ignore
            | '\b' -> sb.Append("\\b") |> ignore
            | '\012' -> sb.Append("\\f") |> ignore
            | c when c < ' ' || (style.EnsureAscii && c > '~') ->
                // Python: \u00xx lowercase; with ensure_ascii every non-ASCII UTF-16 unit escapes (surrogates too).
                sb.Append("\\u").Append(hexLower.[(int c >>> 12) &&& 0xF]).Append(hexLower.[(int c >>> 8) &&& 0xF])
                  .Append(hexLower.[(int c >>> 4) &&& 0xF]).Append(hexLower.[int c &&& 0xF]) |> ignore
            | c -> sb.Append(c) |> ignore
        sb.Append('"') |> ignore
        let text = sb.ToString()
        if style.EnsureAscii then ascii text else buffer.Write(Encoding.UTF8.GetBytes text)
        flushIfLarge ()

let rec private writeRaw (w: Writer) (element: JsonElement) =
    let style = w.Style
    match element.ValueKind with
    | JsonValueKind.Object ->
        let properties = [| for p in element.EnumerateObject() -> p |]
        let properties = if style.SortKeys then properties |> Array.sortWith (fun a b -> String.CompareOrdinal(a.Name, b.Name)) else properties
        w.Ascii "{"
        properties |> Array.iteri (fun i p ->
            if i > 0 then w.Separator()
            w.String p.Name
            w.Colon()
            writeRaw w p.Value)
        w.Ascii "}"
    | JsonValueKind.Array ->
        w.Ascii "["
        let mutable first = true
        for item in element.EnumerateArray() do
            if not first then w.Separator()
            first <- false
            writeRaw w item
        w.Ascii "]"
    | JsonValueKind.String -> w.String(element.GetString())
    | JsonValueKind.Number -> w.Ascii(element.GetRawText()) // written by Python: already repr text
    | JsonValueKind.True -> w.Ascii "true"
    | JsonValueKind.False -> w.Ascii "false"
    | _ -> w.Ascii "null"

let rec private write (w: Writer) (value: J) =
    let style = w.Style
    match value with
    | Raw element -> writeRaw w element
    | Str s -> w.String s
    | Int i -> w.Ascii(i.ToString(Globalization.CultureInfo.InvariantCulture))
    | Float f -> w.Ascii(pythonFloat f)
    | Bool b -> w.Ascii(if b then "true" else "false")
    | Null -> w.Ascii "null"
    | Arr items ->
        w.Ascii "["
        items |> List.iteri (fun i item -> (if i > 0 then w.Separator()); write w item)
        w.Ascii "]"
    | Obj pairs ->
        w.Ascii "{"
        (if style.SortKeys then pairs |> List.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b)) else pairs)
        |> List.iteri (fun i (key, item) ->
            if i > 0 then w.Separator()
            w.String key
            w.Colon()
            write w item)
        w.Ascii "}"

/// Materialize a parsed element as J (integers stay Int when their text has no fraction or exponent).
let rec ofElement (element: JsonElement) : J =
    match element.ValueKind with
    | JsonValueKind.Object -> Obj [ for p in element.EnumerateObject() -> p.Name, ofElement p.Value ]
    | JsonValueKind.Array -> Arr [ for i in element.EnumerateArray() -> ofElement i ]
    | JsonValueKind.String -> Str (element.GetString())
    | JsonValueKind.Number ->
        let raw = element.GetRawText()
        (match element.TryGetInt64() with
         | true, i when not (raw.Contains '.') && not (raw.Contains 'e') && not (raw.Contains 'E') -> Int i
         | _ -> Float (element.GetDouble()))
    | JsonValueKind.True -> Bool true
    | JsonValueKind.False -> Bool false
    | _ -> Null

/// SHA-256 hex and byte length of the serialization, without materializing it.
let digest (style: Style) (value: J) : string * int64 =
    use hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256)
    let mutable total = 0L
    let w = Writer(style, fun chunk -> hash.AppendData(chunk.Span); total <- total + int64 chunk.Length)
    write w value
    w.Finish()
    Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant(), total

/// Serialize to text (small values such as table metadata).
let toText (style: Style) (value: J) : string =
    use collected = new MemoryStream()
    let w = Writer(style, fun chunk -> collected.Write(chunk.Span))
    write w value
    w.Finish()
    Encoding.UTF8.GetString(collected.GetBuffer(), 0, int collected.Length)
