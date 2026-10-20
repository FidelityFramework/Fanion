/// Versioned snapshot byte encoding over Fidelity.Data values and graph projections.
/// Output streams to a SHA-256 plus byte counter
/// or to a buffer, so a 125 MB snapshot is never materialized just to hash it.
module Fanion.Canonical

open System
open System.IO
open System.Security.Cryptography
open System.Text
open Fidelity.Data.JSON

exception JsonInputError of string

/// Fanion's strict input boundary. Fidelity.Data owns parsing and JSON values;
/// these selectors reject missing or mistyped fields instead of supplying defaults.
module JsonInput =
    let private required expected = function
        | Some value -> value
        | None -> raise (JsonInputError ("Expected " + expected))
    let rec private rejectDuplicates = function
        | JsonValue.Object fields ->
            if (fields |> List.map fst |> Set.ofList |> Set.count) <> fields.Length then
                raise (JsonInputError "Duplicate JSON member")
            for _, value in fields do rejectDuplicates value
        | JsonValue.Array values -> values |> List.iter rejectDuplicates
        | _ -> ()
    let parse text =
        match Json.parse text with
        | Ok value -> rejectDuplicates value; value
        | Error error -> raise (JsonInputError error)
    let parseBytes (bytes: byte[]) =
        try UTF8Encoding(false, true).GetString bytes |> parse
        with :? DecoderFallbackException -> raise (JsonInputError "Invalid UTF-8")
    let property name value = Json.prop name value |> required ("property " + name)
    let fields = function
        | JsonValue.Object fields -> fields
        | _ -> raise (JsonInputError "Expected object")
    let items = function
        | JsonValue.Array items -> items
        | _ -> raise (JsonInputError "Expected array")
    let string = function
        | JsonValue.Null -> null
        | value -> Json.asString value |> required "string"
    let int64 value = Json.tryAsInt64 value |> required "integer"
    let int32 value =
        let number = int64 value
        if number < -2147483648L || number > 2147483647L then
            raise (JsonInputError "Integer outside Int32 range")
        int number
    let number value = Json.asNumber value |> required "finite number"
    let boolean value = Json.asBool value |> required "boolean"

/// A JSON value: raw input preserved verbatim, or values produced by this toolchain.
type J =
    | Raw of JsonValue
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

let rec private writeRaw (w: Writer) (element: JsonValue) =
    let style = w.Style
    match element with
    | JsonValue.Object pairs ->
        let properties = Array.ofList pairs
        let properties = if style.SortKeys then properties |> Array.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b)) else properties
        w.Ascii "{"
        properties |> Array.iteri (fun i (name, value) ->
            if i > 0 then w.Separator()
            w.String name
            w.Colon()
            writeRaw w value)
        w.Ascii "}"
    | JsonValue.Array items ->
        w.Ascii "["
        let mutable first = true
        for item in items do
            if not first then w.Separator()
            first <- false
            writeRaw w item
        w.Ascii "]"
    | JsonValue.String value -> w.String value
    | JsonValue.NumberLiteral value -> w.Ascii(JsonNumberLiteral.text value)
    | JsonValue.Number value -> w.Ascii(pythonFloat value)
    | JsonValue.Bool true -> w.Ascii "true"
    | JsonValue.Bool false -> w.Ascii "false"
    | JsonValue.Null -> w.Ascii "null"

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
let rec ofJson (element: JsonValue) : J =
    match element with
    | JsonValue.Object pairs -> Obj [ for name, value in pairs -> name, ofJson value ]
    | JsonValue.Array items -> Arr (List.map ofJson items)
    | JsonValue.String value -> Str value
    | JsonValue.NumberLiteral _ ->
        match Json.tryAsInt64 element with
        | Some value -> Int value
        | None -> Raw element
    | JsonValue.Number value -> Float value
    | JsonValue.Bool value -> Bool value
    | JsonValue.Null -> Null

/// SHA-256 hex and byte length of the serialization, without materializing it.
let digest (style: Style) (value: J) : string * int64 =
    use hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256)
    let mutable total = 0L
    let w = Writer(style, fun chunk -> hash.AppendData(chunk.Span); total <- total + int64 chunk.Length)
    write w value
    w.Finish()
    Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant(), total

/// Serialize to text (small values such as table metadata).
let writeTo (stream: Stream) (style: Style) (value: J) =
    let writer = Writer(style, fun chunk -> stream.Write(chunk.Span))
    write writer value
    writer.Finish()

let toText (style: Style) (value: J) : string =
    use collected = new MemoryStream()
    writeTo collected style value
    Encoding.UTF8.GetString(collected.GetBuffer(), 0, int collected.Length)
