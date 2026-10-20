/// One-for-one port of test_corpus_policy.py against Policy.validatePolicy / Policy.normalize.
/// Python's PolicyError is raised here as Snapshot.RelationshipError with the same messages.
module Fanion.Tests.PolicyTests

open System
open Fidelity.Data.JSON
open Xunit
open Fanion.Canonical
open Fanion.Snapshot
open Fanion.Policy

let private parse (json: string) = JsonInput.parse json
let private normalizeJson (json: string) = normalize (Some(parse json))
/// A JSON string literal for one value (control characters and non-ASCII escaped by the serializer).
let private quote (value: string) = Json.serialize (JsonValue.String value)
let private repeat (text: string) (count: int) = String.replicate count text

let private defaults : Normalized =
    { Role = "component"; Authority = 0L; Capabilities = []; IncludeGenerated = []; IncludePaths = []; ExcludePaths = [] }

/// Re-serialize a normalized policy so it can be normalized again (Python: normalize_policy(result)).
let private serialize (policy: Normalized) =
    toText canonicalStyle
        (Obj [ "role", Str policy.Role; "authority", Int policy.Authority; "capabilities", Arr (List.map Str policy.Capabilities)
               "include_generated", Arr (List.map Str policy.IncludeGenerated); "include_paths", Arr (List.map Str policy.IncludePaths)
               "exclude_paths", Arr (List.map Str policy.ExcludePaths) ])

/// The policy must be rejected (PolicyError); the failing input is named in the assertion message.
let private rejected (json: string) =
    let raised = try normalizeJson json |> ignore; false with RelationshipError _ -> true
    Assert.True(raised, sprintf "expected PolicyError for policy %s" json)

[<Fact>]
let ``defaults partial policy and canonical sets`` () =
    Assert.Equal(defaults, normalize None)
    Assert.Equal(defaults, normalizeJson "{}")
    let result = normalizeJson """{"capabilities": ["ui", "bindings", "ui"], "include_generated": ["src/*.G.FS", "src/*.g.fs", "bindings/[A-Z]*.g.cs"]}"""
    Assert.Equal({ defaults with Capabilities = [ "bindings"; "ui" ]; IncludeGenerated = [ "bindings/[a-z]*.g.cs"; "src/*.g.fs" ] }, result)
    Assert.Equal(result, normalizeJson (serialize result))
    // Normalized policies are immutable values: an earlier result cannot alias the defaults.
    Assert.Equal(defaults, normalize None)

[<Fact>]
let ``invalid policy values are rejected`` () =
    let thirtyThree (item: string) = "[" + String.Join(",", Array.create 33 item) + "]"
    let values =
        [ "false"; "[]"; "\"central\""; """{"extra": 1}"""; """{"role": []}"""; """{"role": "official"}"""
          """{"authority": true}"""; """{"authority": 1.0}"""; """{"authority": 1e0}"""; """{"authority": -1}"""; """{"authority": 101}"""
          """{"authority": 9007199254740993}"""; """{"authority": 1e999}"""
          """{"capabilities": "ui"}"""; """{"capabilities": null}"""; sprintf """{"capabilities": %s}""" (thirtyThree "\"ui\"")
          """{"include_generated": "*"}"""; """{"include_generated": null}"""; sprintf """{"include_generated": %s}""" (thirtyThree "\"*\"") ]
    let paths =
        [ for field in [ "include_paths"; "exclude_paths" ] do
            for value in [ "null"; "\"*\""; thirtyThree "\"*\"" ] do
                yield sprintf """{"%s": %s}""" field value ]
    for value in values @ paths do rejected value

[<Fact>]
let ``capability slug bounds`` () =
    for slug in [ "true"; "null"; quote ""; quote "Fidelity.UI"; quote "native_bindings"; quote "-ui"; quote "ui-"; quote "a--b"; quote "ui/sub"; quote (repeat "a" 65); quote "u\u0000i" ] do
        rejected (sprintf """{"capabilities": [%s]}""" slug)
    Assert.Equal<string list>([ repeat "a" 64 ], (normalizeJson (sprintf """{"capabilities": [%s]}""" (quote (repeat "a" 64)))).Capabilities)

/// Lowercasing can push a glob over the 256-byte bound: U+023A (2 bytes) lowers to U+2C65 (3 bytes)
/// in both Python and .NET. Python's original case used U+0130, whose full case mapping expands only
/// under Python; .NET's simple invariant mapping leaves it unchanged, so that pattern is accepted here.
[<Fact>]
let ``generated glob bounds and syntax`` () =
    let invalid =
        [ "true"; "null"; quote ""; quote "/src/*"; quote "../*"; quote "src/../*"; quote "src/./*"; quote "src//x"; quote "src/"
          quote "src\\*"; quote "src/\u0000*"; quote "src/\u007f*"; quote (repeat "a" 257); quote (repeat "é" 129); quote (repeat "Ⱥ" 86)
          quote "src/[abc"; quote "src/[]"; quote "src/[!]"; quote "src/[z-a]"; quote "src/[a/b]"; quote "src/x]y" ]
    for field in [ "include_generated"; "include_paths"; "exclude_paths" ] do
        for glob in invalid do
            rejected (sprintf """{"%s": [%s]}""" field glob)
    // Invalid Unicode is rejected at the Fidelity.Data input boundary before glob normalization.
    Assert.Throws<JsonInputError>(fun () -> normalizeJson """{"include_generated": ["\ud800"]}""" |> ignore) |> ignore
    for glob in [ "*"; "src/*.g.fs"; "src/**/UI?.generated.cs"; "bindings/[!a-z]*.g.fs"; "src/[[]*.g.fs"; "src/[]]*.g.fs"; repeat "a" 256 ] do
        let result = normalizeJson (sprintf """{"include_generated": [%s]}""" (quote glob))
        Assert.Equal<string list>([ glob.ToLowerInvariant() ], result.IncludeGenerated)
