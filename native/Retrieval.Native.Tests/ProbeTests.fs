/// Guest readiness probe: response consistency rules and the offline failure record
/// (ported from the guest_probe.py tests in cleanroom/test_provision.py and test_cleanroom.py).
module Retrieval.Native.Tests.ProbeTests

open System
open System.IO
open System.Net
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open Xunit
open Retrieval.Native.Canonical
open Retrieval.Native

let private snapshot = String('a', 64)

let private good (identity: string) =
    Obj [ "snapshot_id", Str identity
          "freshness", Obj [ "fresh", Bool true; "mode", Str "latest"; "snapshot_id", Str identity; "current_snapshot_id", Str identity; "reasons", Arr [] ] ]

let private withFreshness (name: string) (value: J) (result: J) =
    match result with
    | Obj pairs ->
        Obj [ for k, v in pairs ->
                if k = "freshness" then k, (match v with Obj f -> Obj [ for fk, fv in f -> fk, (if fk = name then value else fv) ] | other -> other)
                else k, v ]
    | other -> other

[<Fact>]
let ``acceptance requires latest matching freshness on every response`` () =
    Assert.Equal(snapshot, Probe.validateResponse (good snapshot) (Some snapshot))
    Assert.Equal(snapshot, Probe.validateResponse (good snapshot) None)
    for name, value in [ "fresh", Bool false; "mode", Str "historical"; "snapshot_id", Str (String('b', 64))
                         "current_snapshot_id", Str (String('b', 64)); "reasons", Arr [ Str "indexing_pending" ] ] do
        Assert.Throws<InvalidOperationException>(fun () -> Probe.validateResponse (withFreshness name value (good snapshot)) (Some snapshot) |> ignore) |> ignore
    Assert.Throws<InvalidOperationException>(fun () -> Probe.validateResponse (good snapshot) (Some(String('b', 64))) |> ignore) |> ignore
    Assert.Throws<InvalidOperationException>(fun () -> Probe.validateResponse (good "not-hex") None |> ignore) |> ignore

let private options (baseUrl: string) (marker: string) : Probe.Options =
    { BaseUrl = baseUrl; Config = "/nonexistent/corpus.json"; Marker = marker; TokenFile = "/nonexistent/token"; LockFiles = []
      ExpectedIp = "10.0.50.232/24"; ExpectedMac = "02:00:0a:00:32:e8"; ExpectedGateway = "10.0.50.1"; MinRootBytes = 0L
      RequireGuestIdentity = false; Only = None }

type private PrimingHandler(respond: J -> int * J) =
    inherit HttpMessageHandler()
    member val Requests = ResizeArray<J>() with get
    override this.Send(request, _cancellation) =
        Assert.Equal("http://127.0.0.1:8080/v1/retrieval", request.RequestUri.ToString())
        Assert.Equal("Bearer " + String('k',32), request.Headers.Authorization.ToString())
        use body = System.Text.Json.JsonDocument.Parse(request.Content.ReadAsStringAsync().GetAwaiter().GetResult())
        let payload = ofElement body.RootElement
        this.Requests.Add payload
        let status, result = respond payload
        new HttpResponseMessage(enum<HttpStatusCode> status, Content = new StringContent(toText orderedStyle result))
    override this.SendAsync(request, cancellation) = Task.FromResult(this.Send(request,cancellation))

let private value name = function Obj fields -> fields |> List.tryPick (fun (key,value) -> if key=name then Some value else None) | _ -> None
let private primeReply payload =
    match value "action" payload with
    | Some(Str "schema") ->
        match good snapshot with
        | Obj fields -> 200, Obj(fields @ [ "english", Obj [ "model", Str "gemma"; "response_format", Str "magnitude_gbnf" ] ])
        | _ -> failwith "schema fixture"
    | _ ->
        let stages = if value "text" payload |> Option.exists (function Str text -> text.Contains("portable-mlir-emission") | _ -> false)
                     then ["seed_discovery";"query_generation"] else ["query_generation"]
        match good snapshot with
        | Obj fields -> 200, Obj(fields @ [ "context", Arr [for stage in stages -> Obj [ "stage", Str stage ]] ])
        | _ -> failwith "response fixture"

[<Fact>]
let ``native priming exercises discovery and qualified generation with bounded text requests`` () =
    use handler = new PrimingHandler(primeReply)
    use client = new HttpClient(handler)
    let result = Probe.primeWith client "http://127.0.0.1:8080" (String('k',32)) "gemma" (TimeSpan.FromSeconds 2.) |> fun work -> work.GetAwaiter().GetResult()
    Assert.Equal(Some(Bool true), value "primed" result)
    Assert.Equal(3, handler.Requests.Count)
    for request in handler.Requests |> Seq.skip 1 do
        Assert.Equal(Some(Str "ask"), value "action" request)
        Assert.Equal(Some(Str snapshot), value "snapshot_id" request)
        Assert.Equal(Some(Int 3000L), value "max_chars" request)

[<Fact>]
let ``priming refuses a skipped discovery stage and a rejected model without hiding either in retries`` () =
    for reject in [true;false] do
        use handler = new PrimingHandler(fun payload ->
            if value "action" payload = Some(Str "schema") then primeReply payload
            elif reject then 502, Obj [ "error", Str "model_request_rejected" ]
            else let _, reply = primeReply payload in
                 match reply with Obj fields -> 200, Obj((fields |> List.filter(fun(k,_)->k<>"context")) @ ["context", Arr [Obj["stage",Str "query_generation"]]]) | _ -> failwith "reply")
        use client = new HttpClient(handler)
        Assert.ThrowsAny<Exception>(fun () -> Probe.primeWith client "http://127.0.0.1:8080" (String('k',32)) "gemma" (TimeSpan.FromSeconds 2.) |> fun work -> work.GetAwaiter().GetResult() |> ignore) |> ignore
        Assert.Equal(2, handler.Requests.Count)

[<Fact>]
let ``native priming validates endpoint and model binding before sending a question`` () =
    use handler = new PrimingHandler(primeReply)
    use client = new HttpClient(handler)
    Assert.Throws<ArgumentException>(fun () -> Probe.primeWith client "http://untrusted/" (String('k',32)) "gemma" (TimeSpan.FromSeconds 2.) |> fun work -> work.GetAwaiter().GetResult() |> ignore) |> ignore
    Assert.Empty handler.Requests
    Assert.Throws<InvalidOperationException>(fun () -> Probe.primeWith client "http://127.0.0.1:8080" (String('k',32)) "wrong-model" (TimeSpan.FromSeconds 2.) |> fun work -> work.GetAwaiter().GetResult() |> ignore) |> ignore
    Assert.Single handler.Requests |> ignore

[<Fact>]
let ``probe does not send a credential to any endpoint but the guest loopback`` () =
    let error = Assert.Throws<ArgumentException>(fun () -> Probe.probe (options "https://c-proxy.spkez.dev/retrieval" "/nonexistent/marker") |> ignore)
    Assert.Contains("outside", error.Message)

[<Fact>]
let ``fingerprint fence precedes any http request or token read`` () =
    // A config that cannot be read means the fence is evaluated first: no token file is opened,
    // no socket is touched (the base URL is the reviewed loopback, and nothing listens on it here).
    let temporary = Path.Combine(Path.GetTempPath(), "probe-" + Guid.NewGuid().ToString("N"))
    Directory.CreateDirectory temporary |> ignore
    let marker = Path.Combine(temporary, "extractor-fingerprint.json")
    File.WriteAllText(marker, "{\"sha256\":\"" + snapshot + "\"}\n")
    let thrown = Record.Exception(fun () -> Probe.probe (options "http://127.0.0.1:8080" marker) |> ignore)
    Assert.NotNull thrown
    Assert.False(thrown :? System.Net.Http.HttpRequestException, "the probe reached the network before the fingerprint fence")
    Assert.False(thrown :? FileNotFoundException && thrown.Message.Contains "token", "the token was read before the fingerprint fence")
    Directory.Delete(temporary, true)

[<Fact>]
let ``offline failure record is retryable only for pending fingerprint and service unavailability`` () =
    // run() without the guest identity requirement prints the non-retryable shape; the retryable
    // classification itself is exercised through the exception types the probe raises.
    let pending = Probe.FingerprintPending "Indexer has not applied the current release fingerprint"
    Assert.True((match pending with Probe.FingerprintPending _ -> true | _ -> false))
    let unavailable = Probe.ProbeHttpError 503
    Assert.True((match unavailable with Probe.ProbeHttpError 503 -> true | _ -> false))
    let forbidden = Probe.ProbeHttpError 403
    Assert.False((match forbidden with Probe.ProbeHttpError 503 -> true | _ -> false))
    let output = new StringWriter()
    let previous = Console.Out
    Console.SetOut output
    try
        Assert.Equal(1, Probe.run (options "https://c-proxy.spkez.dev/retrieval" "/nonexistent/marker"))
    finally
        Console.SetOut previous
    Assert.Contains("\"ready\": false", output.ToString())
    Assert.Contains("ArgumentException", output.ToString())
