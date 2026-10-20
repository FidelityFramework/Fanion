/// Guest readiness probe (guest_probe.py): bounded acceptance requests against the
/// loopback gateway; bearer credentials never leave this process.
module Fanion.Probe

open System
open System.Diagnostics
open System.IO
open System.Net.Http
open System.Security.Cryptography
open System.Text
open Fidelity.Data.JSON
open System.Text.RegularExpressions
open Fanion.Canonical

exception FingerprintPending of string
exception ProbeHttpError of int

type Options =
    { BaseUrl: string; Config: string; Marker: string; TokenFile: string; LockFiles: string list
      ExpectedIp: string; ExpectedMac: string; ExpectedGateway: string; MinRootBytes: int64; RequireGuestIdentity: bool
      /// "resources" checks only the allocated NIC, address, route and disk (no HTTP, no fingerprint);
      /// "http" skips the resource check (workstation acceptance against a local gateway).
      Only: string option }

let private hex = Regex(@"\A[0-9a-f]{64}\z", RegexOptions.Compiled)

let private ipJson (args: string list) =
    let info = ProcessStartInfo("ip")
    for a in args do info.ArgumentList.Add a
    info.RedirectStandardOutput <- true; info.UseShellExecute <- false
    use p = Process.Start info
    let out = p.StandardOutput.ReadToEnd()
    if not (p.WaitForExit 5000) || p.ExitCode <> 0 then raise (InvalidOperationException "ip command failed")
    JsonInput.parse out

/// network_state + resources: the allocated NIC, static address, default route and disk.
let resources (options: Options) : J =
    let links = ipJson [ "-j"; "link"; "show"; "dev"; "eth0" ] |> JsonInput.items
    let addresses = ipJson [ "-j"; "-4"; "address"; "show"; "dev"; "eth0" ] |> JsonInput.items
    let routeList = ipJson [ "-j"; "-4"; "route"; "show"; "default" ] |> JsonInput.items
    let str value name = match Json.prop name value with Some(JsonValue.String text) -> text | _ -> ""
    let int' value name =
        match Json.prop name value with
        | Some(JsonValue.Number _ as number) | Some(JsonValue.NumberLiteral _ as number) -> JsonInput.int32 number
        | _ -> -1
    if links.Length <> 1 || str links.[0] "ifname" <> "eth0" || (str links.[0] "address").ToLowerInvariant() <> options.ExpectedMac
       || int' links.[0] "ifindex" <= 0 || addresses.Length > 1
       || (not addresses.IsEmpty && (str addresses.[0] "ifname" <> "eth0" || int' links.[0] "ifindex" <> int' addresses.[0] "ifindex")) then
        raise (InvalidOperationException "Guest NIC/MAC differs from allocation")
    let ipv4 =
        [ for a in addresses do
            for item in (Json.prop "addr_info" a |> Option.map JsonInput.items |> Option.defaultValue []) do
                if str item "family" = "inet" then yield str item "local", int' item "prefixlen" ]
    let expectedPrefix = int (options.ExpectedIp.Split('/').[1])
    if ipv4 <> [ options.ExpectedIp.Split('/').[0], expectedPrefix ] || routeList.Length <> 1 || str routeList.[0] "gateway" <> options.ExpectedGateway || str routeList.[0] "dev" <> "eth0" then
        raise (InvalidOperationException "Guest static IP/default gateway differs")
    let total = DriveInfo("/").TotalSize
    if total < options.MinRootBytes then raise (InvalidOperationException "Guest root filesystem is smaller than the allocated disk")
    Obj [ "ip", Str options.ExpectedIp; "mac", Str options.ExpectedMac; "gateway", Str options.ExpectedGateway; "root_filesystem_bytes", Int total ]

/// The marker written by the indexer must equal this binary's fingerprint of the configured corpus.
let verifyFingerprint (options: Options) =
    let sources, _ = Coordinator.configSources options.Config
    let expected = Coordinator.extractionFingerprint sources options.LockFiles
    let actual =
        if File.Exists options.Marker then
            try
                Some(File.ReadAllBytes options.Marker |> JsonInput.parseBytes |> ofJson)
            with JsonInputError _ -> None
        else None
    if actual <> Some(Obj [ "sha256", Str expected ]) then raise (FingerprintPending "Indexer has not applied the current release fingerprint")
    expected

let private field (value: J) (name: string) = match value with Obj pairs -> pairs |> List.tryPick (fun (k, v) -> if k = name then Some v else None) | _ -> None

let validateResponse (result: J) (snapshot: string option) =
    let identity = match field result "snapshot_id" with Some(Str s) -> s | _ -> ""
    let freshness = match field result "freshness" with Some f -> f | None -> Obj []
    let reasonsEmpty = match field freshness "reasons" with Some(Arr []) -> true | _ -> false
    if not (hex.IsMatch identity) || (snapshot.IsSome && snapshot.Value <> identity) || field freshness "fresh" <> Some(Bool true)
       || field freshness "mode" <> Some(Str "latest") || field freshness "snapshot_id" <> Some(Str identity) || field freshness "current_snapshot_id" <> Some(Str identity) || not reasonsEmpty then
        raise (InvalidOperationException "Acceptance response is not consistent fresh latest evidence")
    identity

let private request (client: HttpClient) (baseUrl: string) (token: string) (cancellation: Threading.CancellationToken) (path: string) (payload: J option) authorized : J =
        use message = new HttpRequestMessage((if payload.IsSome then HttpMethod.Post else HttpMethod.Get), baseUrl + path)
        if authorized then message.Headers.Authorization <- Headers.AuthenticationHeaderValue("Bearer", token)
        payload |> Option.iter (fun p -> message.Content <- new StringContent(toText orderedStyle p, Encoding.UTF8, "application/json"))
        use response = client.Send(message, cancellation)
        let status = int response.StatusCode
        if status >= 300 && status < 400 then raise (InvalidOperationException "Unexpected redirect during retrieval acceptance")
        let raw = response.Content.ReadAsByteArrayAsync(cancellation).GetAwaiter().GetResult()
        if raw.Length > 65536 then raise (InvalidOperationException "Acceptance response exceeds budget")
        if status <> 200 then raise (ProbeHttpError status)
        JsonInput.parseBytes raw |> ofJson

/// Startup acceptance exercises both constrained model stages. The service owns
/// inference and graph execution; credentials and priming stay in this binary.
let primeWith (client: HttpClient) (baseUrl: string) (token: string) (model: string) (timeout: TimeSpan) = task {
    if baseUrl <> "http://127.0.0.1:8080" || token.Length < 32 || timeout <= TimeSpan.Zero then
        raise (ArgumentException "Invalid gateway priming configuration")
    use deadline = new Threading.CancellationTokenSource(timeout)
    let request = request client baseUrl token deadline.Token
    let questions = [ "What relates to portable-mlir-emission in FidelityFramework/Alex?", 2
                      "List outgoing links from docs/PSG_Nanopass_Architecture.md in FidelityFramework/Composer.", 1 ]
    let mutable completed = false
    while not completed do
        try
            let schema = request "/v1/retrieval" (Some(Obj [ "action", Str "schema" ])) true
            let snapshot = validateResponse schema None
            let english = field schema "english" |> Option.defaultValue (Obj [])
            if field english "model" <> Some(Str model) || field english "response_format" <> Some(Str "magnitude_gbnf") then
                raise (InvalidOperationException "Gateway priming model binding differs")
            for question, stages in questions do
                let result = request "/v1/retrieval" (Some(Obj [ "action", Str "ask"; "text", Str question; "snapshot_id", Str snapshot;
                                                              "limit", Int 3L; "max_chars", Int 3000L; "include_context", Bool true ])) true
                validateResponse result (Some snapshot) |> ignore
                let actual = match field result "context" with Some(Arr items) -> [ for item in items -> field item "stage" ] | _ -> []
                let expected = if stages = 1 then [ Some(Str "query_generation") ] else [ Some(Str "seed_discovery"); Some(Str "query_generation") ]
                if actual <> expected then raise (InvalidOperationException "Gateway priming skipped a model stage")
            completed <- true
        with
        | ProbeHttpError code when code = 429 || code = 503 || code = 409 -> do! System.Threading.Tasks.Task.Delay(1000, deadline.Token)
        | :? HttpRequestException -> do! System.Threading.Tasks.Task.Delay(1000, deadline.Token)
    return Obj [ "primed", Bool true; "cases", Int 2L ]
}

let prime baseUrl tokenFile model =
    let token = File.ReadAllText(tokenFile).Trim()
    use handler = new HttpClientHandler(AllowAutoRedirect = false, UseProxy = false)
    use client = new HttpClient(handler, Timeout = TimeSpan.FromSeconds 90.0)
    primeWith client baseUrl token model (TimeSpan.FromSeconds 180.0) |> fun work -> work.GetAwaiter().GetResult()

let probe (options: Options) : J =
    if options.BaseUrl <> "http://127.0.0.1:8080" then raise (ArgumentException "Probe destination is outside the reviewed endpoints")
    let fingerprint = verifyFingerprint options
    let token = File.ReadAllText(options.TokenFile).Trim()
    if token.Length < 32 then raise (InvalidOperationException "Invalid API credential file")
    let actualResources = if options.Only = Some "http" then Obj [ "skipped", Str "only-http" ] else resources options
    use handler = new HttpClientHandler(AllowAutoRedirect = false, UseProxy = false)
    use client = new HttpClient(handler, Timeout = TimeSpan.FromSeconds 8.0)
    let request = request client options.BaseUrl token Threading.CancellationToken.None
    let health = request "/health" None false
    if field health "status" <> Some(Str "ready") || field health "service" <> Some(Str "speakez-retrieval") then raise (InvalidOperationException "API is not ready")
    (try
        request "/v1/retrieval" (Some(Obj [ "action", Str "schema" ])) false |> ignore
        raise (InvalidOperationException "Unauthenticated schema request was accepted")
     with ProbeHttpError 401 -> ())
    let schema = request "/v1/retrieval" (Some(Obj [ "action", Str "schema" ])) true
    let snapshot = validateResponse schema None
    if field schema "graph" <> Some(Str "corpus") || (match field schema "graph_schema" with Some(Arr (_ :: _)) -> false | _ -> true) then raise (InvalidOperationException "First-generation graph schema is invalid")
    if (field schema "limits" |> Option.bind (fun l -> field l "query_threads")) <> Some(Int 8L) then raise (InvalidOperationException "Serving DuckDB connection does not report eight query threads")
    // Live repository names and curation policies must be exactly the configured, normalized ones
    // (upgrade.validate_schema did this with corpus_policy.normalize_policy).
    let configured = [ for s in fst (Coordinator.configSources options.Config) -> s.Repo, toText canonicalStyle (Coordinator.policyJ s.Normalized) ] |> List.sort
    let live =
        match field schema "repositories" with
        | Some(Arr repositories) ->
            [ for r in repositories -> (match field r "name" with Some(Str n) -> n | _ -> ""), (match field r "policy" with Some p -> toText canonicalStyle p | None -> "") ] |> List.sort
        | _ -> []
    if live <> configured then raise (InvalidOperationException "Live repository names or curation policies differ")
    let prompt = match field schema "query_prompt" with Some p -> p | None -> Obj []
    let promptText = match field prompt "text" with Some(Str t) -> t | _ -> ""
    let promptSha = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes promptText)).ToLowerInvariant()
    if promptText = "" || field prompt "sha256" <> Some(Str promptSha) || field prompt "version" <> Some(Str "speakez-pql-v2") then raise (InvalidOperationException "Query prompt identity is invalid")
    let found = request "/v1/retrieval" (Some(Obj [ "action", Str "find"; "query", Str "README"; "snapshot_id", Str snapshot; "limit", Int 1L ])) true
    validateResponse found (Some snapshot) |> ignore
    let evidence = match field found "results" with Some(Arr (first :: _)) -> (match field first "id" with Some(Str i) -> i | _ -> "") | _ -> raise (InvalidOperationException "No committed README evidence in first generation")
    let sources = request "/v1/retrieval" (Some(Obj [ "action", Str "sources"; "ids", Arr [ Str evidence ]; "snapshot_id", Str snapshot; "max_chars", Int 200L ])) true
    validateResponse sources (Some snapshot) |> ignore
    (match field sources "sources" with Some(Arr (first :: _)) when (match field first "source_url" with Some(Str u) -> u <> "" | _ -> false) -> () | _ -> raise (InvalidOperationException "Evidence hydration failed"))
    let example = match field schema "examples" with Some(Arr (_ :: second :: _)) -> second | _ -> raise (InvalidOperationException "Schema examples missing")
    let graph = request "/v1/retrieval" (Some(Obj [ "action", Str "pgq"; "query", example; "parameters", Obj [ "seed", Str evidence; "edge_kind", Str "chunks_of" ]; "snapshot_id", Str snapshot; "limit", Int 1L ])) true
    validateResponse graph (Some snapshot) |> ignore
    (match field graph "results" with Some(Arr (first :: _)) when field first "kind" = Some(Str "chunk") -> () | _ -> raise (InvalidOperationException "Incoming chunks_of PGQ did not return actual chunk evidence"))
    Obj [ "ready", Bool true; "snapshot_id", Str snapshot; "repositories", Int (match field schema "repositories" with Some(Arr r) -> int64 r.Length | _ -> 0L)
          "schema_find_sources_pgq", Str "passed"; "query_threads", Int 8L; "extractor_fingerprint", Str fingerprint; "query_prompt_sha256", Str promptSha
          "unauthenticated", Int 401L; "resources", actualResources; "transport", Str "guest-loopback" ]

/// Entry point: prints the receipt, or a retryable/non-retryable failure record with exit 1.
let run (options: Options) : int =
    if options.Only = Some "resources" then
        try Console.Out.WriteLine(toText orderedStyle (resources options)); 0
        with error -> eprintfn "%s" error.Message; 1
    elif options.Only.IsSome && options.Only <> Some "http" then
        eprintfn "probe --only accepts 'resources' or 'http'"
        64
    elif options.RequireGuestIdentity then
        let hostname = Environment.MachineName.Split('.').[0]
        if Environment.UserName <> "root" || hostname <> "duckdb-fidelity-pgq" then
            eprintfn "Run inside the retrieval guest as root"
            64
        else
            try Console.Out.WriteLine(toText orderedStyle (probe options)); 0
            with error ->
                let httpStatus = match error with ProbeHttpError s -> Some s | _ -> None
                let retryable =
                    match error with
                    | FingerprintPending _ -> true
                    | ProbeHttpError 503 -> true
                    | :? HttpRequestException -> true
                    | :? AggregateException as a when (a.InnerException :? HttpRequestException) -> true
                    | _ -> false
                let retryable =
                    if retryable then
                        let info = ProcessStartInfo("systemctl")
                        for a in [ "is-failed"; "--quiet"; "retrieval-indexer.service"; "retrieval-api.service" ] do info.ArgumentList.Add a
                        info.RedirectStandardOutput <- true; info.UseShellExecute <- false
                        use p = Process.Start info
                        p.WaitForExit 5000 |> ignore
                        p.ExitCode <> 0
                    else false
                let status = match httpStatus with Some s -> Int (int64 s) | None -> Null
                Console.Out.WriteLine(toText orderedStyle (Obj [ "ready", Bool false; "error_type", Str (error.GetType().Name); "retryable", Bool retryable; "http_status", status ]))
                1
    else
        try Console.Out.WriteLine(toText orderedStyle (probe options)); 0
        with error -> Console.Out.WriteLine(toText orderedStyle (Obj [ "ready", Bool false; "error_type", Str (error.GetType().Name) ])); 1
