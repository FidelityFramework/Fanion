/// Authenticated retrieval entry point: /health, /v1/retrieval, and the
/// signed Forgejo push-hint endpoint, on Kestrel.
module Fanion.Serve

open System
open System.IO
open System.Security.Cryptography
open System.Text
open Fidelity.Data.JSON
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Server.Kestrel.Core
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Oxpecker
open Fanion.Canonical
open Fanion.Query
open Fanion.English

type Options =
    { Root: string; Config: string; Engine: Load.Engine; PromptFile: string; Token: string; WebhookSecret: byte[] option
      Host: string; Port: int; Model: ModelClient option; Observers: Requests.ObserverOptions; Queue: Requests.QueueOptions }

let private crossingOptional = [ "freshness_mode"; "wait_for_fresh"; "wait_timeout_ms" ]
let private fields : (string * (string list * string list)) list =
    [ "remit", ([], [])
      "schema", ([], crossingOptional)
      "find", ([ "query"; "snapshot_id" ], [ "repo"; "limit"; "corpus_layer" ] @ crossingOptional)
      "pgq", ([ "query"; "parameters"; "snapshot_id" ], [ "limit"; "offset" ] @ crossingOptional)
      "sources", ([ "ids"; "snapshot_id" ], [ "max_chars" ] @ crossingOptional)
      "prompt", ([ "text" ], [ "snapshot_id" ] @ crossingOptional)
      "ask", ([ "text" ], [ "snapshot_id"; "limit"; "max_chars"; "include_context"; "include_query"; "include_results" ] @ crossingOptional) ]

let private fixedTimeEquals (a: string) (b: string) = CryptographicOperations.FixedTimeEquals(ReadOnlySpan(Encoding.UTF8.GetBytes a), ReadOnlySpan(Encoding.UTF8.GetBytes b))

let private readBody (request: HttpRequest) (limit: int) : Task<byte[] option> =
    task {
        use buffer = new MemoryStream()
        let chunk = Array.zeroCreate<byte> 16384
        let mutable over = false
        let mutable reading = true
        while reading && not over do
            let! n = request.Body.ReadAsync(chunk, 0, chunk.Length)
            if n = 0 then reading <- false
            else
                buffer.Write(chunk, 0, n)
                if buffer.Length > int64 limit then over <- true
        return (if over then None else Some(buffer.ToArray()))
    }

let private json (context: HttpContext) (status: int) (value: J) : Task =
    context.Response.StatusCode <- status
    context.Response.ContentType <- "application/json"
    let bytes = Encoding.UTF8.GetBytes(toText orderedStyle value)
    context.Response.Body.WriteAsync(bytes, 0, bytes.Length)

let private header (request: HttpRequest) (name: string) = match request.Headers.TryGetValue name with | true, v -> string v | _ -> ""

let private typed (payload: JsonValue) (name: string) = Json.prop name payload
let private requireInt (payload: JsonValue) (name: string) (fallback: int64) =
    match typed payload name with
    | None -> fallback
    | Some v -> match Json.tryAsInt64 v with Some value -> value | None -> raise (QueryError "integer")
let private optionalString (payload: JsonValue) (name: string) =
    match typed payload name with
    | None | Some JsonValue.Null -> None
    | Some(JsonValue.String value) -> Some value
    | _ -> raise (QueryError "string")
let private requireString (payload: JsonValue) (name: string) = match typed payload name with Some(JsonValue.String value) -> value | _ -> raise (QueryError "string")
let private optionalBool (payload: JsonValue) (name: string) (fallback: bool) =
    match typed payload name with
    | None -> fallback
    | Some(JsonValue.Bool value) -> value
    | _ -> raise (QueryError "bool")

type Ctx = { Options: Options; Store: ManagedStore; English: EnglishService; Requests: Requests.Service; Logger: ILogger }

let private dispatch (ctx: Ctx) (payload: JsonValue) (cancellation: System.Threading.CancellationToken) : Task<J> = task {
    let store, english = ctx.Store, ctx.English
    let action = match typed payload "action" with Some(JsonValue.String value) -> value | _ -> raise (QueryError "action must be schema, find, pgq, sources, prompt or ask")
    let required, optional = match fields |> List.tryFind (fun (a, _) -> a = action) with Some(_, f) -> f | None -> raise (QueryError "action must be schema, find, pgq, sources, prompt or ask")
    let present = [ for name, _ in JsonInput.fields payload do if name <> "action" then yield name ] |> set
    if not (Set.isSubset (set required) present) || not (Set.isSubset present (set (required @ optional))) then raise (QueryError "Request fields do not match the action contract")
    let freshnessMode =
        match typed payload "freshness_mode" with
        | None -> "latest"
        | Some(JsonValue.String "latest") -> "latest"
        | Some(JsonValue.String "pinned") -> "pinned"
        | Some _ -> raise (QueryError "freshness_mode must be latest or pinned")
    let waitForFresh =
        match typed payload "wait_for_fresh" with
        | None -> (freshnessMode <> "pinned")
        | Some(JsonValue.Bool value) -> value
        | Some _ -> raise (QueryError "wait_for_fresh must be a boolean")
    let waitTimeout =
        match typed payload "wait_timeout_ms" with
        | None -> None
        | Some value ->
            match Json.tryAsInt64 value with
            | Some ms when ms > 0L ->
                let clampedMs = min (float ms) ctx.Requests.QueueOptions.MaxWaitTimeout.TotalMilliseconds
                Some(TimeSpan.FromMilliseconds clampedMs)
            | _ -> raise (QueryError "wait_timeout_ms must be a positive integer")
    if freshnessMode = "pinned" then
        if typed payload "wait_for_fresh" = Some(JsonValue.Bool true) then
            raise (QueryError "wait_for_fresh cannot be combined with freshness_mode pinned")
        if action = "schema" || action = "ask" then
            raise (QueryError "freshness_mode pinned is only supported for find, pgq, sources and prompt")
    let runLatest (expectedSnapshot: string option) (readAsync: CancellationToken -> Task<J>) (readSync: unit -> J) =
        if waitForFresh then ctx.Requests.ExecuteReadAsync(waitTimeout, expectedSnapshot, readAsync, cancellation)
        else Task.FromResult(readSync ())
    let runRead (expectedSnapshot: string option) (latestAsync: CancellationToken -> Task<J>) (latestSync: unit -> J) (pinnedAsync: CancellationToken -> CancellationToken -> Task<J>) =
        if freshnessMode = "pinned" then ctx.Requests.ExecutePinnedWithBudgetAsync(waitTimeout, pinnedAsync, cancellation)
        else runLatest expectedSnapshot latestAsync latestSync
    if action = "schema" || action = "find" || action = "pgq" || action = "sources" || action = "prompt" || action = "ask" then
        store.VerifyManifestIdentity()
    match action with
    | "remit" -> return ctx.Requests.Remit()
    | "schema" ->
        let withEnglish result =
            match result with Obj pairs -> Obj (pairs @ [ "english", english.Contract() ]) | other -> other
        return! runLatest
            None
            (fun ct -> task { let! s = store.SchemaAsync ct in return withEnglish s })
            (fun () -> withEnglish (store.Schema()))
    | "find" ->
        let query = requireString payload "query"
        let snapshotId = requireString payload "snapshot_id"
        let repo = optionalString payload "repo"
        let limit = requireInt payload "limit" 10L
        let layer = optionalString payload "corpus_layer"
        if isNull query || query.Trim().Length < 1 || query.Trim().Length > 256 || limit < 1L || limit > 50L then
            raise (QueryError "find needs a 1–256 character query and limit 1–50")
        return! runRead
            (Some snapshotId)
            (fun ct -> store.FindAsync(query, snapshotId, repo, limit, layer, ct))
            (fun () -> store.Find(query, snapshotId, repo, limit, layer))
            (fun gateCt execCt -> store.FindPinnedAsync(query, snapshotId, repo, limit, layer, execCt, gateCancellation = gateCt))
    | "pgq" ->
        let parameters =
            match typed payload "parameters" with
            | Some(JsonValue.Object fields) ->
                let pairs = [ for name, value in fields -> name, ofJson value ]
                { Seed = (match pairs |> List.tryFind (fun (k, _) -> k = "seed") with Some(_, v) -> v | None -> Null)
                  EdgeKind = pairs |> List.tryPick (fun (k, v) -> if k = "edge_kind" then Some v else None)
                  Extra = (pairs |> List.exists (fun (k, _) -> k <> "seed" && k <> "edge_kind")) || not (pairs |> List.exists (fun (k, _) -> k = "seed")) }
            | _ -> raise (QueryError "Query parameters do not match the placeholders")
        let query = requireString payload "query"
        let snapshotId = requireString payload "snapshot_id"
        let limit = requireInt payload "limit" 25L
        let offset = requireInt payload "offset" 0L
        compilePgq query parameters limit offset None |> ignore
        return! runRead
            (Some snapshotId)
            (fun ct -> store.PgqAsync(query, parameters, snapshotId, limit, offset, ct))
            (fun () -> store.Pgq(query, parameters, snapshotId, limit, offset))
            (fun gateCt execCt -> store.PgqPinnedAsync(query, parameters, snapshotId, limit, offset, execCt, gateCancellation = gateCt))
    | "sources" ->
        let ids = match typed payload "ids" with Some(JsonValue.Array values) -> [ for value in values -> (match value with JsonValue.String id -> id | _ -> null) ] | _ -> raise (QueryError "ids")
        let snapshotId = requireString payload "snapshot_id"
        let maxChars = requireInt payload "max_chars" 8000L
        if ids.Length < 1 || ids.Length > 10 || ids |> List.exists isNull || maxChars < 1L || maxChars > 16000L then
            raise (QueryError "sources requires 1–10 evidence IDs and max_chars 1–16000")
        return! runRead
            (Some snapshotId)
            (fun ct -> store.SourcesAsync(ids, snapshotId, maxChars, ct))
            (fun () -> store.Sources(ids, snapshotId, maxChars))
            (fun gateCt execCt -> store.SourcesPinnedAsync(ids, snapshotId, maxChars, execCt, gateCancellation = gateCt))
    | "prompt" ->
        let text = requireString payload "text"
        let snapshotId = optionalString payload "snapshot_id"
        if isNull text || text.Trim().Length < 1 || text.Trim().Length > 2000 then
            raise (QueryError "text must contain 1-2000 characters")
        match freshnessMode, snapshotId with
        | "pinned", None -> return raise (QueryError "freshness_mode pinned requires an explicit snapshot_id")
        | "pinned", Some s -> return! ctx.Requests.ExecutePinnedWithBudgetAsync(waitTimeout, (fun gateCt execCt -> english.PromptPinnedAsync(text, s, execCt, gateCancellation = gateCt)), cancellation)
        | _ -> return! runLatest snapshotId (fun ct -> english.PromptAsync(text, snapshotId, ct)) (fun () -> english.Prompt(text, snapshotId))
    | "ask" ->
        let question : Requests.Question =
            { Text = requireString payload "text"; Snapshot = optionalString payload "snapshot_id"
              Limit = requireInt payload "limit" 8L; MaxChars = requireInt payload "max_chars" 2400L
              IncludeContext = optionalBool payload "include_context" false; IncludeQuery = optionalBool payload "include_query" true; IncludeResults = optionalBool payload "include_results" true }
        return! ctx.Requests.Ask(question, waitTimeout, cancellation, waitForFresh = waitForFresh)
    | _ -> return raise (QueryError "action")
}

// Server-sent readiness frames: authenticated, admitted by Requests, framed here.
let observe (token: string) (requests: Requests.Service) (logger: ILogger) (context: HttpContext) : Task =
    task {
        let authorized = fixedTimeEquals (header context.Request "authorization") ("Bearer " + token)
        if not authorized then do! json context 401 (Obj [ "error", Str "Unauthorized" ])
        else
            let response = context.Response
            let write (text: string) (ct: System.Threading.CancellationToken) : Task = task {
                ct.ThrowIfCancellationRequested()
                if not response.HasStarted then
                    response.StatusCode <- 200
                    response.ContentType <- "text/event-stream; charset=utf-8"
                    response.Headers.CacheControl <- "no-cache, no-store"
                    response.Headers.["X-Accel-Buffering"] <- "no"
                let bytes = Encoding.UTF8.GetBytes text
                do! response.Body.WriteAsync(bytes, 0, bytes.Length, ct)
                do! response.Body.FlushAsync ct
            }
            let sink : Requests.ObserverSink =
                { Emit = fun ready ct -> write ("event: readiness\ndata: " + toText orderedCompactStyle (Requests.readinessJ ready) + "\n\n") ct
                  Keepalive = fun ct -> write ": keepalive\n\n" ct }
            try do! requests.Observe(sink, context.RequestAborted)
            with
            | EnglishError(code, status, retryable) when not response.HasStarted ->
                do! json context status (Obj [ "error", Str code; "retryable", Bool retryable ])
            | _ when context.RequestAborted.IsCancellationRequested -> ()
            | EnglishError("observer_write_timeout", _, _) -> context.Abort()
            | error when not response.HasStarted ->
                logger.LogError(error, "Retrieval observation failed")
                do! json context 503 (Obj [ "error", Str "retrieval_unavailable"; "retryable", Bool true ])
            | error ->
                // Headers are sent: the stream ends after any terminal frame the service could write.
                logger.LogError(error, "Retrieval observation stream ended")
    } :> Task

// The host owns the notification lifetime, including a nonzero stop on failure.
let runWithChangesAsync (createChanges: string -> IndexState.Changes) (configure: WebApplicationBuilder -> unit) (cancellation: CancellationToken) (options: Options) : Task<int> = task {
    let store = ManagedStore(options.Root, options.Config, options.Engine, options.PromptFile)
    use changes = createChanges (Path.Combine(options.Root, "index-state.sqlite"))
    let english = EnglishService(store, options.Model)
    let requestCtx : Requests.Ctx =
        { Readiness = store.ReadinessAsync; WatchReadiness = changes.Watch; Observe = (fun cursor ct -> store.ObserveAsync(cursor, ct)); Sources = store.ConfiguredSources
          WatchFailure = changes.Failure; WhileWatchHealthy = changes.WhileHealthy
          Evaluate = fun q snapshot ct -> english.AskAsync(q.Text, Some snapshot, q.Limit, q.MaxChars, q.IncludeContext, q.IncludeQuery, q.IncludeResults, ct) }
    let requests = new Requests.Service(requestCtx, options.Observers, options.Queue, nextDeadline = store.State.NextCrossingDelay)
    let builder = WebApplication.CreateBuilder()
    builder.WebHost.UseUrls(sprintf "http://%s:%d" options.Host options.Port) |> ignore
    builder.Logging.ClearProviders() |> ignore
    builder.Logging.AddSimpleConsole() |> ignore
    builder.Logging.SetMinimumLevel(LogLevel.Warning) |> ignore
    builder.WebHost.ConfigureKestrel(fun (kestrel: KestrelServerOptions) -> kestrel.AddServerHeader <- false; kestrel.Limits.MaxRequestBodySize <- Nullable 1048576L) |> ignore
    builder.Services.AddRouting().AddOxpecker() |> ignore
    configure builder
    let app = builder.Build()
    let ctx = { Options = options; Store = store; English = english; Requests = requests; Logger = app.Logger }
    let logger = ctx.Logger

    let index (ctx: Ctx) (context: HttpContext) : Task =
        let options, store = ctx.Options, ctx.Store
        task {
            match options.WebhookSecret with
            | None -> do! json context 404 (Obj [ "error", Str "Webhook is not configured" ])
            | Some secret ->
                match! readBody context.Request 524288 with
                | None -> do! json context 413 (Obj [ "error", Str "Event exceeds 512 KiB" ])
                | Some body ->
                    let signature = let s = header context.Request "x-forgejo-signature" in if s <> "" then s else header context.Request "x-gitea-signature"
                    let expected = Convert.ToHexString(HMACSHA256.HashData(secret, body)).ToLowerInvariant()
                    if not (fixedTimeEquals signature expected) then do! json context 401 (Obj [ "error", Str "Unauthorized" ])
                    else
                        let event = let e = header context.Request "x-forgejo-event" in if e <> "" then e else header context.Request "x-gitea-event"
                        if event <> "push" then do! json context 202 (Obj [ "accepted", Bool false; "reason", Str "Only tracked push events are supported" ])
                        else
                            try
                                let document = JsonInput.parseBytes body
                                let name = document |> JsonInput.property "repository" |> JsonInput.property "full_name"
                                let reference = JsonInput.property "ref" document
                                let name, reference =
                                    match name, reference with
                                    | JsonValue.String name, JsonValue.String reference -> name, reference
                                    | _ -> raise (JsonInputError "strings")
                                if not (store.ManifestIdentityMatches()) then
                                    do! json context 409 (Obj [ "accepted", Bool false; "error", Str "corpus_configuration_changed"; "reason", Str IndexState.manifestIdentityErrorMessage ])
                                else
                                    let isTrunk = reference = "refs/heads/main" || reference = "refs/heads/master"
                                    let matches =
                                        if not isTrunk then []
                                        else store.ConfiguredSources |> List.filter (fun s -> String.Equals(s.Repo, name, StringComparison.OrdinalIgnoreCase) && s.Ref = reference)
                                    match matches with
                                    | [] -> do! json context 422 (Obj [ "accepted", Bool false; "error", Str "untracked_repository_or_ref"; "reason", Str "Untracked repository or ref" ])
                                    | source :: _ ->
                                        let delivery = let d = header context.Request "x-forgejo-delivery" in if d <> "" then d else header context.Request "x-gitea-delivery"
                                        let result = store.State.EventHint(delivery, source.Id, "push", expectedManifestIdentity = store.Identity)
                                        do! json context 202 result
                            with
                            | IndexState.StateError msg when msg = IndexState.manifestIdentityErrorMessage ->
                                do! json context 409 (Obj [ "accepted", Bool false; "error", Str "corpus_configuration_changed"; "reason", Str msg ])
                            | JsonInputError _ | :? KeyNotFoundException | :? InvalidOperationException | IndexState.StateError _ ->
                                do! json context 400 (Obj [ "error", Str "Malformed push event" ])
        } :> Task

    let health (ctx: Ctx) (context: HttpContext) : Task =
        let store, logger = ctx.Store, ctx.Logger
        task {
            try
                let ready = store.Readiness()
                let fresh = match ready with Obj pairs -> (pairs |> List.exists (fun (k, v) -> k = "fresh" && v = Bool true)) | _ -> false
                if not fresh then do! json context 503 (Obj [ "status", Str "index_not_current"; "service", Str "speakez-retrieval"; "api_version", Int 1L ])
                else do! json context 200 (Obj [ "status", Str "ready"; "service", Str "speakez-retrieval"; "api_version", Int 1L ])
            with error ->
                logger.LogError(error, "Retrieval readiness failed")
                do! json context 503 (Obj [ "status", Str "unavailable"; "service", Str "speakez-retrieval"; "api_version", Int 1L ])
        } :> Task

    let retrieval (ctx: Ctx) (context: HttpContext) : Task =
        let options, logger = ctx.Options, ctx.Logger
        task {
            let auth = header context.Request "authorization"
            if not (fixedTimeEquals auth ("Bearer " + options.Token)) then do! json context 401 (Obj [ "error", Str "Unauthorized" ])
            else
                match! readBody context.Request 16384 with
                | None -> do! json context 413 (Obj [ "error", Str "Request exceeds 16 KiB" ])
                | Some body ->
                    try
                        let document = (try JsonInput.parseBytes body with JsonInputError _ -> raise (QueryError "Invalid JSON"))
                        match document with JsonValue.Object _ -> () | _ -> raise (QueryError "action must be schema, find, pgq, sources, prompt or ask")
                        let! result = dispatch ctx document context.RequestAborted
                        let encoded = Encoding.UTF8.GetBytes(toText orderedStyle result)
                        if encoded.Length > 65536 then do! json context 413 (Obj [ "error", Str "Response exceeds 64 KiB; reduce limit or max_chars" ])
                        else
                            context.Response.StatusCode <- 200
                            context.Response.ContentType <- "application/json"
                            do! context.Response.Body.WriteAsync(encoded, 0, encoded.Length)
                    with
                    | EnglishError(code, status, retryable)
                    | PinnedSnapshotError(code, status, retryable) ->
                        let recourse = "Report retrieval unavailable for this request. Queueing, indexing and inference failures do not permit repository scraping for covered material; no query was fabricated."
                        do! json context status (Obj [ ("error", Str code); ("retryable", Bool retryable); ("recourse", Str recourse) ])
                    | :? OperationCanceledException when context.RequestAborted.IsCancellationRequested -> ()
                    | QueryError _ | JsonInputError _ | :? ArgumentException | :? FormatException | :? OverflowException ->
                        do! json context 400 (Obj [ "error", Str "Invalid retrieval request; call schema for the current contract" ])
                    | QueryBudgetError _ -> do! json context 503 (Obj [ "error", Str "Graph is busy; retry within the assignment budget" ])
                    | IndexNotCurrent freshness -> do! json context 503 (Obj [ "error", Str "index_not_current"; "freshness", freshness ])
                    | :? DuckDB.NET.Data.DuckDBException as error when error.Message.Contains("INTERRUPT", StringComparison.OrdinalIgnoreCase) ->
                        do! json context 408 (Obj [ "error", Str "Query exceeded the execution budget" ])
                    | :? DuckDB.NET.Data.DuckDBException as error ->
                        logger.LogError(error, "Retrieval query failed")
                        do! json context 503 (Obj [ "error", Str "Graph execution failed" ])
                    | GraphUnavailable _ | Load.LoadError _ | :? IOException | :? InvalidOperationException as error ->
                        logger.LogError(error, "Retrieval generation unavailable")
                        do! json context 503 (Obj [ "error", Str "Graph generation unavailable" ])
        } :> Task

    let configuredById = store.ConfiguredSources |> List.map (fun s -> s.Id, s) |> Map.ofList

    let crossingHold (ctx: Ctx) (context: HttpContext) : Task =
        let options, store = ctx.Options, ctx.Store
        task {
            let auth = header context.Request "authorization"
            if not (fixedTimeEquals auth ("Bearer " + options.Token)) then
                do! json context 401 (Obj [ "error", Str "Unauthorized" ])
            else
                match! readBody context.Request 16384 with
                | None -> do! json context 413 (Obj [ "error", Str "Request exceeds 16 KiB" ])
                | Some body ->
                    try
                        let document = JsonInput.parseBytes body
                        let pairs = JsonInput.fields document
                        let present = pairs |> List.map fst |> set
                        if not (Set.isSubset (set [ "fence_id"; "sources" ]) present)
                           || not (Set.isSubset present (set [ "fence_id"; "sources"; "refs"; "ttl_seconds"; "reason" ])) then
                            do! json context 400 (Obj [ "error", Str "Invalid crossing hold fields" ])
                        else
                            let fenceId =
                                match Json.prop "fence_id" document with
                                | Some(JsonValue.String s) -> s
                                | _ -> raise (QueryError "fence_id must be a string")
                            IndexState.validateIdentifier fenceId "crossing fence_id"
                            let sources =
                                match Json.prop "sources" document with
                                | Some(JsonValue.Array items) when not items.IsEmpty ->
                                    [ for item in items -> match item with JsonValue.String s -> s | _ -> raise (QueryError "sources must be an array of strings") ]
                                | _ -> raise (QueryError "sources must be a non-empty array of strings")
                            if sources.Length <> (sources |> List.distinct).Length then
                                raise (QueryError "sources must not contain duplicates")
                            if not (store.ManifestIdentityMatches()) then
                                do! json context 409 (Obj [ "error", Str "corpus_configuration_changed"; "retryable", Bool false ])
                            else
                            match sources |> List.tryFind (fun s -> not (configuredById.ContainsKey s)) with
                            | Some unmanaged ->
                                do! json context 422 (Obj [ "error", Str "unmanaged_source"; "source", Str unmanaged ])
                            | None ->
                                let requested = set sources
                                let mutable refError : (int * J) option = None
                                match Json.prop "refs" document with
                                | None -> ()
                                | Some(JsonValue.Object refPairs) ->
                                    for (sourceId, refValue) in refPairs do
                                        if refError.IsNone then
                                            let reference =
                                                match refValue with
                                                | JsonValue.String r -> r
                                                | _ -> raise (QueryError "refs entries must map source IDs to strings")
                                            match Map.tryFind sourceId configuredById with
                                            | None ->
                                                refError <- Some(422, Obj [ "error", Str "unmanaged_source"; "source", Str sourceId ])
                                            | Some configured ->
                                                if not (requested.Contains sourceId) then
                                                    raise (QueryError "refs entries must match requested sources")
                                                if reference <> configured.Ref then
                                                    refError <- Some(422, Obj [ "error", Str "invalid_crossing_ref"; "source", Str sourceId; "ref", Str reference ])
                                | Some _ -> raise (QueryError "refs must be an object")
                                match refError with
                                | Some(status, errPayload) -> do! json context status errPayload
                                | None ->
                                    let ttlSeconds =
                                        match Json.prop "ttl_seconds" document with
                                        | None -> IndexState.defaultFenceTtlSeconds
                                        | Some value ->
                                            let ttl = try JsonInput.number value with JsonInputError _ -> raise (QueryError "ttl_seconds must be a finite positive number")
                                            if Double.IsNaN ttl || Double.IsInfinity ttl || ttl <= 0.0 || ttl > IndexState.maxFenceTtlSeconds then
                                                raise (QueryError "ttl_seconds must be in (0, 300]")
                                            ttl
                                    let reason =
                                        match Json.prop "reason" document with
                                        | None -> "multi_repo_crossing"
                                        | Some(JsonValue.String r) when not (String.IsNullOrWhiteSpace r) && r.Length <= 512 -> r
                                        | Some _ -> raise (QueryError "reason must be a 1–512 character string")
                                    let receipt = store.State.HoldCrossing(fenceId, sources, ttlSeconds, reason, expectedManifestIdentity = store.Identity)
                                    do! json context 200 receipt
                    with
                    | IndexState.StateError msg when msg = IndexState.manifestIdentityErrorMessage ->
                        do! json context 409 (Obj [ "error", Str "corpus_configuration_changed"; "retryable", Bool false ])
                    | JsonInputError msg | QueryError msg | IndexState.StateError msg ->
                        do! json context 400 (Obj [ "error", Str msg ])
        } :> Task

    let crossingRelease (ctx: Ctx) (context: HttpContext) : Task =
        let options, store = ctx.Options, ctx.Store
        task {
            let auth = header context.Request "authorization"
            if not (fixedTimeEquals auth ("Bearer " + options.Token)) then
                do! json context 401 (Obj [ "error", Str "Unauthorized" ])
            else
                match! readBody context.Request 16384 with
                | None -> do! json context 413 (Obj [ "error", Str "Request exceeds 16 KiB" ])
                | Some body ->
                    try
                        let document = JsonInput.parseBytes body
                        let pairs = JsonInput.fields document
                        let present = pairs |> List.map fst |> set
                        if not (Set.contains "fence_id" present) || not (Set.isSubset present (set [ "fence_id"; "reason" ])) then
                            do! json context 400 (Obj [ "error", Str "Invalid crossing release fields" ])
                        else
                            let fenceId =
                                match Json.prop "fence_id" document with
                                | Some(JsonValue.String s) -> s
                                | _ -> raise (QueryError "fence_id must be a string")
                            IndexState.validateIdentifier fenceId "crossing fence_id"
                            let reason =
                                match Json.prop "reason" document with
                                | None -> None
                                | Some(JsonValue.String r) when not (String.IsNullOrWhiteSpace r) && r.Length <= 512 -> Some r
                                | Some _ -> raise (QueryError "reason must be a 1–512 character string")
                            try
                                let receipt = store.State.ReleaseCrossing(fenceId, ?reason = reason, expectedManifestIdentity = store.Identity)
                                do! json context 200 receipt
                            with IndexState.StateError "Unknown crossing fence" ->
                                do! json context 404 (Obj [ "error", Str "unknown_fence"; "fence_id", Str fenceId ])
                    with
                    | IndexState.StateError msg when msg = IndexState.manifestIdentityErrorMessage ->
                        do! json context 409 (Obj [ "error", Str "corpus_configuration_changed"; "retryable", Bool false ])
                    | JsonInputError msg | QueryError msg | IndexState.StateError msg ->
                        do! json context 400 (Obj [ "error", Str msg ])
        } :> Task

    let routes =
        [ GET [ route "/health" (health ctx); route "/v1/retrieval/observe" (observe options.Token requests logger) ]
          POST [ route "/v1/retrieval" (retrieval ctx)
                 route "/v1/index/events/forgejo" (index ctx)
                 route "/v1/crossing/hold" (crossingHold ctx)
                 route "/v1/crossing/release" (crossingRelease ctx) ] ]
    app.UseRouting().UseOxpecker(routes).Run(Default.notFoundHandler)
    use stopping = app.Lifetime.ApplicationStopping.Register(Action requests.BeginStop)
    let running = app.RunAsync(cancellation)
    let! _ = Task.WhenAny(running, changes.Failure :> Task)
    let failed = changes.Failure.IsCompletedSuccessfully
    if failed then
        logger.LogCritical(changes.Failure.Result, "Publication notifications failed; stopping for supervised restart")
        app.Lifetime.StopApplication()
    let! ended = task {
        try
            do! running
            return None
        with error -> return Some error
    }
    do! requests.Close()
    do! store.CloseAsync()
    match ended with
    | Some error -> return raise error
    | None -> return if changes.Failure.IsCompletedSuccessfully then 1 else 0
}

let run (options: Options) : int =
    runWithChangesAsync (fun path -> new IndexState.Changes(path)) ignore CancellationToken.None options
    |> Async.AwaitTask
    |> Async.RunSynchronously
