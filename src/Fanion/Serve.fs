/// Authenticated retrieval entry point: /health, /v1/retrieval, and the
/// signed Forgejo push-hint endpoint, on Kestrel.
module Fanion.Serve

open System
open System.IO
open System.Security.Cryptography
open System.Text
open Fidelity.Data.JSON
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
      Host: string; Port: int; Model: ModelClient option }

let private fields : (string * (string list * string list)) list =
    [ "remit", ([], []); "schema", ([], []); "find", ([ "query"; "snapshot_id" ], [ "repo"; "limit"; "corpus_layer" ])
      "pgq", ([ "query"; "parameters"; "snapshot_id" ], [ "limit"; "offset" ]); "sources", ([ "ids"; "snapshot_id" ], [ "max_chars" ])
      "prompt", ([ "text" ], [ "snapshot_id" ]); "ask", ([ "text" ], [ "snapshot_id"; "limit"; "max_chars"; "include_context"; "include_query"; "include_results" ]) ]

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
    match action with
    | "remit" -> return ctx.Requests.Remit()
    | "schema" ->
        return (match store.Schema() with Obj pairs -> Obj (pairs @ [ "english", english.Contract() ]) | other -> other)
    | "find" -> return store.Find(requireString payload "query", requireString payload "snapshot_id", optionalString payload "repo", requireInt payload "limit" 10L, optionalString payload "corpus_layer")
    | "pgq" ->
        let parameters =
            match typed payload "parameters" with
            | Some(JsonValue.Object fields) ->
                let pairs = [ for name, value in fields -> name, ofJson value ]
                { Seed = (match pairs |> List.tryFind (fun (k, _) -> k = "seed") with Some(_, v) -> v | None -> Null)
                  EdgeKind = pairs |> List.tryPick (fun (k, v) -> if k = "edge_kind" then Some v else None)
                  Extra = (pairs |> List.exists (fun (k, _) -> k <> "seed" && k <> "edge_kind")) || not (pairs |> List.exists (fun (k, _) -> k = "seed")) }
            | _ -> raise (QueryError "Query parameters do not match the placeholders")
        return store.Pgq(requireString payload "query", parameters, requireString payload "snapshot_id", requireInt payload "limit" 25L, requireInt payload "offset" 0L)
    | "sources" ->
        let ids = match typed payload "ids" with Some(JsonValue.Array values) -> [ for value in values -> (match value with JsonValue.String id -> id | _ -> null) ] | _ -> raise (QueryError "ids")
        return store.Sources(ids, requireString payload "snapshot_id", requireInt payload "max_chars" 8000L)
    | "prompt" -> return english.Prompt(requireString payload "text", optionalString payload "snapshot_id")
    | "ask" ->
        let question : Requests.Question =
            { Text = requireString payload "text"; Snapshot = optionalString payload "snapshot_id"
              Limit = requireInt payload "limit" 8L; MaxChars = requireInt payload "max_chars" 2400L
              IncludeContext = optionalBool payload "include_context" false; IncludeQuery = optionalBool payload "include_query" true; IncludeResults = optionalBool payload "include_results" true }
        return! ctx.Requests.Ask(question, cancellation)
    | _ -> return raise (QueryError "action")
}

let run (options: Options) : int =
    let store = ManagedStore(options.Root, options.Config, options.Engine, options.PromptFile)
    use changes = new IndexState.Changes(Path.Combine(options.Root, "index-state.sqlite"))
    let english = EnglishService(store, options.Model)
    let requestCtx : Requests.Ctx =
        { Readiness = store.ReadinessAsync; WatchReadiness = changes.Watch; Sources = store.ConfiguredSources
          Evaluate = fun q snapshot ct -> english.AskAsync(q.Text, Some snapshot, q.Limit, q.MaxChars, q.IncludeContext, q.IncludeQuery, q.IncludeResults, ct) }
    let requests = new Requests.Service(requestCtx)
    let builder = WebApplication.CreateBuilder()
    builder.WebHost.UseUrls(sprintf "http://%s:%d" options.Host options.Port) |> ignore
    builder.Logging.ClearProviders() |> ignore
    builder.Logging.AddSimpleConsole() |> ignore
    builder.Logging.SetMinimumLevel(LogLevel.Warning) |> ignore
    builder.WebHost.ConfigureKestrel(fun (kestrel: KestrelServerOptions) -> kestrel.AddServerHeader <- false; kestrel.Limits.MaxRequestBodySize <- Nullable 1048576L) |> ignore
    builder.Services.AddRouting().AddOxpecker() |> ignore
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
                                let matches = store.ConfiguredSources |> List.filter (fun s -> String.Equals(s.Repo, name, StringComparison.OrdinalIgnoreCase) && s.Ref = reference)
                                match matches with
                                | [] -> do! json context 202 (Obj [ "accepted", Bool false; "reason", Str "Untracked repository or ref" ])
                                | source :: _ ->
                                    let delivery = let d = header context.Request "x-forgejo-delivery" in if d <> "" then d else header context.Request "x-gitea-delivery"
                                    let result = store.State.EventHint(delivery, source.Id, "push")
                                    do! json context 202 result
                            with
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
                    | EnglishError(code, status, retryable) ->
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

    let routes = [ GET [ route "/health" (health ctx) ]; POST [ route "/v1/retrieval" (retrieval ctx); route "/v1/index/events/forgejo" (index ctx) ] ]
    app.UseRouting().UseOxpecker(routes).Run(Default.notFoundHandler)
    app.Lifetime.ApplicationStopping.Register(fun () -> requests.Close().GetAwaiter().GetResult(); store.Close()) |> ignore
    app.Run()
    0
