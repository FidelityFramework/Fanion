/// HTTP gateway coverage (app.py behaviors from test_store and test_pipeline): Kestrel
/// started in-process on a loopback port over a real published generation.
/// Requires RETRIEVAL_TEST_EXTENSIONS and RETRIEVAL_TEST_LIBDUCKDB.
module Fanion.Tests.ServeTests

open System
open System.Net
open System.Net.Http
open System.Net.Sockets
open System.Security.Cryptography
open System.Text
open System.Threading
open Xunit
open Fanion
open Fanion.Canonical
open Fanion.Query
open Fanion.English
open Fanion.Tests.IndexStateTests
open Fanion.Tests.EnglishTests

let private freePort () =
    let listener = new TcpListener(IPAddress.Loopback, 0)
    listener.Start()
    let port = (listener.LocalEndpoint :?> IPEndPoint).Port
    listener.Stop()
    port

let private read (response: HttpResponseMessage) =
    int response.StatusCode, response.Content.ReadAsStringAsync().GetAwaiter().GetResult()

/// Serve.run blocks on Kestrel, so the gateway runs on a background thread for the
/// lifetime of the test process; each test owns its root, port and token.
type Server(corpus: Corpus, model: ModelClient option, secret: byte[] option) =
    let port = freePort ()
    let token = String('t', 48)
    let options : Fanion.Serve.Options =
        { Root = corpus.Root; Config = corpus.Config; Engine = corpus.Engine; PromptFile = promptFile; Token = token; WebhookSecret = secret
          Host = "127.0.0.1"; Port = port; Model = model }
    let client = new HttpClient(BaseAddress = Uri(sprintf "http://127.0.0.1:%d/" port), Timeout = TimeSpan.FromSeconds 120.0)
    do
        let thread = Thread(ThreadStart(fun () -> try Fanion.Serve.run options |> ignore with _ -> ()))
        thread.IsBackground <- true
        thread.Start()
        let deadline = DateTime.UtcNow.AddSeconds 120.0
        let mutable ready = false
        while not ready do
            try
                use response = client.Send(new HttpRequestMessage(HttpMethod.Get, "health"))
                ready <- true
            with _ ->
                if DateTime.UtcNow > deadline then failwith "gateway did not start"
                Thread.Sleep 100
    member _.Token = token
    member _.Get(path: string) =
        use response = client.Send(new HttpRequestMessage(HttpMethod.Get, path))
        read response
    member _.Post(path: string, body: byte[], headers: (string * string) list) =
        use request = new HttpRequestMessage(HttpMethod.Post, path)
        request.Content <- new ByteArrayContent(body)
        request.Content.Headers.ContentType <- Headers.MediaTypeHeaderValue("application/json")
        for (name, value) in headers do request.Headers.TryAddWithoutValidation(name, value) |> ignore
        use response = client.Send(request)
        read response
    /// POST /v1/retrieval with the bearer token unless anonymous; the body parsed when it is JSON.
    member this.Retrieval(json: string, ?anonymous: bool) =
        let status, body = this.Post("v1/retrieval", Encoding.UTF8.GetBytes json, (if defaultArg anonymous false then [] else [ "Authorization", "Bearer " + token ]))
        status, (try parse body with _ -> Str body)
    member this.Call(value: J) = this.Retrieval(jsonOf value)
    member this.Health() = let status, body = this.Get "health" in status, parse body

/// Build and publish a corpus, then serve it; a no-op when the engine is not configured for this run.
let withServer (repos: (string * (string * string) list) list) (model: ModelClient option) (secret: byte[] option) (body: Corpus -> Server -> unit) =
    match environment () with
    | None -> ()
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(repos, engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        body corpus (Server(corpus, model, secret))

let private snapshotOf (server: Server) =
    let status, schema = server.Call(Obj [ "action", Str "schema" ])
    Assert.Equal(200, status)
    schema, text (field schema "snapshot_id")

/// The evidence ID of a file node by path, discovered through the gateway's own find action.
let private idOfPath (server: Server) (snapshot: string) (relative: string) =
    let status, found = server.Call(Obj [ "action", Str "find"; "query", Str relative; "snapshot_id", Str snapshot; "limit", Int 50L ])
    Assert.Equal(200, status)
    items (field found "results") |> List.pick (fun row -> if text (field row "path") = relative && text (field row "kind") = "file" then Some(text (field row "id")) else None)

[<Fact>]
let ``health is unauthenticated and the authenticated graph round trip works`` () =
    withServer standard None None (fun _ server ->
        Assert.Equal(200, fst (server.Health()))
        Assert.Equal("ready", text (field (snd (server.Health())) "status"))
        Assert.Equal(401, fst (server.Retrieval("""{"action":"schema"}""", anonymous = true)))
        Assert.Equal(400, fst (server.Retrieval """{"action":"schema","path":"/etc/passwd"}"""))
        Assert.Equal(413, fst (server.Retrieval(String('x', 17000))))
        let schema, snapshot = snapshotOf server
        Assert.True(bool' (field schema "freshness") "fresh")
        Assert.False(bool' (field schema "english") "configured")
        let id0, id1 = idOfPath server snapshot "0.md", idOfPath server snapshot "1.md"
        let status, result = server.Call(Obj [ "action", Str "pgq"; "query", Str examples.[0]; "parameters", Obj [ "seed", Str id0 ]; "snapshot_id", Str snapshot ])
        Assert.Equal(200, status)
        Assert.Equal(id1, text (field (items (field result "results")).[0] "id"))
        let status, sources = server.Call(Obj [ "action", Str "sources"; "ids", Arr [ Str id1 ]; "snapshot_id", Str snapshot; "max_chars", Int 400L ])
        Assert.Equal(200, status)
        Assert.Equal("Content 1\nEvidence\n", text (field (items (field sources "sources")).[0] "text"))
        // A stale snapshot is refused rather than answered from another generation.
        Assert.Equal(400, fst (server.Call(Obj [ "action", Str "find"; "query", Str "0.md"; "snapshot_id", Str (String('f', 64)) ]))))

[<Fact>]
let ``unknown actions fields and malformed bodies are rejected`` () =
    withServer standard None None (fun _ server ->
        for body in [ """{"action":"drop"}"""; """{"action":"find"}"""; """{"action":"pgq","query":"x","parameters":{},"snapshot_id":"a","extra":1}"""; "not json"; "[]"; """{"text":"no action"}""" ] do
            let status, parsed = server.Retrieval body
            Assert.Equal(400, status)
            Assert.Equal("Invalid retrieval request; call schema for the current contract", text (field parsed "error"))
        Assert.Equal(404, fst (server.Post("v1/index/events/forgejo", Encoding.UTF8.GetBytes "{}", []))))

[<Fact>]
let ``authenticated remit stays available during indexing and uses configured source policy`` () =
    withServer standard None None (fun corpus server ->
        corpus.Coordinator.State.EventHint("remit-pending", "test", "push") |> ignore
        Assert.Equal(503, fst (server.Health()))
        Assert.Equal(401, fst (server.Retrieval("""{"action":"remit"}""", anonymous = true)))
        let status, remit = server.Call(Obj [ "action", Str "remit" ])
        Assert.Equal(200, status)
        let repositories = items (field remit "repositories")
        Assert.Single(repositories) |> ignore
        Assert.Equal(Str "FidelityFramework/test", field repositories.Head "repo")
        Assert.Equal(Str "refs/heads/main", field repositories.Head "ref")
        Assert.True(has repositories.Head "policy")
        Assert.Equal(Int 240L, field remit "wait_budget_seconds")
        Assert.DoesNotContain(server.Token, jsonOf remit))

[<Fact>]
let ``ask without a model returns the explicit retryable error and prompt remains inspectable`` () =
    withServer standard None None (fun _ server ->
        let status, body = server.Retrieval(sprintf """{"action":"ask","text":%s}""" (toText defaultUnicodeStyle (Str question)))
        Assert.Equal(503, status)
        Assert.Equal("model_not_configured", text (field body "error"))
        Assert.True(bool' body "retryable")
        Assert.Contains("no query was fabricated", text (field body "recourse"))
        Assert.False(has body "results")
        let status, prompt = server.Retrieval(sprintf """{"action":"prompt","text":%s}""" (toText defaultUnicodeStyle (Str question)))
        Assert.Equal(200, status)
        Assert.Equal(1, (items (field prompt "context")).Length)
        Assert.False(bool' (field prompt "english") "configured"))

[<Fact>]
let ``auth unknown fields and budgets are rejected before the model`` () =
    let model = RecordingModel(fixtureSettings, [], None)
    withServer standard (Some(model :> ModelClient)) None (fun _ server ->
        let ask (extra: string) = server.Retrieval(sprintf """{"action":"ask","text":"Which files does 0.md link to?"%s}""" extra)
        Assert.Equal(401, fst (server.Retrieval("""{"action":"ask","text":"test"}""", anonymous = true)))
        Assert.Equal(400, fst (ask ""","endpoint":"http://attacker.invalid" """))
        for arguments in [ ""","limit":21"""; ""","max_chars":4001"""; ""","include_context":"yes" """; ""","limit":"8" """ ] do
            Assert.Equal(400, fst (ask arguments))
        Assert.Equal(400, fst (server.Retrieval(sprintf """{"action":"ask","text":"%s"}""" (String('x', 2001)))))
        Assert.Empty(model.Calls)
        let status, schema = server.Call(Obj [ "action", Str "schema" ])
        Assert.Equal(200, status)
        Assert.Equal("fixture-model", text (path schema [ "english"; "model" ]))
        Assert.DoesNotContain(fixtureSettings.BaseUrl, jsonOf schema)
        Assert.DoesNotContain(server.Token, jsonOf schema))

[<Fact>]
let ``ask through the gateway runs the scripted model over the real graph`` () =
    let model = RecordingModel(fixtureSettings, [], None)
    withServer standard (Some(model :> ModelClient)) None (fun _ server ->
        let _, snapshot = snapshotOf server
        let id0, id1 = idOfPath server snapshot "0.md", idOfPath server snapshot "1.md"
        model.Responses.AddRange(scripted [ plan; generatedFor id0 ])
        let status, body = server.Call(Obj [ "action", Str "ask"; "text", Str question; "include_context", Bool true ])
        Assert.Equal(200, status)
        Assert.Equal(examples.[0], text (field body "query"))
        Assert.Equal(id1, text (field (items (field body "results")).[0] "id"))
        Assert.Equal("Content 1\nEvidence\n", text (field (items (field body "sources")).[0] "text"))
        Assert.Equal(2, (items (field body "context")).Length)
        Assert.Equal(2, model.Calls.Count)
        Assert.True(bool' (field body "freshness") "fresh")
        // A snapshot_id from another generation is refused, retryable, before any inference.
        let status, stale = server.Call(Obj [ "action", Str "ask"; "text", Str question; "snapshot_id", Str (String('f', 64)) ])
        Assert.Equal(409, status)
        Assert.Equal("snapshot_changed", text (field stale "error"))
        Assert.True(bool' stale "retryable")
        Assert.Equal(2, model.Calls.Count))

[<Fact>]
let ``signed forgejo push hints require a valid hmac and are deduplicated`` () =
    let secret = Encoding.UTF8.GetBytes(String('s', 40))
    withServer standard None (Some secret) (fun corpus server ->
        let payload = Obj [ "repository", Obj [ "full_name", Str "FidelityFramework/test" ]; "ref", Str "refs/heads/main"; "after", Str (String('f', 40)) ]
        let sign (raw: byte[]) = Convert.ToHexString(HMACSHA256.HashData(secret, raw)).ToLowerInvariant()
        let event (body: J) (delivery: string) (overrides: (string * string) list) =
            let raw = Encoding.UTF8.GetBytes(canon body)
            let defaults = [ "X-Forgejo-Signature", sign raw; "X-Forgejo-Event", "push"; "X-Forgejo-Delivery", delivery ]
            let headers = (defaults |> List.filter (fun (name, _) -> not (overrides |> List.exists (fun (o, _) -> o = name)))) @ overrides
            let status, text = server.Post("v1/index/events/forgejo", raw, headers)
            status, parse text
        let withRef reference = Obj [ for (k, v) in pairs payload -> k, (if k = "ref" then Str reference else v) ]
        let withName (name: J) = Obj [ for (k, v) in pairs payload -> k, (if k = "repository" then Obj [ "full_name", name ] else v) ]
        let newRevision = corpus.Commit("test", "2.md", "Content 2 changed\nEvidence\n")
        Assert.Equal(200, fst (server.Health()))
        Assert.Equal(401, fst (event payload "bad-signature" [ "X-Forgejo-Signature", String('0', 64) ]))
        let status, issue = event payload "issue-event" [ "X-Forgejo-Event", "issue" ]
        Assert.Equal(202, status)
        Assert.False(bool' issue "accepted")
        let status, untracked = event (withRef "refs/heads/untracked") "untracked" []
        Assert.Equal(202, status)
        Assert.False(bool' untracked "accepted")
        let status, wrongCase = event (withRef "refs/heads/MAIN") "case-sensitive-branch" []  // Git refs are case-sensitive
        Assert.Equal(202, status)
        Assert.False(bool' wrongCase "accepted")
        let broken = Encoding.UTF8.GetBytes "{\"broken"
        let status, malformed = server.Post("v1/index/events/forgejo", broken, [ "X-Forgejo-Signature", sign broken; "X-Forgejo-Event", "push"; "X-Forgejo-Delivery", "broken" ])
        Assert.Equal(400, status)
        Assert.Equal("Malformed push event", text (field (parse malformed) "error"))
        for (number, name) in List.indexed [ Null; Int 42L; Arr []; Obj [] ] do
            let status, malformed = event (withName name) (sprintf "malformed-repository-%d" number) []
            Assert.Equal(400, status)
            Assert.Equal("Malformed push event", text (field malformed "error"))
        Assert.Empty(corpus.Coordinator.State.PendingPlan().ObservationRequired)
        let status, accepted = event payload "delivery-1" []
        Assert.Equal(202, status)
        Assert.Equal(canon (Obj [ "accepted", Bool true; "duplicate", Bool false ]), canon accepted)
        let status, duplicate = event payload "delivery-1" []
        Assert.Equal(202, status)
        Assert.Equal(canon (Obj [ "accepted", Bool true; "duplicate", Bool true ]), canon duplicate)
        // Forgejo repository lookup is case-insensitive: the alias replays the same delivery.
        let _, alias = event (withName (Str "FidelityFramework/TEST")) "delivery-1" []
        Assert.True(bool' alias "duplicate")
        Assert.Equal<string list>([ "test" ], corpus.Coordinator.State.PendingPlan().ObservationRequired)
        // The hint fences the gateway until the source is observed and rebuilt.
        let status, pending = server.Call(Obj [ "action", Str "schema" ])
        Assert.Equal(503, status)
        Assert.Equal("index_not_current", text (field pending "error"))
        Assert.Contains("test", strings (field pending "freshness") "observation_required")
        Assert.Equal(503, fst (server.Health()))
        Assert.Equal("index_not_current", text (field (snd (server.Health())) "status"))
        Assert.Empty(corpus.Observe())
        Assert.Equal(Some newRevision, corpus.Coordinator.State.PendingPlan().DesiredRevisions.["test"])
        Assert.Equal(503, fst (server.Call(Obj [ "action", Str "schema" ])))
        Assert.True(bool' (corpus.RunOnce()) "published")
        let status, current = server.Call(Obj [ "action", Str "schema" ])
        Assert.Equal(200, status)
        Assert.True(bool' (field current "freshness") "fresh")
        Assert.Equal(200, fst (server.Health()))
        // The republished generation is served: the updated file text is what sources returns now.
        let snapshot = text (field current "snapshot_id")
        let id2 = idOfPath server snapshot "2.md"
        let _, sources = server.Call(Obj [ "action", Str "sources"; "ids", Arr [ Str id2 ]; "snapshot_id", Str snapshot ])
        Assert.Equal("Content 2 changed\nEvidence\n", text (field (items (field sources "sources")).[0] "text")))

// Queue tests exercise the real Incremental host with deterministic I/O fences.
// These are independent of the external graph engine; HTTP/graph tests above bind it.
let private readyAt snapshot fresh = Obj [ "fresh", Bool fresh; "snapshot_id", Str snapshot; "current_snapshot_id", Str snapshot; "mode", Str "latest" ]
let private queuedQuestion text : Requests.Question =
    { Text = text; Snapshot = None; Limit = 8L; MaxChars = 2400L; IncludeContext = false; IncludeQuery = true; IncludeResults = true }
let private completed<'T> () = System.Threading.Tasks.TaskCompletionSource<'T>(System.Threading.Tasks.TaskCreationOptions.RunContinuationsAsynchronously)
let private unchanged = (completed<unit> ()).Task
let private bounded (pending: System.Threading.Tasks.Task<'T>) = pending.WaitAsync(TimeSpan.FromSeconds 10.0)
let private until condition = task {
    use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
    while not (condition ()) do do! System.Threading.Tasks.Task.Delay(1, deadline.Token)
}
let private runQueue ctx action = task {
    let service = new Requests.Service(ctx)
    try do! action service
    finally service.Close().GetAwaiter().GetResult()
}

[<Fact>]
let ``four callers share work and one cancellation cannot cancel peers or repeated answer`` () =
    let entered, release = completed<unit> (), completed<unit> ()
    let mutable calls = 0
    let ctx : Requests.Ctx =
        { Sources = []; Readiness = fun _ -> System.Threading.Tasks.Task.FromResult(readyAt "one" true)
          WatchReadiness = fun () -> unchanged
          Evaluate = fun _ snapshot ct -> task {
              Interlocked.Increment(&calls) |> ignore
              entered.TrySetResult() |> ignore
              do! release.Task.WaitAsync ct
              return Obj [ "snapshot_id", Str snapshot; "answer", Str "cited" ] } }
    runQueue ctx (fun service -> task {
        use canceled = new CancellationTokenSource()
        let question = queuedQuestion "one shared question"
        let first = service.Ask(question, canceled.Token)
        do! bounded entered.Task
        let peers = [ for _ in 1..3 -> service.Ask(question, CancellationToken.None) ]
        do! until (fun () -> service.Consumers = 4)
        canceled.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> first :> System.Threading.Tasks.Task)
        Assert.Equal(1, calls)
        Assert.All(peers, fun p -> Assert.False(p.IsCompleted))
        release.SetResult()
        for peer in peers do
            let! answer = bounded peer
            Assert.Equal(Str "cited", field answer "answer")
        let! repeated = bounded (service.Ask(question, CancellationToken.None))
        Assert.Equal(Str "cited", field repeated "answer")
        Assert.Equal(1, calls)
    })

[<Fact>]
let ``distinct questions queue within one model slot`` () =
    let entered, release = completed<unit> (), completed<unit> ()
    let mutable calls, running, peak = 0, 0, 0
    let ctx : Requests.Ctx =
        { Sources = []; Readiness = fun _ -> System.Threading.Tasks.Task.FromResult(readyAt "one" true)
          WatchReadiness = fun () -> unchanged
          Evaluate = fun question _ ct -> task {
              let active = Interlocked.Increment(&running)
              peak <- max peak active
              let call = Interlocked.Increment(&calls)
              try
                  if call = 1 then entered.SetResult(); do! release.Task.WaitAsync ct
                  return Str question.Text
              finally Interlocked.Decrement(&running) |> ignore } }
    runQueue ctx (fun service -> task {
        let first = service.Ask(queuedQuestion "first", CancellationToken.None)
        do! bounded entered.Task
        let second = service.Ask(queuedQuestion "second", CancellationToken.None)
        do! until (fun () -> (service.Snapshot()).QueuedSteps = 1)
        Assert.Equal(1, calls)
        release.SetResult()
        let! a = bounded first
        let! b = bounded second
        Assert.Equal(Str "first", a); Assert.Equal(Str "second", b)
        Assert.Equal(1, peak)
    })

[<Fact>]
let ``snapshot work waits before admission and an in-flight update rebinds without resubmission`` () =
    let entered, release = completed<unit> (), completed<unit> ()
    let mutable snapshot, fresh, calls = "one", false, 0
    let mutable changed = completed<unit> ()
    let publish current isFresh =
        let previous = changed
        changed <- completed<unit> ()
        snapshot <- current; fresh <- isFresh
        previous.SetResult()
    let ctx : Requests.Ctx =
        { Sources = []; Readiness = fun _ -> System.Threading.Tasks.Task.FromResult(readyAt snapshot fresh)
          WatchReadiness = fun () -> changed.Task
          Evaluate = fun _ bound ct -> task {
              let call = Interlocked.Increment(&calls)
              if call = 1 then entered.SetResult(); do! release.Task.WaitAsync ct
              return Str bound } }
    runQueue ctx (fun service -> task {
        let request = service.Ask(queuedQuestion "held through cross linking", CancellationToken.None)
        Assert.Equal(0, calls)
        Assert.Equal(0, (service.Snapshot()).RunningSteps)
        publish "one" true
        do! bounded entered.Task
        publish "one" false
        release.SetResult()
        do! until (fun () -> (service.Snapshot()).RunningSteps = 0)
        Assert.False(request.IsCompleted)
        publish "two" true
        let! answer = bounded request
        Assert.Equal(Str "two", answer)
        Assert.Equal(2, calls)
    })

[<Fact>]
let ``publication wakes all waiting callers without canceling a shared watch`` () =
    let changed, waiting = completed<unit> (), completed<unit> ()
    let mutable fresh, reads, calls = false, 0, 0
    let ctx : Requests.Ctx =
        { Sources = []; WatchReadiness = fun () -> changed.Task
          Readiness = fun _ ->
              if Interlocked.Increment(&reads) = 4 then waiting.SetResult()
              System.Threading.Tasks.Task.FromResult(readyAt "one" fresh)
          Evaluate = fun _ _ _ ->
              Interlocked.Increment(&calls) |> ignore
              System.Threading.Tasks.Task.FromResult(Str "published") }
    runQueue ctx (fun service -> task {
        use canceled = new CancellationTokenSource()
        let question = queuedQuestion "four waiting readers"
        let abandoned = service.Ask(question, canceled.Token)
        let peers = [ for _ in 1..3 -> service.Ask(question, CancellationToken.None) ]
        do! bounded waiting.Task
        Assert.Equal(0, calls)
        canceled.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> abandoned :> System.Threading.Tasks.Task)
        Assert.False(changed.Task.IsCompleted)
        fresh <- true
        changed.SetResult()
        for peer in peers do
            let! answer = bounded peer
            Assert.Equal(Str "published", answer)
        Assert.Equal(1, calls)
    })

[<Fact>]
let ``publication between watch capture and readiness read is not lost`` () =
    let mutable changed = completed<unit> ()
    let mutable reads = 0
    let ctx : Requests.Ctx =
        { Sources = []; WatchReadiness = fun () -> changed.Task
          Readiness = fun _ ->
              let first = Interlocked.Increment(&reads) = 1
              if first then
                  let previous = changed
                  changed <- completed<unit> ()
                  previous.SetResult()
              // The first read observed old state just before publication.
              System.Threading.Tasks.Task.FromResult(readyAt "new" (not first))
          Evaluate = fun _ snapshot _ -> System.Threading.Tasks.Task.FromResult(Str snapshot) }
    runQueue ctx (fun service -> task {
        let! answer = bounded (service.Ask(queuedQuestion "publication race", CancellationToken.None))
        Assert.Equal(Str "new", answer)
        Assert.Equal(3, reads)
    })

[<Fact>]
let ``failed publication watch fails the request instead of silently waiting`` () =
    let failed = completed<unit> ()
    failed.SetException(System.IO.IOException "publication notification failed")
    let ctx : Requests.Ctx =
        { Sources = []; WatchReadiness = fun () -> failed.Task
          Readiness = fun _ -> System.Threading.Tasks.Task.FromResult(readyAt "one" true)
          Evaluate = fun _ _ _ -> failwith "A failed readiness watch must not start inference" }
    runQueue ctx (fun service -> task {
        let! error = Assert.ThrowsAsync<System.IO.IOException>(fun () -> service.Ask(queuedQuestion "failed watch", CancellationToken.None) :> System.Threading.Tasks.Task)
        Assert.Equal("publication notification failed", error.Message)
    })

[<Fact>]
let ``final cancellation drains I O before next work occupies the model`` () =
    let entered, cleanup, finish = completed<unit> (), completed<unit> (), completed<unit> ()
    let mutable calls = 0
    let ctx : Requests.Ctx =
        { Sources = []; Readiness = fun _ -> System.Threading.Tasks.Task.FromResult(readyAt "one" true)
          WatchReadiness = fun () -> unchanged
          Evaluate = fun _ _ ct -> task {
              let call = Interlocked.Increment(&calls)
              if call = 1 then
                  entered.SetResult()
                  try do! System.Threading.Tasks.Task.Delay(Timeout.Infinite, ct)
                  with :? OperationCanceledException ->
                      cleanup.SetResult()
                      do! finish.Task
              return Str "finished" } }
    runQueue ctx (fun service -> task {
        use cancel = new CancellationTokenSource()
        let abandoned = service.Ask(queuedQuestion "abandoned", cancel.Token)
        do! bounded entered.Task
        cancel.Cancel()
        do! bounded cleanup.Task
        let next = service.Ask(queuedQuestion "next", CancellationToken.None)
        do! until (fun () -> (service.Snapshot()).QueuedSteps = 1)
        Assert.Equal(1, calls)
        finish.SetResult()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> abandoned :> System.Threading.Tasks.Task)
        let! answer = bounded next
        Assert.Equal(Str "finished", answer)
        Assert.Equal(2, calls)
    })

[<Fact>]
let ``bounded epoch rotation retains no work from the previous epoch`` () =
    let mutable calls = 0
    let ctx : Requests.Ctx =
        { Sources = []; Readiness = fun _ -> System.Threading.Tasks.Task.FromResult(readyAt "one" true)
          WatchReadiness = fun () -> unchanged
          Evaluate = fun q _ _ -> System.Threading.Tasks.Task.FromResult(Interlocked.Increment(&calls) |> ignore; Str q.Text) }
    runQueue ctx (fun service -> task {
        for n in 1..140 do
            let! answer = bounded (service.Ask(queuedQuestion (string n), CancellationToken.None))
            Assert.Equal(Str(string n), answer)
        Assert.Equal(140, calls)
        Assert.True((service.Snapshot()).Graph.Works.Length <= 128)
        let! answer = bounded (service.Ask(queuedQuestion "140", CancellationToken.None))
        Assert.Equal(Str "140", answer)
        Assert.Equal(140, calls)
    })

[<Fact>]
let ``last consumer releases all capacity waiters and a canceled waiter does not strand rotation`` () =
    let entered, release = completed<unit> (), completed<unit> ()
    let mutable calls = 0
    let ctx : Requests.Ctx =
        { Sources = []; Readiness = fun _ -> System.Threading.Tasks.Task.FromResult(readyAt "one" true)
          WatchReadiness = fun () -> unchanged
          Evaluate = fun q _ ct -> task {
              Interlocked.Increment(&calls) |> ignore
              if q.Text = "128" then entered.SetResult(); do! release.Task.WaitAsync ct
              return Str q.Text } }
    runQueue ctx (fun service -> task {
        for n in 1..127 do
            let! _ = bounded (service.Ask(queuedQuestion (string n), CancellationToken.None))
            ()
        let held = service.Ask(queuedQuestion "128", CancellationToken.None)
        do! bounded entered.Task
        // Completing a cached request also fences registry admission/release, so
        // the following callers synchronously reach the full epoch's wait.
        let! cached = bounded (service.Ask(queuedQuestion "127", CancellationToken.None))
        Assert.Equal(Str "127", cached)
        use canceled = new CancellationTokenSource()
        let abandoned = service.Ask(queuedQuestion "129", canceled.Token)
        let first = service.Ask(queuedQuestion "130", CancellationToken.None)
        let second = service.Ask(queuedQuestion "131", CancellationToken.None)
        Assert.False(first.IsCompleted); Assert.False(second.IsCompleted)
        Assert.Equal(128, calls)
        canceled.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> abandoned :> System.Threading.Tasks.Task)
        release.SetResult()
        let! _ = bounded held
        let! a = bounded first
        let! b = bounded second
        Assert.Equal(Str "130", a); Assert.Equal(Str "131", b)
        Assert.Equal(130, calls)
        Assert.True((service.Snapshot()).Graph.Works.Length <= 2)
    })

[<Fact>]
let ``readiness waits for a busy graph without requiring a publication event`` () = task {
    match environment () with
    | None -> ()
    | Some(extensions, libduckdb) ->
        use corpus = new Corpus(standard, engineOf extensions libduckdb, extensions)
        corpus.Publish() |> ignore
        let store = ManagedStore(corpus.Root, corpus.Config, corpus.Engine, promptFile)
        let entered, release = completed<unit> (), completed<unit> ()
        let held = System.Threading.Tasks.Task.Run(fun () ->
            store.Call(fun _ -> entered.SetResult(); release.Task.GetAwaiter().GetResult(); Null))
        try
            do! bounded entered.Task
            use canceled = new CancellationTokenSource()
            let abandoned = store.ReadinessAsync(canceled.Token)
            let pending = store.ReadinessAsync(CancellationToken.None)
            Assert.False(pending.IsCompleted)
            canceled.Cancel()
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> abandoned :> System.Threading.Tasks.Task)
            release.SetResult()
            let! ready = bounded pending
            Assert.True(bool' ready "fresh")
            let! _ = bounded held
            ()
        finally
            release.TrySetResult() |> ignore
            held.GetAwaiter().GetResult() |> ignore
            store.Close()
}

[<Fact>]
let ``service shutdown joins observers and cleanup and may be observed twice`` () = task {
    let entered, drained = completed<unit> (), completed<unit> ()
    let ctx : Requests.Ctx =
        { Sources = []; Readiness = fun _ -> System.Threading.Tasks.Task.FromResult(readyAt "one" true)
          WatchReadiness = fun () -> unchanged
          Evaluate = fun _ _ ct -> task {
              entered.SetResult()
              try
                  do! System.Threading.Tasks.Task.Delay(Timeout.Infinite, ct)
                  return Null
              finally drained.SetResult() } }
    let service = new Requests.Service(ctx)
    let pending = service.Ask(queuedQuestion "shutdown", CancellationToken.None)
    do! bounded entered.Task
    do! bounded (service.Close())
    Assert.True(drained.Task.IsCompleted)
    let! failure = Assert.ThrowsAsync<EnglishError>(fun () -> pending :> System.Threading.Tasks.Task)
    Assert.Equal(("retrieval_stopping", 503, false), englishError failure)
    do! bounded (service.Close())
}
