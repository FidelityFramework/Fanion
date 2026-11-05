# English retrieval API

Clients send authenticated JSON to `POST /v1/retrieval` at `https://duckdb-fidelity-pgq.spkez.dev`. VM71 owns the graph and validation; MGR's GPU VM supplies model inference. Pi does not sit between the gateway and the model.

The retrieval service is an F#/.NET process using Oxpecker on ASP.NET Core and Kestrel. Handlers pass an explicit service context. It calls the configured model server over asynchronous HTTP, validates the generated query, executes it through DuckDB.NET against embedded DuckDB/DuckPGQ, and returns JSON. DuckDB has no separate HTTP endpoint in this configuration. Pi's `ask` tool call receives the query, results and source evidence together; Pi does not need to resubmit the generated query.

## Requests and evidence

`remit` returns the configured repositories, refs and indexing policies without requiring a fresh graph. Worker prefill reads this manifest before routing repository work. Covered evidence uses text retrieval; excluded materials, uncommitted changes and repositories outside the corpus can use direct reads. An empty match, pending index or failed request does not establish a coverage exception.

An unpinned `ask` stays pending while the index settles and cross-linking completes. Fidelity.FSharp.Incremental's async mailbox owns one model execution slot. Identical questions, budgets and snapshot bindings share a computation; each caller owns a separate demand. Canceling one caller leaves other consumers intact. Ranvier derives the fresh/latest snapshot guard and remit from their existing owners; it does not schedule inference. Publication rechecks freshness, and a raced update rebinds the same request. An explicitly pinned request retains its original snapshot contract.

The service admits 64 callers and retains at most 128 exact work identities per epoch. Rotation drains the old host before releasing its state. The total wait budget is 240 seconds; the transport allows 270, nginx 280 and the Pi tool 300. These are upper bounds, not expected query durations. Pi keeps one tool call pending and shows a waiting message. No query progress percentage or ETA is needed.

`schema` describes current capabilities and freshness. `find` locates literal seeds, `pgq` executes the supported graph fragment, and `sources` hydrates evidence. Pin all related calls to one fresh snapshot.

`prompt` accepts `text` and optional `snapshot_id`, exposing the exact initial messages and constraint receipts without inference. `ask` accepts the same text plus `limit`, `max_chars`, `include_context`, `include_query` and `include_results`.

```json
{"action":"ask","text":"List outgoing links from docs/PSG_Nanopass_Architecture.md in FidelityFramework/Composer.","limit":3,"max_chars":3000,"include_query":true,"include_results":true}
```

Text is 1–2,000 characters. The graph limit is 1–20 rows; source budget is 4–4,000 shared characters across at most four sources. Query and rows are returned by default; complete context is opt-in. The response includes the executed query and parameters, graph results, pagination, source excerpts, prompt/request hashes and freshness. Inspect actual excerpt spans before citing a source.

With the Magnitude profile, a request naming a file and one configured repository uses native literal discovery and one constrained model completion. A bare filename resolves a unique matching file in that scope; a supplied repository-relative path retains its path constraint. Multiple matching files return `ambiguous_seed_candidates` rather than selecting a directory arbitrarily. Requests without that scope retain bounded model planning. Freshness and source checks apply in both paths.

The index retains a document's declared title. Discovery's exact title/name or
path matches constrain the seeds offered to query generation, so documents that
merely mention the same title cannot displace the named document.

The server validates permitted query syntax, parameter names, seed IDs and scope before graph execution. It checks freshness throughout the request. Errors and stale generations are explicit failures, not successful empty answers. A constrained JSON response still needs semantic checks of seed, direction and relation.

`context` records the stages actually executed. Full context includes messages, request options and grammar bytes; compact context retains their hashes. The complete request hash covers messages and options without credentials. Do not substitute a reconstructed prompt for the request that actually ran.

## Magnitude configuration

The operator-owned `/etc/retrieval/model.env` selects the model connection. Keep it root-owned, mode 0600; the service-readable token is a separate private file. Application upgrades preserve this binding.

| Variable | Magnitude profile |
| --- | --- |
| `RETRIEVAL_MODEL_BASE_URL` | `http://gpu-mgr.spkez.dev:10100/inference/v1` |
| `RETRIEVAL_MODEL` | `gemma-4-e4b-it-qat:gguf:q4` |
| `RETRIEVAL_MODEL_TOKEN_FILE` | Private model bearer-key file |
| `RETRIEVAL_MODEL_ENABLE_THINKING` | `false` |
| `RETRIEVAL_MODEL_RESPONSE_FORMAT` | `magnitude_gbnf` |
| `RETRIEVAL_MODEL_TIMEOUT_SECONDS` | Explicit bounded per-call deadline |
| `RETRIEVAL_MODEL_MAX_TOKENS` | Explicit output budget |

The native Magnitude mode sends `reasoning_effort: "none"`, `cache_prompt: true` and `response_format: {"type":"grammar","grammar":...}`. `Gbnf.fs` generates the request-specific constraint from schema and permitted seeds. No chat-template reasoning override or second wire constraint is added. The model client rejects reported reasoning output.

The accepted bounded check returned 12/12 correct queries with no observed reasoning. This is the deployed configuration; [validation](VALIDATION.md) records production acceptance and complete service response times.

## Runtime acceptance

Keep the model resident, use the fixed prompt and structural constraint, and measure the complete text-to-JSON service request with correctness checks. Report graph results, pagination and source provenance with latency. Do not replace that outcome with token rate or an internal stage timing.

Worker clients cannot override model URLs, credentials or database settings. The model service has no database credentials and does not execute the graph itself. The gateway remains the authority for validation and evidence.
