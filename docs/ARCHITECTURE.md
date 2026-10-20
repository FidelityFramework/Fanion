# Architecture

Fanion has two long-lived processes from the same F# executable: `serve`
handles retrieval and signed push hints; `index` reconciles sources and
publishes generations. They share local durable coordinator state.

| Owner | Responsibility |
| --- | --- |
| `Serve.fs` | Oxpecker/Kestrel HTTP boundary, explicit ctx, authentication and bounded bodies |
| `Requests.fs` | Caller lifetimes, exact question/snapshot identities, admission and retained results |
| Incremental `AsyncMailbox` | One model execution slot, independent demand, cancellation and physical drain |
| Ranvier graph in `Requests.fs` | Pure pull projections of published readiness and configured remit |
| `English.fs` | Seed discovery, constrained model calls and query/result validation |
| `Query.fs` | Pinned graph access, bounded execution and source hydration |
| `IndexState.fs` | Durable hints, observations, dependency plans, build leases and publication fence |
| `Coordinator.fs` | Authoritative Git observation, component reuse, cross-linking and staged generation checks |
| `Extract.fs`, `Enrich.fs`, `Generation.fs` | Committed source extraction, evidenced relationships and immutable graph artifacts |
| `Load.fs` | Native engine connection lifetime, verified engine/extension artifacts and graph construction |

## Request lifetime

An agent submits text and waits for JSON. The service admits up to 64 callers.
Readiness must establish a fresh latest snapshot with matching current identity
before model execution starts. Waiting for indexing consumes the caller's
budget without occupying the model execution slot.

A question's options and snapshot form its work identity. Independent callers
hold independent demands on the same computation. Canceling one consumer
releases only that demand. The last release cancels the owned I/O; the physical
slot remains occupied until cleanup completes. Failed work is not reused.

The result is checked against publication again. An unpinned request that
crossed a source change waits and binds to the next complete snapshot. An
explicitly pinned request preserves its mismatch semantics. A repeated answer
is eligible only while the same snapshot remains fresh.

Retention is bounded to 128 work identities per epoch. Rotation waits for
consumers to leave, joins the old async host and clears its retained state.
Shutdown cancels and joins callers and owned I/O before closing the graph store.
Pending HTTP requests are process-local; durable indexing state survives a
service restart, while callers must treat a broken connection as a failed
request.

## Generation lifetime

Push deliveries are authenticated hints. The configured remote and explicit
ref determine authoritative revisions. Changed components and declared
dependents are extracted; unchanged components are reused. Cross-links are
recomputed over the complete assembled revision set.

The coordinator writes and checks an immutable DuckDB artifact before asking
`IndexState` to publish it. SQLite compares the build lease, desired epoch,
revision set, unresolved hints and base generation in one transaction. A newer
change supersedes the build. The active-generation record is the only published
pointer; a staged file is not evidence of publication.

Queries use one generation throughout discovery, traversal and hydration.
Freshness is checked before and after access. Corpus construction and
cross-linking must finish before waiting text requests proceed.

The engine locks bind the native artifact. Its identity participates in generation
hashing and is checked on build, renamed-file verification and store opening.
`Load.openEngine` activates DuckPGQ independently on every connection. The
[native-engine contract](DUCKDB-2.md) belongs to this same publication path.

## Inference boundary

The configured local model generates a permitted query object, not the answer
prose. Qualified repository/file questions use native literal discovery and one
model completion. Broader questions can require a seed-planning completion
followed by query generation.

The accepted Magnitude request sends `reasoning_effort: "none"` and
`response_format: {"type":"grammar","grammar":...}`. The generated grammar
allows bounded whitespace and the query JSON from the first token. The service
also rejects observed reasoning, validates query meaning and executes only
its restricted graph language.

Pi owns the calling agent's context. Fanion owns retrieval and returns source
evidence. Bozzetto owns build execution and leases. GPU serving and infrastructure
provisioning remain deployment concerns.

## Native hosting direction

Keep pure data, validation and pull-state contracts independent of CLR types.
Ranvier owns pure projections and Incremental owns execution demand; neither
gives a corpus relationship compiler-proof authority. The current .NET host
still uses BCL I/O, HTTP and database adapters. Those adapters remain explicit
replacement boundaries for Clef hosting. Removing `System.` constants from
Incremental's portable core does not by itself port Fanion's host.

The longer-term remit includes [adaptive domain models](https://arxiv.org/abs/2603.18104v6).
Fanion can provide a revision-bound route to producer-owned domain contracts,
program relationships and numerical evidence. Compiler and numerical producers
must establish those facts; retrieval preserves their identity, provenance and
scope. A corpus snapshot alone cannot authorize a model update. Connecting an
accepted model's evidence to warm rotation is a future integration boundary.
The current graph contains source relationships and explicitly graded heuristic
leads, not a compiler type/program hypergraph or a numerical proof system.
