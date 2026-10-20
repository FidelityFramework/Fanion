# Validation

Correctness and complete response time are the acceptance measures. The
published source revision, graph snapshot, model profile and query constraints
belong with each result.

## Local gates

The F# suite covers source extraction, policy, deterministic identity,
query constraints, model transport, coordinator recovery, HTTP authentication,
webhooks, freshness and request lifetimes. Engine-backed cases need the pinned
DuckDB library with built-in DuckPGQ.

From this checkout, with artifacts under `.scratch/engine/`:

```bash
mkdir -p .scratch/test-tmp
dotnet build tests/Fanion.Tests/Fanion.Tests.fsproj -nologo
TMPDIR="$PWD/.scratch/test-tmp" \
RETRIEVAL_TEST_EXTENSIONS="$PWD/.scratch/engine/extensions" \
RETRIEVAL_TEST_LIBDUCKDB="$PWD/.scratch/engine/libduckdb.so" \
  tests/Fanion.Tests/bin/Debug/net10.0/linux-x64/Fanion.Tests \
  -noLogo -noColor -maxThreads 2
```

In the shared lab, wrap substantial build/test commands with Bozzetto's
`tools/build/lease.fsx` using `full_build` or `test_suite_run`. Keep source and
dependency checkouts fixed until the gate finishes.

The lab owns hook-registration tests alongside the administrative installer in
`platform/retrieval/integration`. These use mocked I/O and require no secrets.
It separately tests packaging, rollout rollback, MGR hook transport and Pi.

The `src/` and `tests/` reorganization and Fidelity.Data migration passed 152
Fanion tests with two retained external-validator skips. The owning JSON/TOML changes
passed 444 Fidelity.Data tests with one existing ignored case. All fields,
row ordering, metadata and canonical identity of the 23-repository graph match
the retained baseline in three trials.

The first Fidelity.Data integration exposed per-character string allocations.
The fix is in its existing XParsec parser: ordinary text is read in contiguous
runs; escapes and surrogate pairs retain strict validation. On the same 27.8 MB
component, median parsing fell from 2,782 to 215 ms and allocated bytes from
5.01 GB to 270 MB. Removing either the Unicode or raw-control guard fails its
regression test. The restored complete suite passes.
TOML dotted inline tables also retain their implied-table representation through
serialization; that regression failed before the owning serializer fix and
passes afterward.

Three complete generation trials now take 3.697, 3.304 and 2.983 seconds,
with component parsing at 824, 585 and 578 ms. Every stored field and snapshot
identity still matches. In Ubuntu 24.04 userland, the published executable
builds the same graph under the indexer's 2,048 MiB / 128-task limits:
1,596 MiB peak, 59 tasks, and no memory-limit, OOM or task-limit events.
That gate covers generation from 23 retained components, not extraction, the
coordinator's own memory or the configured maximum source budget.

These local candidate gates are distinct from the live-service measurements
below. A complete rebuild under the actual `retrieval-indexer.service` unit
remains a pending capacity gate; the rollout's side-index build does not establish
that result. Broader TOML invalid-input validation and XML rejection
semantics remain separate owning-library work, not conformance claims for this
increment.
The runtime queue acceptance has six lifecycle controls: shared computation
with independent cancellation; distinct requests respecting model capacity;
publication waiting and rebind; physical cancellation drain; bounded retention;
and shutdown joining all consumers. Mutations that duplicated work, skipped
publication waiting or allowed an obsolete result were all detected.

## Live full-response measurement

On October 20, 2026, at 18:19:57 UTC, the SQL cross-linking and Fidelity.Data release
`f936a4cdad287c1babafff0a07c74d074c2bbc2747a6496d765e182292db614d`
completed its supervised rollout. All 24 configured repositories were published
and fresh; loopback schema, find, sources and PGQ acceptance passed. The live
service was then exercised over HTTPS against the loaded Gemma E4B Q4 model on MGR's RTX
3050. Thinking remained off with immediate nested GBNF. Timing includes the
text request, service processing and complete JSON response. The engine is
the pinned `v2.0.0-dev1` source build described in [DuckDB 2.0](DUCKDB-2.md).

| Question about one named Composer file | First measured request | Identical repeat | Four callers sharing a new question |
| --- | ---: | ---: | ---: |
| Outgoing links | 1.017 s | 0.008 s | 1.299–1.309 s |
| Incoming links | 1.487 s | 0.009 s | 1.035–1.044 s |
| Source chunks | 1.292 s | 0.008 s | 1.085–1.094 s |

All 18 responses matched direct snapshot-pinned graph oracles, pagination and
source provenance. The snapshot was
`3a5d6bb56f160fdd1c260c6e308bcfbcf35fa61836393d5cc711062301a6630a`.
The target was `docs/PSG_Nanopass_Architecture.md` in
`FidelityFramework/Composer`. The model was resident and startup priming had
run; these are ordinary runtime requests, not model-loading measurements.

The rollout's recorded complete-generation phases total 4.822 seconds. The
relationship phase accounts for 1.755 seconds, including a 0.407-second SQL join;
neither is the complete indexing duration. Component parsing, hashing,
validation, graph loading and checkpointing make up the rest. Source extraction,
ref observation and service activation are outside those phase timings. The
[phase breakdown](DUCKDB-2.md#indexing-and-cross-linking-measurements) and
[numeric receipt](measurements/2026-10-20.json) retain those boundaries.

The earlier paired-engine rollout on the same date,
`7cf3dfc0673f8bb4f805fb92a2ae32762e8734a72444c0afb6c00f4a0a521848`,
used snapshot `4e0641013249649bfcb1587f25fd62889ba61be5a66a11dff008df39cc97b832`.
Its 18 correct responses took 1.005–1.338 seconds on first measured requests,
8–10 ms on repeats and 1.043–1.268 seconds with four callers. These remain
separate observations, not a controlled speed comparison between releases.

This bounded check establishes those graph operations. Broader research
questions can require two model completions and still need their own semantic
acceptance cases. Grammar-valid JSON alone does not establish relevance.

The earlier paired 2.0 implementation passed 140 native tests, with two retained
external-validator skips. Removing grammar activation, engine source checking,
generation binding, renamed-target verification or the active-file lock guard
each failed its intended test;
the restored full suite passed. The lab packaging and rollback suite passed
91 tests, including the legacy replacement suite discovered at this acceptance
gate. The unchanged hook reconciler retains its 11-test accepted baseline.
The deployed no-thinking profile separately passed 12/12 graph-correct responses
without observed reasoning. Local engine acceptance and live deployment are
distinct gates.

The pinned Ubuntu 24.04 userland also passed the published executable's engine
preflight, 25 query/probe tests and two coordinator tests, including native
directory durability and rejected-target publication. The same native artifact
passed 4,944 upstream SQL assertions in 73 cases in that environment. The full
native suite on Z13 includes the separate-process active-file lock regression.

The 25 query/probe cases also pass inside that Ubuntu userland under the API's
1,536 MiB memory and 128-task limits (186 MiB peak in this run). Connection
initialization bounds both DuckDB thread pools before opening the database.
A control using the earlier executable hit the task ceiling at 80 MiB with no
kernel memory-limit events; the corrected engine-only preflight used 42 MiB
without a task-limit event. This tests resource initialization separately from
corpus size or model inference.
The rollout repeats this engine preflight on the guest under limits read from
the candidate API unit, before it builds the side index or stops live services.

A separate capacity gate ran the published HTTP server with those same limits
against 23 pinned repositories: 16,273 nodes and 29,458 edges. Schema, seed
discovery, source hydration and the three named-file graph operations passed.
A nonempty two-hop shortest-path query returned 819 rows across nine bounded
pages, matching an independent breadth-first traversal of the stored adjacency
rows. The server peaked at 669.7 MiB and 34 tasks, with zero memory-limit, OOM
or task-limit events. This isolated fixture used synthetic observation freshness;
it verifies graph serving capacity, not inference or live-corpus acceptance.

## Context carried into the agent

For the same outgoing-links question on the earlier accepted 1.5.4 release,
a separate measured HTTPS request returned
five oracle-checked relationships in 1.138 seconds and 8,162 UTF-8 bytes at
14:26 UTC on October 20, at snapshot
`7f77abc7662170936bd639dda50a09733015f5d07ed23581e7594438b8759576`.
The request specified a 4,000-character excerpt budget. Those
relationships identify four linked files across two repositories and a repository
resource. Source hydration used 3,000 characters and explicitly omitted one
target's excerpt; all five relationship rows were returned.

A controlled eager full-file reading policy over the seed and those four targets
read 4,128 lines and 292,614 bytes at the same published commits. The bounded
answer was about 36 times smaller. The raw Git I/O took 0.010 seconds, excluding
any agent inference. This establishes a context-volume comparison, not a 36-fold
agent speedup. Within those five files, the seed filename had zero literal
matches and “nanopass” had 45 matching lines. Following recorded relationships
answers a different question from matching words.

A bounded agent-loop replay on ONE used the same resident Qwen3.5-4b Q5 model with two
tool policies. Raw reading selected six search/read calls in each of two trials
(2.46–3.56 seconds, 12,959 tool-result bytes). The graph policy selected one or
two calls (2.60–3.53 seconds, 7,830–15,660 bytes including a repeated retrieval).
All final responses violated the requested object schema. The first graph
response contained all five correct resource tuples as an array; the second
omitted repository identities. The raw responses failed to normalize the
cross-repository link and omitted the repository resource. This is useful
diagnostic evidence for discovery steps, but it does not establish accepted
agent answer quality or a wall-clock speedup. The raw fixture was only the five
graph-located files, not a crawl of all configured repositories.

The [DuckDB 2.0 contract](DUCKDB-2.md) records the engine measurements and
their acceptance boundaries. Current production engine locks remain authoritative.
