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
Fanion tests with two retained external-validator skips. The owning TOML changes
passed 432 Fidelity.Data tests with one existing ignored case. All fields,
row ordering, metadata and canonical identity of the 23-repository graph match
the retained baseline in three trials.

This is a source checkpoint. Complete generation currently takes 11.8–15.2 s
after the parser migration; the component phase accounts for 8.2–9.3 s.
An isolated 27.8 MB component allocates about 5 GB while parsing JSON strings.
The existing duplicate-member check takes about 5 ms. Optimize the owning
Fidelity.Data parser and repeat the target memory-capacity gate before rollout.
These results do not change the accepted live-service measurements below.
The runtime queue acceptance has six lifecycle controls: shared computation
with independent cancellation; distinct requests respecting model capacity;
publication waiting and rebind; physical cancellation drain; bounded retention;
and shutdown joining all consumers. Mutations that duplicated work, skipped
publication waiting or allowed an obsolete result were all detected.

## Live full-response measurement

On October 20, 2026, the accepted paired DuckDB/DuckPGQ release
`7cf3dfc0673f8bb4f805fb92a2ae32762e8734a72444c0afb6c00f4a0a521848`
was exercised over HTTPS against the loaded Gemma E4B Q4 model on MGR's RTX
3050. Thinking remained off with immediate nested GBNF. Timing includes the
text request, service processing and complete JSON response. The engine is
the pinned `v2.0.0-dev1` source build described in [DuckDB 2.0](DUCKDB-2.md).

| Question about one named Composer file | First measured request | Identical repeat | Four callers sharing a new question |
| --- | ---: | ---: | ---: |
| Outgoing links | 1.005 s | 0.008 s | 1.259–1.268 s |
| Incoming links | 1.338 s | 0.010 s | 1.064–1.073 s |
| Source chunks | 1.290 s | 0.008 s | 1.043–1.051 s |

All 18 responses matched direct snapshot-pinned graph oracles, pagination and
source provenance. The snapshot was
`4e0641013249649bfcb1587f25fd62889ba61be5a66a11dff008df39cc97b832`.
The target was `docs/PSG_Nanopass_Architecture.md` in
`FidelityFramework/Composer`. The model was resident and startup priming had
run; these are ordinary runtime requests, not model-loading measurements.

This bounded check establishes those graph operations. Broader research
questions can require two model completions and still need their own semantic
acceptance cases. Grammar-valid JSON alone does not establish relevance.

The paired 2.0 implementation passed 140 native tests, with two retained
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
