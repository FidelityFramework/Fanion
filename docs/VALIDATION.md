# Validation

Correctness and complete response time are the acceptance measures. The
published source revision, graph snapshot, model profile and query constraints
belong with each result.

## Local gates

The native suite covers source extraction, policy, deterministic identity,
query constraints, model transport, coordinator recovery, HTTP authentication,
webhooks, freshness and request lifetimes. Engine-backed cases need the pinned
DuckDB library with built-in DuckPGQ.

From this checkout, with artifacts under `.scratch/engine/`:

```bash
mkdir -p .scratch/test-tmp
dotnet build native/Retrieval.Native.Tests/Retrieval.Native.Tests.fsproj -nologo
TMPDIR="$PWD/.scratch/test-tmp" \
RETRIEVAL_TEST_EXTENSIONS="$PWD/.scratch/engine/extensions" \
RETRIEVAL_TEST_LIBDUCKDB="$PWD/.scratch/engine/libduckdb.so" \
  native/Retrieval.Native.Tests/bin/Debug/net10.0/linux-x64/Retrieval.Native.Tests \
  -noLogo -noColor -maxThreads 2
```

In the shared lab, wrap substantial build/test commands with Bozzetto's
`tools/build/lease.fsx` using `full_build` or `test_suite_run`. Keep source and
dependency checkouts fixed until the gate finishes.

Hook-registration tests use mocked administrative I/O and require no secrets:

```bash
cd tools/forgejo
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -q test_forgejo_corpus_hooks
```

The lab separately tests packaging, rollout rollback, MGR hook transport and Pi.
The runtime queue acceptance has six lifecycle controls: shared computation
with independent cancellation; distinct requests respecting model capacity;
publication waiting and rebind; physical cancellation drain; bounded retention;
and shutdown joining all consumers. Mutations that duplicated work, skipped
publication waiting or allowed an obsolete result were all detected.

## Live full-response measurement

On October 20, 2026, the accepted queue release
`19be8e01853df2e11dc7a6db23a463c834bf0195c023aa927fd3a5cb89ddfc45`
was exercised over HTTPS against the loaded Gemma E4B Q4 model on MGR's RTX
3050. Thinking remained off with immediate nested GBNF. Timing includes the
text request, service processing and complete JSON response.

| Question about one named Composer file | First measured request | Identical repeat | Four callers sharing a new question |
| --- | ---: | ---: | ---: |
| Outgoing links | 1.635 s | 0.010 s | 1.196–1.200 s |
| Incoming links | 1.133 s | 0.010 s | 0.918–0.926 s |
| Source chunks | 1.194 s | 0.007 s | 0.948–0.954 s |

All 18 responses matched direct snapshot-pinned graph oracles, pagination and
source provenance. The snapshot was
`7f77abc7662170936bd639dda50a09733015f5d07ed23581e7594438b8759576`.
The target was `docs/PSG_Nanopass_Architecture.md` in
`FidelityFramework/Composer`. The model was resident and startup priming had
run; these are ordinary runtime requests, not model-loading measurements.

This bounded check establishes those graph operations. Broader research
questions can require two model completions and still need their own semantic
acceptance cases. Grammar-valid JSON alone does not establish relevance.

The paired 2.0 implementation passed 139 native tests, with two retained
external-validator skips. Removing grammar activation, engine source checking,
generation binding, renamed-target verification or the active-file lock guard
each failed its intended test;
the restored full suite passed. The lab packaging and rollback suite passed
76 tests. The unchanged hook reconciler retains its 11-test accepted baseline.
The deployed no-thinking profile separately passed 12/12 graph-correct responses
without observed reasoning. Local engine acceptance and live deployment are
distinct gates.

## Context carried into the agent

For the outgoing-links question above, a separate measured HTTPS request returned
five oracle-checked relationships in 1.138 seconds and 8,162 UTF-8 bytes at
14:26 UTC on October 20. The request specified a 4,000-character excerpt budget. Those
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
