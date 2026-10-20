# Validation

Correctness and complete response time are the acceptance measures. The
published source revision, graph snapshot, model profile and query constraints
belong with each result.

## Local gates

The native suite covers source extraction, policy, deterministic identity,
query constraints, model transport, coordinator recovery, HTTP authentication,
webhooks, freshness and request lifetimes. Engine-backed cases need the pinned
DuckDB library and signed extension.

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

The extracted Fanion checkout passed 134 native tests, with two retained
external-validator skips, and 11 hook-reconciler tests. The lab packaging and
rollout suite passed 73 tests; its hook installer passed 19. Publishing and
packaging from the new checkout also passed, with the deployed artifact names
preserved. The deployed no-thinking profile separately passed
12/12 graph-correct responses without observed reasoning. Retain those scopes
when reporting results.
