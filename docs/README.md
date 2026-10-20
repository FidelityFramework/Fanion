# Fanion documentation

Fanion turns a short text question into bounded graph results and supporting
source passages, tied to a published corpus snapshot. The background and
motivation are in [Retrieving Fidelity](https://clef-lang.com/blog/retrieving-fidelity/).

Start with the [architecture](ARCHITECTURE.md) for ownership and execution flow,
or the [API](ENGLISH-API.md) to connect an agent. For deployment, read
[operations](OPERATIONS.md), select the [corpus](CORPUS.md), then configure
[push notifications](WEBHOOKS.md).

| Document | Covers |
| --- | --- |
| [Architecture](ARCHITECTURE.md) | F# service owners, pull evaluation, queued requests and generation publication |
| [API](ENGLISH-API.md) | Text requests, corpus remit, evidence, wait budgets and operator graph inspection |
| [Corpus](CORPUS.md) | Repository and ref selection, inclusion policy and scope |
| [Extractor](EXTRACTOR.md) | Committed source extraction, classification, limits and provenance |
| [Webhooks](WEBHOOKS.md) | Forgejo authentication, enrollment, debounce, reconciliation, differential extraction and recovery |
| [Operations](OPERATIONS.md) | Build, engine pins, service configuration and release activation |
| [Validation](VALIDATION.md) | Test commands, correctness controls, measured timings and their scope |
| [DuckDB 2.0](DUCKDB-2.md) | Paired engine build, connection contract, graph publication and indexing measurements |
| [Query prompt](PQL-PROMPT.md) | Supported graph language and worked examples |

Fanion owns retrieval, the indexing receiver and corpus publication. The lab
repository owns VM provisioning, Forgejo hook enrollment, nginx and agent
installation. See the [project README](../README.md) for repository layout and
build prerequisites.

## Measurements

The October 20, 2026 installed-service trials took **11.957 s, 24.767 s,
11.508 s and 15.326 s** to re-index **1, 3, 5 and all 24 repositories**,
respectively, without service restarts. The three-repository outlier is retained;
its extra delay occurred almost entirely before the build began.

See [measured re-indexing](VALIDATION.md#measured-re-indexing) for the nested
stage breakdown, exact configuration, memory observations and post-build
retrieval checks. These are current-head re-extractions, not changed-push
latency measurements. The [corpus configuration](../config/corpus.framework.json)
and [engine pins](../config/libduckdb-lock.json) identify the inputs.
