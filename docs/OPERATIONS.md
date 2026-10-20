# Operations

Build and test in the primary checkout. Use `.scratch/` for local receipts,
test fixtures and acquired engine artifacts; generated .NET files remain under
`native/`. The application is self-contained F#/.NET. Python is used only by
the existing Forgejo enrollment and lab deployment helpers.

## Build inputs

Use .NET 10 and the sibling primary source checkouts listed in
[`source-dependencies.json`](../config/source-dependencies.json). Freeze those
revisions and their working trees during acceptance. The project references
use source because the required Incremental and Ranvier APIs are ahead of
their published packages. Record dependency commits with each release.

```bash
bash native/build.sh
```

Publishing produces `native/publish/retrieval-native` for Linux x64, including
the .NET runtime. It does not bundle model weights, provision a guest or start
services.

DuckDB.NET supplies bindings. The paired source build embeds DuckPGQ in the
native library pinned by [`libduckdb-lock.json`](../config/libduckdb-lock.json).
[`duckpgq-lock.json`](../config/duckpgq-lock.json) records its matching source.
Keep `libduckdb.so` beside the executable. The runtime verifies its digest and
source identity; it performs no runtime extension download. See the
[native-engine contract](DUCKDB-2.md) for the reproducible build and acceptance.

## Configuration and startup

The API and indexer use the same corpus, local state root and engine locks.
Run them as an unprivileged service account with private state permissions.
The indexer needs Git and access to configured source remotes. A model serves
only inference; it receives no graph database access.

For the deployed layout, the indexer command is:

```bash
/opt/retrieval/current/bundle/retrieval-native index \
  --root /var/lib/retrieval \
  --config /etc/retrieval/corpus.json \
  --duckpgq-lock /opt/retrieval/current/app/duckpgq-lock.json \
  --libduckdb-lock /opt/retrieval/current/app/libduckdb-lock.json
```

The API command is:

```bash
/opt/retrieval/current/bundle/retrieval-native serve \
  --root /var/lib/retrieval \
  --config /etc/retrieval/corpus.json \
  --token-file /etc/retrieval/api.token \
  --webhook-secret-file /etc/retrieval/forgejo-webhook.secret \
  --prompt-file /opt/retrieval/current/app/PQL-PROMPT.md \
  --duckpgq-lock /opt/retrieval/current/app/duckpgq-lock.json \
  --libduckdb-lock /opt/retrieval/current/app/libduckdb-lock.json \
  --host 0.0.0.0 --port 8080
```

Paths above are service-account inputs, not commands to run alongside an
already active indexer. Its exclusive lock prevents a second publisher.
`--once` is available for an explicitly controlled offline build.
Existing lab units may still pass `--extensions`; it is ignored by the static
engine path and is not a required input.

The standalone `load` command is an offline snapshot import. It preserves the
input's logical snapshot identity and records its engine binding within the
metadata budget. Only `generation` through the coordinator constructs and
publishes an engine-bound production generation.

The [API document](ENGLISH-API.md) lists model environment variables. For the
accepted Magnitude configuration, thinking is explicitly false and response
format is `magnitude_gbnf`. The startup hook runs `retrieval-native prime`
against the authenticated local API after the model is available. It exercises
both planning and qualified-file retrieval with the actual grammar.

Use distinct credentials for the API bearer token, model bearer token and
Forgejo webhook HMAC. Store secrets as private files. Only paths belong in
configuration examples and process arguments.

The default text-request budget is 240 seconds. The current transport, ingress
and Pi budgets are 270, 280 and 300 seconds respectively. Keep the outer
deadlines long enough for the service to complete or return its own failure.
`/health` returns 503 while latest evidence is unavailable; `remit` remains
available, and an unpinned text request waits within its budget.

## Lab release integration

The lab's
[cleanroom packaging and updater](https://forge.spkez.dev/SpeakEZ.Tech/speakez-lab/src/branch/main/platform/retrieval/cleanroom)
own VM identity, units, release switching and rollback. They consume Fanion
with `--app /home/hhh/repos/fanion` and
`--corpus /home/hhh/repos/fanion/config/corpus.framework.json`.
Packaging maps `docs/PQL-PROMPT.md` and `config/*-lock.json` to their stable
deployed filenames. It freezes exact corpus bytes and artifact hashes.

On an application update, the updater checks the accepted guest, builds a
candidate generation beside the live one, probes it, retains coherent old
state, switches the application/index and checks fresh graph access. Failure
restores the previous release, state, configuration and units. Initial VM
provisioning is a separate lab operation.

Current placement is MGR VM71 for the API/indexer and VM72 for Magnitude on
the RTX 3050. The HTTPS endpoint is
`https://duckdb-fidelity-pgq.spkez.dev/v1/retrieval`. Lab operations, GPU
allocation, nginx and agent installation remain in the lab repository.

For corpus changes, also perform the
[webhook enrollment and receipt checks](WEBHOOKS.md). A successful application
deployment does not by itself verify Forgejo delivery.
