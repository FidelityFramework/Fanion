# DuckDB 2.0 native engine

Fanion targets the paired source build recorded in its engine locks: DuckDB
`v2.0.0-dev1`, source `45f4fdf315`, with built-in DuckPGQ `6b1d4b021d`.
This is a pinned development build, not the final 2.0.0 release. It was deployed
on October 20, 2026, after the complete 24-repository corpus passed side-index
validation. All 18 subsequent HTTPS text-to-JSON acceptance responses matched
snapshot-pinned graph results and source provenance.

## Build and connection contract

The [library lock](../config/libduckdb-lock.json) binds the complete native
artifact by SHA-256. The [DuckPGQ lock](../config/duckpgq-lock.json) records the
matching extension source. Build DuckPGQ at
`6b1d4b021daefb8b4e906b58def263f99158effb`, with its DuckDB submodule at
`45f4fdf3153c89cd38fdf6fb20b95646a466c5ac` and extension-ci-tools at
`39ffc46ca8c70021d741c7be2e7bcadf3fd1c503`.

Build inside an Ubuntu 24.04 userland with the primary source checkout mounted
in place. The build environment is part of the lock: a matching source revision
alone does not establish libc compatibility with the deployment guest.

In that environment, from the upstream primary source checkout:

```bash
git submodule update --init --recursive
CC=clang-18 CXX=clang++-18 cmake -G Ninja \
  -DEXTENSION_STATIC_BUILD=1 -DCMAKE_CXX_STANDARD=17 \
  -DDUCKDB_EXTENSION_CONFIGS="$PWD/extension_config.cmake" \
  -DUNITTEST_ROOT_DIRECTORY="$PWD/" -DBENCHMARK_ROOT_DIRECTORY="$PWD/" \
  -DENABLE_UNITTEST_CPP_TESTS=FALSE -DBUILD_EXTENSION_TEST_DEPS=default \
  -DBUILD_UNITTESTS=ON -DDISABLE_UNITY=ON \
  '-DSTATICALLY_LINK_EXTENSIONS=core_functions;json;parquet;icu;duckpgq' \
  -DCMAKE_BUILD_TYPE=Release -S duckdb -B build/release-ubuntu24
cmake --build build/release-ubuntu24 --config Release --parallel 6
```

The artifact is `build/release-ubuntu24/src/libduckdb.so`. Its accepted identity is the
lock digest, not a filename. Rebuilding with another toolchain can produce a
new digest and requires its own acceptance. Keep all ordinary core extensions
in the static list; linking only DuckPGQ leaves basic functions unavailable.

Before building a corpus, deployment runs `retrieval-native probe --only engine`
with the candidate locks inside the guest. It loads the actual library without
corpus access or credentials, so missing runtime symbols fail before indexing.
The probe runs under the memory/task limits and GC policy declared by the
candidate API unit, before either live service stops.
`Load.openEngine` owns every production connection. It verifies the artifact,
process binding, runtime version/source, platform, built-in linkage and extension
version. Automatic extension installation and loading are disabled. It then
executes this separately, before any PGQ statement is parsed:

```sql
SET active_grammar_extensions = ['duckpgq'];
```

Both native thread pools and the memory budget are supplied as connection
options before opening the database. The service's eight-thread budget also
bounds DuckDB's asynchronous pool to eight; host CPU discovery must not create
extra pools before those limits take effect. Service acceptance includes the
actual 128-task and 1,536 MiB limits, including graph reads.

DuckDB.NET.Data/Bindings 1.5.3 remain the host adapter. Extension DDL uses the
existing native `DuckDBQuery` API with result destruction in `finally` because
implicit preparation is unsupported for this graph-creation path. Validated
graph reads retain the normal binding. The gateway regenerates SQL from its
restricted grammar and validated seed/edge parameters.

The service uses read-write connections and unqualified `GRAPH_TABLE(corpus, …)`.
Read-only graph queries and references qualified with a renamed catalog are
outside this accepted contract. These limitations do not require a second
connection pool or a new connector.

## Generation and rollback

`Generation` includes the native engine identity in the canonical generation
body before hashing it. `Coordinator` verifies the staged graph and its final
filename, then asks `IndexState` to publish through its existing lease/epoch
fence. An already active target retains its prior verification: the serving
process holds its read-write lock, and `Query.Store` checks the engine identity
when opening it. Other pre-existing targets must pass verification. There is
one publication authority.

Deployment builds the complete candidate corpus alongside the serving database.
The updater retains the previous executable, engine, generation and coordinator
state together. It stops and switches only after candidate validation, and
restores that coherent set if activation fails. Never open a new-format database
with the old engine as a rollback strategy.

The native suite covers independently activated connections, changed artifacts,
version/source/extension mismatches, generation binding and publication. Removing
grammar activation, source checking, generation checking or target verification
causes the respective discriminating test to fail. A separate-process test
holds the active graph open while unchanged-revision invalidation resolves.
The final static build passed 4,944 upstream SQL
assertions in 73 cases. Live acceptance is separate from these local gates.

A refused inactive target remains unpublished. Stop the indexer, retain its
`last-build.json` failure receipt and move only that rejected inactive file out
of `generations/` into private quarantine before resuming reconciliation.
Check the active-generation identity first; never move a serving generation.

## Indexing and cross-linking measurements

`Enrich` remains the authority for relationship semantics, with its cross-file
matching now expressed as SQL over temporary staging tables. The earlier
4,685-row module-open experiment covered only one rule family.

The full migration fixture contains 23 pinned repositories, 16,273 nodes,
29,458 total edges and 10,760 derived edges across all relationship families.
Both one-thread and eight-thread runs of the SQL migration matched every stored
column, row order, metadata and canonical snapshot hash. The subsequent
Fidelity.Data integration preserves that parity. Its first parser implementation
regressed generation to 11.8–15.2 seconds; fixing ordinary string-run parsing
in Fidelity.Data brings the three candidate trials to 3.697, 3.304 and 2.983
seconds. The previous pre-SQL implementation took 2.42–2.86 seconds, so this
does not establish an overall speedup. SQL joining takes 268–321 ms in the
latest trials. Parsing, hashing and the durable commit remain part of the
queued generation interval. These measurements exclude source extraction and
remote ref observation.

The coordinator also avoids re-extracting unchanged declared dependents:
verified structural components are reused, while cross-links are rebuilt over
all peers. The following earlier loading experiments describe separate
prototypes, not the production transaction's phase timings.

With the full production node/edge layout—16,273 nodes and 29,458 edges—three
trials through `Load.openEngine` on the locked Ubuntu-built artifact gave these
medians in milliseconds. Loading includes every appender connection's artifact
checks. Measurements ran on Z13 against 23 pinned local repository components.

| Appenders | Load | Graph construction | Explicit checkpoint |
| ---: | ---: | ---: | ---: |
| 1 | 299 | 315 | 3.2 |
| 4 | 233 | 344 | 2.8 |
| 8 | 252 | 363 | 3.5 |

One eight-appender trial reported a different phase split: 538 ms loading and
13 ms graph construction. It remains in the receipt and in the three-trial
medians above; the cause of that variation has not been isolated.

Outgoing, incoming and chunk queries matched edge-table oracles, including after
reopening. Four appenders helped loading; eight gave no consistent further gain.
This establishes the tested workload, not a general 2.0 speed claim. The unchanged extractor separately
scaled from 6.142 seconds at one processor to 1.902 at four and 1.564 at eight.

The parallel-loading fixture commits independent appenders inside an unpublished
candidate. The production loader retains its single validated transaction;
parallel staging needs the same complete-generation and publication guarantees.

A SQL join for module-open matching took 4–9 ms versus about 21 ms for the F#
matcher with already parsed facts. Appending the facts adds work, so these figures
do not establish faster complete cross-linking. Any owning-rule migration must
preserve complete edge provenance, precedence, ordinal identity and publication.

[Quack](https://duckdb.org/docs/preview/connect/concurrency) provides a separate
multiprocess client facility. Four F#/.NET clients wrote and read 40,000 distinct
rows through authenticated loopback in a preview smoke test using an attached
remote catalog. This does not establish production durability or LAN throughput.
The embedded generation pipeline needs no network database service.

The [numeric receipt](measurements/2026-10-20.json) records source revisions,
artifact hashes, scopes and trial measurements. See [Validation](VALIDATION.md)
for complete text-to-JSON and context-volume evidence.
