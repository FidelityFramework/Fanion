# Push-driven corpus updates

The receiver is part of `retrieval-native serve`, in `Serve.fs`. The indexer is
`retrieval-native index`, in `Coordinator.fs`. Both use `IndexState.fs` over the
same local SQLite state. There is one publication authority.

## Wire contract

Forgejo sends JSON to `POST /v1/index/events/forgejo`. Configure a repository
push hook for each source's explicit branch, with TLS verification enabled.
The API bearer token and webhook HMAC secret are different credentials.

| Field | Requirement |
| --- | --- |
| `X-Forgejo-Signature` | Lowercase hex HMAC-SHA256 of the exact request bytes using the webhook secret |
| `X-Forgejo-Event` | `push` |
| `X-Forgejo-Delivery` | Unique delivery identifier, 1–128 allowed ASCII identifier characters |
| `repository.full_name` | Configured repository; comparison is case-insensitive |
| `ref` | Configured explicit ref; comparison is exact |

Equivalent `X-Gitea-*` headers are accepted when the Forgejo header is absent.
The body limit is 512 KiB. Signature comparison is constant-time. Missing or
invalid signatures return 401; malformed accepted-event fields return 400;
oversized bodies return 413. Without a configured secret, the route returns 404.
Signed non-push events and untracked repository/ref pairs return 202 with
`accepted:false`.

A tracked signed push is durably deduplicated by delivery ID before HTTP 202.
Reusing an ID with different event identity is rejected. The payload's commit,
clone URL and commands are not revision authority and are never executed.
Acknowledgment means the hint was recorded; it does not mean the graph is ready.

## Observation and publication

1. A new hint marks its source as requiring observation. Latest freshness fails
   immediately.
2. A local state notification wakes the indexer's Incremental execution slot.
   It observes the hinted sources against the configured remote. Startup and
   a 30-second reconciliation deadline observe all sources to recover missed
   hooks; ordinary push handling does not wait for that deadline.
3. Observation time is taken when fetching begins. A hint arriving during that
   observation remains pending. Network/authentication failures do not become
   deletion tombstones.
4. After an existing generation, changed heads settle for 250 milliseconds,
   capped at one second from the first change. Further changed heads reset the
   short debounce, including repeated changes to one source, without moving
   the cap. Rechecks of unchanged heads do not extend it. First generation and
   explicit `--once` skip settling.
5. The durable plan includes changed components, transitive dependents, desired
   revisions and the base generation. Extraction reuses unaffected components;
   cross-linking runs against the complete assembled corpus.
6. A renewable build lease protects publication. The staged immutable graph
   must pass identity, engine and graph checks. `CompleteBuild` compares the
   current desired epoch/revisions and unresolved hints before atomically
   publishing. A superseded build cannot replace the current generation.
7. Freshness requires recent observations, no pending changes and matching
   published revisions. The service uses a 90-second observation age bound.
   Publication wakes waiting text requests, which proceed only after this
   fence passes. Waiters subscribe before reading durable state. State reads
   share SQLite's write barrier, so an early file notification cannot expose a
   partially committed update. Notifications carry no authoritative state.

Force pushes are revision changes without an ancestry assumption. Confirmed
branch deletion removes that source's content through a tombstone plan.
Duplicates and reordered deliveries can cause another observation, but cannot
roll the authoritative desired revision backward.

## Enrollment

The checked hook reconciler is
[`forgejo_corpus_hooks.py` in the lab's integration tooling](https://forge.spkez.dev/SpeakEZ.Tech/speakez-lab/src/branch/main/platform/retrieval/integration/forgejo_corpus_hooks.py).
It is existing deployment tooling for the current Forgejo installation; the
receiver and indexer themselves are F#.

The helper runs as root inside the verified Forgejo guest. Its JSON input
arrives on stdin: `corpus` and `webhook_secret`. `--preflight` accepts only
`corpus` and uses an existing credential for read-only inspection. Never put
the secret in command arguments, shell history, repository files or logs.

It manages the dedicated `retrieval-corpus-webhooks` token with
`write:repository` scope, stored mode 0600 at
`/etc/forgejo/retrieval-hooks.token`. It verifies repository/ref identity and
matching hooks through the Forgejo API. It refuses ambiguous duplicates,
keeps unrelated hooks, and narrowly permits the retrieval hostname in
Forgejo's outbound webhook allowlist. Required configuration changes retain
a private rollback copy. It tests delivery and emits a sanitized receipt.

The lab's
[MGR hook installer](https://forge.spkez.dev/SpeakEZ.Tech/speakez-lab/src/branch/main/platform/retrieval/integration/install_corpus_webhooks.py)
owns VM identity checks, ingress and private transport between guests. Stage
the reviewed helper from the same lab integration directory alongside that installer and pass
the helper's SHA-256. The installer reads VM71's **active** corpus and HMAC
secret privately, invokes the helper in VM55, and joins each Forgejo delivery
UUID to its exact durable receiver event. It then requires fresh schema.
No handwritten source list or copied secret belongs in the rollout.

After staging and verifying the frozen installer and helper on MGR, run from
that private rollout directory. Replace `REVIEWED_HELPER_SHA256` with the
helper hash bound by the reviewed bundle:

```bash
sudo python3 install_corpus_webhooks.py \
  forgejo_corpus_hooks.py REVIEWED_HELPER_SHA256 \
  --receipt webhook-acceptance.json
```

Successful registration records the public acceptance receipt. If registration
fails partway through, the installer retains the accepted ingress route and
already installed hooks so a verified retry can continue. Do not remove working
hooks to recover another repository's enrollment.

Current lab bindings are Forgejo VM55 (`forge`) and retrieval VM71
(`duckdb-fidelity-pgq`) on MGR, with destination
`https://duckdb-fidelity-pgq.spkez.dev/v1/index/events/forgejo`.
The helper checks Forgejo `15.0.9+gitea-1.22.0`; another version or installation
requires reviewing these bindings before enrollment.

## Verification and recovery

| Observation | Check and action |
| --- | --- |
| Delivery fails before reaching receiver | Inspect Forgejo delivery status, DNS/TLS and the exact outbound destination allowlist |
| Receiver returns 401 | Compare credential provenance and signature generation privately; do not disable verification |
| Receiver returns 202 with `accepted:false` | Check event kind and configured repository/ref |
| Delivery accepted, graph pending | Inspect `retrieval-indexer` journal and coordinator state; acknowledgment precedes extraction and cross-linking |
| Hook is missed | Authoritative reconciliation detects it; verify the next observed head and published revision |
| Indexer restarted during build | Durable lease expiry permits recovery; an expired builder cannot publish |
| Source advances mid-build | Expect supersession and a new plan; never switch a filesystem pointer manually |
| Freshness remains stale | Resolve remote access or extraction errors, then let the single indexer reconcile |

On the retrieval guest:

```bash
systemctl status retrieval-api retrieval-indexer --no-pager
journalctl -u retrieval-indexer -n 60 --no-pager
```

Verify a real push by recording the configured ref's exact commit, the signed
delivery UUID, its durable event receipt, and a subsequent fresh/latest schema
with the indexed revision. Then pin `find`, `pgq` and `sources` to that snapshot
and check a changed source span. Receiver success alone is insufficient.

Keep `index-state.sqlite` on local persistent storage with its WAL; do not place the
live coordinator database on NFS. Back up coherent coordinator state and graph
artifacts together. Query progress percentages do not describe this lifecycle:
ordinary graph execution is milliseconds, while requests wait for admission,
source observation or completed publication.

## Measuring a reindex

The operator command submits invalidation to the running indexer and waits for
its publication notification. It does not restart either service:

```bash
retrieval-native reindex --root /var/lib/retrieval \
  --config /etc/retrieval/corpus.json --sources composer,clef,alex
```

Use `--sources all` for the full configured corpus. Run each case from a fresh,
idle generation. Concurrent source activity invalidates the timing trial rather
than being attributed to it. This re-extracts current heads; it is not a synthetic
code push. A real push additionally includes webhook delivery and authoritative
discovery of the new head.

The JSON records acceptance and durable publication timestamps, with receipt
delivery measured separately. Its build timeline partitions execution into
prepare, fetch/extract, generation, verify/persist, final head observation and
publication. Child timings under `nested` are already included in their parent;
never add them to the parent. Fetch/extract includes only the targeted component
work, while cross-linking considers the complete assembled corpus. Keep every
trial, including failures, when comparing one, three, five and all sources.
