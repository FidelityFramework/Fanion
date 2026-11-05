# Committed-Git extractor

`retrieval-native extract` calls `Extract.build` in
[`Extract.fs`](../src/Fanion/Extract.fs), using Git as its only
external program. It reads exact committed objects and returns a deterministic
source snapshot. Fetching, reconciliation and publication belong to the
coordinator.

The manifest is an explicit
allowlist; its only top-level key is `repositories`:

```json
{
  "repositories": [
    {
      "path": "/absolute/local/clone",
      "name": "FidelityFramework/example",
      "commit": "0123456789abcdef0123456789abcdef01234567",
      "source_url": "https://forge.spkez.dev/FidelityFramework/example",
      "ref": "refs/heads/main",
      "layer_rules": [
        {"name": "decisions", "patterns": ["docs/decisions/*"]},
        {"name": "architecture", "patterns": ["docs/design/*"]}
      ],
      "policy": {
        "role": "generated",
        "authority": 30,
        "capabilities": ["native-bindings", "ui"],
        "include_generated": ["bindings/*.generated.fs"],
        "include_paths": [],
        "exclude_paths": ["archive/*"]
      }
    }
  ]
}
```

`path` must be a repository root or bare repository, not a nested directory
that would implicitly select a parent repository. Duplicate logical names
and duplicate resolved clone paths are rejected. Revision values must be full
40-hex SHA-1 commit IDs; branches, abbreviated IDs, tags, tree/blob IDs and
missing objects are rejected. Git SHA-256 repositories are not yet supported.
`source_url` is an HTTP(S) Forgejo repository base URL without credentials,
query or fragment. `ref`, `layer_rules` and `policy` are optional; older manifests remain
valid. `ref` must be an explicit bounded `refs/heads/...` name and is recorded
as provenance. It never replaces `commit` as the object selector and does not
prove that a remote branch still points to that commit. The event controller
must verify that relationship. The extractor does not resolve a default
branch, fetch refs, or crawl adjacent checkouts. The configured ref is authoritative even when it differs from a repository's default branch.

Run locally with:

```bash
.scratch/publish/retrieval-native extract manifest.json snapshot.json
# tests: tests/Fanion.Tests (ExtractTests.fs); see README.md for the run command
```

The CLI writes a private `0600` temporary file, flushes it, atomically replaces
the specified output and flushes its directory. Build failure leaves an
existing snapshot untouched. The output directory must already exist.

## Snapshot and provenance

The output contains `format_version: 1`,
`extractor_version: "committed-git-lines-v4"`, `snapshot_id`, `repositories`,
`nodes`, `edges`, `classification_version`, `classification_defaults`, and
`stats`, plus `policy_version: "repository-policy-v1"`. Repository metadata
contains `name`, `commit`, `source_url`, optional `ref`, normalized explicit
`layer_rules`, normalized `policy`, and per-repository `stats`. Local
clone paths do not appear in the output or hash.

Every node has `id`, `repo`, `revision`, `path`, `kind`, `title`, `line_start`,
`line_end`, `text`, `source_url`, `corpus_layer`, and `classification_rule`.
Kinds are `repository`, `file` and `chunk`.
Markdown file titles come from YAML/TOML front matter through Fidelity.Data,
then a top-level heading outside fenced code when no title is declared. Files
without a document title retain their path as the label. Declared malformed
metadata reports its source path instead of silently losing the title.
Repository nodes have empty path/text and zero line positions; empty files
also have zero line positions and produce no chunks. File nodes retain the
complete accepted text. Source URLs use Forgejo's
`/src/commit/<SHA>/<quoted-path>#L<start>-L<end>` form.

Node IDs are lowercase, 64-hex SHA-256 values over canonical JSON identities
containing logical repository, exact revision and node kind, plus path and
chunk span where applicable. They are deterministic for a revision, and
intentionally change between revisions. The snapshot ID hashes the complete
canonical JSON payload excluding `snapshot_id` itself. Object keys, node/edge
ordering and repository ordering are deterministic. No timestamps or local
clone locations enter the hash.

Source reads use `git ls-tree` and `git cat-file` against the declared commit
and its blob IDs. They do not read working-tree source, staging contents or
later HEAD commits. Git replacement objects, lazy network fetch, global/system
configuration and inherited `GIT_*` overrides are disabled. No textconv,
smudge filters, checkout or hooks are invoked.

Chunks are **100 source lines with 10-line overlap**, not tokenizer chunks.
They preserve LF/CRLF and a final unterminated line. A 100-line chunk can still be large; response hydration applies a separate character budget.

Edges contain only `src`, `dst` and `kind`:

- `contains`: repository to included file.
- `chunks_of`: chunk to its file.
- `links_to`: Markdown file to another included file in the same manifested
  repository and commit.

Markdown extraction recognizes simple inline links and explicit/collapsed
reference links. It omits images and links in fenced/inline code. Relative
paths are URL-decoded once, resolved lexically from the source's directory,
then looked up only in the included committed-file map. External URLs,
root-absolute paths, backslashes, repository escapes, missing/excluded targets
and untracked files produce no edge. Queries/fragments do not change the
target file; directory indexes and cross-repository aliases are not guessed.
This is a conservative extractor, not a complete CommonMark parser: nested
brackets, escaped delimiters, shortcut references and HTML links may be missed.
It makes no `calls`, `depends_on`, `implements` or symbol-resolution claims.

## First-class corpus layers

Documentation and code use the same inclusion and byte limits. Classification
does not prioritize one layer or discard another. Each accepted file and all
its chunks share a `corpus_layer` and `classification_rule`. Repository nodes
use `repository` for both fields. Other allowed layers are:

`docs`, `spec`, `architecture`, `decisions`, `guides`, `research`,
`implementation`, `tests`, `config`, `examples`, `tooling`, and `other`.

The versioned default taxonomy uses ordered path/name/suffix rules: decision
and supersession paths; research paths; specification paths; architecture
paths/names; guides/tutorials; documentation paths; tests; samples/examples;
remaining document suffixes; configuration suffixes; tooling; implementation;
then `other`. The exact catalog is serialized once in
`classification_defaults`; `classification_version` is `path-layers-v1`.

Optional per-repository `layer_rules` are ordered objects with exactly
`name` (one allowed layer) and `patterns` (a nonempty list of globs). Explicit
rules run before defaults; the first matching rule wins. Matching uses
case-insensitive, repository-relative **whole-path `fnmatch` globs**: `*`
can cross `/`; `**` is not a separate recursive operator. For example,
`docs/*` matches every descendant of the root `docs` directory, whereas
`*/docs/*` requires a preceding directory. There is no regex, absolute path,
parent traversal, source-content evaluation, or shell execution. Input
patterns are normalized to lowercase. Rules cannot override exclusions.

`classification_rule` is `manifest:<zero-based rule index>:<layer>` or
`default:<zero-based rule index>:<layer>`. Explicit rules are retained in each
repository's metadata and defaults once at the top level, so the decision can
be reproduced without duplicating the catalog for every repository. Rule
ordering, normalized patterns, classification version and optional branch
provenance all participate in the snapshot hash. Node identity remains the
committed repository/path/span identity; reclassification changes the snapshot
without pretending the source object changed.

These are **syntactic evidence labels**, not inferred semantic authority or
implementation status. A `spec` chapter may declare itself draft/informative;
an architecture document can be superseded. The extractor preserves such
declarations as source text but does not infer a normative status from a path.
`AGENTS.md` and `CLAUDE.md` are ordinary untrusted documentation evidence when
otherwise eligible; their contents do not become extractor or agent commands.

Both top-level and per-repository `stats` contain `tree_entries`,
`candidate_files`, `candidate_bytes`, `accepted_files`, `accepted_bytes`,
`excluded_entries`, `excluded_by_reason` and `accepted_by_layer`. Layer values
are `{files, bytes}` counts of accepted source blobs, not duplicated chunks.
`accepted_selected_generated_files` and `accepted_selected_generated_bytes`
count accepted files admitted through a generated-source selection. They are
subsets of the ordinary accepted counts; rejected candidates never enter them.
Exclusion reasons distinguish operational configuration, excluded directories/
suffixes, secret filenames, nonregular entries, invalid paths, oversized
files, binary/non-UTF-8 content, private-key headers, LFS pointers and curated
`policy_scope` omissions. Counts
expose no excluded path, blob ID or content. Deterministic counts belong to
the snapshot; they are not live-service freshness/readiness status.

## Curated repository policy

An optional repository `policy` object accepts only these fields:

| Field | Default | Meaning |
| --- | --- | --- |
| `role` | `component` | One of `central`, `component`, `generator`, `generated`, `example` |
| `authority` | `0` | Integer 0–100 recording an operator's retrieval preference |
| `capabilities` | `[]` | Up to 32 lowercase ASCII slugs, at most 64 characters each |
| `include_generated` | `[]` | Up to 32 repository-relative globs, at most 256 UTF-8 bytes each |
| `include_paths` | `[]` | Up to 32 globs selecting repository scope; empty means all eligible paths |
| `exclude_paths` | `[]` | Up to 32 globs removing paths from that scope |

Capability slugs use letters/digits separated by single hyphens, for example
`native-bindings`. Authority and capabilities are curation, not claims that a
source is correct or that its described capability has been implemented. The
extractor records them without interpreting source prose or changing source
identity. Retrieval may use this policy to prefer central context while
retaining targeted library evidence.

Path selection applies before reading source blobs. Exclusions take precedence
over inclusions. Each path omitted by `include_paths` or `exclude_paths` is
counted under `policy_scope`; its contents never consume the candidate source
byte budget. For example, `exclude_paths: ["Hardware/*"]` retains a platform
repository's contracts, documentation and profiles without indexing its bulk
hardware definitions. Scope selection cannot bypass hard exclusions or admit
generated suffixes without a separate `include_generated` selection.

Generated-source selection uses the same case-insensitive whole-path `fnmatch`
semantics as layer rules and the scope globs. All policy globs have the same
256-byte bound. Generated selection only admits otherwise excluded `.generated.fs`,
`.generated.cs`, `.g.fs` and `.g.cs` source suffixes. `.clef` sources already
qualify without an override. It cannot override excluded directories, secret
filenames, operational configuration, binary/private-key content, LFS pointers,
nonregular entries, invalid paths or size limits. Even `"*"` cannot admit a
file from `build/` or `vendor/`. This selects committed sources only.

Unknown fields, wrong types, out-of-range values, malformed glob character
classes, absolute paths, traversal, backslashes and control characters are
rejected. Booleans are not integer authority values. Globs are lowercased;
all lists are sorted and deduplicated. Partial policy objects fill missing
defaults. Omitted policy and `{}` yield the same snapshot. Normalized policy
and its version participate in the snapshot hash, including a selection that
currently matches no files. Reprioritizing a repository changes its snapshot
without changing the IDs of unchanged committed source nodes.

## Exclusions and hard limits

Only regular Git blobs (modes `100644` and `100755`) are candidates. Symlinks
and submodules are never followed. Non-UTF-8 text, binary control bytes, LFS
pointers, non-UTF-8/control-character paths and files over 1 MiB are omitted.
Common generated/vendor directories and artifacts are excluded, including
`node_modules`, `vendor`, `third_party`, `bin`, `obj`, `build`, `dist`, `target`,
virtual environments, caches, minified JS/CSS, lockfiles and binary formats.
The exact fixed lists are `_EXCLUDED_DIRS` and `_EXCLUDED_SUFFIXES` in code.
Only the four generated source suffixes described above can be selected by
repository policy; all other artifact exclusions remain unconditional.
Operational agent-state directories and private deployment-state files are
excluded at any nesting depth; the extraction owner maintains the exact names,
before any source blob read. The latter deployment state contains credentials
as well as useful status; a controller must emit a separate sanitized receipt.

Secret-like names are rejected from indexing: `.env` and `.env.*` (including
examples), private-key extensions/standard SSH key names, `.netrc`, `.npmrc`,
`.pypirc`, the listed credentials/secrets configuration names and credential
directories. Text containing a PEM/OpenSSH private-key header is also omitted.
This is a conservative filename/header policy, not a complete secret scanner;
the operator still controls which repositories are authorized for the corpus.

| Limit | Value |
| --- | ---: |
| Manifest input | 128 KiB in CLI |
| Repositories | 64 |
| Explicit layer rules | 64 per repository |
| Patterns | 32 per rule, 256 total per repository, 256 UTF-8 bytes each |
| Policy capabilities / each glob list | 32 each per repository |
| Tree listing output | 16 MiB per repository |
| Total tree entries | 100,000 |
| Individual accepted candidate blob | 1 MiB |
| Candidate blobs read | 20,000 total |
| Candidate blob bytes read | 128 MiB total |
| Nodes / edges | 100,000 / 200,000 |
| Markdown link occurrences | 1,024 per file |
| Serialized snapshot | 384 MiB |
| Git subprocess | 15 seconds; stdout bounded by operation, stderr 8 KiB |
| Build deadline | 600 seconds, checked across subprocesses and emitted nodes/edges |

Aggregate source, file and output budgets abort a runaway build instead of
silently truncating the corpus. Size changes require a measured acceptance
check against the deployment's memory budget.
Candidate blob counts/bytes include files later rejected as binary or by
content headers. Excluded filenames and oversized blobs are skipped before
reading. Aggregate, output, link and time limit failures abort the build;
they do not publish a silently truncated corpus. The coordinator owns synchronization, deletion observation and publication;
this extraction pass owns only the supplied committed objects.

Tests use real temporary Git repositories to verify commit-only reads,
symlink/submodule rejection, exclusions, source spans, same-repository link
bounds, revision/clone-location determinism, invalid revisions, hard limits,
atomic CLI failure behavior, equal document/code eligibility, reproducible
ordered classification, explicit branch provenance, and operational exclusion
without blob reads. [Corpus scope](CORPUS.md) and [webhooks](WEBHOOKS.md) describe selection and publication.
