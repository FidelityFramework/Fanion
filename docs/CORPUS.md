# Corpus scope

[`config/corpus.framework.json`](../config/corpus.framework.json) is the
selected Fidelity corpus. It owns repository identities, explicit refs,
dependency declarations and inclusion policies.
[`config/corpus.example.json`](../config/corpus.example.json) is a smaller
configuration illustrating the same schema.

Use the service's `remit` action to discover configured coverage, including
while indexing is pending. Use fresh `schema` to establish the revisions
currently published. A configuration describes intended scope; it does not
prove that its new generation has been published.

Each source has a unique `id`, `repo`, canonical `refs/heads/...` and
`depends_on` list. Default branches are never substituted. The coordinator
resolves remote heads and reads committed Git objects. Dirty working trees,
unpushed commits and neighboring repositories are outside that evidence.

`depends_on` controls extraction invalidation. A changed upstream causes its
declared dependents to be rebuilt even if their own source commit is unchanged.
This is separate from the semantic edges extracted into the graph.

Optional `policy` records role, bounded authority, capabilities, included paths,
excluded paths and selected generated source. Optional `layer_rules` classify
paths. [Extractor rules](EXTRACTOR.md) define their exact limits. Secret and
operational-state exclusions run before blob reads and cannot be overridden
by a broad inclusion pattern.

Authority affects retrieval preference; it is not a truth score. An inferred
edge remains a research lead with its extraction basis. A committed document,
test or implementation has the authority of its actual source and revision.

To change coverage, edit the configuration, run extraction and publication
checks, deploy the frozen application/configuration together, then reconcile
Forgejo hooks from the active configuration. [The webhook runbook](WEBHOOKS.md)
describes enrollment and acceptance. Do not maintain a second handwritten
repository list in client prefill.

An agent may read directly for an explicit outside-corpus, policy-exclusion or
working-tree task. Empty matches, response truncation, omitted excerpts,
queueing and transport errors do not establish a coverage exception.
