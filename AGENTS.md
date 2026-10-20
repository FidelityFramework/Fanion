# Working agreements

Use `/home/hhh/repos/fanion` as the primary checkout. No secondary worktrees,
clones or copied repositories. Put scratch fixtures, receipts and logs in
`.scratch/`; .NET outputs remain in `src/**/bin`, `src/**/obj`,
`tests/**/bin`, `tests/**/obj` and `.scratch/publish`.
Keep credentials and corpus contents out of Git.

Keep the standard solution layout: `Fanion.slnx`, `src/Fanion` and
`tests/Fanion.Tests`. The service is .NET hosted; reserve "native" for the
embedded engine or an actual native compilation target. Lab-specific enrollment
and deployment belong in the lab repository.

Use Fidelity.Data for data-format processing. Fix missing capabilities in its
existing format owner instead of introducing a competing parser. Host primitives
such as threading and I/O are a distinct concern; do not treat those dependencies
as justification for bypassing Fidelity libraries. Preserve canonical identities
and validate parser migrations against retained corpus evidence.

Use the live retrieval schema to establish corpus coverage. For covered
repositories require fresh/latest matching snapshot IDs, then use
`find` → `pgq` → `sources`, pinned to that snapshot. Cite repository, revision,
path and lines. Direct reads require an outside-corpus, stale-index,
uncommitted-diff or bounded-confirmation exception. Do not replace unavailable
retrieval with repository scans. Use LAN workers for scoped first-pass reading.

Extend the owning module. `IndexState` owns durable publication, `Requests`
owns request lifetimes, Incremental owns async execution and Ranvier owns pure
pull projections. No second scheduler, freshness authority or agent query path.
Keep thinking disabled and immediate nested GBNF for the accepted Magnitude
profile. Preserve query validation and provenance.

Trust retained accepted evidence. Freeze dependency revisions for substantial
gates, use existing Bozzetto build/test leases, and use one reviewer per
increment. Run discriminating controls for behavioral changes.

Report the retrieval snapshot, read exceptions, validation, residuals and
`git diff --shortstat <base>..<head>`. Give each new file or owner a one-line
reason. Include these rules in delegated work.
