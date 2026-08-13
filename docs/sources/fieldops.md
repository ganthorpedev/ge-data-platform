# FieldOps

**Status: PLANNED.**

No FieldOps ingestion pipeline, client, connection code, or dataset
extraction exists anywhere in this repository today. `raw_fieldops` and
`stg_fieldops` exist as empty schemas only (created by
`sql/migrations/001_create_platform_schemas.sql`, structurally identical to
the other four sources' schemas) so the platform's schema layout doesn't
need a migration just to make room for this source when it arrives.

There is no `ge_data_platform.sources.fieldops` package, no `FIELDOPS_*`
environment variable, and no FieldOps mention anywhere in `pyproject.toml`,
`requirements.txt`, or the Dagster orchestration layer
(`ge_data_platform.orchestration`). Nothing about FieldOps's expected
business domain (likely Maintenance and/or Operations -- see
`docs/architecture/platform-overview.md`'s source-system-to-domain table) is
committed; it is a reasonable expectation, not a decision.

Do not build a FieldOps pipeline, client, or schema object based on this
page -- it exists only to mark the placeholder explicitly rather than leave
it undocumented and easy to mistake for an oversight.

When FieldOps integration begins, this document should be rewritten
following the same structure as `docs/sources/trackunit.md` (authentication,
data pulled, retry behavior, known limitations, reconciliation schedule) --
grounded in the actual client code at that point, not this placeholder.
