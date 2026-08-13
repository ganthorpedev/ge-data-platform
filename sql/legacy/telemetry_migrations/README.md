# Legacy telemetry_warehouse migrations

These 12 files are the original, already-applied `telemetry_warehouse`
migrations (numbered 001–029, plus the standalone `sendem_tables.sql`),
moved here verbatim from `sql/migrations/` with git history preserved.

**Do not edit these for content.** They document exactly what was run
against the live `telemetry_warehouse` database and remain the historical
record of that database's migration sequence. `sql/migrations/` now holds a
separate, independent numbering sequence for the new `ge_warehouse` platform
database — see `docs/ge_warehouse_architecture.md`. The two sequences are
unrelated: a `sql/migrations/001_*.sql` file here is not "the same 001" as
`sql/legacy/telemetry_migrations/001_create_sendem_schema.sql`.

To apply one of these files manually (unchanged from before the move, other
than the file's location):

```powershell
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\001_create_sendem_schema.sql
```
