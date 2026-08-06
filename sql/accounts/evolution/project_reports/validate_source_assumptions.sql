-- Source-assumption checks for dbo.vwProjectsReports (GE and TLS).
--
-- Not a migration: run manually against EACH Evolution database (connect
-- to the GE database, run it; connect to the TLS database, run it again)
-- to confirm or refute the assumptions documented in
-- sql/029_create_accounts_evolution_project_reports_schema.sql before
-- trusting them in production. Read-only; makes no changes.

-- 1) Actual data type of Id (and its declared precision/scale, if numeric).
--    Confirms/refutes the BIGINT assumption in sql/029_create_accounts_
--    evolution_project_reports_schema.sql.
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = 'vwProjectsReports'
  AND COLUMN_NAME = 'Id';

-- 2) Whether Id contains nulls. If this is > 0, replace_accounts_evolution_
--    project_reports() will refuse to load (validate_combined_for_full_
--    replace rejects missing id) -- confirming this here avoids finding
--    out for the first time during a production run.
SELECT COUNT(*) AS null_id_count
FROM dbo.vwProjectsReports
WHERE Id IS NULL;

-- 3) Whether Id is unique within this database. The destination primary
--    key is (company, id) -- id only needs to be unique WITHIN one
--    company's database, not globally, but confirm that here too.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT Id) AS distinct_ids,
    COUNT(*) - COUNT(DISTINCT Id) AS duplicate_row_count
FROM dbo.vwProjectsReports;

-- 3b) If duplicate_row_count above is > 0, list the offending Id values.
SELECT Id, COUNT(*) AS occurrences
FROM dbo.vwProjectsReports
GROUP BY Id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- 4) Whether DDate carries a time component. Confirms/refutes the DATE
--    (not TIMESTAMP) assumption in sql/029_create_accounts_evolution_
--    project_reports_schema.sql. DATA_TYPE alone is often decisive (e.g.
--    "date" vs "datetime"/"datetime2"); the second query is a value-level
--    cross-check and is safe to run even if DDate is already a plain date.
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = 'vwProjectsReports'
  AND COLUMN_NAME = 'DDate';

SELECT COUNT(*) AS rows_with_a_nonzero_time_component
FROM dbo.vwProjectsReports
WHERE CAST(DDate AS TIME) <> '00:00:00';

-- 5) Monetary column precision/scale vs. destination NUMERIC(20, 4).
--    Declared source type/precision/scale for the four money columns.
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = 'vwProjectsReports'
  AND COLUMN_NAME IN ('Credit', 'Debit', 'InclusiveAmount', 'TaxAmount');

-- 5b) Range check: NUMERIC(20, 4) allows at most 16 integer digits before
--     the decimal point (max magnitude ~= 9,999,999,999,999,999.9999). Any
--     row here would overflow the destination column.
SELECT COUNT(*) AS rows_exceeding_numeric_20_4_range
FROM dbo.vwProjectsReports
WHERE ABS(Credit) >= 1e16
   OR ABS(Debit) >= 1e16
   OR ABS(InclusiveAmount) >= 1e16
   OR ABS(TaxAmount) >= 1e16;

-- 5c) Scale check: rows where the source value carries more than 4 decimal
--     places, which sql/accounts/evolution/project_reports/extract_project_
--     reports.sql's CAST(... AS DECIMAL(20, 4)) would silently round.
SELECT COUNT(*) AS rows_with_excess_scale
FROM dbo.vwProjectsReports
WHERE TRY_CAST(Credit AS DECIMAL(20, 4)) <> TRY_CAST(Credit AS DECIMAL(38, 8))
   OR TRY_CAST(Debit AS DECIMAL(20, 4)) <> TRY_CAST(Debit AS DECIMAL(38, 8))
   OR TRY_CAST(InclusiveAmount AS DECIMAL(20, 4)) <> TRY_CAST(InclusiveAmount AS DECIMAL(38, 8))
   OR TRY_CAST(TaxAmount AS DECIMAL(20, 4)) <> TRY_CAST(TaxAmount AS DECIMAL(38, 8));
