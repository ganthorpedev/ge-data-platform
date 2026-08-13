-- Evolution Project Reports extraction query.
--
-- Ported verbatim (column list, order, and casts) from the working
-- evolution_extraction_pipeline notebook's build_select_columns()/
-- extract_data(). Executed once per configured Evolution source database
-- (GE, TLS) by connectors/accounts/evolution/project_reports.py -- the
-- query itself is identical for both companies; only the connection's
-- target database differs.
--
-- Money columns (Credit, Debit, InclusiveAmount, TaxAmount) are explicitly
-- CAST to DECIMAL(20, 4) here. Combined with reading results using
-- coerce_float=False in pandas, this returns Python decimal.Decimal values
-- instead of floats, preserving exact accounting precision from the source
-- through to the PostgreSQL NUMERIC destination columns.
--
-- {view_name} is substituted from EvolutionSettings.project_reports_view
-- (EVOLUTION_PROJECT_REPORTS_VIEW, default dbo.vwProjectsReports) -- an
-- operator-controlled configuration value, not user input.
SELECT
    [AccountDescription],
    [AccountTypeDescription],
    [CostType],
    CAST([Credit] AS DECIMAL(20, 4)) AS [Credit],
    [Customer],
    [CustomerUniqueID],
    [DDate],
    CAST([Debit] AS DECIMAL(20, 4)) AS [Debit],
    [Description],
    [FleetNumber],
    [Id],
    CAST([InclusiveAmount] AS DECIMAL(20, 4)) AS [InclusiveAmount],
    [Master_Sub_Account],
    [Module],
    [Project],
    [ProjectCode],
    [ProjectName],
    [QuantityInvoiced],
    [Reference],
    CAST([TaxAmount] AS DECIMAL(20, 4)) AS [TaxAmount],
    [TransactionDescription]
FROM {view_name};
