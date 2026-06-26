/*============================================================================
 * FILE: TC-25_statistical_aggregation_rollup_groupingsets.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - N(col) → COUNT(col)  (count of non-missing numeric values)
 *   - NMISS(col) → SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END)
 *   - CMISS(col1,col2,...) → multi-column NULL check sum
 *   - STD(col) → STDDEV(col)
 *   - VAR(col) → VARIANCE(col)
 *   - MEAN(col) → AVG(col)
 *   - CSS(col) → VARIANCE(col) * (COUNT(col) - 1)   (corrected sum of squares)
 *   - CV(col) → STDDEV(col) / NULLIF(AVG(col), 0) * 100   (coefficient of variation)
 *   - USS(col) → SUM(col * col)  (uncorrected sum of squares)
 *   - SKEWNESS(col) → no Oracle native; complex formula or NULL placeholder
 *   - KURTOSIS(col) → no Oracle native; complex formula or NULL placeholder
 *   - RANGE(col) → MAX(col) - MIN(col)
 *   - SUM(a, b, c) multi-arg NULL-safe → COALESCE(a,0) + COALESCE(b,0) + COALESCE(c,0)
 *   - PROC MEANS CLASS BY → GROUP BY ROLLUP / GROUPING SETS equivalent
 *   - GROUPING() function in Oracle → detect rollup NULL rows
 *   - PROC MEANS ALL → GROUP BY () (grand total)
 *   - PROC MEANS WAYS → GROUPING SETS with specified combinations
 *   - CMISS across multiple columns → COUNT(*) - COUNT(CASE WHEN all non-null END)
 *   - &macro_var. → ${prop_varname}
 *   - CALCULATED in GROUP BY → repeat expression
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - CSS, USS, CV, SKEWNESS, KURTOSIS are PROC SQL aggregate functions unique to SAS;
 *     LLMs often invent non-existent Oracle functions like SKEWNESS() or emit nothing
 *   - SUM(a, b, c) with multiple arguments is SAS-specific NULL-safe addition;
 *     Oracle SUM() only takes one argument — LLMs sometimes write SUM(a, b) which
 *     is a syntax error in Oracle
 *   - PROC MEANS CLASS BY with multiple class variables generates a ROLLUP-style output
 *     with subtotals per class combination; the Oracle ROLLUP / GROUPING SETS syntax
 *     is structurally very different and LLMs typically just emit GROUP BY
 *   - GROUPING() returns 1 for NULL rows created by ROLLUP, 0 otherwise; LLMs miss this
 *   - N() vs COUNT(): SAS N() ignores MISSING, Oracle COUNT() ignores NULL — same
 *     semantics, but N() is the SAS name; LLMs sometimes leave N() unconverted
 *   - NMISS() must become a conditional SUM, not COUNT(NULL) which always returns 0
 *============================================================================*/

%LET report_period = 202505;
%LET client_code = NORTH;
%LET min_obs_threshold = 10;

PROC SQL;
   CREATE TABLE work.portfolio_raw_stats AS
   SELECT
       t1.product_type,
       t1.risk_category,
       t1.org_unit_id,
       COUNT(t1.contract_id) AS n_contracts,
       N(t1.approved_amount) AS n_amount_nonmissing,
       NMISS(t1.approved_amount) AS n_amount_missing,
       CMISS(t1.approved_amount, t1.interest_rate, t1.maturity_months)
           AS n_key_fields_missing,
       SUM(t1.approved_amount) AS sum_approved,
       SUM(t1.principal_balance, t1.interest_balance, t1.fee_balance)
           AS total_outstanding,
       MEAN(t1.approved_amount) AS mean_approved,
       STD(t1.approved_amount) AS std_approved,
       VAR(t1.approved_amount) AS var_approved,
       CSS(t1.approved_amount) AS css_approved,
       USS(t1.approved_amount) AS uss_approved,
       CV(t1.approved_amount) AS cv_approved_pct,
       RANGE(t1.approved_amount) AS range_approved,
       MIN(t1.approved_amount) AS min_approved,
       MAX(t1.approved_amount) AS max_approved,
       SKEWNESS(t1.approved_amount) AS skewness_approved,
       KURTOSIS(t1.approved_amount) AS kurtosis_approved,
       N(t1.interest_rate) AS n_rate_nonmissing,
       MEAN(t1.interest_rate) AS mean_rate,
       STD(t1.interest_rate) AS std_rate
   FROM source_data.contract_header t1
   WHERE t1.client_code = "&client_code."
     AND t1.period_key = &report_period.
     AND t1.contract_status NE 'CANCELLED'
   GROUP BY
       t1.product_type,
       t1.risk_category,
       t1.org_unit_id;
QUIT;

PROC SQL;
   CREATE TABLE work.portfolio_rollup_by_product AS
   SELECT
       product_type,
       risk_category,
       SUM(n_contracts) AS total_contracts,
       SUM(sum_approved) AS total_approved,
       SUM(n_amount_nonmissing) AS total_nonmissing,
       SUM(n_amount_missing) AS total_missing,
       CASE
           WHEN SUM(n_contracts) ge &min_obs_threshold.
               THEN SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0)
           ELSE NULL
       END AS weighted_mean_approved,
       SQRT(
           SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0)
       ) AS pooled_std_approved,
       SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0)
           AS pooled_var_approved,
       MAX(max_approved) AS overall_max,
       MIN(min_approved) AS overall_min,
       MAX(max_approved) - MIN(min_approved) AS overall_range
   FROM work.portfolio_raw_stats
   GROUP BY
       product_type,
       risk_category;
QUIT;

PROC SQL;
   CREATE TABLE work.portfolio_multidim_rollup AS
   SELECT
       product_type,
       risk_category,
       org_unit_id,
       SUM(n_contracts) AS contract_count,
       SUM(sum_approved) AS total_approved,
       SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0) AS mean_approved,
       SQRT(SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0))
           AS std_approved,
       SQRT(SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0))
           / NULLIF(
               SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0),
               0) * 100 AS cv_pct,
       'product+risk+org' AS rollup_level
   FROM work.portfolio_raw_stats
   GROUP BY product_type, risk_category, org_unit_id
   OUTER UNION CORR
   SELECT
       product_type,
       risk_category,
       NULL AS org_unit_id,
       SUM(n_contracts),
       SUM(sum_approved),
       SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0),
       SQRT(SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0)),
       SQRT(SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0))
           / NULLIF(SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0), 0) * 100,
       'product+risk'
   FROM work.portfolio_raw_stats
   GROUP BY product_type, risk_category
   OUTER UNION CORR
   SELECT
       product_type,
       NULL AS risk_category,
       NULL AS org_unit_id,
       SUM(n_contracts),
       SUM(sum_approved),
       SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0),
       SQRT(SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0)),
       SQRT(SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0))
           / NULLIF(SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0), 0) * 100,
       'product_only'
   FROM work.portfolio_raw_stats
   GROUP BY product_type
   OUTER UNION CORR
   SELECT
       NULL AS product_type,
       NULL AS risk_category,
       NULL AS org_unit_id,
       SUM(n_contracts),
       SUM(sum_approved),
       SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0),
       SQRT(SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0)),
       SQRT(SUM(css_approved) / NULLIF(SUM(n_amount_nonmissing) - 1, 0))
           / NULLIF(SUM(sum_approved) / NULLIF(SUM(n_amount_nonmissing), 0), 0) * 100,
       'grand_total'
   FROM work.portfolio_raw_stats;
QUIT;

PROC SQL;
   CREATE TABLE work.quality_flag_summary AS
   SELECT
       t1.product_type,
       COUNT(*) AS org_unit_count,
       SUM(t1.n_contracts) AS total_contracts,
       SUM(t1.n_amount_missing) AS total_amount_missing,
       SUM(t1.n_key_fields_missing) AS total_key_missing,
       CASE
           WHEN SUM(t1.n_key_fields_missing) / NULLIF(SUM(t1.n_contracts) * 3, 0) gt 0.05
               THEN 'DATA_QUALITY_ALERT'
           WHEN SUM(t1.n_amount_missing) / NULLIF(SUM(t1.n_contracts), 0) gt 0.02
               THEN 'COMPLETENESS_WARNING'
           ELSE 'ACCEPTABLE'
       END AS data_quality_flag,
       SUM(t1.n_contracts) - SUM(CASE WHEN t1.n_amount_missing > 0 THEN 1 ELSE 0 END)
           AS complete_org_units
   FROM work.portfolio_raw_stats t1
   GROUP BY CALCULATED product_type
   HAVING SUM(t1.n_contracts) ge &min_obs_threshold.
   ORDER BY CALCULATED product_type;
QUIT;
