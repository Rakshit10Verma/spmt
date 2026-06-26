/*============================================================================
 * FILE: TC-30_analytic_remerge_ratio_running_rank.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - SAS PROC SQL REMERGE: aggregate + non-aggregate in SELECT without GROUP BY →
 *     Oracle raises ORA-00937; fix is AGG() OVER () window function
 *   - Ratio to portfolio: col / SUM(col) via REMERGE → col / SUM(col) OVER ()
 *   - Ratio to product group: col / (correlated subquery SUM per group) →
 *     col / SUM(col) OVER (PARTITION BY product_type)
 *   - Running total via correlated count: SUM where id <= outer.id → SUM() OVER (ORDER BY)
 *   - LAG emulation in PROC SQL: self-join on computed row number → LAG() OVER
 *   - Row numbering: COUNT(*) self-join on <= → ROW_NUMBER() OVER
 *   - Moving average via bounded correlated subquery (BETWEEN n-2 AND n) →
 *     AVG() OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
 *   - Percent rank via correlated COUNT ratio → PERCENT_RANK() OVER
 *   - PROC SQL OUTOBS=n with ORDER BY → FETCH FIRST n ROWS ONLY after ORDER BY
 *   - PROC SQL RESET NOPRINT → mid-session options change; no Oracle equivalent
 *   - PROC SQL NOPRINT on COUNT(*) INTO :macro → Oracle has no INTO; execute via bind
 *   - SUM(a, b) two-arg NULL-safe add → COALESCE(a,0) + COALESCE(b,0)
 *   - CALCULATED alias references → repeat full expression in Oracle
 *   - SELECT INTO :macrovar TRIMMED → infrastructure; remove from output SQL
 *   - &macro_var. → ${prop_varname}
 *   - SAS comparison operators (ge, le) → >=, <=
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - SAS PROC SQL REMERGE warning: SAS silently attaches grand-total aggregates back to
 *     every detail row; Oracle raises ORA-00937 (not a single-group group function); the
 *     FIX is NEVER to add GROUP BY (which collapses rows and changes results) but to use
 *     window functions: SUM(col) OVER (), AVG(col) OVER (), etc.
 *   - LAG emulation: SAS PROC SQL has no LAG() function; SAS coders generate a row number
 *     via a self-join (COUNT of rows with id <= current id) then join on rn-1; the Oracle
 *     form is simply LAG(col) OVER (PARTITION BY key ORDER BY seq); LLMs often miss the
 *     PARTITION BY boundary on the self-join and produce incorrect cross-partition lag
 *   - Moving average via correlated subquery: the BETWEEN month_num-2 AND month_num range
 *     is the frame; Oracle ROWS BETWEEN 2 PRECEDING AND CURRENT ROW is equivalent but
 *     LLMs often emit RANGE BETWEEN (which uses value distances, not row counts)
 *   - PROC SQL OUTOBS= applies a row limit to the result set independent of ORDER BY;
 *     Oracle FETCH FIRST must appear AFTER ORDER BY; LLMs sometimes put it before ORDER BY
 *     or emit ROWNUM < n in a WHERE which interacts badly with ORDER BY
 *   - Percent rank via correlated COUNT / total: this is an approximation pattern in SAS;
 *     Oracle PERCENT_RANK() OVER is exact; the correlated-subquery form can return values
 *     > 1 in edge cases due to ties; LLMs should prefer PERCENT_RANK() but often keep the
 *     correlated form which is harder to read and subtly different for tied values
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET top_n = 20;

PROC SQL NOPRINT;
    SELECT COUNT(*) INTO :total_contracts TRIMMED
    FROM source_data.contract_header
    WHERE client_code = "&client_code."
      AND period_key  = &report_date.
      AND contract_status NOT IN ('CANCELLED', 'REJECTED');
QUIT;

PROC SQL;
    CREATE TABLE work.exposure_with_ratios AS
    SELECT
        t1.contract_id,
        t1.product_type,
        t1.org_unit_id,
        t1.approved_amount,
        t1.credit_score,
        t1.booking_date,
        SUM(t1.approved_amount)                                    AS portfolio_grand_total,
        AVG(t1.approved_amount)                                    AS portfolio_avg_amount,
        t1.approved_amount / SUM(t1.approved_amount)               AS pct_of_portfolio,
        t1.approved_amount / (
            SELECT SUM(t2.approved_amount)
            FROM source_data.contract_header t2
            WHERE t2.product_type    = t1.product_type
              AND t2.period_key      = &report_date.
              AND t2.client_code     = "&client_code."
              AND t2.contract_status NOT IN ('CANCELLED', 'REJECTED')
        ) AS pct_of_product_type,
        t1.approved_amount / (
            SELECT SUM(t3.approved_amount)
            FROM source_data.contract_header t3
            WHERE t3.org_unit_id    = t1.org_unit_id
              AND t3.period_key     = &report_date.
              AND t3.client_code    = "&client_code."
              AND t3.contract_status NOT IN ('CANCELLED', 'REJECTED')
        ) AS pct_of_org_unit,
        (
            SELECT COUNT(*)
            FROM source_data.contract_header t4
            WHERE t4.approved_amount <= t1.approved_amount
              AND t4.period_key       = &report_date.
              AND t4.client_code      = "&client_code."
              AND t4.contract_status NOT IN ('CANCELLED', 'REJECTED')
        ) / &total_contracts. AS amount_percentile_rank
    FROM source_data.contract_header t1
    WHERE t1.client_code     = "&client_code."
      AND t1.period_key      = &report_date.
      AND t1.contract_status NOT IN ('CANCELLED', 'REJECTED');
QUIT;

PROC SQL;
    CREATE TABLE work.numbered_monthly AS
    SELECT
        t1.contract_id,
        t1.month_num,
        t1.month_balance,
        t1.org_unit_id,
        COUNT(*) AS rn
    FROM work.contract_monthly_stats t1
    LEFT JOIN work.contract_monthly_stats t2
        ON t1.contract_id = t2.contract_id
       AND t1.month_num   >= t2.month_num
    GROUP BY t1.contract_id,
             t1.month_num,
             t1.month_balance,
             t1.org_unit_id;
QUIT;

PROC SQL;
    CREATE TABLE work.monthly_with_lag_and_mavg AS
    SELECT
        t1.contract_id,
        t1.month_num,
        t1.month_balance,
        t1.rn,
        t2.month_balance    AS prior_month_balance,
        SUM(t1.month_balance, -t2.month_balance)
                            AS month_over_month_change,
        (
            SELECT AVG(t3.month_balance)
            FROM work.numbered_monthly t3
            WHERE t3.contract_id = t1.contract_id
              AND t3.month_num BETWEEN t1.month_num - 2 AND t1.month_num
        ) AS moving_avg_3m,
        (
            SELECT SUM(t4.month_balance)
            FROM work.numbered_monthly t4
            WHERE t4.contract_id = t1.contract_id
              AND t4.month_num  <= t1.month_num
        ) AS running_total_balance,
        (
            SELECT COUNT(*)
            FROM work.numbered_monthly t5
            WHERE t5.contract_id  = t1.contract_id
              AND t5.month_balance <= t1.month_balance
        ) AS balance_rank_within_contract
    FROM work.numbered_monthly t1
    LEFT JOIN work.numbered_monthly t2
        ON t1.contract_id = t2.contract_id
       AND t1.rn = t2.rn + 1
    ORDER BY t1.contract_id,
             t1.month_num;
QUIT;

PROC SQL RESET NOPRINT;
    SELECT COUNT(*) INTO :flagged_contracts TRIMMED
    FROM work.exposure_with_ratios
    WHERE pct_of_portfolio ge 0.05;
QUIT;

PROC SQL OUTOBS=&top_n.;
    CREATE TABLE work.top_concentration_risks AS
    SELECT
        t1.contract_id,
        t1.product_type,
        t1.org_unit_id,
        t1.approved_amount,
        t1.pct_of_portfolio,
        t1.pct_of_product_type,
        t1.pct_of_org_unit,
        t1.amount_percentile_rank,
        t2.moving_avg_3m,
        t2.running_total_balance,
        t2.month_over_month_change,
        CASE
            WHEN t1.pct_of_portfolio ge 0.05 THEN 'CONCENTRATION_BREACH'
            WHEN t1.pct_of_product_type ge 0.15 THEN 'PRODUCT_CONCENTRATION'
            WHEN t1.pct_of_org_unit ge 0.20 THEN 'ORG_CONCENTRATION'
            ELSE 'WITHIN_LIMITS'
        END AS concentration_flag,
        SUM(t1.approved_amount, t1.pct_of_portfolio * t1.portfolio_grand_total)
            / 2 AS blended_exposure_estimate
    FROM work.exposure_with_ratios t1
    LEFT JOIN work.monthly_with_lag_and_mavg t2
        ON t1.contract_id = t2.contract_id
       AND t2.month_num = 12
    ORDER BY t1.pct_of_portfolio DESC,
             t1.approved_amount DESC;
QUIT;
