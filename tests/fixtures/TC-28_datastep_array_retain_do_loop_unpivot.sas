/*============================================================================
 * FILE: TC-28_datastep_array_retain_do_loop_unpivot.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - DATA step ARRAY{n} col_1 ... col_n + DO i = 1 TO n / OUTPUT → Oracle UNPIVOT
 *     or 12-branch UNION ALL (one SELECT per month column)
 *   - VNAME(array{i}) → hardcoded string for the column name; no Oracle equivalent
 *   - RETAIN accumulator reset on FIRST.group → SUM() OVER (PARTITION BY key
 *     ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
 *   - FIRST.by_variable, LAST.by_variable in DATA step → ROW_NUMBER() = 1 / last row
 *   - LAG(col) in DATA step with FIRST. reset → LAG(col) OVER (PARTITION BY key ORDER BY seq)
 *   - SUM(a, b) two-arg NULL-safe add inside DATA step → COALESCE(a,0) + COALESCE(b,0)
 *   - DATA step MERGE (IN=a) (IN=b) BY key; IF a; → LEFT JOIN with a as driving table
 *   - DATA step SET ... (WHERE=(...)) → inline filter
 *   - KEEP / DROP statement → SELECT explicit column list
 *   - PROC SORT DATA= OUT= → ORDER BY in subquery or separate step
 *   - PUT(month_idx, MONNAME3.) inside DO loop → TO_CHAR(ADD_MONTHS(...), 'Mon')
 *   - INTNX('MONTH', start_date, i-1, 'B') → ADD_MONTHS(TRUNC(start_date,'MM'), i-1)
 *   - MDY(1, 1, year) for year-start date → TO_DATE(year||'0101','YYYYMMDD')
 *   - NOT MISSING(col) → col IS NOT NULL
 *   - PROC SQL on DATA step output → standard SELECT on the unpivoted table
 *   - &macro_var. → ${prop_varname}
 *   - CALCULATED in GROUP BY / ORDER BY → repeat full expression
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - DATA step ARRAY + DO + OUTPUT is the SAS idiom for wide-to-long (column unpivot);
 *     Oracle UNPIVOT clause or UNION ALL expansion is the equivalent; LLMs frequently
 *     attempt PL/SQL ARRAY syntax or leave the columns still wide
 *   - VNAME(array{i}) returns the *name* of the i-th array element as a string at compile
 *     time (e.g., 'BAL_M01'); there is no Oracle runtime equivalent — the column names
 *     must be hardcoded into the UNPIVOT or UNION ALL; LLMs often leave VNAME() in output
 *   - RETAIN with FIRST.group reset: RETAIN keeps the previous value between rows, and
 *     FIRST. triggers a reset at the start of each BY group; Oracle SUM() OVER() with
 *     PARTITION BY achieves this but the frame must include the current row
 *   - DATA step LAG() inside a BY-group with FIRST.group reset: SAS LAG() returns the
 *     previous row's value and is set to missing at FIRST.group; Oracle LAG() OVER()
 *     with PARTITION BY achieves the same; LLMs often use a subquery instead and
 *     miss the partition boundary reset
 *   - DATA step MERGE (IN=a)(IN=b); IF a; is a LEFT JOIN keeping only a-side rows;
 *     LLMs sometimes emit an INNER JOIN or keep both sides
 *============================================================================*/

%LET report_year = 2025;
%LET client_code = NORTH;

DATA work.balance_long;
    SET source_data.contract_monthly_wide (
        WHERE=(client_code = "&client_code." AND year_key = &report_year.)
    );
    ARRAY bal_arr{12}  bal_m01  bal_m02  bal_m03  bal_m04  bal_m05  bal_m06
                       bal_m07  bal_m08  bal_m09  bal_m10  bal_m11  bal_m12;
    ARRAY rate_arr{12} rate_m01 rate_m02 rate_m03 rate_m04 rate_m05 rate_m06
                       rate_m07 rate_m08 rate_m09 rate_m10 rate_m11 rate_m12;
    DO month_idx = 1 TO 12;
        month_num        = month_idx;
        month_label      = PUT(month_idx, MONNAME3.);
        month_date       = INTNX('MONTH', MDY(1, 1, &report_year.), month_idx - 1, 'B');
        month_balance    = bal_arr{month_idx};
        month_rate       = rate_arr{month_idx};
        balance_col_name = VNAME(bal_arr{month_idx});
        IF NOT MISSING(month_balance) THEN OUTPUT;
    END;
    KEEP contract_id product_type org_unit_id year_key client_code
         month_num month_label month_date month_balance month_rate balance_col_name;
RUN;

PROC SORT DATA=work.balance_long;
    BY contract_id month_num;
RUN;

DATA work.balance_with_running;
    SET work.balance_long;
    BY contract_id month_num;
    RETAIN running_balance 0 prior_month_balance .;
    IF FIRST.contract_id THEN DO;
        running_balance     = 0;
        prior_month_balance = .;
    END;
    prior_month_balance    = LAG(month_balance);
    IF FIRST.contract_id THEN prior_month_balance = .;
    running_balance        = SUM(running_balance, month_balance);
    mom_change             = SUM(month_balance, -prior_month_balance);
RUN;

DATA work.balance_enriched;
    MERGE work.balance_with_running (IN=a)
          source_data.contract_header (
              IN=b
              KEEP=contract_id approved_amount booking_date interest_rate
              WHERE=(client_code = "&client_code.")
          );
    BY contract_id;
    IF a;
    utilisation_ratio = month_balance / approved_amount;
RUN;

PROC SQL;
    CREATE TABLE work.contract_monthly_stats AS
    SELECT
        t1.contract_id,
        t1.product_type,
        t1.org_unit_id,
        t1.month_num,
        t1.month_label,
        t1.month_date,
        t1.month_balance,
        t1.running_balance,
        t1.mom_change,
        t1.month_rate,
        t1.balance_col_name,
        t1.approved_amount,
        t1.booking_date,
        t1.utilisation_ratio,
        CASE
            WHEN t1.month_balance < 0                       THEN 'OVERDRAFT'
            WHEN t1.month_balance = 0                       THEN 'ZERO'
            WHEN t1.utilisation_ratio ge 0.90               THEN 'NEAR_LIMIT'
            WHEN t1.utilisation_ratio ge 0.50               THEN 'MODERATE'
            ELSE 'LOW'
        END AS utilisation_band
    FROM work.balance_enriched t1
    WHERE t1.month_balance IS NOT MISSING;
QUIT;

PROC SQL;
    CREATE TABLE work.monthly_org_summary AS
    SELECT
        org_unit_id,
        month_num,
        month_label,
        COUNT(DISTINCT contract_id)                                   AS active_contracts,
        SUM(month_balance)                                            AS total_balance,
        AVG(month_balance)                                            AS avg_balance,
        SUM(CASE WHEN utilisation_band = 'NEAR_LIMIT' THEN 1 ELSE 0 END)
                                                                      AS near_limit_count,
        SUM(CASE WHEN utilisation_band = 'OVERDRAFT'  THEN 1 ELSE 0 END)
                                                                      AS overdraft_count,
        SUM(CASE WHEN mom_change > 0 THEN mom_change ELSE 0 END)     AS total_growth,
        SUM(CASE WHEN mom_change < 0 THEN mom_change ELSE 0 END)     AS total_decline,
        MAX(running_balance)                                          AS peak_running_balance
    FROM work.contract_monthly_stats
    GROUP BY org_unit_id,
             month_num,
             month_label
    ORDER BY org_unit_id,
             month_num;
QUIT;

PROC SQL;
    CREATE TABLE work.balance_trend_flags AS
    SELECT
        t1.contract_id,
        t1.product_type,
        COUNT(*)                                                           AS months_observed,
        SUM(t1.month_balance)                                              AS total_balance_sum,
        MAX(t1.month_balance)                                              AS peak_balance,
        MIN(CASE WHEN t1.month_num = 1  THEN t1.month_balance ELSE . END) AS jan_balance,
        MIN(CASE WHEN t1.month_num = 12 THEN t1.month_balance ELSE . END) AS dec_balance,
        SUM(CASE WHEN t1.mom_change > 0 THEN 1 ELSE 0 END)               AS months_growing,
        SUM(CASE WHEN t1.mom_change < 0 THEN 1 ELSE 0 END)               AS months_declining,
        CASE
            WHEN SUM(CASE WHEN t1.mom_change > 0 THEN 1 ELSE 0 END) >= 9 THEN 'STRONG_GROWTH'
            WHEN SUM(CASE WHEN t1.mom_change < 0 THEN 1 ELSE 0 END) >= 9 THEN 'STRONG_DECLINE'
            ELSE 'MIXED'
        END AS trend_classification
    FROM work.contract_monthly_stats t1
    GROUP BY t1.contract_id,
             t1.product_type
    HAVING COUNT(*) = 12
    ORDER BY CALCULATED trend_classification,
             t1.product_type;
QUIT;
