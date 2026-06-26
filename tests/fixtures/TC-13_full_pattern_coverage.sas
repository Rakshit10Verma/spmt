/*******************************************************************************
 * FILE: TC-13_full_pattern_coverage.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - %LET / %GLOBAL macro declarations → Pentaho parameters
 *   - &macro_var. substitution → ${prop_variable}
 *   - IS MISSING / IS NOT MISSING → IS NULL / IS NOT NULL
 *   - INTNX('MONTH', d, n, 'END') → LAST_DAY(ADD_MONTHS())
 *   - INTNX('MONTH', d, n, 'B')   → TRUNC(ADD_MONTHS(), 'MM')
 *   - INTNX('QTR', d, n, 'END')   → LAST_DAY(ADD_MONTHS(TRUNC(d,'Q'), n))
 *   - INTNX('YEAR', d, n, 'END')  → LAST_DAY(ADD_MONTHS(TRUNC(d,'YEAR'), n*12))
 *   - INTNX('DAY', d, 1)          → d + 1
 *   - IFN() / IFC()               → CASE WHEN ... THEN ... ELSE ... END
 *   - DATDIF(d1, d2, 'ACT/ACT')   → d2 - d1
 *   - MDY(m, d, y)                → TO_DATE(y||LPAD(m,2,'0')||LPAD(d,2,'0'),'YYYYMMDD')
 *   - YEAR() / MONTH()            → EXTRACT(YEAR FROM ...) / EXTRACT(MONTH FROM ...)
 *   - SAS date literal '01JAN2025'd → TO_DATE('2025-01-01','YYYY-MM-DD')
 *   - today()                     → TRUNC(SYSDATE)
 *   - INPUT(x, BEST.) / INPUT(x, DATE9.) → TO_NUMBER() / TO_DATE()
 *   - PUT(x, format.)             → TO_CHAR() or CASE WHEN lookup
 *   - COMPRESS(x)                 → REPLACE(x,' ','')
 *   - COMPRESS(x, '', 'kd')       → REGEXP_REPLACE(x,'[^0-9]','')
 *   - COMPRESS(x, '', 'ka')       → REGEXP_REPLACE(x,'[^A-Za-z]','')
 *   - SCAN(str, n, delim)         → REGEXP_SUBSTR()
 *   - CATS() / CATX()             → TRIM() || ... concatenation
 *   - STRIP()                     → TRIM()
 *   - UPCASE() / LOWCASE()        → UPPER() / LOWER()
 *   - TRANWRD()                   → REPLACE()
 *   - INDEX()                     → INSTR()
 *   - SUBSTR()                    → SUBSTR() (same, verify 1-based)
 *   - INT()                       → TRUNC()
 *   - ROUND(x, 0.001)             → ROUND(x, 3)  [SAS decimal arg → Oracle integer]
 *   - Multi-arg SAS sum()         → NVL(a,0) + NVL(b,0) pattern
 *   - CALCULATED keyword           → repeat full expression (Oracle disallows alias in GROUP BY)
 *   - ORDER BY inside CTAS         → removed (ORDER BY only in final SELECT)
 *   - OUTER UNION CORR             → UNION ALL with explicit NULL column padding
 *   - PROC TRANSPOSE equivalent    → CASE WHEN pivot / UNION ALL unpivot
 *   - Dynamic period-suffix table  → STAGE_HIST_... with idwh_berichtszeit filter
 *   - Custom SAS format PUT()      → full CASE WHEN translation
 *   - RETAIN / BY FIRST. LAST.    → SUM() OVER (PARTITION BY ... ORDER BY ...)
 *   - Correlated subquery rewrite  → ROW_NUMBER() to avoid Pentaho JDBC failure
 *   - GUELTIG_BIS exclusive end    → GUELTIG_BIS > date  (not >=)
 *   - RIGHT JOIN + IS NULL trap    → AND pk_col IS NOT NULL guard added
 *   - LABEL= / FORMAT= on columns  → removed entirely
 *   - SAS name literal ('col'n)    → renamed to valid Oracle identifier
 *   - FROM DUAL                    → FROM sys.dual
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. ROUND(x, 0.001) looks like 3-decimal precision in SAS but Oracle
 *      truncates the second argument to integer → ROUND(x, 3).
 *   2. COMPRESS(...,'kd') keeps only digits; Oracle uses REGEXP_REPLACE
 *      with a negated character class.
 *   3. OUTER UNION CORR aligns by column name, not position — Oracle
 *      UNION ALL requires explicit NULL AS col_name padding.
 *   4. GUELTIG_BIS is stored as the first day the record is NO LONGER
 *      valid (exclusive end). Must use > not >= when filtering.
 *   5. Correlated subquery with MAX() inside WHERE can silently return
 *      wrong results via Pentaho JDBC; rewrite with ROW_NUMBER().
 *   6. SAS RETAIN + BY FIRST./LAST. running totals have no single Oracle
 *      equivalent; use analytic SUM() OVER (PARTITION BY ... ORDER BY ...).
 *   7. PROC TRANSPOSE wide→long requires a UNION ALL per column in Oracle;
 *      column labels from the SAS LABEL= attribute are lost.
 ******************************************************************************/

LIBNAME SOURCE  META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Source Data']";
LIBNAME STAGING META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Staging Tables']";
LIBNAME DWH     META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Data Warehouse']";

%macro _eg_conditional_dropds /parmbuff;
    %local num dsname;
    %let num=1;
    %let dsname=%qscan(&syspbuff,&num,',()');
    %do %while(&dsname ne);
        %if %sysfunc(exist(&dsname)) %then %do;
            proc sql; drop table &dsname; quit;
        %end;
        %let num=%eval(&num+1);
        %let dsname=%qscan(&syspbuff,&num,',()');
    %end;
%mend _eg_conditional_dropds;

%GLOBAL gPeriodEnd;
%GLOBAL gPeriodStart;
%GLOBAL gTenant;
%GLOBAL gReportYear;
%GLOBAL gReportMonth;

%LET gPeriodEnd   = &DWH_PERIOD_END_DATE.;
%LET gPeriodStart = %SYSFUNC(INTNX(MONTH, &gPeriodEnd., -11, B));
%LET gTenant      = NORTH;
%LET gReportYear  = %SYSFUNC(YEAR(&gPeriodEnd.));
%LET gReportMonth = %SYSFUNC(MONTH(&gPeriodEnd.));

%_eg_conditional_dropds(WORK.BASE_ORDERS);

PROC SQL;
    CREATE TABLE WORK.BASE_ORDERS AS
    SELECT
        t1.order_id,
        t1.customer_id,
        t1.product_code,
        t1.order_date,
        t1.delivery_date,
        t1.amount,
        t1.currency_code,
        t1.status_code,
        t1.region_code
    FROM staging.ORDERS_&gPeriodeTable. t1
    WHERE t1.order_date <= &gPeriodEnd.
      AND t1.order_date >= '01JAN2020'd
      AND NOT (t1.status_code IS MISSING)
      AND t1.delivery_date IS NOT MISSING;
QUIT;

%_eg_conditional_dropds(WORK.DATE_INTERVALS);

PROC SQL;
    CREATE TABLE WORK.DATE_INTERVALS AS
    SELECT
        t1.contract_id,
        t1.start_date,
        t1.end_date,
        (INTNX('MONTH', t1.end_date, 0, 'END'))                 AS period_month_end,
        (INTNX('MONTH', t1.start_date, -1, 'B'))                AS prev_month_start,
        (INTNX('QTR', t1.end_date, -1, 'END'))                  AS prev_qtr_end,
        (INTNX('YEAR', t1.end_date, 1, 'END'))                  AS next_year_end,
        (INTNX('DAY', t1.start_date, 1))                        AS next_day,
        (MDY(&gReportMonth., 1, &gReportYear.))                  AS report_period_start,
        (YEAR(t1.start_date))                                    AS contract_year,
        (MONTH(t1.start_date))                                   AS contract_month,
        (DATDIF(t1.start_date, t1.end_date, 'ACT/ACT'))         AS days_outstanding,
        (DATDIF(t1.start_date, t1.end_date, 'ACT/ACT') + 1)    AS days_inclusive
    FROM staging.CONTRACTS t1
    WHERE t1.start_date <= &gPeriodEnd.
      AND t1.tenant_code = "&gTenant.";
QUIT;

%_eg_conditional_dropds(WORK.PRODUCT_STRINGS);

PROC SQL;
    CREATE TABLE WORK.PRODUCT_STRINGS AS
    SELECT
        t1.product_id,
        (COMPRESS(t1.product_code))                              AS product_code_clean,
        (COMPRESS(t1.serial_number, '', 'kd'))                   AS serial_digits_only,
        (COMPRESS(t1.serial_number, '', 'ka'))                   AS serial_letters_only,
        (SCAN(t1.product_code, 2, '-'))                          AS product_segment,
        (CATS(t1.category_prefix, t1.product_id))               AS full_product_key,
        (CATX('-', t1.region_code, t1.product_code))             AS region_product_key,
        (STRIP(t1.description))                                  AS description_trimmed,
        (UPCASE(t1.status_code))                                 AS status_upper,
        (LOWCASE(t1.category_prefix))                            AS category_lower,
        (TRANWRD(t1.description, 'OLD', 'NEW'))                  AS description_updated,
        (INDEX(t1.description, 'SPECIAL'))                       AS special_flag_pos,
        (SUBSTR(t1.product_code, 1, 3))                          AS product_prefix,
        (INPUT(t1.amount_text, BEST.))                           AS amount_numeric,
        (PUT(t1.product_id, Z5.))                                AS product_id_padded
    FROM staging.PRODUCTS t1;
QUIT;

%_eg_conditional_dropds(WORK.ORDER_VALUES);

PROC SQL;
    CREATE TABLE WORK.ORDER_VALUES AS
    SELECT
        t1.order_id,
        t1.customer_id,
        t1.amount,
        t1.discount,
        t1.tax,
        t1.surcharge,
        (IFN(t1.discount IS MISSING, t1.amount, t1.amount - t1.discount))
                                                                 AS net_amount,
        (IFC(t1.amount > 10000, 'HIGH', 'NORMAL'))              AS value_category,
        (INT(t1.amount / 1000))                                  AS amount_thousands,
        (ROUND(t1.amount / 1000, 0.001))                        AS amount_teur,
        (SUM(t1.amount, t1.discount, t1.surcharge))              AS total_components
    FROM WORK.BASE_ORDERS t1;
QUIT;

%_eg_conditional_dropds(WORK.ORDER_STATUS_LABELS);

PROC SQL;
    CREATE TABLE WORK.ORDER_STATUS_LABELS AS
    SELECT
        t1.order_id,
        t1.status_code,
        (PUT(t1.status_code, $STATUSFMT.))                       AS status_label,
        (PUT(t1.region_code, REGIONFMT.))                        AS region_label,
        t1.amount,
        t1.amount                                                AS amount_formatted
    FROM WORK.ORDER_VALUES t1;
QUIT;

%_eg_conditional_dropds(WORK.REGION_SUMMARY);

PROC SQL;
    CREATE TABLE WORK.REGION_SUMMARY AS
    SELECT
        t1.region_code,
        (PUT(t1.region_code, REGIONFMT.))                        AS region_label,
        (SUM(t1.amount))                                         AS total_amount,
        (COUNT(t1.order_id))                                     AS order_count,
        (ROUND(SUM(t1.amount) / 1000, 0.001))                    AS total_amount_teur
    FROM WORK.ORDER_STATUS_LABELS t1
    GROUP BY
        t1.region_code,
        (PUT(t1.region_code, REGIONFMT.))
    HAVING
        (SUM(t1.amount)) > 0
    ORDER BY total_amount_teur DESC;
QUIT;

%_eg_conditional_dropds(WORK.ALL_POSITIONS);

PROC SQL;
    CREATE TABLE WORK.ALL_POSITIONS AS
    SELECT
        order_id,
        customer_id,
        product_code,
        amount,
        'SECURITY'   AS position_type,
        yield_rate,
        duration_years,
        NULL         AS deposit_rate
    FROM WORK.SECURITIES_POSITIONS

    OUTER UNION CORR

    SELECT
        order_id,
        customer_id,
        product_code,
        amount,
        'DEPOSIT'    AS position_type,
        NULL         AS yield_rate,
        NULL         AS duration_years,
        deposit_rate
    FROM WORK.DEPOSIT_POSITIONS

    OUTER UNION CORR

    SELECT
        order_id,
        customer_id,
        product_code,
        amount,
        'FUND'       AS position_type,
        fund_yield   AS yield_rate,
        NULL         AS duration_years,
        NULL         AS deposit_rate
    FROM WORK.FUND_POSITIONS;
QUIT;

%_eg_conditional_dropds(WORK.RATE_CURVE_LONG);

PROC SQL;
    CREATE TABLE WORK.RATE_CURVE_LONG AS
    SELECT 'rate_1m'  AS tenor_code, t.rate_1m  AS rate_value FROM WORK.RATE_CURVE_WIDE t UNION ALL
    SELECT 'rate_3m'  AS tenor_code, t.rate_3m  AS rate_value FROM WORK.RATE_CURVE_WIDE t UNION ALL
    SELECT 'rate_6m'  AS tenor_code, t.rate_6m  AS rate_value FROM WORK.RATE_CURVE_WIDE t UNION ALL
    SELECT 'rate_1y'  AS tenor_code, t.rate_1y  AS rate_value FROM WORK.RATE_CURVE_WIDE t UNION ALL
    SELECT 'rate_2y'  AS tenor_code, t.rate_2y  AS rate_value FROM WORK.RATE_CURVE_WIDE t UNION ALL
    SELECT 'rate_5y'  AS tenor_code, t.rate_5y  AS rate_value FROM WORK.RATE_CURVE_WIDE t UNION ALL
    SELECT 'rate_10y' AS tenor_code, t.rate_10y AS rate_value FROM WORK.RATE_CURVE_WIDE t;
QUIT;

%_eg_conditional_dropds(WORK.RUNNING_BALANCE);

PROC SQL;
    CREATE TABLE WORK.RUNNING_BALANCE AS
    SELECT
        t1.customer_id,
        t1.order_id,
        t1.order_date,
        t1.amount,
        (SUM(t1.amount) OVER (
            PARTITION BY t1.customer_id
            ORDER BY t1.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ))                                                       AS cumulative_amount,
        (ROW_NUMBER() OVER (
            PARTITION BY t1.customer_id
            ORDER BY t1.order_date
        ))                                                       AS row_within_customer
    FROM WORK.ALL_POSITIONS t1;
QUIT;

%_eg_conditional_dropds(WORK.CURRENT_RATES);

PROC SQL;
    CREATE TABLE WORK.CURRENT_RATES AS
    SELECT
        t1.product_code,
        t1.valid_from,
        t1.valid_to,
        t1.base_rate,
        t1.margin_rate,
        (t1.base_rate + t1.margin_rate) AS effective_rate
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.product_code
                ORDER BY r.valid_from DESC
            ) AS rn
        FROM staging.RATE_HISTORY r
        WHERE r.valid_from <= &gPeriodEnd.
          AND r.valid_to > &gPeriodEnd.
    ) t1
    WHERE t1.rn = 1;
QUIT;

%_eg_conditional_dropds(WORK.ACTIVE_CONTRACTS);

PROC SQL;
    CREATE TABLE WORK.ACTIVE_CONTRACTS AS
    SELECT
        t2.contract_id,
        t2.customer_id,
        t2.product_code,
        t2.start_date,
        t1.close_date,
        t1.close_reason
    FROM staging.CONTRACT_CLOSURES t1
        RIGHT JOIN staging.CONTRACTS t2
            ON (t1.contract_id = t2.contract_id)
    WHERE t1.close_date IS NULL
      AND t1.contract_id IS NOT NULL;
QUIT;

%_eg_conditional_dropds(WORK.FINAL_REPORT);

PROC SQL;
    CREATE TABLE WORK.FINAL_REPORT AS
    SELECT
        t1.contract_id,
        t1.customer_id,
        t2.region_label,
        t3.effective_rate,
        t1.start_date,
        t1.close_date,
        t4.cumulative_amount,
        t2.total_amount_teur    AS total_amount_teur,
        t1.product_code         AS product_code,
        (IFC(t4.cumulative_amount > 50000, 'PREMIUM', 'STANDARD'))
                                AS customer_segment
    FROM WORK.ACTIVE_CONTRACTS t1
        LEFT JOIN WORK.REGION_SUMMARY   t2  ON (t1.customer_id  = t2.region_code)
        LEFT JOIN WORK.CURRENT_RATES    t3  ON (t1.product_code = t3.product_code)
        LEFT JOIN WORK.RUNNING_BALANCE  t4  ON (t1.customer_id  = t4.customer_id);
QUIT;

PROC SQL;
    SELECT
        (INTNX('MONTH', today(), 0, 'END'))  AS current_month_end,
        (INTNX('QTR',   today(),-1, 'END'))  AS prev_quarter_end
    FROM sys.dual;
QUIT;
