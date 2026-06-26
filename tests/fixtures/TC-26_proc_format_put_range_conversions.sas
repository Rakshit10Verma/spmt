/*============================================================================
 * FILE: TC-26_proc_format_put_range_conversions.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - PROC FORMAT VALUE with numeric range A - <B = 'label' → CASE WHEN >= A AND < B
 *   - PROC FORMAT VALUE with character multi-value list ('X','Y' = 'label') → CASE WHEN IN
 *   - PROC FORMAT VALUE . = 'MISSING' (SAS numeric missing) → IS NULL arm in CASE WHEN
 *   - PROC FORMAT INVALUE (char → numeric mapping) → INPUT(col, invalue.) in SQL
 *   - PUT(num, user_fmt.)   → CASE WHEN with range comparisons returning varchar
 *   - PUT(char, $user_fmt.) → CASE WHEN with IN() lists returning varchar
 *   - PUT(date, DDMMYY10.)  → TO_CHAR(date, 'DD/MM/YYYY')
 *   - PUT(date, MONNAME3.)  → TO_CHAR(date, 'Mon', 'NLS_DATE_LANGUAGE=AMERICAN')
 *   - PUT(date, WORDDATE20.)→ RTRIM(TO_CHAR(date,'Month'))||' '||TO_CHAR(date,'DD, YYYY')
 *   - PUT(date, JULIAN7.)   → TO_CHAR(date, 'YYYYDDD')   [YYYYDDD not Julian Day Number]
 *   - PUT(num, COMMA15.2)   → TO_CHAR(num, 'FM999,999,999,999.99')
 *   - PUT(num, EURDFDD10.)  → TO_CHAR(num, 'FM999G999G999D99') with NLS swap
 *   - PUT(num, Z10.)        → LPAD(TO_CHAR(TRUNC(num)), 10, '0')
 *   - PUT(num, 8.3)         → TO_CHAR(num, 'FM99999.999')
 *   - INPUT(char, invalue_fmt.) → CASE WHEN lookup returning number
 *   - CALCULATED in GROUP BY / ORDER BY → repeat full expression
 *   - &macro_var. → ${prop_varname}
 *   - %EVAL arithmetic → pre-computed literal
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - PROC FORMAT VALUE range 'A - <B' uses an EXCLUSIVE upper bound; converting to
 *     BETWEEN is WRONG (BETWEEN is inclusive both ends); Oracle must use >= A AND < B
 *   - PROC FORMAT '.' = 'MISSING' maps SAS numeric missing to a label; the Oracle CASE
 *     WHEN must be WHEN col IS NULL THEN 'MISSING' — LLMs often emit WHEN col = '.' THEN
 *   - PUT(date, WORDDATE20.) produces "May 31, 2025"; Oracle: RTRIM(TO_CHAR(date,'Month'))
 *     is needed because TO_CHAR pads the month name with spaces to a fixed width
 *   - PUT(date, JULIAN7.) is YEAR + day-of-year (e.g. 2025151); Oracle TO_CHAR(d,'YYYYDDD')
 *     is correct; LLMs often confuse this with Julian Day Number which is a completely
 *     different astronomical epoch-based number
 *   - PROC FORMAT INVALUE + INPUT(): the INVALUE maps character values to numeric; in Oracle
 *     this must become a CASE WHEN returning a NUMBER, not a TO_NUMBER() call
 *   - PUT(num, EURDFDD10.) reverses locale: '.' = thousands separator, ',' = decimal;
 *     the Oracle NLS equivalent requires TO_CHAR with 'D'/'G' or NLS_NUMERIC_CHARACTERS
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET prior_period = %EVAL(&report_date. - 1);
%LET min_exposure = 1000;

PROC FORMAT;
    VALUE risk_band_fmt
        LOW      - <30   = 'LOW'
        30       - <60   = 'MEDIUM'
        60       - <80   = 'HIGH'
        80       - HIGH  = 'CRITICAL'
        .                = 'UNSCORED';
    VALUE exp_tier_fmt
        0        - <10000     = 'MICRO'
        10000    - <100000    = 'SMALL'
        100000   - <1000000  = 'MEDIUM'
        1000000  - HIGH       = 'LARGE'
        .                     = 'MISSING';
    VALUE pd_band_fmt
        LOW      - <0.005  = 'AAA-AA'
        0.005    - <0.010  = 'A'
        0.010    - <0.020  = 'BBB'
        0.020    - <0.050  = 'BB'
        0.050    - <0.100  = 'B'
        0.100    - HIGH    = 'CCC-D'
        .                  = 'NR';
    VALUE $contract_class_fmt
        'MORT', 'HMORT'         = 'MORTGAGE'
        'CONS', 'PCONS'         = 'CONSUMER'
        'CORP', 'SME', 'MICRO'  = 'CORPORATE'
        OTHER                   = 'OTHER';
    INVALUE risk_score_in
        'LOW'                   = 15
        'MEDIUM'                = 45
        'HIGH'                  = 70
        'CRITICAL'              = 92
        OTHER                   = .;
RUN;

PROC SQL;
    CREATE TABLE work.exposure_scored AS
    SELECT
        t1.contract_id,
        t1.contract_type,
        t1.approved_amount,
        t1.credit_score,
        t1.probability_of_default,
        t1.booking_date,
        t1.legacy_risk_label,
        PUT(t1.credit_score, risk_band_fmt.)          AS risk_band,
        PUT(t1.approved_amount, exp_tier_fmt.)         AS exposure_tier,
        PUT(t1.probability_of_default, pd_band_fmt.)   AS pd_rating,
        PUT(t1.contract_type, $contract_class_fmt.)    AS contract_class,
        PUT(t1.booking_date, DDMMYY10.)                AS booking_date_eu,
        PUT(t1.booking_date, MONNAME3.)                AS booking_month_abbrev,
        PUT(t1.booking_date, WORDDATE20.)              AS booking_date_words,
        PUT(t1.booking_date, JULIAN7.)                 AS booking_julian,
        PUT(t1.approved_amount, COMMA15.2)             AS amount_comma,
        PUT(t1.approved_amount, EURDFDD10.)            AS amount_european,
        PUT(t1.contract_id, Z10.)                      AS contract_id_padded,
        PUT(t1.approved_amount / 1000000, 8.3)         AS amount_millions,
        INPUT(t1.legacy_risk_label, risk_score_in.)    AS legacy_derived_score,
        t1.period_key,
        t1.org_unit_id
    FROM source_data.contract_header t1
    WHERE t1.client_code = "&client_code."
      AND t1.period_key = &report_date.
      AND t1.approved_amount ge &min_exposure.;
QUIT;

PROC SQL;
    CREATE TABLE work.exposure_band_rollup AS
    SELECT
        PUT(credit_score, risk_band_fmt.)          AS risk_band,
        PUT(approved_amount, exp_tier_fmt.)         AS exposure_tier,
        PUT(probability_of_default, pd_band_fmt.)   AS pd_rating,
        PUT(contract_type, $contract_class_fmt.)    AS contract_class,
        COUNT(*)                                    AS contract_count,
        SUM(approved_amount)                        AS total_exposure,
        AVG(credit_score)                           AS avg_score,
        MIN(probability_of_default)                 AS best_pd,
        MAX(probability_of_default)                 AS worst_pd,
        SUM(approved_amount * probability_of_default)
            / NULLIF(SUM(approved_amount), 0)       AS weighted_avg_pd,
        PUT(SUM(approved_amount), COMMA20.2)        AS total_exposure_fmt,
        PUT(MIN(booking_date), DDMMYY10.)           AS earliest_booking_eu,
        PUT(MAX(booking_date), WORDDATE20.)         AS latest_booking_words
    FROM work.exposure_scored
    GROUP BY CALCULATED risk_band,
             CALCULATED exposure_tier,
             CALCULATED pd_rating,
             CALCULATED contract_class
    HAVING COUNT(*) >= 5
    ORDER BY CALCULATED risk_band,
             CALCULATED pd_rating;
QUIT;

PROC SQL;
    CREATE TABLE work.rating_migration AS
    SELECT
        t1.contract_id,
        t1.risk_band                                  AS current_risk_band,
        PUT(t2.credit_score, risk_band_fmt.)           AS prior_risk_band,
        t1.credit_score                               AS current_score,
        t2.credit_score                               AS prior_score,
        t1.credit_score - t2.credit_score             AS score_change,
        PUT(t1.probability_of_default, pd_band_fmt.)   AS current_pd_band,
        PUT(t2.probability_of_default, pd_band_fmt.)   AS prior_pd_band,
        CASE
            WHEN PUT(t1.credit_score, risk_band_fmt.) =
                 PUT(t2.credit_score, risk_band_fmt.) THEN 'STABLE'
            WHEN t1.credit_score > t2.credit_score   THEN 'UPGRADED'
            ELSE 'DOWNGRADED'
        END AS migration_direction,
        PUT(t1.booking_date, MONNAME3.) AS report_month
    FROM work.exposure_scored t1
    LEFT JOIN source_data.contract_score_history t2
        ON t1.contract_id = t2.contract_id
       AND t2.period_key = &prior_period.
    WHERE t2.contract_id IS NOT MISSING;
QUIT;

PROC SQL;
    CREATE TABLE work.legacy_score_reconciliation AS
    SELECT
        t1.contract_id,
        t1.legacy_risk_label,
        t1.legacy_derived_score,
        t1.credit_score AS system_score,
        ABS(t1.legacy_derived_score - t1.credit_score) AS score_discrepancy,
        PUT(t1.legacy_derived_score, risk_band_fmt.)    AS legacy_risk_band,
        t1.risk_band                                    AS system_risk_band,
        CASE
            WHEN PUT(t1.legacy_derived_score, risk_band_fmt.) = t1.risk_band
                THEN 'CONSISTENT'
            ELSE 'DISCREPANT'
        END AS consistency_flag,
        PUT(ABS(t1.legacy_derived_score - t1.credit_score), COMMA8.2)
            AS discrepancy_fmt
    FROM work.exposure_scored t1
    WHERE t1.legacy_risk_label IS NOT MISSING
      AND t1.legacy_derived_score IS NOT MISSING
    ORDER BY score_discrepancy DESC;
QUIT;
