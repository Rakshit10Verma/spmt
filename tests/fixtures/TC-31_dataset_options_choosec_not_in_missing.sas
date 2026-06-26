/*============================================================================
 * FILE: TC-31_dataset_options_choosec_not_in_missing.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * BASED ON:    LBS KfW-Abgleich and Kreditkontrolle patterns
 *
 * CONVERSION PATTERNS PRESENT:
 *   - FROM table(FIRSTOBS=2) → subquery with ROW_NUMBER() OVER () >= 2
 *   - FROM table(OBS=n) → subquery with FETCH FIRST n ROWS ONLY
 *   - FROM table(DROP=col1 col2) → explicit SELECT column list excluding named cols
 *   - FROM table(KEEP=col1 col2 col3) → SELECT with only the named columns
 *   - FROM table(RENAME=(old=new other=alias)) → SELECT old AS new in subquery
 *   - FROM table(WHERE=(filter)) → additional WHERE condition on source
 *   - CHOOSEC(INPUT(col, 10.), 'v1', 'v2', 'v3') → CASE WHEN CAST AS INT = 1 THEN v1...
 *   - CHOOSEN(n, col1, col2, col3) → CASE WHEN n=1 THEN col1 WHEN n=2 THEN col2...
 *   - NOT IN (0, .) → NOT IN (0) AND col IS NOT NULL  [. = SAS numeric missing]
 *   - col NOT = 'value' → col <> 'value'
 *   - '31DEC9999'd sentinel for open-ended records → TO_DATE('99991231','YYYYMMDD')
 *   - MDY(MONTH(date), 1, YEAR(date)) → TRUNC(date, 'MM')
 *   - INTNX('YEAR', date, 0, 'B') → TRUNC(date, 'YYYY')
 *   - CALCULATED in HAVING / GROUP BY → repeat expression
 *   - &macro_var. → ${prop_varname}
 *   - SAS comparison operators (ge, le, gt, lt) → >=, <=, >, <
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - SAS dataset options (FIRSTOBS=, OBS=, DROP=, KEEP=, RENAME=, WHERE=) are
 *     applied at the SAS dataset layer BEFORE the SQL engine; each must become a
 *     different SQL construct; LLMs almost always drop them entirely or error
 *   - FIRSTOBS=2 means "skip row 1" (often a header); Oracle needs ROW_NUMBER() > 1
 *     in a subquery; ROWNUM > 1 in an outer WHERE does NOT work correctly with ORDER BY
 *   - RENAME=(old=new) applied inside FROM changes the column name before the outer SELECT
 *     sees it; the outer query must reference the NEW name, not the old one
 *   - CHOOSEC(INPUT(col,10.), 'A','B','C') selects the n-th string where n = INPUT(col);
 *     Oracle has no equivalent; must become a CASE WHEN CAST(col AS INT) = 1 THEN 'A'...
 *   - NOT IN (0, .) — the dot is SAS numeric missing; this means exclude zeros AND
 *     exclude NULLs; Oracle NOT IN (0, NULL) is ALWAYS empty due to NULL logic — must
 *     split into: col NOT IN (0) AND col IS NOT NULL
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET kfw_min_contracts = 5;

PROC SQL;
    CREATE TABLE work.kfw_loan_base AS
    SELECT
        t1.contract_id,
        t1.partner_id,
        t1.kfw_program_num,
        CHOOSEC(INPUT(t1.kfw_program_num, 10.),
            'ENERGY_EFFICIENCY',
            'RENEWABLE_ENERGY',
            'SOCIAL_HOUSING',
            'INFRASTRUCTURE',
            'SME_INVESTMENT')                             AS kfw_program_label,
        t1.approved_amount,
        t1.disbursement_date,
        t1.repayment_type_code,
        CHOOSEC(INPUT(t1.repayment_type_code, 10.),
            'ANNUITY', 'BULLET', 'VARIABLE', 'GRACE')    AS repayment_type_label,
        t1.fixed_rate_period_months,
        t1.maturity_date,
        CASE WHEN t1.maturity_date = '31DEC9999'd THEN 'OPEN_ENDED'
             ELSE 'FIXED'
        END AS maturity_type,
        t1.interest_rate,
        MDY(MONTH(t1.disbursement_date), 1, YEAR(t1.disbursement_date))
            AS disbursement_month_start,
        INTNX('YEAR', t1.disbursement_date, 0, 'B')
            AS disbursement_year_start
    FROM source_data.kfw_contract_header(
        FIRSTOBS = 2
        DROP     = internal_seq batch_id etl_ts load_user
        WHERE    = (disbursement_date IS NOT MISSING
                    AND kfw_program_num NOT IN (0, .))
    ) t1
    WHERE t1.client_code = "&client_code."
      AND t1.kfw_program_num NOT IN (0, .)
      AND t1.contract_status NOT = 'CANCELLED'
      AND t1.disbursement_date le "&report_date."d;
QUIT;

PROC SQL;
    CREATE TABLE work.kfw_repayment_sample AS
    SELECT
        t1.contract_id,
        t1.instalment_nr,
        t1.due_date,
        t1.principal_due,
        t1.interest_due,
        t1.total_due,
        t1.outstanding_balance,
        CHOOSEN(t1.rate_band, t1.rate_tier_1, t1.rate_tier_2, t1.rate_tier_3)
            AS applicable_rate,
        t2.kfw_program_label,
        t2.repayment_type_label
    FROM source_data.kfw_repayment_plan(
        OBS  = 100000
        KEEP = contract_id instalment_nr due_date principal_due interest_due
               total_due outstanding_balance rate_band rate_tier_1 rate_tier_2 rate_tier_3
        WHERE = (outstanding_balance NOT IN (0, .))
    ) t1
    INNER JOIN work.kfw_loan_base t2
        ON t1.contract_id = t2.contract_id
    WHERE t1.due_date ge "&report_date."d
      AND t1.principal_due NOT IN (0, .);
QUIT;

PROC SQL;
    CREATE TABLE work.kfw_partner_base AS
    SELECT
        t1.partner_id,
        t1.partner_legal_name,
        t1.segment_code,
        t1.domicile_region
    FROM source_data.partner_master(
        RENAME = (partner_nr      = partner_id
                  legal_name      = partner_legal_name
                  hs_segment      = segment_code
                  region_kz       = domicile_region)
        WHERE  = (valid_to ge "&report_date."d
                  AND partner_status NOT = 'INACTIVE')
    ) t1
    WHERE t1.partner_id NOT IN (
        SELECT DISTINCT partner_id
        FROM source_data.exclusion_list
        WHERE exclusion_reason NOT = 'EXPIRED'
    );
QUIT;

PROC SQL;
    CREATE TABLE work.kfw_program_summary AS
    SELECT
        t1.kfw_program_num,
        t1.kfw_program_label,
        t2.segment_code,
        COUNT(DISTINCT t1.contract_id)                 AS contract_count,
        SUM(t1.approved_amount)                        AS total_approved,
        AVG(t1.approved_amount)                        AS avg_approved,
        AVG(t1.interest_rate)                          AS avg_interest_rate,
        MIN(t1.disbursement_date)                      AS earliest_disbursement,
        MAX(t1.disbursement_date)                      AS latest_disbursement,
        SUM(CASE WHEN t1.maturity_type = 'OPEN_ENDED'
                 THEN 1 ELSE 0 END)                    AS open_ended_count,
        SUM(CASE WHEN t1.repayment_type_label = 'BULLET'
                 THEN t1.approved_amount ELSE 0 END)   AS bullet_exposure
    FROM work.kfw_loan_base t1
    LEFT JOIN work.kfw_partner_base t2
        ON t1.partner_id = t2.partner_id
    WHERE t1.kfw_program_num NOT IN (0, .)
    GROUP BY t1.kfw_program_num,
             t1.kfw_program_label,
             t2.segment_code
    HAVING COUNT(DISTINCT t1.contract_id) ge &kfw_min_contracts.
    ORDER BY total_approved DESC;
QUIT;
