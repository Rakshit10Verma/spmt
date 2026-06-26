/*============================================================================
 * FILE: TC-22_macro_conditional_dynamic_sql.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - %IF %THEN %ELSE inside PROC SQL → LLM must choose or rewrite as CASE WHEN
 *   - %IF &flag. = 1 %THEN col_a,; → conditional column inclusion
 *   - %EVAL(&num. + 1) → pre-computed literal
 *   - %UPCASE(&str.) → uppercased literal value
 *   - %SYSFUNC(INTNX('MONTH', TODAY(), -1), YYMMN6.) → Pentaho param or pre-computed
 *   - %SYSFUNC(CATS(&prefix., &suffix.)) → concatenated literal
 *   - &&double.deref. double-deferred macro → ${prop_varname}
 *   - PROC SQL NOPRINT → remove; no Oracle equivalent
 *   - PROC SQL STIMER → remove option
 *   - PROC SQL FEEDBACK → remove option
 *   - OPTIONS MPRINT MLOGIC → remove; SAS-only debug option
 *   - %MACRO / %MEND wrappers → flatten out; emit the resolved SQL directly
 *   - Multiple PROC SQL sharing a calculated macro variable
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - %IF %THEN inside PROC SQL generates different SQL based on runtime flag;
 *     an LLM cannot evaluate the condition so must pick a branch or emit CASE WHEN —
 *     CASE WHEN is wrong here because the condition controls column presence, not value
 *   - Double-deferred macros &&prefix.code. resolve to the *value* of a macro whose
 *     name itself was built from another macro — LLMs almost always get this wrong
 *   - %EVAL does integer arithmetic; %SYSEVALF does float arithmetic — LLMs confuse them
 *   - %SYSFUNC wraps SAS DATA step functions; the nested INTNX result is a date number
 *     that must then be formatted with a format argument (third arg to %SYSFUNC)
 *   - PROC SQL NOPRINT suppresses output but the query still runs; LLMs sometimes drop
 *     the whole block or incorrectly comment it out
 *============================================================================*/

OPTIONS MPRINT MLOGIC SYMBOLGEN;

%LET report_period = 202505;
%LET client_code = NORTH;
%LET include_broker = 1;
%LET include_risk = 0;
%LET lag_months = 3;
%LET prev_period = %EVAL(&report_period. - 1);
%LET period_start = %SYSFUNC(INTNX(MONTH, %SYSFUNC(TODAY()), -%EVAL(&lag_months.), B), YYMMN6.);
%LET table_prefix = DWH;
%LET schema_suffix = DAILY;
%LET full_source = %SYSFUNC(CATS(&table_prefix., _, &schema_suffix.));

PROC SQL NOPRINT STIMER FEEDBACK;
   SELECT COUNT(*) INTO :row_count TRIMMED
   FROM &&full_source..contract_header
   WHERE period_key = &report_period.
     AND client_code = "%UPCASE(&client_code.)";
QUIT;

%PUT NOTE: Row count for period &report_period. = &row_count.;

PROC SQL;
   CREATE TABLE work.contract_base AS
   SELECT
       t1.contract_id,
       t1.contract_type,
       t1.credit_type,
       t1.approved_amount,
       t1.booking_date,
       t1.partner_id,
       t1.case_number,
       %IF &include_broker. = 1 %THEN
           t1.broker_id,
           t1.broker_name,
       ;
       %IF &include_risk. = 1 %THEN
           t1.risk_score,
           t1.risk_category,
       ;
       t2.customer_key,
       t2.pep_flag,
       t2.regulatory_flag,
       t1.period_key
   FROM &&full_source..contract_header t1
   LEFT JOIN &&full_source..customer_link t2
       ON t1.partner_id = t2.partner_key
      AND t2.period_key = &report_period.
      AND t2.client_code = "%UPCASE(&client_code.)"
   WHERE t1.period_key = &report_period.
     AND t1.client_code = "%UPCASE(&client_code.)";
QUIT;

PROC SQL;
   CREATE TABLE work.contract_prev_period AS
   SELECT
       t1.contract_id,
       t1.approved_amount AS prev_approved_amount,
       t1.period_key AS prev_period_key
   FROM &&full_source..contract_header t1
   WHERE t1.period_key = &prev_period.
     AND t1.client_code = "%UPCASE(&client_code.)"
     AND t1.contract_type NOT IN ('CANCELLED', 'REJECTED');
QUIT;

PROC SQL;
   CREATE TABLE work.contract_delta AS
   SELECT
       t1.contract_id,
       t1.contract_type,
       t1.approved_amount AS curr_amount,
       COALESCE(t2.prev_approved_amount, 0) AS prev_amount,
       t1.approved_amount - COALESCE(t2.prev_approved_amount, 0) AS delta_amount,
       CASE
           WHEN t2.contract_id IS MISSING
               THEN 'NEW'
           WHEN t1.approved_amount gt COALESCE(t2.prev_approved_amount, 0)
               THEN 'INCREASED'
           WHEN t1.approved_amount lt COALESCE(t2.prev_approved_amount, 0)
               THEN 'DECREASED'
           ELSE 'UNCHANGED'
       END AS movement_flag,
       %IF &include_broker. = 1 %THEN
           t1.broker_id,
       ;
       t1.pep_flag,
       t1.regulatory_flag,
       "&period_start." AS reporting_period_label
   FROM work.contract_base t1
   LEFT JOIN work.contract_prev_period t2
       ON t1.contract_id = t2.contract_id;
QUIT;

PROC SQL;
   CREATE TABLE work.contract_summary AS
   SELECT
       movement_flag,
       %IF &include_broker. = 1 %THEN broker_id,;
       COUNT(*) AS contract_count,
       SUM(curr_amount) AS total_curr_amount,
       SUM(prev_amount) AS total_prev_amount,
       SUM(delta_amount) AS total_delta,
       SUM(CASE WHEN pep_flag = 'Y' THEN 1 ELSE 0 END) AS pep_count,
       SUM(CASE WHEN regulatory_flag = 'Y' THEN 1 ELSE 0 END) AS regulatory_count
   FROM work.contract_delta
   GROUP BY
       movement_flag
       %IF &include_broker. = 1 %THEN , broker_id;
   ORDER BY
       movement_flag;
QUIT;
