/*============================================================================
 * FILE: TC-34_proc_sql_dml_insert_update_delete_bitmask.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * BASED ON:    LBS Cashflows and Abgleich_ET-URSPRUNGSSPREADS patterns
 *
 * CONVERSION PATTERNS PRESENT:
 *   - PROC SQL; INSERT INTO table SET col1=val1, col2=val2 → Oracle INSERT INTO VALUES(...)
 *   - PROC SQL; INSERT INTO table (col list) VALUES (val list) → Oracle: compatible
 *   - PROC SQL; UPDATE table SET col=expr WHERE condition → Oracle: compatible
 *   - PROC SQL; UPDATE table SET col=(scalar subquery) WHERE ... → Oracle: compatible
 *   - PROC SQL; DELETE FROM table WHERE condition → Oracle: compatible
 *   - PROC SQL; DELETE FROM table WHERE NOT EXISTS (correlated subquery) → Oracle: compatible
 *   - 2**&macro. power operator in %EVAL → POWER(2, n) in Oracle or SQL expression
 *   - %SYSFUNC(FLOOR(%SYSFUNC(LOG2(n)))) bitmask decode → Oracle FLOOR(LOG(2,n))
 *   - %GLOBAL gErrorSum with bitmask accumulation logic → infrastructure
 *   - PROC SQL; INSERT INTO ... SELECT ... (append pattern) → Oracle: compatible
 *   - DATETIME() → SYSDATE  (current datetime)
 *   - TODAY() → TRUNC(SYSDATE)
 *   - &macro_var. → ${prop_varname}
 *   - IS MISSING / IS NOT MISSING → IS NULL / IS NOT NULL
 *   - NOT = value → <> value
 *   - ORDER BY inside CREATE TABLE AS SELECT → forbidden in Oracle; remove
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - PROC SQL INSERT INTO ... SET col=val is a SAS extension that is NOT standard SQL;
 *     standard SQL (and Oracle) require INSERT INTO t (col1, col2) VALUES (val1, val2);
 *     LLMs sometimes keep the SET syntax which is a syntax error in Oracle
 *   - 2**n — SAS uses ** for exponentiation; Oracle uses POWER(2, n); Python uses 2**n;
 *     LLMs sometimes leave ** in the Oracle output which is invalid
 *   - PROC SQL UPDATE with a subquery in the SET clause is standard SQL and Oracle supports
 *     it, but the subquery must return exactly one row or Oracle raises ORA-01427;
 *     LLMs often add incorrect ROWNUM = 1 or FETCH FIRST 1 ROW ONLY inside the SET subquery
 *   - PROC SQL DELETE FROM t WHERE NOT EXISTS (...) is standard and Oracle supports it;
 *     but the correlated subquery alias scoping must be preserved exactly
 *   - PROC SQL; INSERT INTO t SELECT ... (no VALUES) — standard; Oracle supports this;
 *     LLMs sometimes add VALUES or wrap incorrectly
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%GLOBAL gProcessLog;
%LET gProcessLog = PROCESS_LOG;

PROC SQL;
    CREATE TABLE work.process_run_log (
        run_id       CHAR(36),
        run_start    DOUBLE,
        run_end      DOUBLE,
        step_name    CHAR(100),
        row_count    DOUBLE,
        status       CHAR(20),
        error_code   DOUBLE
    );
QUIT;

PROC SQL;
    INSERT INTO work.process_run_log
    SET run_id    = COMPRESS(PUT(DATETIME(), DATETIME20.)),
        run_start = DATETIME(),
        run_end   = .,
        step_name = 'INIT',
        row_count = 0,
        status    = 'RUNNING',
        error_code = 0;
QUIT;

PROC SQL;
    CREATE TABLE work.disbursement_errors AS
    SELECT
        t1.contract_id,
        t1.partner_id,
        t1.disbursement_date,
        t1.disbursement_amount,
        t1.processing_status,
        t1.error_code,
        t1.error_text,
        TODAY() AS validation_date,
        DATETIME() AS validation_timestamp
    FROM source_data.disbursement_transactions t1
    WHERE t1.client_code = "&client_code."
      AND t1.disbursement_date le "&report_date."d
      AND t1.processing_status NOT = 'OK'
      AND t1.error_code IS NOT MISSING
      AND t1.error_code NOT IN (0, 999)
    ORDER BY t1.error_code, t1.disbursement_date;
QUIT;

PROC SQL;
    CREATE TABLE work.validation_results AS
    SELECT
        t1.contract_id,
        t1.partner_id,
        t1.disbursement_amount,
        t1.error_code,
        CASE
            WHEN t1.error_code = 1  THEN 'MISSING_PARTNER'
            WHEN t1.error_code = 2  THEN 'AMOUNT_MISMATCH'
            WHEN t1.error_code = 4  THEN 'DATE_OUT_OF_RANGE'
            WHEN t1.error_code = 8  THEN 'PRODUCT_INACTIVE'
            WHEN t1.error_code = 16 THEN 'KFW_LIMIT_EXCEEDED'
            WHEN t1.error_code = 32 THEN 'DUPLICATE_SUBMISSION'
            ELSE 'UNKNOWN_ERROR_' || PUT(t1.error_code, 8.)
        END AS error_category,
        0 AS is_resolved,
        TODAY() AS flagged_date
    FROM work.disbursement_errors t1;
QUIT;

PROC SQL;
    UPDATE work.validation_results
    SET is_resolved = 1
    WHERE contract_id IN (
        SELECT DISTINCT r.contract_id
        FROM source_data.resolution_register r
        WHERE r.resolution_date le "&report_date."d
          AND r.resolution_status = 'CONFIRMED'
          AND r.client_code = "&client_code."
    );
QUIT;

PROC SQL;
    UPDATE work.validation_results v
    SET error_category = (
        SELECT ec.category_label
        FROM source_data.error_code_master ec
        WHERE ec.error_code = v.error_code
          AND ec.valid_from le "&report_date."d
          AND ec.valid_to gt "&report_date."d
    )
    WHERE v.error_category LIKE 'UNKNOWN_ERROR_%';
QUIT;

PROC SQL;
    DELETE FROM work.validation_results
    WHERE NOT EXISTS (
        SELECT 1
        FROM source_data.disbursement_transactions t
        WHERE t.contract_id  = work.validation_results.contract_id
          AND t.client_code  = "&client_code."
          AND t.processing_status NOT = 'CANCELLED'
    );
QUIT;

PROC SQL;
    INSERT INTO work.process_run_log (run_id, run_start, run_end,
                                       step_name, row_count, status, error_code)
    SELECT
        COMPRESS(PUT(DATETIME(), DATETIME20.)),
        DATETIME(),
        DATETIME(),
        'VALIDATION_COMPLETE',
        COUNT(*),
        CASE WHEN SUM(CASE WHEN is_resolved = 0 THEN 1 ELSE 0 END) > 0
             THEN 'WARNINGS' ELSE 'SUCCESS' END,
        SUM(CASE WHEN is_resolved = 0 THEN 1 ELSE 0 END)
    FROM work.validation_results;
QUIT;

PROC SQL;
    CREATE TABLE work.error_resolution_summary AS
    SELECT
        error_category,
        COUNT(*)                                     AS total_errors,
        SUM(is_resolved)                             AS resolved_count,
        COUNT(*) - SUM(is_resolved)                  AS open_count,
        SUM(disbursement_amount)
            FORMAT=COMMAX20.2                        AS total_error_amount,
        SUM(CASE WHEN is_resolved = 1
                 THEN disbursement_amount ELSE 0 END)
            FORMAT=COMMAX20.2                        AS resolved_amount,
        MIN(flagged_date)                            AS earliest_flag,
        MAX(flagged_date)                            AS latest_flag
    FROM work.validation_results
    GROUP BY error_category
    ORDER BY total_errors DESC;
QUIT;
