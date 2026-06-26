/*******************************************************************************
 * FILE: TC-12_set_ops_monotonic_outobs_self_join.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - EXCEPT (SAS) → MINUS (Oracle)
 *   - INTERSECT (SAS) → INTERSECT (Oracle — compatible)
 *   - PROC SQL OUTOBS=n → Oracle FETCH FIRST n ROWS ONLY  (or ROWNUM in subquery)
 *   - MONOTONIC() pseudo-column → Oracle ROWNUM  (only valid in FROM subquery)
 *   - Self-join on same table with role aliases
 *   - Recursive-style hierarchy flattening via chained self-joins (manager/employee)
 *   - CASE WHEN inside aggregate: SUM(CASE WHEN ... END)
 *   - Conditional aggregation pivot (manual PIVOT without SAS PROC TRANSPOSE)
 *   - HAVING clause referencing aliased calculated columns (CALCULATED)
 *   - SAS PROC SQL REMERGE warning pattern (aggregate + non-aggregate in same SELECT)
 *   - DISTINCT ON equivalent: GROUP BY to de-duplicate
 *   - FULL OUTER JOIN
 *   - CROSS JOIN (Cartesian product)
 *   - Multi-table UNION + EXCEPT combination
 *   - ORDER BY ordinal position (ORDER BY 1, 3 DESC)
 *   - Subquery in SELECT clause returning scalar
 *   - Multiple correlated subqueries in WHERE with AND / OR
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. SAS EXCEPT is set-difference; Oracle uses MINUS — keyword substitution only,
 *      but behaviour with NULLs differs: Oracle MINUS treats NULL=NULL.
 *   2. PROC SQL OUTOBS=n applies per PROC SQL block; in Oracle each SELECT needs
 *      its own row-limit clause — tool must distribute the limit correctly.
 *   3. MONOTONIC() is only meaningful with ORDER; SAS docs warn it is unreliable —
 *      tool should emit a warning comment and use ROWNUM.
 *   4. SAS REMERGE: if a SELECT has both an aggregate (SUM) and a non-aggregate
 *      without a GROUP BY, SAS silently adds a full-table aggregate and re-merges
 *      the result row-by-row. Oracle raises ORA-00937 instead — must add GROUP BY
 *      or wrap in subquery.
 *   5. FULL OUTER JOIN: compatible in Oracle, but NVL/COALESCE needed on join keys
 *      in SELECT when columns come from both sides.
 *   6. CROSS JOIN with large tables: no conversion issue but tool should warn about
 *      Cartesian product risk.
 *   7. CALCULATED in HAVING with a complex expression: must repeat the full expression.
 *   8. ORDER BY ordinal position is Oracle-compatible but bad practice — tool should
 *      preserve but optionally warn.
 *   9. Conditional aggregation (SUM CASE WHEN) is Oracle-compatible — no change needed,
 *      but verify the ELSE NULL vs ELSE 0 semantics match requirements.
 *   10. Self-join hierarchy: SAS aliases manager/employee on the same table —
 *       Oracle handles this identically; confirm alias resolution is correct.
 ******************************************************************************/

%LET report_date   = 20250531;
%LET period_key    = 202505;
%LET min_balance   = 5000;
%LET top_n         = 50;
%LET client_code   = CENTRAL;
%LET max_level     = 3;

PROC SQL;
   CREATE TABLE WORK.ACTIVE_NOT_DORMANT AS
   SELECT t1.CUSTOMER_ID, t1.SEGMENT_CODE, t1.REGION_CODE, t1.ONBOARDING_DATE
   FROM SOURCE.CUSTOMER_MASTER t1
   WHERE t1.STATUS_FLAG = 'A'
     AND t1.CLIENT_CODE = "&client_code."

   EXCEPT

   SELECT t2.CUSTOMER_ID, t2.SEGMENT_CODE, t2.REGION_CODE, t2.ONBOARDING_DATE
   FROM SOURCE.DORMANT_REGISTER t2
   WHERE t2.DORMANT_FLAG = 'Y'
     AND t2.CLIENT_CODE = "&client_code.";
QUIT;

PROC SQL;
   CREATE TABLE WORK.ACTIVE_AND_FLAGGED AS
   SELECT t1.CUSTOMER_ID, t1.SEGMENT_CODE, t1.REGION_CODE, t1.ONBOARDING_DATE
   FROM WORK.ACTIVE_NOT_DORMANT t1

   INTERSECT

   SELECT t2.CUSTOMER_ID, t2.SEGMENT_CODE, t2.REGION_CODE, t2.ONBOARDING_DATE
   FROM STAGING.REVIEW_WATCHLIST t2
   WHERE t2.REVIEW_TYPE IN ('AML', 'KYC')
     AND t2.REVIEW_DATE <= &report_date.;
QUIT;

PROC SQL OUTOBS=&top_n.;
   CREATE TABLE WORK.TOP_CUSTOMERS AS
   SELECT MONOTONIC()                           AS ROW_NUM,
          t1.CUSTOMER_ID,
          t1.SEGMENT_CODE,
          SUM(t2.OUTSTANDING_BALANCE)           AS TOTAL_EXPOSURE,
          COUNT(t2.CONTRACT_ID)                 AS CONTRACT_COUNT
   FROM WORK.ACTIVE_NOT_DORMANT t1
        INNER JOIN SOURCE.CONTRACTS_RAW t2
             ON t2.CUSTOMER_ID = t1.CUSTOMER_ID
            AND t2.SNAPSHOT_PERIOD = &period_key.
            AND t2.CLIENT_CODE = "&client_code."
   WHERE t2.OUTSTANDING_BALANCE > &min_balance.
   GROUP BY t1.CUSTOMER_ID, t1.SEGMENT_CODE
   ORDER BY 3 DESC, 1;
QUIT;

PROC SQL;
   CREATE TABLE WORK.CUSTOMER_VS_AVG AS
   SELECT t1.CUSTOMER_ID,
          t1.OUTSTANDING_BALANCE,
          (SELECT AVG(t2.OUTSTANDING_BALANCE)
           FROM SOURCE.CONTRACTS_RAW t2
           WHERE t2.SNAPSHOT_PERIOD = &period_key.) AS PORTFOLIO_AVG,
          t1.OUTSTANDING_BALANCE -
          (SELECT AVG(t3.OUTSTANDING_BALANCE)
           FROM SOURCE.CONTRACTS_RAW t3
           WHERE t3.SNAPSHOT_PERIOD = &period_key.) AS DEVIATION_FROM_AVG
   FROM SOURCE.CONTRACTS_RAW t1
   WHERE t1.SNAPSHOT_PERIOD = &period_key.
     AND t1.OUTSTANDING_BALANCE IS NOT MISSING;
QUIT;

PROC SQL;
   CREATE TABLE WORK.SEGMENT_PIVOT AS
   SELECT t1.REGION_CODE,
          SUM(CASE WHEN t1.SEGMENT_CODE = 'RETAIL'    THEN t2.OUTSTANDING_BALANCE ELSE 0 END)
                                                       AS RETAIL_BALANCE,
          SUM(CASE WHEN t1.SEGMENT_CODE = 'CORPORATE' THEN t2.OUTSTANDING_BALANCE ELSE 0 END)
                                                       AS CORP_BALANCE,
          SUM(CASE WHEN t1.SEGMENT_CODE = 'SME'       THEN t2.OUTSTANDING_BALANCE ELSE 0 END)
                                                       AS SME_BALANCE,
          COUNT(DISTINCT CASE WHEN t2.OUTSTANDING_BALANCE > &min_balance.
                              THEN t1.CUSTOMER_ID END) AS HIGH_VALUE_CUSTOMERS,
          SUM(t2.OUTSTANDING_BALANCE)                  AS TOTAL_REGION_BALANCE
   FROM WORK.ACTIVE_NOT_DORMANT t1
        LEFT JOIN SOURCE.CONTRACTS_RAW t2
             ON t2.CUSTOMER_ID = t1.CUSTOMER_ID
            AND t2.SNAPSHOT_PERIOD = &period_key.
   GROUP BY t1.REGION_CODE
   HAVING (CALCULATED TOTAL_REGION_BALANCE) > 1000000;
QUIT;

PROC SQL;
   CREATE TABLE WORK.MANAGER_HIERARCHY AS
   SELECT emp.EMPLOYEE_ID,
          emp.FULL_NAME                        AS EMPLOYEE_NAME,
          emp.JOB_TITLE,
          mgr1.EMPLOYEE_ID                    AS DIRECT_MANAGER_ID,
          mgr1.FULL_NAME                      AS DIRECT_MANAGER_NAME,
          mgr2.EMPLOYEE_ID                    AS L2_MANAGER_ID,
          mgr2.FULL_NAME                      AS L2_MANAGER_NAME,
          mgr3.EMPLOYEE_ID                    AS L3_MANAGER_ID,
          mgr3.FULL_NAME                      AS L3_MANAGER_NAME,
          (CASE
              WHEN mgr3.EMPLOYEE_ID IS NOT MISSING THEN 3
              WHEN mgr2.EMPLOYEE_ID IS NOT MISSING THEN 2
              WHEN mgr1.EMPLOYEE_ID IS NOT MISSING THEN 1
              ELSE 0
           END)                               AS HIERARCHY_DEPTH
   FROM SOURCE.EMPLOYEES emp
        LEFT JOIN SOURCE.EMPLOYEES mgr1
             ON mgr1.EMPLOYEE_ID = emp.REPORTS_TO_ID
            AND mgr1.STATUS = 'ACTIVE'
        LEFT JOIN SOURCE.EMPLOYEES mgr2
             ON mgr2.EMPLOYEE_ID = mgr1.REPORTS_TO_ID
            AND mgr2.STATUS = 'ACTIVE'
        LEFT JOIN SOURCE.EMPLOYEES mgr3
             ON mgr3.EMPLOYEE_ID = mgr2.REPORTS_TO_ID
            AND mgr3.STATUS = 'ACTIVE'
   WHERE emp.STATUS = 'ACTIVE'
     AND emp.DEPARTMENT_CODE NOT = 'EXTERN';
QUIT;

PROC SQL;
   CREATE TABLE WORK.COVERAGE_MATRIX AS
   SELECT COALESCE(a.REGION_CODE, b.REGION_CODE)     AS REGION_CODE,
          COALESCE(a.SEGMENT_CODE, b.SEGMENT_CODE)   AS SEGMENT_CODE,
          NVL(a.RETAIL_BALANCE, 0)                   AS ACTUAL_BALANCE,
          NVL(b.TARGET_BALANCE, 0)                   AS TARGET_BALANCE,
          NVL(a.RETAIL_BALANCE, 0) -
          NVL(b.TARGET_BALANCE, 0)                   AS VARIANCE,
          (CASE
              WHEN NVL(b.TARGET_BALANCE, 0) = 0 THEN NULL
              ELSE (NVL(a.RETAIL_BALANCE, 0) / b.TARGET_BALANCE) * 100
           END)                                      AS PCT_OF_TARGET
   FROM WORK.SEGMENT_PIVOT a
        FULL OUTER JOIN STAGING.REGIONAL_TARGETS b
             ON b.REGION_CODE  = a.REGION_CODE
            AND b.SEGMENT_CODE = 'RETAIL'
            AND b.PERIOD_KEY   = &period_key.
   ORDER BY COALESCE(a.REGION_CODE, b.REGION_CODE),
            COALESCE(a.SEGMENT_CODE, b.SEGMENT_CODE);
QUIT;

PROC SQL;
   CREATE TABLE WORK.FINAL_RISK_DASHBOARD AS
   SELECT t1.CUSTOMER_ID,
          t1.SEGMENT_CODE,
          t1.REGION_CODE,
          t2.TOTAL_EXPOSURE,
          t2.CONTRACT_COUNT,
          t3.DEVIATION_FROM_AVG,
          h.HIERARCHY_DEPTH,
          h.DIRECT_MANAGER_NAME,
          p.RETAIL_BALANCE        AS REGION_RETAIL_TOTAL,
          cm.PCT_OF_TARGET        AS REGION_PCT_OF_TARGET
   FROM WORK.ACTIVE_AND_FLAGGED   t1
        INNER JOIN WORK.TOP_CUSTOMERS       t2  ON t2.CUSTOMER_ID = t1.CUSTOMER_ID
        LEFT  JOIN WORK.CUSTOMER_VS_AVG     t3  ON t3.CUSTOMER_ID = t1.CUSTOMER_ID
        LEFT  JOIN WORK.MANAGER_HIERARCHY   h   ON h.EMPLOYEE_ID  = t1.CUSTOMER_ID
        LEFT  JOIN WORK.SEGMENT_PIVOT       p   ON p.REGION_CODE  = t1.REGION_CODE
        LEFT  JOIN WORK.COVERAGE_MATRIX     cm  ON cm.REGION_CODE = t1.REGION_CODE
                                                AND cm.SEGMENT_CODE = t1.SEGMENT_CODE
   WHERE t2.TOTAL_EXPOSURE > &min_balance.
     AND (
           EXISTS (
              SELECT 1
              FROM STAGING.REVIEW_WATCHLIST rw
              WHERE rw.CUSTOMER_ID  = t1.CUSTOMER_ID
                AND rw.REVIEW_TYPE  = 'AML'
                AND rw.REVIEW_DATE <= &report_date.
           )
           OR t3.DEVIATION_FROM_AVG > 50000
         )
   ORDER BY t2.TOTAL_EXPOSURE DESC,
            t1.CUSTOMER_ID;
QUIT;
