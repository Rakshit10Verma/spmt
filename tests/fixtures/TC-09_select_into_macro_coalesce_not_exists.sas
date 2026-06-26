/*******************************************************************************
 * FILE: TC-09_select_into_macro_coalesce_not_exists.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - PROC SQL NOPRINT → comment or drop
 *   - SELECT ... INTO :macrovar TRIMMED → cannot convert; emit warning comment
 *   - SELECT ... INTO :macrovar SEPARATED BY ',' → cannot convert; emit warning
 *   - COALESCEC() (character coalesce) → COALESCE()
 *   - COALESCE() (numeric) → COALESCE() (same name, compatible)
 *   - NOT EXISTS subquery → Oracle NOT EXISTS (compatible)
 *   - EXISTS subquery → Oracle EXISTS (compatible)
 *   - SAS date literals in subquery predicates → TO_DATE()
 *   - MISSING() function → IS NULL
 *   - NMISS() / CMISS() → SAS-specific; convert to CASE WHEN IS NULL
 *   - PROC SQL STIMER → ignored option
 *   - PROC SQL FEEDBACK → ignored option
 *   - %IF %THEN inside macro → ignored / cannot convert
 *   - Double-ampersand macro ref (&&prefix.code.) → ${prefix.code}
 *   - %EVAL() → pre-computed constant or comment
 *   - PROC SQL RESET statement → ignored
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. SELECT INTO :var is a SAS macro-population technique — Oracle has no
 *      direct equivalent; tool must emit a comment block explaining the gap.
 *   2. COALESCEC is character-specific in SAS; Oracle COALESCE handles both —
 *      rename only, no logic change needed.
 *   3. NMISS(a, b, c) returns count of missing numerics — no Oracle equiv;
 *      must expand to: (CASE WHEN a IS NULL THEN 1 ELSE 0 END + ...).
 *   4. CMISS is the character counterpart of NMISS — same expansion needed.
 *   5. NOT EXISTS / EXISTS are syntactically compatible but SAS handles NULL
 *      propagation differently — flag for QA.
 *   6. PROC SQL NOPRINT suppresses output — irrelevant in Oracle; strip it.
 *   7. &&prefix.code. is a double-macro-resolution reference; Pentaho maps
 *      it to ${prefix.code}.
 *   8. %EVAL() arithmetic (e.g. %EVAL(&year - 1)) must become a literal or
 *      be left as ${} expression for Pentaho to resolve.
 ******************************************************************************/

%GLOBAL report_date threshold_amount min_rows;
%LET report_date  = 20250531;
%LET threshold_amount = 10000;
%LET region_prefix = EMEA;

PROC SQL NOPRINT STIMER FEEDBACK;
   SELECT COUNT(*) INTO :min_rows TRIMMED
   FROM SOURCE.CONTRACTS_RAW
   WHERE STATUS_FLAG = 'A';
QUIT;

PROC SQL NOPRINT;
   SELECT MAX(SNAPSHOT_DATE) INTO :last_snapshot TRIMMED
   FROM STAGING.DAILY_SNAPSHOTS
   WHERE SNAPSHOT_DATE <= &report_date.;
QUIT;

PROC SQL NOPRINT;
   SELECT DISTINCT REGION_CODE INTO :region_list SEPARATED BY ','
   FROM SOURCE.REGION_MASTER
   WHERE ACTIVE_FLAG = 'Y';
QUIT;

PROC SQL;
   CREATE TABLE WORK.ACTIVE_CUSTOMERS AS
   SELECT t1.CUSTOMER_ID,
          COALESCEC(t1.PREFERRED_NAME, t1.LEGAL_NAME, t1.SYSTEM_NAME) AS DISPLAY_NAME,
          COALESCE(t1.MOBILE_PHONE, t1.HOME_PHONE, t1.WORK_PHONE)    AS CONTACT_PHONE,
          t1.EMAIL_ADDRESS,
          t1.SEGMENT_CODE,
          t1.&&region_prefix.REGION_ID. AS REGION_ID,
          t1.ONBOARDING_DATE,
          t1.RISK_TIER
   FROM SOURCE.CUSTOMER_MASTER t1
   WHERE t1.STATUS_FLAG = 'A'
     AND NOT MISSING(t1.CUSTOMER_ID)
     AND EXISTS (
            SELECT 1
            FROM SOURCE.CONTRACTS_RAW c
            WHERE c.CUSTOMER_ID = t1.CUSTOMER_ID
              AND c.CONTRACT_STATUS NOT = 'X'
              AND c.START_DATE >= '01Jan2023'd
         );
QUIT;

PROC SQL RESET NOPRINT;

PROC SQL;
   CREATE TABLE WORK.CUSTOMER_QUALITY AS
   SELECT t1.CUSTOMER_ID,
          t1.DISPLAY_NAME,
          t1.CONTACT_PHONE,
          t1.EMAIL_ADDRESS,
          t1.SEGMENT_CODE,
          t1.RISK_TIER,
          NMISS(t1.RISK_SCORE, t1.CREDIT_LIMIT, t1.EXPOSURE_AMT)   AS MISSING_NUMERIC_CNT,
          CMISS(t1.EMAIL_ADDRESS, t1.CONTACT_PHONE, t1.SEGMENT_CODE) AS MISSING_CHAR_CNT,
          (CASE
              WHEN NMISS(t1.RISK_SCORE, t1.CREDIT_LIMIT) > 0 THEN 'INCOMPLETE'
              ELSE 'COMPLETE'
           END) AS DATA_QUALITY_FLAG
   FROM WORK.ACTIVE_CUSTOMERS t1
        LEFT JOIN SOURCE.CUSTOMER_SCORES t2
             ON t2.CUSTOMER_ID = t1.CUSTOMER_ID
            AND t2.SCORE_DATE = &last_snapshot.
   WHERE NOT EXISTS (
            SELECT 1
            FROM STAGING.BLACKLIST bl
            WHERE bl.CUSTOMER_ID = t1.CUSTOMER_ID
              AND bl.BLACKLIST_TYPE IN ('FRAUD', 'SANCTIONS')
              AND bl.EFFECTIVE_DATE <= '31May2025'd
         );
QUIT;

PROC SQL;
   CREATE TABLE WORK.CONTRACT_SUMMARY AS
   SELECT t1.CUSTOMER_ID,
          COUNT(t2.CONTRACT_ID)                                        AS TOTAL_CONTRACTS,
          SUM(COALESCE(t2.OUTSTANDING_BALANCE, 0))                     AS TOTAL_BALANCE,
          MAX(t2.CONTRACT_START_DATE)                                   AS LATEST_START,
          MIN(t2.CONTRACT_START_DATE)                                   AS EARLIEST_START,
          COALESCE(MAX(t2.CREDIT_LIMIT), 0)                            AS MAX_CREDIT,
          SUM(COALESCE(t2.OUTSTANDING_BALANCE, 0)) /
              NULLIF(SUM(COALESCE(t2.CREDIT_LIMIT, 0)), 0)             AS UTILISATION_RATIO
   FROM WORK.ACTIVE_CUSTOMERS t1
        LEFT JOIN SOURCE.CONTRACTS_RAW t2
             ON t2.CUSTOMER_ID = t1.CUSTOMER_ID
            AND t2.CONTRACT_STATUS NOT = 'X'
   GROUP BY t1.CUSTOMER_ID
   HAVING SUM(COALESCE(t2.OUTSTANDING_BALANCE, 0)) >= &threshold_amount.;
QUIT;

PROC SQL;
   CREATE TABLE WORK.ENRICHED_CUSTOMER_REPORT AS
   SELECT q.CUSTOMER_ID,
          q.DISPLAY_NAME,
          q.CONTACT_PHONE,
          q.EMAIL_ADDRESS,
          q.SEGMENT_CODE,
          q.RISK_TIER,
          q.DATA_QUALITY_FLAG,
          q.MISSING_NUMERIC_CNT,
          q.MISSING_CHAR_CNT,
          s.TOTAL_CONTRACTS,
          s.TOTAL_BALANCE,
          s.LATEST_START,
          s.EARLIEST_START,
          s.MAX_CREDIT,
          s.UTILISATION_RATIO,
          r.REGION_NAME,
          r.COUNTRY_CODE,
          r.REPORTING_CLUSTER
   FROM WORK.CUSTOMER_QUALITY q
        LEFT JOIN WORK.CONTRACT_SUMMARY s
             ON s.CUSTOMER_ID = q.CUSTOMER_ID
        LEFT JOIN SOURCE.REGION_MASTER r
             ON r.REGION_ID = q.REGION_ID
            AND r.ACTIVE_FLAG = 'Y'
   WHERE q.DATA_QUALITY_FLAG = 'COMPLETE'
      OR s.TOTAL_BALANCE > &threshold_amount.
   ORDER BY s.TOTAL_BALANCE DESC,
            q.CUSTOMER_ID;
QUIT;
