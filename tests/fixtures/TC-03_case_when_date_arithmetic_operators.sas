/*******************************************************************************
 * FILE: TC-03_case_when_date_arithmetic_operators.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * 
 * CONVERSION PATTERNS PRESENT:
 *   - Macro variable references (&var.) → Pentaho ${var} with TO_DATE()
 *   - String literals: double quotes → single quotes
 *   - Comparison operators: NOT = → <>
 *   - WORK library tables → temp schema with prefix
 *   - %_eg_conditional_dropds macro → DROP TABLE IF EXISTS equivalent
 *   - Simple CASE WHEN statements
 *   - LEFT JOIN / INNER JOIN syntax
 *   - TO_NUMBER() for CASE comparisons on string columns
 *   - INTNX('month',...,6,'E') → ADD_MONTHS() date arithmetic
 *   - Column aliasing for downstream compatibility
 *   - ORDER BY with calculated columns
 *   - IN operator with multiple values
 *   - Subquery for MAX() period filtering
 *
 * COMPLEXITY: Medium
 *
 * EDGE CASES / TRICKY PARTS:
 *   - Date macro variables need TO_DATE() wrapper in Oracle
 *   - CASE with numeric comparison on VARCHAR column needs TO_NUMBER()
 *   - Double quotes valid in SAS but invalid in Oracle for strings
 *   - INTNX alignment 'E' means end-of-month → needs LAST_DAY()
 *   - Calculated column aliases referenced in ORDER BY
 ******************************************************************************/

%LET REPORT_DATE = 20250531;
%LET PERIOD_KEY = 202505;

%_eg_conditional_dropds(WORK.ACTIVE_CONTRACTS);

PROC SQL;
   CREATE TABLE WORK.ACTIVE_CONTRACTS AS 
   SELECT 
          t1.CONTRACT_KEY       AS DWH_CONTRACT_ID, 
          t1.DEPOT_KEY          AS DWH_DEPOT_ID,
          t1.PRODUCT_CODE       AS DWH_PRODUCT_TYPE,
          t1.PARTNER_KEY        AS DWH_PARTNER_ID,
          t1.START_DATE         AS DWH_START_DATE,
          t1.END_DATE           AS DWH_END_DATE,
          t1.BALANCE_AMOUNT     AS DWH_BALANCE,
          t1.INTEREST_RATE      AS DWH_RATE,
          t1.RATING_CODE        AS DWH_RATING,
          t1.REGION_CODE        AS DWH_REGION
      FROM SOURCE.CONTRACT_MASTER t1
      WHERE t1.PRODUCT_CODE IN ('LOAN', 'MORTGAGE', 'CREDIT')
        AND t1.PERIOD_KEY = &PERIOD_KEY.;
QUIT;

%_eg_conditional_dropds(WORK.CONTRACT_STATUS_REPORT);

PROC SQL;
   CREATE TABLE WORK.CONTRACT_STATUS_REPORT AS 
   SELECT 
          t1.DWH_CONTRACT_ID,
          t1.DWH_DEPOT_ID,
          t1.DWH_PARTNER_ID,
          t1.DWH_PRODUCT_TYPE,
          t1.DWH_BALANCE,
          t1.DWH_RATE,
          t1.DWH_START_DATE,
          t1.DWH_END_DATE,
          t2.PARTNER_NAME,
          t2.PARTNER_TYPE,
          CASE t1.DWH_RATING
              WHEN '1' THEN "Fixed Rate"
              WHEN '2' THEN "Variable Rate"
              WHEN '3' THEN "Mixed Rate"
              ELSE "Unknown"
          END AS RATE_TYPE_DESC,
          CASE
              WHEN t1.DWH_END_DATE < INTNX('month', &REPORT_DATE., 6, 'E')
              THEN "Expiring Soon"
              ELSE "Active"
          END AS DATE_STATUS,
          INTNX('month', &REPORT_DATE., 6, 'E') AS THRESHOLD_DATE
      FROM WORK.ACTIVE_CONTRACTS t1
      LEFT JOIN SOURCE.PARTNER_MASTER t2 
          ON t1.DWH_PARTNER_ID = t2.PARTNER_KEY
      ORDER BY DATE_STATUS, RATE_TYPE_DESC;
QUIT;

%_eg_conditional_dropds(WORK.EXPIRING_CONTRACTS);

PROC SQL;
   CREATE TABLE WORK.EXPIRING_CONTRACTS AS 
   SELECT t1.*
      FROM WORK.CONTRACT_STATUS_REPORT t1
      WHERE t1.DATE_STATUS NOT = 'Active';
QUIT;

%_eg_conditional_dropds(WORK.LATEST_TRANSACTIONS);

PROC SQL;
   CREATE TABLE WORK.LATEST_TRANSACTIONS AS 
   SELECT 
          t1.TRANSACTION_ID,
          t1.CONTRACT_ID,
          t1.TRANSACTION_DATE,
          t1.AMOUNT,
          t1.TRANSACTION_TYPE
      FROM SOURCE.TRANSACTION_HISTORY t1
      INNER JOIN (
          SELECT MAX(PERIOD_KEY) AS MAX_PERIOD 
          FROM SOURCE.TRANSACTION_HISTORY
      ) t2 ON t1.PERIOD_KEY = t2.MAX_PERIOD
      WHERE t1.AMOUNT > 0;
QUIT;
