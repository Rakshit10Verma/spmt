/*******************************************************************************
 * FILE: TC-07_chained_tables_calculated_having.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * 
 * CONVERSION PATTERNS PRESENT:
 *   - Multiple table joins with different join types
 *   - Chained table creation (temp table dependencies across steps)
 *   - SAS sum() for NULL-safe arithmetic → NVL() chains
 *   - CALCULATED keyword in HAVING → repeat aggregate expression
 *   - mdy() date function → TO_DATE()
 *   - INNER JOIN with snapshot period filter (PK_STAND)
 *   - LEFT JOIN between work tables
 *   - MAX() aggregation with GROUP BY
 *   - DISTINCT with complex expressions
 *   - Macro variables in JOIN conditions and WHERE
 *
 * COMPLEXITY: High
 *
 * EDGE CASES / CHALLENGES:
 *   - SAS sum() ignores NULLs (returns non-NULL if any arg is non-NULL)
 *     Oracle + operator propagates NULL — must wrap in NVL()
 *   - Chained temp tables must be created in correct order
 *   - CALCULATED keyword not valid in Oracle
 *   - mdy(month, day, year) argument order differs from TO_DATE
 *   - Column aliasing needed for downstream table references
 *   - LEFT JOIN + date condition: SAS includes NULLs differently
 ******************************************************************************/

%LET report_date = 20250531;
%LET report_month = 05;
%LET report_year = 2025;
%LET client_code = ABC;
%LET queue_entry = 202505;

PROC SQL;
   CREATE TABLE WORK.CREDIT_CONTRACTS AS 
   SELECT DISTINCT 
          t1.customer_id, 
          t1.contract_id, 
          t1.contract_type, 
          t1.principal_balance, 
          t1.interest_balance, 
          t1.credit_limit
   FROM WORK.CONTRACT_AMOUNTS t1
   WHERE t1.contract_type > 10;
QUIT;

PROC SQL;
   CREATE TABLE WORK.SAVINGS_CONTRACTS AS 
   SELECT DISTINCT 
          t1.customer_id, 
          t1.contract_id, 
          t1.contract_type, 
          t1.principal_balance, 
          t1.interest_balance, 
          t1.credit_limit
   FROM WORK.CONTRACT_AMOUNTS t1
   WHERE t1.contract_type = 10;
QUIT;

PROC SQL;
   CREATE TABLE WORK.COMBINED_POSITION AS 
   SELECT t1.customer_id, 
          t1.contract_id, 
          (sum(t1.credit_limit, (-1)* (t2.principal_balance + t2.interest_balance))) AS 
            net_credit_available, 
          (sum(t1.credit_limit, (-1)* (t2.principal_balance + t2.interest_balance))) AS 
            available_balance
   FROM WORK.CREDIT_CONTRACTS t1
        LEFT JOIN WORK.SAVINGS_CONTRACTS t2 
          ON t1.contract_id = t2.contract_id;
QUIT;

PROC SQL;
   CREATE TABLE WORK.ENRICHED_CONTRACTS AS 
   SELECT t1.contract_id, 
          t1.customer_id, 
          t1.principal_balance, 
          (MAX(t2.regulatory_classification)) AS regulatory_class, 
          (MAX(t2.originating_broker)) AS broker_id, 
          t1.risk_category
   FROM WORK.MONTHLY_TRANSACTIONS t1
        INNER JOIN staging.contract_details t2 
          ON t1.contract_id = t2.contract_id
         AND t2.snapshot_period = &queue_entry.
         AND t2.client_code = "&client_code."
   GROUP BY t1.contract_id,
            t1.customer_id,
            t1.principal_balance,
            t1.risk_category;
QUIT;

PROC SQL;
   CREATE TABLE WORK.AGGREGATED_BY_CUSTOMER AS 
   SELECT t1.customer_id AS primary_customer, 
          (SUM(t1.available_balance)) AS total_available_credit
   FROM WORK.COMBINED_POSITION t1
   GROUP BY t1.customer_id
   HAVING (CALCULATED total_available_credit) > 750000;
QUIT;

PROC SQL;
   CREATE TABLE WORK.HIGH_EXPOSURE_DETAILS AS 
   SELECT t1.contract_id, 
          t1.customer_id, 
          t1.principal_balance, 
          t1.regulatory_class, 
          t1.broker_id, 
          t1.risk_category,
          t1.approval_date,
          t2.total_available_credit
   FROM WORK.ENRICHED_CONTRACTS t1
        INNER JOIN WORK.AGGREGATED_BY_CUSTOMER t2 
          ON t1.customer_id = t2.primary_customer
   WHERE t1.approval_date >= mdy(&report_month., 1, &report_year.);
QUIT;
