/*============================================================================
 * FILE: TC-06_put_formats_time_slices.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * 
 * CONVERSION PATTERNS PRESENT:
 *   - SAS PUT() function with formats → CASE WHEN or lookup subquery
 *   - Multiple PUT() calls in single SELECT (4 different formats)
 *   - Time slice filter patterns (valid_from / valid_to)
 *   - Monthly vs daily table filter patterns (6-digit vs 8-digit PK_STAND)
 *   - CASE WHEN for code-to-description mapping
 *   - Correlated subquery for lookup
 *   - NVL for null-safe arithmetic
 *   - Multiple table joins with different filter patterns
 *   - SAS comparison operators (gt, le) → Oracle (>, <=)
 *   - mdy() date function → TO_DATE()
 *   - Negative condition: NOT (X AND Y)
 *   - ORDER BY in CTAS (to be removed for Oracle)
 *
 * COMPLEXITY: High
 *
 * EDGE CASES / CHALLENGES:
 *   - PUT() uses SAS format catalogs — no direct Oracle equivalent
 *   - Must create CASE WHEN for each format or use lookup tables
 *   - $PNRTYP. is character format (note $ prefix)
 *   - Numeric formats (TARIF., CATFMT., PRFGRP.) have no $ prefix
 *   - Time slice tables (_TGL) need validity period filters
 *   - Monthly tables (_MTL) need 6-digit YYYYMM PK_STAND
 *   - Daily tables need 8-digit YYYYMMDD PK_STAND
 *   - NVL chains in arithmetic expressions
 *============================================================================*/

%LET report_end_date = 20250531;
%LET report_period = 202505;
%LET client_code = NORTH;

PROC SQL;
   CREATE TABLE work.enriched_transactions AS 
   SELECT t1.case_number, 
          t1.approved_amount, 
          t1.contract_id, 
          t1.product_type_code, 
          (SELECT product_desc 
           FROM staging.product_lookup 
           WHERE sysdate BETWEEN valid_from AND valid_to 
             AND product_type = t1.product_type_code) AS product_type_desc, 
          t1.tariff_code, 
          (PUT(t1.tariff_code, TARIF.)) AS tariff_desc,
          t1.payment_amount, 
          t1.category_code, 
          (PUT(t1.category_code, CATFMT.)) AS category_desc,
          t1.transaction_date, 
          t2.customer_id,
          t1.partner_type_code, 
          (PUT(t1.partner_type_code, $PNRTYP.)) AS partner_type_desc,
          t1.partner_id, 
          t1.first_name, 
          t1.profession_code, 
          (PUT(t1.profession_code, PRFGRP.)) AS profession_desc,
          t1.contract_approved_amt, 
          t1.contract_approved_date, 
          t1.branch_code, 
          t1.security_flag, 
          t1.reference_number, 
          t1.process_type, 
          t1.credit_type, 
          t1.last_name, 
          t1.release_type, 
          (CASE t1.org_unit_code 
             WHEN 4153100 THEN '53-10'
             WHEN 4153200 THEN '53-20'
             WHEN 4153300 THEN '53-30'
             WHEN 4153500 THEN '53-50'
             WHEN 4188010 THEN '88-01'
             ELSE 'Other'
          END) AS org_unit_desc, 
          t1.risk_score
   FROM work.transaction_details t1
        LEFT JOIN dwh.customer_master t2 
           ON (t1.partner_id = t2.partner_key)
   WHERE NOT (t1.process_type = 'BLANKO' AND t1.security_flag = '5')
   ORDER BY t1.case_number,
            t1.contract_id,
            t1.transaction_date;
QUIT;

PROC SQL;
   CREATE TABLE work.contract_with_customer AS 
   SELECT t1.contract_id, 
          t1.credit_type, 
          t1.case_number, 
          t1.profession_code_primary, 
          t1.tariff_code, 
          t1.approval_date, 
          t1.approved_amount, 
          t1.booking_date, 
          t1.partner_id, 
          t1.partner_type, 
          t1.spouse_partner_id, 
          t1.profession_code, 
          t1.customer_key, 
          t1.last_name, 
          t1.first_name, 
          t1.primary_customer_id,
          t2.pep_flag
   FROM dwh.approval_daily t1
   LEFT JOIN dwh.customer_daily t2 
       ON t1.primary_customer_id = t2.customer_key
       AND t2.client_code = "&client_code."
       AND t2.valid_from le "&report_end_date."d
       AND t2.valid_to gt "&report_end_date."d
   WHERE t1.approval_date >= mdy(05,1,2025)
     AND t1.approval_date le "&report_end_date."d
   ORDER BY t1.approval_date DESC;
QUIT;

PROC SQL;
   CREATE TABLE work.contract_enriched AS 
   SELECT t1.contract_id, 
          t1.credit_type, 
          t1.case_number, 
          t1.profession_code_primary, 
          t1.tariff_code, 
          t1.approval_date, 
          t1.approved_amount, 
          t1.booking_date, 
          t1.partner_id, 
          t1.partner_type, 
          t1.spouse_partner_id, 
          t1.profession_code, 
          t1.customer_key, 
          t1.last_name, 
          t1.first_name, 
          t1.primary_customer_id, 
          MAX(t2.regulatory_flag) AS regulatory_flag, 
          MAX(t2.broker_id) AS broker_id, 
          t1.pep_flag
   FROM work.contract_with_customer t1
   INNER JOIN dwh.account_monthly t2 
       ON t1.contract_id = t2.contract_key
       AND t2.period_key = &report_period.
       AND t2.client_code = "&client_code."
   GROUP BY t1.contract_id,
            t1.credit_type,
            t1.case_number,
            t1.profession_code_primary,
            t1.tariff_code,
            t1.approval_date,
            t1.approved_amount,
            t1.booking_date,
            t1.partner_id,
            t1.partner_type,
            t1.spouse_partner_id,
            t1.profession_code,
            t1.customer_key,
            t1.last_name,
            t1.first_name,
            t1.primary_customer_id,
            t1.pep_flag;
QUIT;

PROC SQL;
   CREATE TABLE work.final_calculations AS 
   SELECT t1.partner_id, 
          t1.contract_ref, 
          NVL(t1.contract_approved_amt, 0) 
            - (NVL(t2.principal_balance, 0) + NVL(t2.interest_balance, 0)) 
            AS net_approved_amount, 
          NVL(t1.contract_approved_amt, 0) 
            - (NVL(t2.principal_balance, 0) + NVL(t2.interest_balance, 0)) 
            AS approved_less_balance
   FROM work.credit_contracts t1
   LEFT JOIN work.savings_contracts t2 
       ON t1.contract_ref = t2.contract_ref;
QUIT;

PROC SQL;
   CREATE TABLE work.contract_amounts AS 
   SELECT DISTINCT 
          t1.contract_id, 
          t1.principal_balance, 
          t1.interest_balance, 
          CASE 
             WHEN t1.account_type IN (20, 21) THEN t1.approved_credit_amt
             WHEN t1.account_type IN (33) THEN t1.approved_special_amt
             ELSE t1.approved_standard_amt
          END AS approved_amount, 
          t1.case_number, 
          CASE 
             WHEN t1.account_type IN (20, 21) THEN t1.approved_credit_date
             WHEN t1.account_type IN (33) THEN t1.approved_special_date
             ELSE t1.approved_standard_date
          END AS approval_date
   FROM dwh.account_monthly t1
        INNER JOIN work.period_transactions t2 
           ON t1.case_number = t2.case_number 
          AND t1.account_type gt 10
          AND t1.close_date IS NULL
   WHERE t1.period_key = &report_period.
     AND t1.client_code = "&client_code.";
QUIT;
