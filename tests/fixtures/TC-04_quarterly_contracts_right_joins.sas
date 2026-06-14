/* ============================================================
   FILE: TC-04_quarterly_contracts_right_joins.sas
   SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
   
   CONVERSION PATTERNS PRESENT:
   * MISSING() function → IS NULL
   * SAS date literal '31Dec9999'd → IS NULL (active record pattern)
   * today() function → TRUNC(SYSDATE)
   * intnx() with 'E' alignment → LAST_DAY(ADD_MONTHS())
   * CALCULATED keyword → inline expression repetition
   * PUT() with numeric format → CASE WHEN
   * SAS macro variables (&var) → Pentaho ${var}
   * NOT = operator → <>
   * Column names with spaces ('Name'n) → underscore naming
   * RIGHT JOIN + WHERE on outer table → effective INNER JOIN
   * Date output formatting (FORMAT=) → TO_CHAR() or removed
   * ORDER BY with multiple columns and DESC
   
   COMPLEXITY: Medium
   
   EDGE CASES / TRICKY PARTS:
   - '31Dec9999'd means "active/open" but Oracle uses NULL
   - RIGHT JOIN with WHERE filter on LEFT table excludes NULL rows,
     effectively becoming INNER JOIN
   - CALCULATED keyword not valid in Oracle; must repeat expression
   - PUT() requires extracting format definitions from SAS catalog
   - intnx() alignment 'E' means end-of-month, needs LAST_DAY()
   - NOT = is SAS-specific, Oracle uses <>
============================================================ */

%LET report_date = '31May2025'd;
%LET period_key = 202505;
%LET client_code = MAIN;

PROC SQL;
   CREATE TABLE work.active_contracts AS 
   SELECT t1.contract_id, 
          t1.contract_type_cd, 
          t1.customer_id, 
          t1.start_date,
          t1.contract_amount, 
          t1.balance_amount, 
          t1.assigned_partner_id
   FROM source.contract_master t1
   WHERE t1.contract_type_cd = 10 
     AND t1.closing_date = '31Dec9999'd 
     AND t1.special_flag = 'X' 
     AND t1.status_cd = 1 
     AND t1.assigned_partner_id NOT = 0;
QUIT;

PROC SQL;
   CREATE TABLE work.valid_orgs AS 
   SELECT t1.org_id, 
          t1.bank_code,
          t1.org_name
   FROM source.organization t1
   WHERE t1.org_id NOT IN (63, 99999);
QUIT;

PROC SQL;
   CREATE TABLE work.contracts_with_org AS 
   SELECT t1.contract_id, 
          t1.contract_type_cd, 
          (PUT(t1.contract_type_cd, CTYPE.)) AS 'Contract Type'n, 
          t1.customer_id, 
          t1.start_date,
          t1.contract_amount, 
          t1.balance_amount, 
          t1.assigned_partner_id, 
          t2.org_id, 
          t2.bank_code, 
          t3.org_full_name
   FROM work.active_contracts t1
        LEFT JOIN work.valid_orgs t2 ON (t1.assigned_partner_id = t2.bank_code)
        LEFT JOIN dwh.organization_dim t3 ON (t2.org_id = t3.org_key);
QUIT;

PROC SQL;
   CREATE TABLE work.new_assignments AS 
   SELECT t1.contract_id, 
          t1.'Contract Type'n, 
          t1.customer_id, 
          t1.start_date,
          t1.contract_amount, 
          t1.balance_amount, 
          t1.org_full_name, 
          t2.assignment_start_date, 
          t2.assignment_end_date, 
          (intnx('month', &report_date, -3, 'E')) FORMAT=DDMMYYP10. AS prev_quarter
   FROM source.assignment_history t2
        RIGHT JOIN work.contracts_with_org t1 
          ON (t2.contract_id = t1.contract_id) 
         AND (t1.assigned_partner_id = t2.partner_id)
   WHERE t2.assignment_start_date >= (CALCULATED prev_quarter)
   ORDER BY t2.assignment_end_date,
            t2.assignment_start_date DESC;
QUIT;

PROC SQL;
   CREATE TABLE work.terminated_assignments AS 
   SELECT t1.contract_id, 
          t1.'Contract Type'n, 
          t1.customer_id, 
          t1.start_date,
          t1.contract_amount, 
          t1.balance_amount, 
          t2.assignment_start_date, 
          t2.assignment_end_date, 
          (intnx('month', &report_date, -3, 'E')) FORMAT=DDMMYYP10. AS prev_quarter
   FROM source.assignment_history t2
        RIGHT JOIN work.contracts_with_org t1 
          ON (t2.contract_id = t1.contract_id) 
         AND (t1.assigned_partner_id = t2.partner_id)
   WHERE t2.assignment_end_date >= (CALCULATED prev_quarter) 
     AND t2.assignment_end_date NOT = '31Dec9999'd
   ORDER BY t2.assignment_end_date,
            t2.assignment_start_date DESC;
QUIT;
