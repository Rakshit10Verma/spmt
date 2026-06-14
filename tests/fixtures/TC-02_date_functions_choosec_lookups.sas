/*
 * =============================================================================
 * FILE: TC-02_date_functions_choosec_lookups.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * =============================================================================
 * COMPLEXITY: Basic / Medium
 * 
 * CONVERSION PATTERNS PRESENT:
 *   - today() → TRUNC(SYSDATE)
 *   - SAS date literal '31Dec9999'd → IS NULL (active record pattern)
 *   - SAS date literal 'DDMonYYYY'd → TO_DATE('YYYYMMDD', 'YYYYMMDD')
 *   - PUT(numeric, format.) → TO_CHAR()
 *   - INTNX('month', date, n) → ADD_MONTHS()
 *   - CHOOSEC(INPUT(...), ...) → CASE TO_NUMBER() WHEN ... END
 *   - &macro_variable → ${pentaho_variable}
 *   - LABEL= clause → Remove (not supported in Oracle)
 *   - FORMAT= clause → Remove
 *   - DISTINCT keyword
 *   - LEFT JOIN with lookup table
 *   - CASE WHEN for conditional text output
 *   - ORDER BY with multiple columns
 *   - Redundant WHERE logic (IN + NOT IN same values) = always TRUE
 *
 * EDGE CASES / TRICKY PARTS:
 *   - CHOOSEC uses 1-based indexing: CHOOSEC(1, "A", "B", "C") returns "A"
 *   - INPUT() converts string to number before CHOOSEC processes it
 *   - '31Dec9999'd means "active/open" — Oracle often uses NULL instead
 *   - INTNX returns beginning of interval by default
 *   - LABEL= is SAS-specific metadata, no Oracle equivalent
 *   - Redundant (IN list OR NOT IN same list) is always TRUE — can be removed
 * =============================================================================
 */

%LET report_date = 20250531;
%LET report_period = 202505;

PROC SQL;
   CREATE TABLE WORK.CONTRACT_TYPES_VALID AS 
   SELECT DISTINCT 
          t1.CONTRACT_TYPE_CD, 
          t1.CONTRACT_TYPE_DESC, 
          t1.IS_REFINANCING_FL
      FROM SOURCE.CONTRACT_TYPE_REF t1
      WHERE t1.VALID_FROM_DT < today() 
        AND t1.VALID_TO_DT > today();
QUIT;

PROC SQL;
   CREATE TABLE WORK.CONTRACTS_WITH_DETAILS AS 
   SELECT t1.CONTRACT_NR, 
          t1.CONTRACT_TYPE_CD, 
          t1.PRODUCT_TYPE_CD, 
          t2.CONTRACT_TYPE_DESC, 
          t2.IS_REFINANCING_FL, 
          t1.PRINCIPAL_AMT, 
          t1.INTEREST_AMT, 
          (PUT(t1.PRODUCT_TYPE_CD, 3.)) AS PRODUCT_TYPE_STR, 
          t1.CUSTOMER_NR, 
          t1.CUSTOMER_TYPE_CD, 
          t1.CUSTOMER_NAME, 
          t1.CUSTOMER_NAME_2, 
          t1.INTEREST_LOCK_TYPE_CD, 
          t1.INTEREST_LOCK_END_DT, 
          (CHOOSEC(INPUT(t1.INTEREST_LOCK_TYPE_CD, 10.), "Fixed", "Variable", "Allocated")) 
            LABEL="Interest Lock Type Description" AS INTEREST_LOCK_TYPE_DESC, 
          t1.REGION_CD, 
          t1.BRANCH_NR, 
          t1.AGENT_NR, 
          (INTNX('month', &report_date, 6)) FORMAT=DDMMYYP10. AS INTEREST_REVIEW_THRESHOLD, 
          (CASE
              WHEN t1.INTEREST_LOCK_END_DT < (INTNX('month', &report_date, 6)) THEN 
                'Interest review due within six months'
          END) AS INTEREST_REVIEW_FLAG
      FROM SOURCE.CONTRACTS t1
           LEFT JOIN WORK.CONTRACT_TYPES_VALID t2 
             ON (t1.CONTRACT_TYPE_CD = t2.CONTRACT_TYPE_CD)
      WHERE ( t1.PRODUCT_TYPE_CD IN (996, 998) 
              OR t1.PRODUCT_TYPE_CD NOT IN (996, 998) ) 
        AND t1.CONTRACT_CLOSE_DT = '31Dec9999'd
      ORDER BY t2.IS_REFINANCING_FL,
               t1.CONTRACT_TYPE_CD;
QUIT;

PROC SQL;
   CREATE TABLE WORK.CONTRACTS_FINAL AS 
   SELECT t2.PERSON_NR, 
          t1.CONTRACT_NR, 
          t1.CONTRACT_TYPE_CD, 
          t1.PRODUCT_TYPE_CD, 
          t1.CONTRACT_TYPE_DESC, 
          t1.IS_REFINANCING_FL, 
          t1.PRINCIPAL_AMT, 
          t1.INTEREST_AMT, 
          t1.PRODUCT_TYPE_STR, 
          t1.CUSTOMER_NR, 
          t1.CUSTOMER_TYPE_CD, 
          t1.CUSTOMER_NAME, 
          t1.CUSTOMER_NAME_2, 
          t1.INTEREST_LOCK_TYPE_CD, 
          t1.INTEREST_LOCK_END_DT, 
          t1.INTEREST_LOCK_TYPE_DESC, 
          t1.INTEREST_REVIEW_THRESHOLD, 
          t1.INTEREST_REVIEW_FLAG
      FROM WORK.CONTRACTS_WITH_DETAILS t1
           LEFT JOIN SOURCE.CUSTOMER_PERSON_XREF t2 
             ON (t1.CUSTOMER_NR = t2.CUSTOMER_NR)
      WHERE t1.IS_REFINANCING_FL = 'X'
      ORDER BY t1.INTEREST_REVIEW_FLAG DESC,
               t1.INTEREST_LOCK_END_DT;
QUIT;
