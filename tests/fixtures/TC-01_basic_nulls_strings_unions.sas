/******************************************************************************
 * FILE: TC-01_basic_nulls_strings_unions.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * 
 * CONVERSION PATTERNS PRESENT:
 *   - IS MISSING → IS NULL
 *   - NOT IS MISSING → IS NOT NULL  
 *   - &macro_var. → ${pentaho_variable}
 *   - %LET declarations
 *   - INTNX("MONTH", date, 0, "BEGIN") → TRUNC(date, 'MM')
 *   - INTNX("MONTH", date, 0, "END") → LAST_DAY(date)
 *   - WORK.tablename → schema.prefix_tablename
 *   - UPCASE() → UPPER()
 *   - LOWCASE() → LOWER()
 *   - COMPRESS() with modifiers ('kd', 'ka') → REGEXP_REPLACE()
 *   - STRIP() → TRIM()
 *   - Double quotes → Single quotes
 *   - OUTER UNION CORR → UNION ALL
 *   - FORMAT statement removal
 *   - Basic JOINs with date filters
 *   - ORDER BY with column aliases
 *
 * COMPLEXITY: Basic / Medium
 *
 * EDGE CASES / TRICKY PARTS:
 *   - Multiple macro variables in date comparisons
 *   - FORMAT on columns must be removed entirely
 *   - OUTER UNION CORR requires column alignment check
 *   - COMPRESS with 'kd' modifier (keep digits) has no simple Oracle equiv
 *   - WORK schema maps to temp schema with prefix
 ******************************************************************************/

%LET report_date = &DWH_MONTH_END_DATE.;
%LET org_unit = &DWH_ORG_UNIT.;
%LET mandant_code = NOS;

PROC SQL;
CREATE TABLE WORK.CUSTOMER_FILTERED AS
SELECT 
    t1.CUSTOMER_ID,
    t1.CUSTOMER_NAME,
    t1.EMAIL_ADDRESS,
    t1.PHONE_NUMBER,
    t1.REGISTRATION_DATE
FROM SOURCE.CUSTOMER_MASTER t1
WHERE t1.ORG_UNIT_CODE = "&org_unit."
  AND t1.EMAIL_ADDRESS IS NOT MISSING
  AND t1.PHONE_NUMBER IS MISSING
  AND t1.STATUS_FLAG = "A"
;
QUIT;

PROC SQL;
CREATE TABLE WORK.MONTHLY_TRANSACTIONS AS
SELECT 
    t1.TRANSACTION_ID,
    t1.CUSTOMER_ID,
    UPCASE(STRIP(t1.TRANSACTION_TYPE)) AS TRANSACTION_TYPE_CLEAN,
    LOWCASE(t1.CHANNEL_CODE) AS CHANNEL_LOWER,
    t1.TRANSACTION_DATE,
    t1.AMOUNT
FROM SOURCE.TRANSACTIONS t1
WHERE t1.TRANSACTION_DATE >= INTNX("MONTH", &report_date., 0, "BEGIN")
  AND t1.TRANSACTION_DATE <= INTNX("MONTH", &report_date., 0, "END")
  AND t1.MANDANT = "&mandant_code."
;
QUIT;

PROC SQL;
CREATE TABLE WORK.CLEANED_CONTRACTS AS
SELECT 
    t1.CONTRACT_ID,
    COMPRESS(t1.CONTRACT_NUMBER) AS CONTRACT_NUMBER_CLEAN,
    COMPRESS(t1.PHONE_RAW, , "kd") AS PHONE_DIGITS_ONLY,
    COMPRESS(t1.NAME_RAW, , "ka") AS NAME_ALPHA_ONLY,
    t1.START_DATE FORMAT=DATE9.,
    t1.END_DATE FORMAT=DDMMYY10.,
    t1.AMOUNT FORMAT=COMMA12.2
FROM SOURCE.CONTRACTS_RAW t1
WHERE t1.CONTRACT_STATUS IS NOT MISSING
;
QUIT;

PROC SQL;
CREATE TABLE WORK.COMBINED_SOURCES AS
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    "RETAIL" AS SOURCE_SYSTEM,
    REGISTRATION_DATE
FROM SOURCE.RETAIL_CUSTOMERS
WHERE STATUS = "ACTIVE"

OUTER UNION CORR

SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    "CORPORATE" AS SOURCE_SYSTEM,
    REGISTRATION_DATE
FROM SOURCE.CORPORATE_CUSTOMERS
WHERE STATUS = "ACTIVE"
;
QUIT;

PROC SQL;
CREATE TABLE WORK.CUSTOMER_ORDERS AS
SELECT DISTINCT
    t1.CUSTOMER_ID,
    t1.CUSTOMER_NAME,
    t2.ORDER_ID,
    t2.ORDER_DATE,
    t2.ORDER_AMOUNT,
    t3.PRODUCT_NAME
FROM WORK.CUSTOMER_FILTERED t1
INNER JOIN SOURCE.ORDERS t2
    ON t1.CUSTOMER_ID = t2.CUSTOMER_ID
    AND t2.MANDANT = "&mandant_code."
INNER JOIN SOURCE.PRODUCTS t3
    ON t2.PRODUCT_ID = t3.PRODUCT_ID
WHERE t2.ORDER_DATE >= INTNX("MONTH", &report_date., 0, "BEGIN")
  AND t2.ORDER_AMOUNT IS NOT MISSING
ORDER BY t2.ORDER_DATE DESC, t1.CUSTOMER_ID
;
QUIT;
