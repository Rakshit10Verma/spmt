/*******************************************************************************
 * FILE: TC-14_contract_customer_multijoin_fullpatterns.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - %LET macro variables → Pentaho ${variable} substitution
 *   - %_eg_WhereParam macro expansion → explicit WHERE conditions
 *   - SAS date literals ('01JAN1900'd, '31DEC2400'd, today()) → TO_DATE() / SYSDATE
 *   - MISSING() / x = . / IS MISSING → IS NULL
 *   - NOT IN (0, .) → NOT IN (0) AND col IS NOT NULL
 *   - NOT = operator → <>
 *   - CONTAINS operator → LIKE '%...%'
 *   - BETWEEN with SAS date literals → BETWEEN with TO_DATE()
 *   - TGL time-slice filter pattern (valid_from <= date AND valid_to > date)
 *   - GUELTIG_BIS exclusive end date (> not >=)
 *   - MAX(PK_STAND) subquery for latest snapshot in structure tables
 *   - FORMAT=$30. / FORMAT=EURDFDD10. → stripped in Oracle
 *   - 'Column Name'n SAS name literal → renamed to valid identifier
 *   - CASE WHEN multi-branch column selection (amount/date by contract type)
 *   - IFN() → CASE WHEN
 *   - PUT() with custom format → CASE WHEN lookup
 *   - INTNX('MONTH', d, n, 'END') → LAST_DAY(ADD_MONTHS())
 *   - INTCK('MONTH', d1, d2) → MONTHS_BETWEEN()
 *   - CATS() / CATX() → || concatenation with TRIM()
 *   - TRANWRD() → REPLACE()
 *   - UPCASE() / LOWCASE() → UPPER() / LOWER()
 *   - SCAN() → REGEXP_SUBSTR()
 *   - COMPRESS(x, '', 'kd') → REGEXP_REPLACE() keep digits only
 *   - LEFT JOIN WHERE trap → conditions moved to ON clause
 *   - CALCULATED keyword in GROUP BY → expression repeated
 *   - ORDER BY inside CREATE TABLE → forbidden in Oracle, moved to SELECT
 *   - OUTER UNION CORR → UNION ALL with explicit NULL column padding
 *   - PROC TRANSPOSE equivalent → conditional aggregation CASE WHEN pivot
 *   - NVL(col, sentinel) in WHERE → (col IS NULL OR col >= value)
 *   - WORK. tables → dwh_temp.PREFIX_ tables
 *   - NOLOGGING PARALLEL(4) hint for large CTAS
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. %_eg_WhereParam macro with TYPE=D generates date comparisons using
 *      SAS date literals — must inspect %LET blocks to find actual values
 *   2. NVL(close_date, TO_DATE('99991231',...)) >= date silently excludes
 *      NULL close_dates in Oracle — must rewrite as (IS NULL OR >= date)
 *   3. NOT IN (0, .) — the dot is SAS numeric missing; Oracle equivalent
 *      requires a separate IS NOT NULL check
 *   4. GUELTIG_BIS is an exclusive end date in Oracle TGL tables (first day
 *      record is NO LONGER valid), SAS stored it as inclusive last day
 *   5. LEFT JOIN conditions in WHERE clause silently become INNER JOINs —
 *      all TGL filter conditions must be in the ON clause
 *   6. 'Linkage Type'n and 'Contract Status'n contain spaces — must be
 *      renamed to valid Oracle identifiers everywhere they appear
 *   7. CALCULATED keyword used in GROUP BY refers to aliased expressions —
 *      Oracle does not allow aliases in GROUP BY, expression must be repeated
 *   8. OUTER UNION CORR aligns columns by name; Oracle UNION ALL requires
 *      explicit NULL padding for missing columns in each branch
 *   9. Structure dimension table uses YYYYMMDD (8-digit) PK_STAND, not
 *      YYYYMM — must use reporting_date variable not period variable
 *  10. Partner name column in Oracle is PARTNERNAME (not DF_NAME as guessed)
 ******************************************************************************/

LIBNAME SOURCE META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Source Data']";
LIBNAME STAGING META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Staging Tables']";
LIBNAME DWH META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Data Warehouse']";

%macro _eg_conditional_dropds /parmbuff;
    %local num dsname;
    %let num=1;
    %let dsname=%qscan(&syspbuff,&num,',()');
    %do %while(&dsname ne);
        %if %sysfunc(exist(&dsname)) %then %do;
            proc sql; drop table &dsname; quit;
        %end;
        %let num=%eval(&num+1);
        %let dsname=%qscan(&syspbuff,&num,',()');
    %end;
%mend _eg_conditional_dropds;

%GLOBAL gReportingDate gPeriodKey;
%LET gReportingDate = &DWH_REPORTING_DATE.;
%LET gPeriodKey     = &DWH_PERIOD_KEY.;

%LET VON           = 01Jan1900;
%LET BIS           = 31Dec2400;
%LET MIN_CLOSE_DATE = %sysfunc(today(), date9.);

%_eg_conditional_dropds(WORK.VALID_REGIONS);

PROC SQL;
   CREATE TABLE WORK.VALID_REGIONS AS
   SELECT t1.REGION_KEY,
          t2.PARTNERNAME    AS REGION_NAME,
          t1.REGION_SHORT,
          t1.DISTRICT_CODE,
          t1.PARTNER_NR_REGION
     FROM STAGING.REGION_DIM t1
          INNER JOIN DWH.PARTNER_HISTORY t2
            ON  t1.PARTNER_NR_REGION = t2.PARTNER_KEY
            AND t2.VALID_FROM_DATE  <= "&gReportingDate."d
            AND t2.VALID_TO_DATE     > "&gReportingDate."d
            AND t2.MANDANT_CODE      = 'NOS'
    WHERE t1.PK_STAND = (
              SELECT MAX(PK_STAND)
                FROM STAGING.REGION_DIM
               WHERE PK_STAND <= "&gReportingDate."
          )
      AND UPCASE(t1.REGION_SHORT) CONTAINS 'NORTH';
QUIT;

%_eg_conditional_dropds(WORK.OPEN_CONTRACTS);

PROC SQL;
   CREATE TABLE WORK.OPEN_CONTRACTS AS
   SELECT t1.CONTRACT_ID,
          t1.PRODUCT_TYPE_CODE,
          t1.ACCOUNT_CLASS_CODE,
          t1.REGION_KEY_ORIG,
          t1.REGION_BRANCH_ORIG,
          t1.ADVISOR_PLACE_NR,
          t1.CUSTOMER_NR,
          t1.PARTNER_NR,
          (CASE
               WHEN t1.ACCOUNT_CLASS_CODE = 33
                    THEN t1.APPROVAL_AMOUNT_SUBORDINATED
               WHEN t1.ACCOUNT_CLASS_CODE NOT = 33
                    AND t1.ACCOUNT_CLASS_CODE >= 30
                    THEN t1.APPROVAL_AMOUNT_MORTGAGE
               ELSE t1.APPROVAL_AMOUNT_CREDIT
           END) FORMAT=16.2 AS APPROVAL_AMOUNT,
          (CASE
               WHEN t1.ACCOUNT_CLASS_CODE = 33
                    THEN t1.APPROVAL_DATE_SUBORDINATED
               WHEN t1.ACCOUNT_CLASS_CODE NOT = 33
                    AND t1.ACCOUNT_CLASS_CODE >= 30
                    THEN t1.APPROVAL_DATE_MORTGAGE
               ELSE t1.APPROVAL_DATE_CREDIT
           END) FORMAT=EURDFDD10. AS APPROVAL_DATE,
          t1.CAPITAL_BALANCE,
          t1.SERVICE_BALANCE,
          (CASE
               WHEN t1.ACCOUNT_CLASS_CODE = 10 THEN 'Savings'
               WHEN t1.ACCOUNT_CLASS_CODE = 20 THEN 'Bridge Loan'
               WHEN t1.ACCOUNT_CLASS_CODE = 30 THEN 'Building Loan'
               WHEN t1.ACCOUNT_CLASS_CODE = 31 THEN 'Repayment Extension'
               WHEN t1.ACCOUNT_CLASS_CODE = 32 THEN 'Other Loan'
               WHEN t1.ACCOUNT_CLASS_CODE = 33 THEN 'Subordinated Loan'
               ELSE 'Unknown (' || PUT(t1.ACCOUNT_CLASS_CODE, best12.) || ')'
           END) FORMAT=$30. AS 'Account Class Label'n,
          t1.DUNNING_INDICATOR,
          t1.CONTRACT_CLOSE_DATE,
          IFN(t1.CAPITAL_BALANCE < 0, 1, 0) AS IS_NEGATIVE_BALANCE_FLAG,
          INTNX('MONTH', t1.APPROVAL_DATE_MORTGAGE, 12, 'END') AS APPROVAL_PLUS_12M,
          INTCK('MONTH', t1.APPROVAL_DATE_MORTGAGE, today()) AS MONTHS_SINCE_APPROVAL
     FROM DWH.CONTRACT_MONTHLY t1
    WHERE t1.PK_STAND          = "&gPeriodKey."
      AND t1.MANDANT_CODE       = 'NOS'
      AND t1.REGION_KEY_ORIG   NOT IN (0, .)
      AND t1.ACCOUNT_CLASS_CODE NOT = 10
      AND NOT MISSING(t1.PARTNER_NR)
      AND NVL(t1.CONTRACT_CLOSE_DATE, '31DEC9999'd) >= "&MIN_CLOSE_DATE."d
      AND (CASE WHEN t1.ACCOUNT_CLASS_CODE = 33 THEN t1.APPROVAL_DATE_SUBORDINATED
                WHEN t1.ACCOUNT_CLASS_CODE >= 30 THEN t1.APPROVAL_DATE_MORTGAGE
                ELSE t1.APPROVAL_DATE_CREDIT END)   >= "&VON."d
      AND (CASE WHEN t1.ACCOUNT_CLASS_CODE = 33 THEN t1.APPROVAL_DATE_SUBORDINATED
                WHEN t1.ACCOUNT_CLASS_CODE >= 30 THEN t1.APPROVAL_DATE_MORTGAGE
                ELSE t1.APPROVAL_DATE_CREDIT END)   <= "&BIS."d
   ORDER BY t1.REGION_KEY_ORIG,
            t1.CONTRACT_ID,
            t1.CONTRACT_CLOSE_DATE;
QUIT;

%_eg_conditional_dropds(WORK.CONTRACT_WITH_ADDRESS);

PROC SQL;
   CREATE TABLE WORK.CONTRACT_WITH_ADDRESS AS
   SELECT t1.CONTRACT_ID,
          t1.PARTNER_NR,
          t1.APPROVAL_AMOUNT,
          t1.APPROVAL_DATE,
          t1.ACCOUNT_CLASS_CODE,
          t1.'Account Class Label'n,
          t1.CAPITAL_BALANCE,
          t1.SERVICE_BALANCE,
          t1.DUNNING_INDICATOR,
          t1.CONTRACT_CLOSE_DATE,
          t2.PARTNERNAME                          AS CUSTOMER_NAME,
          t2.BIRTH_DATE,
          CATS(t3.STREET_NAME, ' ', t3.HOUSE_NUMBER) AS FULL_STREET,
          CATX(', ', t3.POSTAL_CODE, t3.CITY_NAME) AS POSTAL_CITY,
          TRANWRD(t3.CITY_NAME, 'St.', 'Saint')   AS CITY_NORMALIZED,
          UPCASE(t3.COUNTRY_CODE)                  AS COUNTRY_UPPER,
          STRIP(t2.PARTNERNAME)                    AS CUSTOMER_NAME_TRIMMED,
          INDEX(t3.CITY_NAME, 'burg')              AS CITY_BURG_POS,
          SCAN(t2.PARTNERNAME, 1, ' ')             AS FIRST_NAME_TOKEN,
          COMPRESS(t3.PHONE_NUMBER, '', 'kd')      AS PHONE_DIGITS_ONLY,
          t3.CITY_NAME                             AS CITY
     FROM WORK.OPEN_CONTRACTS t1
          LEFT JOIN DWH.PARTNER_HISTORY t2
            ON  t1.PARTNER_NR           = t2.PARTNER_KEY
            AND t2.MANDANT_CODE         = 'NOS'
            AND t2.VALID_FROM_DATE     <= "&gReportingDate."d
            AND t2.VALID_TO_DATE        > "&gReportingDate."d
          LEFT JOIN DWH.ADDRESS_HISTORY t3
            ON  t1.CUSTOMER_NR          = t3.ADDRESS_KEY
            AND t3.MANDANT_CODE         = 'NOS'
            AND t3.VALID_FROM_DATE     <= "&gReportingDate."d
            AND t3.VALID_TO_DATE        > "&gReportingDate."d
    WHERE t1.ACCOUNT_CLASS_CODE NOT = 10;
QUIT;

%_eg_conditional_dropds(WORK.CONTRACT_REGION_SUMMARY);

PROC SQL;
   CREATE TABLE WORK.CONTRACT_REGION_SUMMARY AS
   SELECT t1.REGION_KEY_ORIG,
          (CASE
               WHEN t1.ACCOUNT_CLASS_CODE >= 30 THEN 'Loan'
               WHEN t1.ACCOUNT_CLASS_CODE = 10  THEN 'Savings'
               ELSE 'Other'
           END) FORMAT=$10. AS PRODUCT_CATEGORY,
          COUNT(*)                                     AS CONTRACT_COUNT,
          SUM(t1.APPROVAL_AMOUNT)                      AS TOTAL_APPROVAL_AMOUNT,
          SUM(t1.CAPITAL_BALANCE)                      AS TOTAL_CAPITAL_BALANCE,
          AVG(INTCK('MONTH',
                    t1.APPROVAL_DATE,
                    today()))                           AS AVG_MONTHS_SINCE_APPROVAL,
          SUM(IFN(t1.IS_NEGATIVE_BALANCE_FLAG = 1,
                  1, 0))                               AS NEGATIVE_BALANCE_COUNT
     FROM WORK.OPEN_CONTRACTS t1
    GROUP BY t1.REGION_KEY_ORIG,
             CALCULATED PRODUCT_CATEGORY
    ORDER BY t1.REGION_KEY_ORIG,
             CALCULATED PRODUCT_CATEGORY;
QUIT;

%_eg_conditional_dropds(WORK.ALL_CONTRACTS_UNION);

PROC SQL;
   CREATE TABLE WORK.ALL_CONTRACTS_UNION AS
   SELECT t1.CONTRACT_ID,
          t1.ACCOUNT_CLASS_CODE,
          t1.APPROVAL_AMOUNT,
          t1.APPROVAL_DATE,
          t1.CAPITAL_BALANCE,
          t1.DUNNING_INDICATOR,
          t1.ADVISOR_PLACE_NR,
          t1.CONTRACT_CLOSE_DATE,
          'ACTIVE' AS CONTRACT_STATUS
     FROM DWH.CONTRACT_ACTIVE t1
    WHERE t1.PK_STAND     = "&gPeriodKey."
      AND t1.MANDANT_CODE = 'NOS'
   OUTER UNION CORR
   SELECT t2.CONTRACT_ID,
          t2.ACCOUNT_CLASS_CODE,
          t2.APPROVAL_AMOUNT,
          t2.APPROVAL_DATE,
          t2.CAPITAL_BALANCE,
          . AS DUNNING_INDICATOR,
          . AS ADVISOR_PLACE_NR,
          t2.CONTRACT_CLOSE_DATE,
          'INACTIVE' AS CONTRACT_STATUS
     FROM DWH.CONTRACT_INACTIVE t2
    WHERE t2.MANDANT_CODE = 'NOS'
      AND t2.CONTRACT_CLOSE_DATE >= '01JAN2020'd;
QUIT;

%_eg_conditional_dropds(WORK.APPROVAL_PIVOT);

PROC SQL;
   CREATE TABLE WORK.APPROVAL_PIVOT AS
   SELECT t1.REGION_KEY_ORIG,
          SUM(CASE WHEN t1.PRODUCT_CATEGORY = 'Loan'
                   THEN t1.TOTAL_APPROVAL_AMOUNT ELSE 0 END) AS AMT_LOAN,
          SUM(CASE WHEN t1.PRODUCT_CATEGORY = 'Savings'
                   THEN t1.TOTAL_APPROVAL_AMOUNT ELSE 0 END) AS AMT_SAVINGS,
          SUM(CASE WHEN t1.PRODUCT_CATEGORY = 'Other'
                   THEN t1.TOTAL_APPROVAL_AMOUNT ELSE 0 END) AS AMT_OTHER,
          SUM(t1.TOTAL_APPROVAL_AMOUNT)                       AS AMT_TOTAL
     FROM WORK.CONTRACT_REGION_SUMMARY t1
    GROUP BY t1.REGION_KEY_ORIG
    ORDER BY t1.REGION_KEY_ORIG;
QUIT;

%_eg_conditional_dropds(WORK.FINAL_REPORT);

PROC SQL;
   CREATE TABLE WORK.FINAL_REPORT AS
   SELECT t1.CONTRACT_ID,
          t1.CUSTOMER_NAME,
          t1.ACCOUNT_CLASS_CODE,
          t1.'Account Class Label'n    FORMAT=$30. AS 'Account Class Label'n,
          t2.REGION_KEY,
          t2.REGION_NAME,
          t2.DISTRICT_CODE,
          t1.APPROVAL_AMOUNT,
          t1.APPROVAL_DATE             FORMAT=EURDFDD10.,
          t1.CAPITAL_BALANCE,
          t1.SERVICE_BALANCE,
          t1.DUNNING_INDICATOR,
          t1.CITY,
          t1.FULL_STREET,
          t1.CONTRACT_CLOSE_DATE       FORMAT=EURDFDD10.,
          INTNX('MONTH', t1.APPROVAL_DATE, 0, 'END') AS APPROVAL_MONTH_END,
          (CASE
               WHEN t1.APPROVAL_DATE
                    BETWEEN '01JAN2020'd AND '31DEC2022'd
                    THEN 'Legacy'
               WHEN t1.APPROVAL_DATE
                    BETWEEN '01JAN2023'd AND today()
                    THEN 'Current'
               ELSE 'Pre-2020'
           END)                        AS APPROVAL_COHORT,
          t3.TOTAL_APPROVAL_AMOUNT     AS REGION_TOTAL_APPROVALS,
          t3.CONTRACT_COUNT            AS REGION_CONTRACT_COUNT,
          t4.AMT_LOAN                  AS REGION_LOAN_TOTAL,
          t4.AMT_SAVINGS               AS REGION_SAVINGS_TOTAL
     FROM WORK.CONTRACT_WITH_ADDRESS t1
          INNER JOIN WORK.VALID_REGIONS t2
            ON t1.REGION_KEY_ORIG = t2.REGION_KEY
          LEFT JOIN WORK.CONTRACT_REGION_SUMMARY t3
            ON  t1.REGION_KEY_ORIG     = t3.REGION_KEY_ORIG
            AND t3.PRODUCT_CATEGORY    = (CASE
                                              WHEN t1.ACCOUNT_CLASS_CODE >= 30
                                                   THEN 'Loan'
                                              WHEN t1.ACCOUNT_CLASS_CODE = 10
                                                   THEN 'Savings'
                                              ELSE 'Other'
                                          END)
          LEFT JOIN WORK.APPROVAL_PIVOT t4
            ON t1.REGION_KEY_ORIG = t4.REGION_KEY_ORIG
    ORDER BY t2.REGION_KEY,
             t1.ACCOUNT_CLASS_CODE,
             t1.CONTRACT_ID;
QUIT;

%GLOBAL gExportDate;
%LET gExportDate = &DWH_REPORTING_DATE.;
%include "\\fileserver\scripts\macros\standard_export.sas";
%EXPORT_TO_DIRECTORY(WORK.FINAL_REPORT, "\\output\contracts\final_report_&gExportDate..xlsx");
