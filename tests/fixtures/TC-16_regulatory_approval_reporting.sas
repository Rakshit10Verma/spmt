/*******************************************************************************
 * FILE: TC-16_regulatory_approval_reporting.sas
 * SOURCE CHAT: SAS-to-Pentaho/Oracle migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - %LET macro variable declaration       → infrastructure, not converted
 *   - &macro_var. references                → ${prop_...} Pentaho parameters
 *   - WORK. prefix                          → DATAMART_SAS_TEMP.PREFIX_ schema
 *   - FROM DUAL                             → FROM sys.dual
 *   - STAGE_EXT_..._YYYYMM (month suffix)   → STAGE_HIST_... + idwh_berichtszeit filter
 *   - MISSING(col)                          → col IS NULL
 *   - NOT IS MISSING (unusual SAS syntax)   → IS NOT NULL
 *   - PUT(col, USERFORMAT.)                 → CASE WHEN decode block (custom SAS format)
 *   - MDY(MONTH(date), 1, YEAR(date))       → TRUNC(date, 'MM')
 *   - DWH_BOOKING_DATE absent from source   → derived as TO_DATE(macro, 'YYYYMMDD')
 *   - ORDER BY inside CREATE TABLE AS SELECT → forbidden in Oracle; move to export SELECT
 *   - IFN(condition, a, b)                  → CASE WHEN condition THEN a ELSE b END
 *   - INTNX('MONTH', date, -1, 'END')       → LAST_DAY(ADD_MONTHS(date, -1))
 *   - INTNX('MONTH', date,  0, 'B')         → TRUNC(date, 'MM')
 *   - YEAR(date) / MONTH(date)              → EXTRACT(YEAR FROM ...) / EXTRACT(MONTH FROM ...)
 *   - CATS(a, b)                            → TRIM(a) || b  (Oracle concatenation)
 *   - COMPRESS(x, '', 'kd')                 → REGEXP_REPLACE(x, '[^0-9]', '')
 *   - UPCASE(x) / LOWCASE(x)               → UPPER(x) / LOWER(x)
 *   - OUTER UNION CORR                      → UNION ALL with explicit NULL column padding
 *   - CALCULATED keyword in GROUP BY        → repeat full expression (Oracle disallows alias)
 *   - Correlated date filter using macro    → date comparison with TO_DATE()
 *   - Validity period filter (GUELTIG_BIS)  → exclusive end date: > not >=
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. "NOT IS MISSING" is unusual SAS syntax — easy to misread; translates to IS NOT NULL
 *   2. DWH_BOOKING_DATE does not exist in the raw STAGE source table — must be
 *      derived as a constant from the macro parameter, not read from the source
 *   3. PUT(col, ACCOUNT_TYPE.) uses a custom SAS format — no Oracle equivalent;
 *      must be replaced with a full CASE WHEN decode block
 *   4. MDY(MONTH(date), 1, YEAR(date)) constructs the first day of a given date's month
 *      → Oracle equivalent: TRUNC(date, 'MM')
 *   5. STAGE_EXT table includes a month suffix in its name (_202604) — Oracle must
 *      use the STAGE_HIST equivalent with an idwh_berichtszeit filter instead
 *   6. ORDER BY inside CTAS is syntactically valid in SAS but forbidden in Oracle
 *   7. OUTER UNION CORR auto-pads missing columns in SAS; Oracle UNION ALL requires
 *      explicit NULL AS col_name for every column absent from either branch
 *   8. CALCULATED keyword in GROUP BY is SAS-only — Oracle requires the full expression
 *   9. Validity period VALID_TO on joined tables uses exclusive end date logic —
 *      filter must use > not >= to avoid off-by-one errors
 ******************************************************************************/

LIBNAME SOURCE  META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Source Data']";
LIBNAME STAGING META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Staging']";

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

%GLOBAL gReportingDate;
%GLOBAL gMandant;
%LET gReportingDate = &DWH_MW_BOOKING_DATE.;
%LET gMandant       = &DWH_MANDANT.;

%_eg_conditional_dropds(WORK.APPROVAL_RECORDS_RAW);

PROC SQL;
   CREATE TABLE WORK.APPROVAL_RECORDS_RAW AS
   SELECT t1.CONTRACT_NR,
          t1.ACCOUNT_TYPE_CD,
          t1.REPORTING_OBLIGATION_NR,
          t1.APPROVAL_DATE,
          t1.APPROVAL_AMOUNT,
          t1.PERSON_KEY,
          t1.GROUP_NR
     FROM STAGING.APPROVAL_DATA_202604 t1
    WHERE t1.REPORTING_OBLIGATION_NR IN (105, 201)
      AND NOT MISSING(t1.CONTRACT_NR);
QUIT;

%_eg_conditional_dropds(WORK.APPROVALS_FILTERED);

PROC SQL;
   CREATE TABLE WORK.APPROVALS_FILTERED AS
   SELECT t1.CONTRACT_NR,
          t1.ACCOUNT_TYPE_CD,
          (CASE t1.ACCOUNT_TYPE_CD
              WHEN 10 THEN 'Savings Contract'
              WHEN 20 THEN 'Bridge Loan'
              WHEN 21 THEN 'Pre-Financing Loan'
              WHEN 30 THEN 'Standard Loan'
              WHEN 31 THEN 'Extended Repayment'
              WHEN 32 THEN 'Other Loan'
              WHEN 33 THEN 'Other Secured Loan'
              WHEN 34 THEN 'Default Claim Internal'
              WHEN 35 THEN 'Default Claim External'
              WHEN 90 THEN 'Clearing Account'
              WHEN 99 THEN 'General Ledger'
              ELSE 'Unknown (' || t1.ACCOUNT_TYPE_CD || ')'
           END) AS ACCOUNT_TYPE_DESC,
          t1.REPORTING_OBLIGATION_NR,
          t1.APPROVAL_DATE,
          t1.APPROVAL_AMOUNT,
          t1.PERSON_KEY,
          IFN(t1.APPROVAL_AMOUNT > 50000, 1, 0) AS HIGH_VALUE_FLAG,
          &gReportingDate. AS DWH_BOOKING_DATE
     FROM WORK.APPROVAL_RECORDS_RAW t1
    WHERE t1.REPORTING_OBLIGATION_NR = 105
      AND t1.APPROVAL_DATE NOT IS MISSING
      AND t1.APPROVAL_DATE >= MDY(MONTH(&gReportingDate.), 1, YEAR(&gReportingDate.))
    ORDER BY t1.APPROVAL_DATE DESC;
QUIT;

%_eg_conditional_dropds(WORK.APPROVALS_ENRICHED);

PROC SQL;
   CREATE TABLE WORK.APPROVALS_ENRICHED AS
   SELECT t1.CONTRACT_NR,
          t1.ACCOUNT_TYPE_CD,
          t1.ACCOUNT_TYPE_DESC,
          t1.REPORTING_OBLIGATION_NR,
          t1.APPROVAL_DATE,
          t1.APPROVAL_AMOUNT,
          t1.HIGH_VALUE_FLAG,
          t1.DWH_BOOKING_DATE,
          t2.LAST_NAME,
          t2.FIRST_NAME,
          t2.DEPARTMENT_CD,
          t2.EMPLOYEE_NR,
          CATS(t2.LAST_NAME, ', ') || t2.FIRST_NAME AS FULL_NAME,
          UPCASE(t2.DEPARTMENT_CD) AS DEPARTMENT_CD_UPPER,
          COMPRESS(t2.EMPLOYEE_NR, '', 'kd') AS EMPLOYEE_NR_DIGITS,
          YEAR(t1.APPROVAL_DATE)  AS APPROVAL_YEAR,
          MONTH(t1.APPROVAL_DATE) AS APPROVAL_MONTH,
          INTNX('MONTH', t1.APPROVAL_DATE, -1, 'END') AS PREV_MONTH_END,
          INTNX('MONTH', t1.APPROVAL_DATE, 0, 'B') AS APPROVAL_MONTH_START
     FROM WORK.APPROVALS_FILTERED t1
     LEFT JOIN SOURCE.EMPLOYEES t2
            ON t1.PERSON_KEY = t2.PERSON_KEY
           AND t2.VALID_FROM <= &gReportingDate.
           AND t2.VALID_TO    > &gReportingDate.;
QUIT;

%_eg_conditional_dropds(WORK.APPROVAL_SUMMARY);

PROC SQL;
   CREATE TABLE WORK.APPROVAL_SUMMARY AS
   SELECT
          (CASE t1.ACCOUNT_TYPE_CD
              WHEN 10 THEN 'Savings Contract'
              WHEN 20 THEN 'Bridge Loan'
              WHEN 21 THEN 'Pre-Financing Loan'
              WHEN 30 THEN 'Standard Loan'
              WHEN 31 THEN 'Extended Repayment'
              WHEN 32 THEN 'Other Loan'
              WHEN 33 THEN 'Other Secured Loan'
              WHEN 34 THEN 'Default Claim Internal'
              WHEN 35 THEN 'Default Claim External'
              WHEN 90 THEN 'Clearing Account'
              WHEN 99 THEN 'General Ledger'
              ELSE 'Unknown (' || t1.ACCOUNT_TYPE_CD || ')'
           END)                    AS ACCOUNT_TYPE_DESC,
          t1.DEPARTMENT_CD_UPPER,
          COUNT(*)                 AS APPROVAL_COUNT,
          SUM(t1.APPROVAL_AMOUNT)  AS TOTAL_APPROVED,
          MAX(t1.APPROVAL_DATE)    AS LATEST_APPROVAL
     FROM WORK.APPROVALS_ENRICHED t1
    GROUP BY
          CALCULATED ACCOUNT_TYPE_DESC,
          t1.DEPARTMENT_CD_UPPER;
QUIT;

%_eg_conditional_dropds(WORK.APPROVALS_COMBINED);

PROC SQL;
   CREATE TABLE WORK.APPROVALS_COMBINED AS
   SELECT t1.CONTRACT_NR,
          t1.ACCOUNT_TYPE_DESC,
          t1.APPROVAL_DATE,
          t1.APPROVAL_AMOUNT,
          t1.FULL_NAME,
          t1.EMPLOYEE_NR_DIGITS,
          t1.DWH_BOOKING_DATE,
          'CURRENT'   AS RECORD_TYPE
     FROM WORK.APPROVALS_ENRICHED t1

   OUTER UNION CORR

   SELECT t2.CONTRACT_NR,
          t2.ACCOUNT_TYPE_DESC,
          t2.APPROVAL_DATE,
          t2.APPROVAL_AMOUNT,
          t2.DWH_BOOKING_DATE,
          'CARRYOVER' AS RECORD_TYPE
     FROM SOURCE.PRIOR_MONTH_APPROVALS t2
    WHERE t2.REPORTING_OBLIGATION_NR = 105;
QUIT;

%GLOBAL gEGPDatum;
%LET gEGPDatum = &DWH_MW_BOOKING_DATE.;
%COPY_WORK_DIRECTORY;
