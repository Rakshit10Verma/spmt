/*******************************************************************************
 * FILE: TC-17_transactions_yrdif_roundfmt_outerunion.sas
 * SOURCE CHAT: SAS-to-Pentaho/Oracle migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - %LET / %SYSFUNC(INTNX(...)) macro variable declarations → ignored / converted
 *   - INTNX('MONTH', d, 0, 'B')  → TRUNC(date, 'MM')  (start of current month)
 *   - INTNX('MONTH', d, -2, 'B') → TRUNC(ADD_MONTHS(date, -2), 'MM')
 *   - MDY(1, 1, YEAR(&macro.))   → TO_DATE(year_var || '0101', 'YYYYMMDD')
 *   - IFC(condition, 'a', 'b')   → CASE WHEN condition THEN 'a' ELSE 'b' END
 *   - YRDIF(d1, d2, IFC(cond,'30/360','act/act')) → Oracle CASE + 30/360 formula
 *   - DAY(date) / MONTH(date) / YEAR(date) → EXTRACT(DAY/MONTH/YEAR FROM date)
 *   - ROUND(x, 0.001) / ROUND(x, 0.1) → ROUND(x, 3) / ROUND(x, 1) (Oracle integer arg)
 *   - "STRING" double-quoted literals → 'STRING' single-quoted
 *   - LENGTH=12 in SELECT column list → remove (SAS-only syntax)
 *   - LABEL= in SELECT column list  → remove (SAS-only syntax)
 *   - FORMAT= in SELECT column list → remove (SAS-only syntax)
 *   - ORDER BY inside CREATE TABLE AS SELECT → remove (forbidden in Oracle CTAS)
 *   - OUTER UNION CORR with mismatched column sets → UNION ALL + explicit NULL padding
 *   - SELECT * in UNION context → explicit column list (Michael's rule)
 *   - FROM DUAL → FROM sys.dual
 *   - WORK.table → DATAMART_SAS_TEMP.PREFIX_table
 *   - Staging TGL table access: PK_GUELTIG_AB / GUELTIG_BIS time-slice filter
 *   - &macro_var. → ${prop_...} Pentaho variables
 *   - COALESCE around division for zero-denominator protection
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. YRDIF second argument is IFC() — the day-count method itself is conditional.
 *      Oracle has no YRDIF; must wrap the full 30/360 formula inside a CASE WHEN.
 *   2. ROUND(x, 0.001) looks like 3-decimal rounding but Oracle truncates 0.001 → 0,
 *      silently rounding everything to a whole number. Must change to ROUND(x, 3).
 *   3. OUTER UNION CORR automatically aligns by column name and NULLs missing ones.
 *      Oracle UNION ALL requires the same number of columns in the same position —
 *      must manually add NULL AS colname for every column absent from the narrower side.
 *   4. MDY(1, 1, YEAR(&report_date.)) produces Jan 1 of the reporting year.
 *      Oracle equivalent requires extracting the year component separately.
 *   5. ORDER BY is present inside two CREATE TABLE AS SELECT statements — these
 *      must be removed; ORDER BY only belongs in the final SELECT for export.
 *   6. DAY/MONTH/YEAR SAS functions inside a CASE WHEN for anniversary-date check:
 *      all three must become EXTRACT(...) calls.
 *   7. IFC inside YRDIF second argument produces a string value at runtime;
 *      in Oracle the equivalent CASE must return a numeric result (not a string label).
 *   8. Staging table uses PK_GUELTIG_AB / GUELTIG_BIS with exclusive upper bound —
 *      filter must use > not >= on GUELTIG_BIS.
 ******************************************************************************/

LIBNAME SOURCE META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Source Data']";
LIBNAME STAGING META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Staging']";
LIBNAME DWH META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='DWH Final']";

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

%GLOBAL report_date month_start three_months_back year_start;
%LET report_date       = &DWH_REPORT_DATE.;
%LET month_start       = %SYSFUNC(INTNX(MONTH, &report_date., 0, B));
%LET three_months_back = %SYSFUNC(INTNX(MONTH, &report_date., -2, B));
%LET year_start        = %SYSFUNC(MDY(1, 1, %SYSFUNC(YEAR(&report_date.))));

%_eg_conditional_dropds(WORK.BASIS_TRANSACTIONS);

PROC SQL;
   CREATE TABLE WORK.BASIS_TRANSACTIONS AS
   SELECT t1.TRANSACTION_ID,
          t1.PRODUCT_TYPE,
          t1.INSTRUMENT_ID,
          t1.COUNTERPARTY_ID,
          t1.ISSUER_ID,
          t1.SHORT_DESCRIPTION,
          t1.CLASSIFICATION,
          t1.DIRECTION_CODE,
          t1.BOOKING_DATE,
          t1.VALUE_DATE,
          t1.MATURITY_DATE,
          t1.NOMINAL_AMOUNT,
          t1.COUPON_RATE,
          t1.EFFECTIVE_RATE,
          t1.ACQUISITION_VALUE,
          t1.ACCRUED_INTEREST,
          t1.DAY_COUNT_METHOD,
          t1.LONG_DESCRIPTION,
          t1.TRANSACTION_NUMBER,
          t1.REFERENCE_ID,
          t1.ACCOUNT_ID
     FROM STAGING.INVESTMENT_TRANSACTIONS t1
    WHERE t1.PK_GUELTIG_AB <= "&report_date."d
      AND t1.GUELTIG_BIS   >  "&report_date."d
      AND t1.MANDANT       =  "&prop_mandant.";
QUIT;

%_eg_conditional_dropds(WORK.BASIS_COUNTERPARTIES);

PROC SQL;
   CREATE TABLE WORK.BASIS_COUNTERPARTIES AS
   SELECT t1.COUNTERPARTY_KEY,
          t1.SHORT_NAME
     FROM STAGING.COUNTERPARTY_MASTER t1
    WHERE t1.PK_GUELTIG_AB <= "&report_date."d
      AND t1.GUELTIG_BIS   >  "&report_date."d
      AND t1.MANDANT       =  "&prop_mandant.";
QUIT;

%_eg_conditional_dropds(WORK.BASIS_TRANSACTIONS_TYPED);

PROC SQL;
   CREATE TABLE WORK.BASIS_TRANSACTIONS_TYPED AS
   SELECT t1.TRANSACTION_ID,
          t1.PRODUCT_TYPE,
          t1.INSTRUMENT_ID,
          t1.COUNTERPARTY_ID,
          t1.ISSUER_ID,
          t1.SHORT_DESCRIPTION,
          t1.CLASSIFICATION,
          CASE
            WHEN UPCASE(SUBSTR(t1.PRODUCT_TYPE, 1, 1)) IN ('B','N','S')
              THEN IFC(t1.DIRECTION_CODE = 'I', "PURCHASE", "SALE")
            WHEN t1.PRODUCT_TYPE IN ('FN','FS','FZ','FP')
              THEN IFC(t1.DIRECTION_CODE = 'I', "PURCHASE", "SALE")
            WHEN t1.PRODUCT_TYPE IN ('RGN','RG','RMN','RM')
              THEN IFC(t1.DIRECTION_CODE = 'O', "MATURITY", "DRAWDOWN")
            WHEN t1.PRODUCT_TYPE IN ('TGN','TG','TMN','TM','FT')
              THEN IFC(t1.DIRECTION_CODE = 'O', "MATURITY", "PLACEMENT")
            WHEN t1.PRODUCT_TYPE = 'IF'
              THEN IFC(t1.DIRECTION_CODE = 'I', "PURCHASE", "SALE")
            WHEN t1.PRODUCT_TYPE = 'EM' THEN 'ISSUANCE'
            ELSE '--- UNKNOWN ---'
          END                                          AS ACTION_TYPE,
          t1.BOOKING_DATE,
          t1.VALUE_DATE,
          t1.MATURITY_DATE,
          t1.NOMINAL_AMOUNT,
          t1.COUPON_RATE,
          t1.EFFECTIVE_RATE,
          COALESCE((t1.ACQUISITION_VALUE / t1.NOMINAL_AMOUNT * 100), 0)
                                                       AS ACQUISITION_PRICE,
          t1.ACCRUED_INTEREST,
          t1.DAY_COUNT_METHOD,
          t1.LONG_DESCRIPTION,
          t1.TRANSACTION_NUMBER,
          t1.REFERENCE_ID,
          t1.ACCOUNT_ID
     FROM WORK.BASIS_TRANSACTIONS t1;
QUIT;

%_eg_conditional_dropds(WORK.TP_TRANSACTIONS);

PROC SQL;
   CREATE TABLE WORK.TP_TRANSACTIONS AS
   SELECT t1.TRANSACTION_ID,
          t1.PRODUCT_TYPE,
          t1.INSTRUMENT_ID,
          t1.COUNTERPARTY_ID,
          t1.ISSUER_ID,
          t1.SHORT_DESCRIPTION,
          t1.CLASSIFICATION,
          t1.ACTION_TYPE,
          t1.BOOKING_DATE,
          t1.VALUE_DATE,
          t1.MATURITY_DATE,
          t1.NOMINAL_AMOUNT,
          t1.COUPON_RATE,
          t1.EFFECTIVE_RATE,
          t1.ACQUISITION_PRICE,
          t1.ACCRUED_INTEREST,
          t1.DAY_COUNT_METHOD,
          t2.SHORT_NAME                               AS ISSUER_NAME,
          t3.SHORT_NAME                               AS COUNTERPARTY_NAME
     FROM WORK.BASIS_TRANSACTIONS_TYPED t1
     LEFT JOIN WORK.BASIS_COUNTERPARTIES t2
            ON t1.ISSUER_ID = t2.COUNTERPARTY_KEY
     LEFT JOIN WORK.BASIS_COUNTERPARTIES t3
            ON t1.COUNTERPARTY_ID = t3.COUNTERPARTY_KEY;
QUIT;

%_eg_conditional_dropds(WORK.AT_PURCHASE_DETAIL);

PROC SQL;
   CREATE TABLE WORK.AT_PURCHASE_DETAIL AS
   SELECT t1.ACTION_TYPE,
          t1.PRODUCT_TYPE,
          t1.INSTRUMENT_ID,
          t1.VALUE_DATE,
          t1.MATURITY_DATE,
          YRDIF(t1.VALUE_DATE, t1.MATURITY_DATE,
                IFC(t1.DAY_COUNT_METHOD = '7', '30/360', 'act/act'))
                                        LENGTH=12 LABEL='Duration (Years)'
                                                             AS DURATION_YEARS,
          t1.NOMINAL_AMOUNT,
          ROUND(t1.COUPON_RATE,     0.001) FORMAT=12.4 LABEL='Coupon %'
                                                             AS COUPON_RATE,
          ROUND(t1.EFFECTIVE_RATE,  0.001) FORMAT=12.4 LABEL='Yield %'
                                                             AS EFFECTIVE_RATE,
          (t1.NOMINAL_AMOUNT * t1.ACQUISITION_PRICE / 100)  AS ACQUISITION_VALUE,
          (t1.NOMINAL_AMOUNT * t1.COUPON_RATE)              AS NOMINAL_X_COUPON,
          (t1.NOMINAL_AMOUNT * t1.ACQUISITION_PRICE / 100
             * t1.EFFECTIVE_RATE)                           AS ACQVAL_X_YIELD,
          (YRDIF(t1.VALUE_DATE, t1.MATURITY_DATE,
                 IFC(t1.DAY_COUNT_METHOD = '7', '30/360', 'act/act'))
           * t1.NOMINAL_AMOUNT)                             AS NOMINAL_X_DURATION
     FROM WORK.TP_TRANSACTIONS t1
    WHERE UPCASE(SUBSTR(t1.PRODUCT_TYPE, 1, 1)) IN ('B', 'N', 'S')
      AND UPCASE(t1.ACTION_TYPE) = "PURCHASE"
      AND t1.VALUE_DATE >= MDY(1, 1, YEAR("&report_date."d))
    ORDER BY t1.VALUE_DATE, t1.INSTRUMENT_ID;
QUIT;

%_eg_conditional_dropds(WORK.AT_PURCHASE_SUMMARY);

PROC SQL;
   CREATE TABLE WORK.AT_PURCHASE_SUMMARY AS
   SELECT SUM(t1.NOMINAL_AMOUNT)      AS NOMINAL_AMOUNT,
          SUM(t1.ACQUISITION_VALUE)   AS ACQUISITION_VALUE,
          SUM(t1.NOMINAL_X_DURATION)  AS NOMINAL_X_DURATION,
          SUM(t1.NOMINAL_X_COUPON)    AS NOMINAL_X_COUPON,
          SUM(t1.ACQVAL_X_YIELD)      AS ACQVAL_X_YIELD
     FROM WORK.AT_PURCHASE_DETAIL t1;
QUIT;

%_eg_conditional_dropds(WORK.AT_CAPITAL_MARKET);

PROC SQL;
   CREATE TABLE WORK.AT_CAPITAL_MARKET AS
   SELECT 'Capital Market'                                              AS CATEGORY,
          ''                                                            AS COL_BLANK,
          COALESCE(ROUND(t1.NOMINAL_X_DURATION / t1.NOMINAL_AMOUNT, 0.1),  0)
                                        LABEL='Avg Duration'           AS AVG_DURATION,
          COALESCE(ROUND(t1.NOMINAL_AMOUNT / 1000000,                  0.1), 0)
                                        LABEL='Nominal (Mio)'         AS NOMINAL_MIO,
          COALESCE(ROUND(t1.NOMINAL_X_COUPON   / t1.NOMINAL_AMOUNT,   0.001), 0)
                                        LABEL='Avg Coupon %'           AS AVG_COUPON,
          COALESCE(ROUND(t1.ACQVAL_X_YIELD     / t1.ACQUISITION_VALUE, 0.001), 0)
                                        LABEL='Avg Yield %'            AS AVG_YIELD,
          '' AS COL_G,
          '' AS COL_H,
          '' AS COL_I,
          '' AS COL_J,
          1  AS SORT_GROUP
     FROM WORK.AT_PURCHASE_SUMMARY t1;
QUIT;

%_eg_conditional_dropds(WORK.AT_PLACEMENT_DETAIL);

PROC SQL;
   CREATE TABLE WORK.AT_PLACEMENT_DETAIL AS
   SELECT t1.ACTION_TYPE,
          t1.PRODUCT_TYPE,
          t1.INSTRUMENT_ID,
          t1.VALUE_DATE,
          t1.MATURITY_DATE,
          YRDIF(t1.VALUE_DATE, t1.MATURITY_DATE,
                IFC(t1.DAY_COUNT_METHOD = '7', '30/360', 'act/act'))
                                        LENGTH=12 LABEL='Duration (Years)'
                                                             AS DURATION_YEARS,
          t1.NOMINAL_AMOUNT,
          ROUND(t1.COUPON_RATE,    0.001)                             AS COUPON_RATE,
          ROUND(t1.EFFECTIVE_RATE, 0.001)                             AS EFFECTIVE_RATE,
          (t1.NOMINAL_AMOUNT * t1.ACQUISITION_PRICE / 100)           AS ACQUISITION_VALUE,
          (t1.NOMINAL_AMOUNT * t1.COUPON_RATE)                       AS NOMINAL_X_COUPON,
          (t1.NOMINAL_AMOUNT * t1.ACQUISITION_PRICE / 100
             * t1.EFFECTIVE_RATE)                                    AS ACQVAL_X_YIELD,
          (YRDIF(t1.VALUE_DATE, t1.MATURITY_DATE,
                 IFC(t1.DAY_COUNT_METHOD = '7', '30/360', 'act/act'))
           * t1.NOMINAL_AMOUNT)                                      AS NOMINAL_X_DURATION
     FROM WORK.TP_TRANSACTIONS t1
    WHERE UPCASE(SUBSTR(t1.PRODUCT_TYPE, 1, 2)) = 'TM'
      AND UPCASE(t1.ACTION_TYPE) = "PLACEMENT"
      AND t1.VALUE_DATE >= MDY(1, 1, YEAR("&report_date."d))
    ORDER BY t1.VALUE_DATE;
QUIT;

%_eg_conditional_dropds(WORK.AT_PLACEMENT_SUMMARY);

PROC SQL;
   CREATE TABLE WORK.AT_PLACEMENT_SUMMARY AS
   SELECT SUM(t1.NOMINAL_AMOUNT)      AS NOMINAL_AMOUNT,
          SUM(t1.ACQUISITION_VALUE)   AS ACQUISITION_VALUE,
          SUM(t1.NOMINAL_X_DURATION)  AS NOMINAL_X_DURATION,
          SUM(t1.NOMINAL_X_COUPON)    AS NOMINAL_X_COUPON,
          SUM(t1.ACQVAL_X_YIELD)      AS ACQVAL_X_YIELD
     FROM WORK.AT_PLACEMENT_DETAIL t1;
QUIT;

%_eg_conditional_dropds(WORK.AT_MONEY_MARKET);

PROC SQL;
   CREATE TABLE WORK.AT_MONEY_MARKET AS
   SELECT 'Money Market'                                                AS CATEGORY,
          COALESCE(ROUND(t1.NOMINAL_X_DURATION / t1.NOMINAL_AMOUNT,   0.1),  0)
                                                                        AS AVG_DURATION,
          COALESCE(ROUND(t1.NOMINAL_X_COUPON   / t1.NOMINAL_AMOUNT,   0.001), 0)
                                                                        AS AVG_COUPON,
          COALESCE(ROUND(t1.ACQVAL_X_YIELD     / t1.ACQUISITION_VALUE, 0.001), 0)
                                                                        AS AVG_YIELD,
          COALESCE(ROUND(t1.NOMINAL_AMOUNT / 1000000,                  0.1),  0)
                                                                        AS NOMINAL_MIO,
          1 AS SORT_GROUP
     FROM WORK.AT_PLACEMENT_SUMMARY t1;
QUIT;

%_eg_conditional_dropds(WORK.AT_COMBINED_SUMMARY);

PROC SQL;
   CREATE TABLE WORK.AT_COMBINED_SUMMARY AS
   SELECT * FROM WORK.AT_CAPITAL_MARKET
   OUTER UNION CORR
   SELECT * FROM WORK.AT_MONEY_MARKET;
QUIT;

%_eg_conditional_dropds(WORK.AT_TRANSACTIONS_CAPITAL);

PROC SQL;
   CREATE TABLE WORK.AT_TRANSACTIONS_CAPITAL AS
   SELECT t1.ACTION_TYPE,
          t1.ISSUER_NAME                   LABEL='Issuer',
          t1.MATURITY_DATE                 FORMAT=EURDFDD10.,
          ROUND(t1.NOMINAL_AMOUNT / 1000000,  0.1)  LABEL='Nominal (Mio)'
                                                     AS NOMINAL_MIO,
          ROUND(t1.COUPON_RATE,    0.001)  LABEL='Coupon %'    AS COUPON_RATE,
          ROUND(t1.EFFECTIVE_RATE, 0.001)  LABEL='Yield %'     AS EFFECTIVE_RATE,
          t1.BOOKING_DATE                  FORMAT=EURDFDD10.,
          t1.VALUE_DATE                    FORMAT=EURDFDD10.,
          2 AS SORT_GROUP
     FROM WORK.TP_TRANSACTIONS t1
    WHERE UPCASE(t1.ACTION_TYPE) IN ("PURCHASE", "SALE")
      AND UPCASE(SUBSTR(t1.PRODUCT_TYPE, 1, 1)) IN ('B', 'N', 'S')
      AND t1.VALUE_DATE >= "&month_start."d
    ORDER BY t1.BOOKING_DATE, t1.VALUE_DATE;
QUIT;

%_eg_conditional_dropds(WORK.AT_TRANSACTIONS_MONEY);

PROC SQL;
   CREATE TABLE WORK.AT_TRANSACTIONS_MONEY AS
   SELECT t1.ACTION_TYPE,
          t1.COUNTERPARTY_NAME             LABEL='Counterparty',
          t1.VALUE_DATE                    FORMAT=EURDFDD10.,
          t1.MATURITY_DATE                 FORMAT=EURDFDD10.,
          ROUND(t1.NOMINAL_AMOUNT / 1000000,  0.1)  LABEL='Nominal (Mio)'
                                                     AS NOMINAL_MIO,
          ROUND(t1.COUPON_RATE,    0.001)  LABEL='Coupon %'    AS COUPON_RATE,
          t1.BOOKING_DATE                  FORMAT=EURDFDD10.,
          3 AS SORT_GROUP
     FROM WORK.TP_TRANSACTIONS t1
    WHERE UPCASE(t1.ACTION_TYPE) IN ('PLACEMENT', 'DRAWDOWN')
      AND UPCASE(SUBSTR(t1.PRODUCT_TYPE, 1, 1)) IN ('T', 'R')
      AND t1.VALUE_DATE >= "&month_start."d
    ORDER BY t1.VALUE_DATE;
QUIT;

%_eg_conditional_dropds(WORK.TP_LIMIT_REPORT);

PROC SQL;
   CREATE TABLE WORK.TP_LIMIT_REPORT AS
   SELECT CASE
            WHEN UPCASE(t1.ACTION_TYPE) IN ('PURCHASE', 'SALE')     THEN 'Capital Market'
            WHEN UPCASE(t1.ACTION_TYPE) IN ('PLACEMENT', 'DRAWDOWN') THEN 'Money Market'
            ELSE 'Other'
          END                                                 AS MARKET_SEGMENT,
          t1.ACTION_TYPE,
          t1.ISSUER_ID,
          t1.ISSUER_NAME,
          t1.PRODUCT_TYPE,
          t1.INSTRUMENT_ID,
          t1.VALUE_DATE,
          t1.MATURITY_DATE,
          CASE
            WHEN DAY(t1.VALUE_DATE) = DAY(t1.MATURITY_DATE)
             AND MONTH(t1.VALUE_DATE) = MONTH(t1.MATURITY_DATE)
            THEN YEAR(t1.MATURITY_DATE) - YEAR(t1.VALUE_DATE)
            ELSE YRDIF(t1.VALUE_DATE, t1.MATURITY_DATE,
                       IFC(t1.DAY_COUNT_METHOD = '7', '30/360', 'act/act'))
          END                                                 AS DURATION_YEARS,
          ROUND(t1.NOMINAL_AMOUNT / 1000000,    0.1)         AS NOMINAL_MIO,
          t1.COUPON_RATE,
          (t1.ACQUISITION_PRICE / 100 * t1.NOMINAL_AMOUNT) / 1000000
                                                              AS ACQUISITION_VALUE_MIO,
          t1.ACQUISITION_PRICE,
          t1.EFFECTIVE_RATE
     FROM WORK.TP_TRANSACTIONS t1
    WHERE UPCASE(t1.ACTION_TYPE) = "PURCHASE"
      AND UPCASE(SUBSTR(t1.PRODUCT_TYPE, 1, 1)) IN ('B', 'N', 'S')
    ORDER BY MARKET_SEGMENT DESC, t1.ACTION_TYPE, t1.VALUE_DATE, t1.INSTRUMENT_ID;
QUIT;

%_eg_conditional_dropds(WORK.TP_LIMIT_3MONTH);

PROC SQL;
   CREATE TABLE WORK.TP_LIMIT_3MONTH AS
   SELECT t1.*
     FROM WORK.TP_LIMIT_REPORT t1
    WHERE t1.VALUE_DATE >= "&three_months_back."d
    ORDER BY t1.MARKET_SEGMENT DESC, t1.ACTION_TYPE,
             t1.VALUE_DATE, t1.INSTRUMENT_ID;
QUIT;

%_eg_conditional_dropds(WORK.DOCUMENTATION);

PROC SQL;
   CREATE TABLE WORK.DOCUMENTATION AS
   SELECT CAST(SUBSTR("&prop_monatsendedatum.", 5, 2) AS VARCHAR(2)) AS MONTH_NR,
          CAST(0 AS NUMERIC(1))                                       AS FILE_RELEVANT,
          CAST('DOCUMENTATION' AS VARCHAR(4000))                      AS CONTENT
   FROM DUAL;
QUIT;

PROC SQL;
   INSERT INTO WORK.DOCUMENTATION (MONTH_NR, FILE_RELEVANT, CONTENT)
   SELECT SUBSTR("&prop_monatsendedatum.", 5, 2), 0,
          'Program: ' || "&prop_name_batchstep." FROM DUAL
   UNION ALL
   SELECT SUBSTR("&prop_monatsendedatum.", 5, 2), 0,
          'Reporting date: ' || PUT("&report_date."d, EURDFDD10.) FROM DUAL
   UNION ALL
   SELECT SUBSTR("&prop_monatsendedatum.", 5, 2), 0, ' ' FROM DUAL
   UNION ALL
   SELECT SUBSTR("&prop_monatsendedatum.", 5, 2), 0,
          'Sheet "Summary": Capital Market + Money Market YTD aggregates' FROM DUAL
   UNION ALL
   SELECT SUBSTR("&prop_monatsendedatum.", 5, 2), 0,
          'Source: staging.INVESTMENT_TRANSACTIONS (daily, TGL table)' FROM DUAL;
QUIT;
