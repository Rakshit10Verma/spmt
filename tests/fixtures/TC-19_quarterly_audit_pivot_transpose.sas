/*******************************************************************************
 * FILE: TC-19_quarterly_audit_pivot_transpose.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - %LET / %GLOBAL macro variable declarations
 *   - &macro_var. references → ${prop_variable} Pentaho parameters
 *   - &gQuartal. computed inline → CASE WHEN on EXTRACT(MONTH/YEAR)
 *   - IS MISSING / IS NOT MISSING → IS NULL / IS NOT NULL
 *   - IFN() → CASE WHEN ... THEN ... ELSE ... END
 *   - CATS() string concatenation → TRIM() || concatenation
 *   - PUT() with format → CASE WHEN lookup (code-to-description mapping)
 *   - UPCASE() → UPPER()
 *   - INT() → TRUNC()
 *   - SAS date literal ('31Dec9999'd) → sentinel value / IS NULL pattern
 *   - RIGHT JOIN + '31Dec9999'd sentinel → Oracle RIGHT JOIN + IS NULL trap
 *   - OUTER UNION CORR → UNION ALL with explicit NULL column padding
 *   - PROC TRANSPOSE (BY / ID / VAR) → UNION ALL unpivot + PIVOT
 *   - CALCULATED keyword in WHERE/GROUP BY → repeated expression
 *   - MISSING() function → IS NULL
 *   - FROM DUAL (wrong) → FROM sys.dual (correct Oracle pattern)
 *   - Dynamic table name (&gPeriodeTable.) → STAGE_HIST + idwh_berichtszeit filter
 *   - MAX() self-referencing subquery for closest available snapshot
 *   - STAGE_EXT_... with month suffix → STAGE_HIST_... with idwh_berichtszeit
 *   - SAS numeric missing (.) in Excel → Oracle NULL / NVL handling
 *   - DATALINES / hardcoded lookup rows → UNION ALL SELECT FROM sys.dual
 *   - DF_KONTO_SCHLIESSUNG_DTM = '31Dec9999'd filter → IS NULL + NOT NULL trap
 *   - PK_OSP_MANDANT / prop_mandant filter pattern
 *   - GUELTIG_BIS exclusive-end date (> not >=) on TGL tables
 *   - DROP TABLE IF EXISTS pattern → sys.drop_table_if_exists()
 *   - WORK. tables → DATAMART_SAS_TEMP.PREFIX_ tables
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. PROC TRANSPOSE with BY SORT_ORDER; ID DEPT_CODE; VAR multiple metrics —
 *      Oracle requires UNION ALL unpivot of each VAR column, then PIVOT on ID.
 *      The transposed column names come from DEPT_CODE values, which must be
 *      hardcoded in the PIVOT IN (...) list. Cannot be dynamic without PL/SQL.
 *   2. OUTER UNION CORR stacks tables with different column sets — Oracle UNION ALL
 *      requires equal column count; missing columns must be padded with NULL AS col.
 *   3. RIGHT JOIN + sentinel date '31Dec9999'd: SAS WHERE excludes non-matches;
 *      Oracle WHERE IS NULL INCLUDES non-matches — must add AND pk IS NOT NULL.
 *   4. &gQuartal. has no Pentaho equivalent — must compute inline every time
 *      from prop_monatsendedatum using EXTRACT + CASE WHEN.
 *   5. Dynamic table suffix (&gPeriodeTable.) cannot exist in Oracle SQL —
 *      replace with STAGE_HIST table + idwh_berichtszeit filter.
 *   6. STAGE snapshot tables may only retain a rolling window of ~12 working
 *      days. MAX(idwh_berichtszeit) <= date subquery needed instead of direct
 *      equality, since quarter-end dates may have no exact snapshot.
 *   7. SAS CALCULATED keyword allows referencing a SELECT alias in WHERE/GROUP BY —
 *      Oracle does not; repeat the full expression.
 *   8. GUELTIG_BIS on TGL tables is exclusive (first day record is NO LONGER valid),
 *      so filter must use > date, not >= date.
 ******************************************************************************/

LIBNAME STAGING META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Staging Tables']";
LIBNAME DWH     META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Data Warehouse']";

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


%GLOBAL gMonatsendeDatum;
%GLOBAL gStand;
%GLOBAL gMandant;
%GLOBAL gQuartal;
%GLOBAL gPeriodeTable;

%LET gMonatsendeDatum = &DWH_REPORTING_DATE.;
%LET gStand           = &DWH_STAND.;
%LET gMandant         = &DWH_MANDANT.;

%LET gQuartal = %SYSFUNC(CATS(
    %SYSFUNC(YEAR(&gMonatsendeDatum.)),
    '-Q',
    %EVAL((%SYSFUNC(MONTH(&gMonatsendeDatum.)) + 2) / 3)
));

%LET gPeriodeTable = %SYSFUNC(PUTN(&gStand., Z6.));


%_eg_conditional_dropds(WORK.DEPT_MAPPING);

DATA WORK.DEPT_MAPPING;
    INFILE DATALINES DSD MISSOVER;
    INPUT DEPT_CODE $   DEPT_NAME $40.   OE_LEVEL $3.   DISPLAY_ORDER 8.;
    DATALINES;
D001,Customer Operations North,52,1
D002,Customer Operations South,52,2
D003,Customer Operations East,52,3
D004,Loan Processing,53,4
D005,Loan Disbursement,53,5
D006,Risk & Compliance,88,6
;
RUN;


%_eg_conditional_dropds(WORK.SAMPLE_CHECKS_RAW);

PROC SQL;
    CREATE TABLE WORK.SAMPLE_CHECKS_RAW AS
    SELECT
        t1.CHECK_ID,
        t1.DEPT_CODE,
        t1.TRANSACTION_TYPE,
        t1.CHECK_RESULT_CODE,
        t1.POSTING_DATE,
        t1.TOTAL_POPULATION_COUNT,
        t1.FOUREYES_COUNT,
        t1.IDWH_BERICHTSZEIT,
        t1.IDWH_OSP_MANDANT
    FROM staging.DAILY_SAMPLES_&gPeriodeTable. t1
    WHERE t1.IDWH_OSP_MANDANT = "&gMandant."
      AND t1.TRANSACTION_TYPE IN ('TypeA', 'TypeB');
QUIT;


%_eg_conditional_dropds(WORK.ERRORS_BY_DEPT);

PROC SQL;
    CREATE TABLE WORK.ERRORS_BY_DEPT AS
    SELECT
        CATS(YEAR(t1.POSTING_DATE), '-Q',
             INT((MONTH(t1.POSTING_DATE) + 2) / 3)) AS REPORT_QUARTER,

        t1.DEPT_CODE,

        PUT(t1.CHECK_RESULT_CODE, $RESULT_FMT.) AS RESULT_DESCRIPTION,

        IFN(MISSING(t1.ERROR_REASON_CODE), 0, 1) AS HAS_ERROR_FLAG,

        COUNT(*)                          AS TOTAL_CHECKS,
        SUM(t1.FOUREYES_COUNT)            AS TOTAL_FOUREYES,
        SUM(t1.TOTAL_POPULATION_COUNT)    AS TOTAL_POPULATION,

        CALCULATED TOTAL_CHECKS - SUM(t1.FOUREYES_COUNT) AS NON_FOUREYES_COUNT,

        SUM(IFN(t1.ERROR_REASON_CODE IS MISSING, 0, t1.REJECTED_COUNT)) AS TOTAL_REJECTED,
        SUM(IFN(t1.ERROR_REASON_CODE IS MISSING, 0, t1.DELETED_COUNT))  AS TOTAL_DELETED

    FROM WORK.SAMPLE_CHECKS_RAW t1
    WHERE t1.POSTING_DATE <= "&gMonatsendeDatum."d
    GROUP BY
        CATS(YEAR(t1.POSTING_DATE), '-Q',
             INT((MONTH(t1.POSTING_DATE) + 2) / 3)),
        t1.DEPT_CODE,
        PUT(t1.CHECK_RESULT_CODE, $RESULT_FMT.),
        CALCULATED HAS_ERROR_FLAG;
QUIT;


%_eg_conditional_dropds(WORK.ERRORS_WITH_ACCOUNTS);

PROC SQL;
    CREATE TABLE WORK.ERRORS_WITH_ACCOUNTS AS
    SELECT
        t1.REPORT_QUARTER,
        t1.DEPT_CODE,
        t1.RESULT_DESCRIPTION,
        t1.HAS_ERROR_FLAG,
        t1.TOTAL_CHECKS,
        t1.TOTAL_FOUREYES,
        t1.TOTAL_REJECTED,
        t1.TOTAL_DELETED,
        t2.ACCOUNT_ID,
        t2.ACCOUNT_STATUS,
        UPCASE(t2.PRODUCT_CODE)  AS PRODUCT_CODE_UPPER,
        t2.CLOSE_DATE,
        t3.REGION_NAME,
        t3.REGION_LEVEL_1
    FROM WORK.ERRORS_BY_DEPT t1
        LEFT JOIN dwh.ACCOUNT_ATTRIBUTES_TGL t2
            ON  t1.DEPT_CODE        = t2.DEPT_CODE
            AND t2.VALID_FROM_DATE <= "&gMonatsendeDatum."d
            AND t2.VALID_TO_DATE    > "&gMonatsendeDatum."d
            AND t2.PK_MANDANT       = "&gMandant."

        RIGHT JOIN dwh.REGION_HIERARCHY t3
            ON t2.REGION_ID = t3.REGION_ID
    WHERE t3.CLOSE_DATE = '31Dec9999'd
      AND t3.REGION_ID IS NOT NULL;
QUIT;


%_eg_conditional_dropds(WORK.DEPT_QUARTER_SUMMARY);

PROC SQL;
    CREATE TABLE WORK.DEPT_QUARTER_SUMMARY AS
    SELECT
        t1.REPORT_QUARTER,
        t1.DEPT_CODE,
        SUM(t1.TOTAL_POPULATION)                               AS TOTAL_POPULATION,
        SUM(t1.TOTAL_FOUREYES)                                 AS TOTAL_FOUREYES,
        SUM(IFN(t1.HAS_ERROR_FLAG = 0, t1.TOTAL_FOUREYES, 0)) AS ERROR_FREE,
        SUM(IFN(t1.HAS_ERROR_FLAG = 1, t1.TOTAL_FOUREYES, 0)) AS ERROR_COUNT,
        CALCULATED ERROR_COUNT / NULLIF(CALCULATED TOTAL_FOUREYES, 0) AS ERROR_RATE,
        SUM(t1.TOTAL_CHECKS) - SUM(t1.TOTAL_FOUREYES)         AS SAMPLE_COUNT
    FROM WORK.ERRORS_WITH_ACCOUNTS t1
    GROUP BY t1.REPORT_QUARTER, t1.DEPT_CODE;
QUIT;


%_eg_conditional_dropds(WORK.SUMMARY_WIDE);

PROC TRANSPOSE
    DATA=WORK.DEPT_QUARTER_SUMMARY
    OUT=WORK.SUMMARY_WIDE
    PREFIX=DEPT_;
    BY REPORT_QUARTER;
    ID DEPT_CODE;
    VAR TOTAL_POPULATION TOTAL_FOUREYES ERROR_FREE ERROR_COUNT ERROR_RATE SAMPLE_COUNT;
RUN;


%_eg_conditional_dropds(WORK.COMBINED_REPORT);

PROC SQL;
    CREATE TABLE WORK.COMBINED_REPORT AS
    SELECT
        t1.REPORT_QUARTER,
        t1._NAME_    AS METRIC_NAME,
        t1.DEPT_D001 AS DEPT_D001_Z,
        t1.DEPT_D002 AS DEPT_D002_Z,
        t1.DEPT_D003 AS DEPT_D003_Z,
        t1.DEPT_D004 AS DEPT_D004_Z,
        t1.DEPT_D005 AS DEPT_D005_Z,
        t1.DEPT_D006 AS DEPT_D006_Z,
        .            AS DEPT_D001_L,
        .            AS DEPT_D002_L,
        .            AS DEPT_D003_L,
        .            AS DEPT_D004_L,
        .            AS DEPT_D005_L,
        .            AS DEPT_D006_L,
        1            AS SORT_ORDER
    FROM WORK.SUMMARY_WIDE t1

    OUTER UNION CORR

    SELECT
        t2.REPORT_QUARTER,
        t2.ERROR_REASON_TEXT AS METRIC_NAME,
        t2.D001_REJECTED     AS DEPT_D001_Z,
        t2.D002_REJECTED     AS DEPT_D002_Z,
        t2.D003_REJECTED     AS DEPT_D003_Z,
        t2.D004_REJECTED     AS DEPT_D004_Z,
        t2.D005_REJECTED     AS DEPT_D005_Z,
        t2.D006_REJECTED     AS DEPT_D006_Z,
        t2.D001_DELETED      AS DEPT_D001_L,
        t2.D002_DELETED      AS DEPT_D002_L,
        t2.D003_DELETED      AS DEPT_D003_L,
        t2.D004_DELETED      AS DEPT_D004_L,
        t2.D005_DELETED      AS DEPT_D005_L,
        t2.D006_DELETED      AS DEPT_D006_L,
        3                    AS SORT_ORDER
    FROM WORK.ERROR_DETAIL_WIDE t2;
QUIT;


%_eg_conditional_dropds(WORK.COMBINED_REPORT_FINAL);

PROC SQL;
    CREATE TABLE WORK.COMBINED_REPORT_FINAL AS
    SELECT
        t1.REPORT_QUARTER,
        t1.METRIC_NAME,
        t1.SORT_ORDER,
        t1.DEPT_D001_Z, t1.DEPT_D002_Z, t1.DEPT_D003_Z,
        t1.DEPT_D004_Z, t1.DEPT_D005_Z, t1.DEPT_D006_Z,
        t1.DEPT_D001_L, t1.DEPT_D002_L, t1.DEPT_D003_L,
        t1.DEPT_D004_L, t1.DEPT_D005_L, t1.DEPT_D006_L,
        (CASE t1.METRIC_NAME
            WHEN 'TOTAL_POPULATION' THEN 'Transactions total'
            WHEN 'TOTAL_FOUREYES'   THEN 'of which four-eyes check'
            WHEN 'ERROR_FREE'       THEN 'of which error-free'
            WHEN 'ERROR_COUNT'      THEN 'of which faulty with return reason'
            WHEN 'ERROR_RATE'       THEN 'Error rate per unit'
            WHEN 'SAMPLE_COUNT'     THEN 'Random samples'
            ELSE t1.METRIC_NAME
        END) AS DISPLAY_LABEL,
        IFN(t1.METRIC_NAME = 'ERROR_RATE',  5,
        IFN(t1.METRIC_NAME = 'SAMPLE_COUNT', 6,
            t1.SORT_ORDER)) AS FINAL_SORT
    FROM WORK.COMBINED_REPORT t1
    WHERE t1.SORT_ORDER >= 1;
QUIT;


%_eg_conditional_dropds(WORK.MISSING_DEPTS);

PROC SQL;
    CREATE TABLE WORK.MISSING_DEPTS AS
    SELECT
        t1.REPORT_QUARTER,
        t1.DEPT_CODE,
        t1.OE_LEVEL_1,
        COUNT(*) AS ROW_COUNT
    FROM WORK.ERRORS_BY_DEPT t1
        LEFT JOIN WORK.DEPT_MAPPING t2
            ON t1.DEPT_CODE = t2.DEPT_CODE
    WHERE (t1.OE_LEVEL_1 IN ('52', '53', '88')
           OR t1.DEPT_CODE IN ('D001', 'D002'))
      AND t2.DEPT_CODE IS NULL
    GROUP BY t1.REPORT_QUARTER, t1.DEPT_CODE, t1.OE_LEVEL_1;
QUIT;


%GLOBAL gExportPath;
%LET gExportPath = &DWH_EXPORT_DIR.;
%include "\\fileserver\scripts\macros\export_helpers.sas";
%EXPORT_XLSX(data=WORK.COMBINED_REPORT_FINAL, outpath=&gExportPath.);
