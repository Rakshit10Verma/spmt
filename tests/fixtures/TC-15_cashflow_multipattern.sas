/*******************************************************************************
 * FILE: TC-15_cashflow_multipattern.sas
 * SOURCE CHAT: SAS-to-Oracle/Pentaho migration (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - %LET / %GLOBAL macro variable declarations
 *   - &macro_var. references → ${prop_...} Pentaho parameters
 *   - DATA step DO loop (generate rows) → CONNECT BY LEVEL in Oracle CTE
 *   - INTNX('MONTH', date, n, 'END') → LAST_DAY(ADD_MONTHS(date, n))
 *   - INTNX('MONTH', date, n, 'B')  → TRUNC(ADD_MONTHS(date, n), 'MM')
 *   - IFN(condition, a, b)          → CASE WHEN condition THEN a ELSE b END
 *   - YEAR(date) / MONTH(date)      → EXTRACT(YEAR FROM ...) / EXTRACT(MONTH FROM ...)
 *   - ROUND(x, 0.001)               → ROUND(x, 3)  [unit arg vs precision arg]
 *   - ABS(SUM(col))                 → identical in Oracle
 *   - COALESCE(subquery, 0)         → identical in Oracle
 *   - Correlated subqueries in SELECT list per group × period cell
 *   - CALCULATED alias in GROUP BY  → repeat full expression in Oracle
 *   - Nested CASE WHEN (14 branches) for product/movement classification
 *   - Double-quoted string literals "value" → single-quoted 'value'
 *   - LABEL= / FORMAT= column decorators → removed in Oracle
 *   - DATALINES hardcoded rows → UNION ALL SELECT ... FROM sys.dual
 *   - INPUT(x, BEST.) → TO_NUMBER(x)
 *   - MISSING(x) / NOT MISSING(x) → x IS NULL / x IS NOT NULL
 *   - OUTER UNION CORR → UNION ALL with explicit NULL column padding
 *   - PROC TRANSPOSE with ID / IDLABEL → Oracle PIVOT
 *   - FROM DUAL → FROM sys.dual
 *   - WORK. tables → DATAMART_SAS_TEMP.PREFIX_ tables
 *   - ORDER BY inside CREATE TABLE AS SELECT → forbidden in Oracle (remove)
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. ROUND(x, 0.001): SAS second arg is the rounding unit (0.001 = 3 decimal
 *      places), Oracle second arg is the integer decimal precision → ROUND(x, 3)
 *   2. Interval count is conditional on current month: IF MONTH=12 THEN 24,
 *      ELSE 24-MONTH — must survive as a CASE inside the CONNECT BY CTE
 *   3. IFN() tests two date inequalities on booking_date simultaneously; in
 *      Oracle this becomes CASE WHEN col > x AND col <= y THEN ... END
 *   4. Correlated subquery pattern (one per group column) can fail silently on
 *      Pentaho JDBC; correct fix is ROW_NUMBER() or an explicit JOIN + PIVOT
 *   5. CALCULATED alias in GROUP BY is SAS-specific; Oracle GROUP BY cannot
 *      reference a SELECT alias — must repeat the EXTRACT(...) expression
 *   6. OUTER UNION CORR: SAS aligns columns by name across mismatched SELECTs;
 *      Oracle UNION ALL requires positional alignment with explicit NULL padding
 *   7. PROC TRANSPOSE with ID/IDLABEL: period dates become dynamic column names;
 *      Oracle PIVOT requires a fixed, design-time column list
 *   8. INPUT(string, BEST.) on a string position key: Oracle TO_NUMBER() will
 *      hard-fail on non-numeric values, so TRIM() the source first
 *   9. ORDER BY inside CREATE TABLE AS SELECT is forbidden in Oracle — move it
 *      to the final SELECT that feeds the export step only
 ******************************************************************************/

LIBNAME SOURCE  META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Source Data']";
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

%GLOBAL gReportingDate gPeriodLabel gClientCode;
%LET gReportingDate = &DWH_REPORTING_DATE.;
%LET gPeriodLabel   = %SYSFUNC(PUTN(&gReportingDate, YYMMDD10.));
%LET gClientCode    = &DWH_CLIENT_CODE.;

%_eg_conditional_dropds(WORK.BASIS_PERIOD_GRID);

DATA WORK.BASIS_PERIOD_GRID (DROP = num_intervals);
    ATTRIB period_label  FORMAT = $30.   LABEL = 'Period label';
    ATTRIB period_end    FORMAT = DDMMYYP10. LABEL = 'Period end date';

    IF MONTH(&gReportingDate) = 12 THEN num_intervals = 24;
    ELSE num_intervals = 24 - MONTH(&gReportingDate);

    DO i = 0 TO num_intervals;
        period_label = PUT(INTNX('MONTH', &gReportingDate., i, 'END'), DDMMYYP10.);
        period_end   = INTNX('MONTH', &gReportingDate., i, 'END');
        OUTPUT;
    END;
RUN;

%_eg_conditional_dropds(WORK.BASIS_POSITION_STRUCTURE);

DATA WORK.BASIS_POSITION_STRUCTURE;
    INFILE DATALINES DELIMITER = '#';
    ATTRIB position_nr   FORMAT = BEST.;
    ATTRIB position_name FORMAT = $50.;
    INPUT position_nr position_name $;
    DATALINES;
11#Incoming External Settlements
12#Development Bank Disbursements
20#Development Bank Interest Payments
41#Outstanding Cheques Issued
42#Development Bank Principal S
43#Development Bank Principal H
50#Net Money Market Position
51#Net Current and Central Bank Accounts
;
RUN;

%_eg_conditional_dropds(WORK.TXN_CLASSIFIED);

PROC SQL;
   CREATE TABLE WORK.TXN_CLASSIFIED AS
   SELECT t1.client_id,
          t1.booking_date,
          t1.net_amount,
          t1.currency_code,
          t1.deal_reference,
          t1.product_category,
          t1.product_subcategory,
          t1.movement_code,
          t1.movement_description,
          (CASE
             WHEN t1.movement_code = "0100" THEN "Purchase"
             WHEN t1.movement_code = "0120" THEN "Drawdown"
             WHEN t1.movement_code = "0200" THEN "Sale"
             WHEN t1.movement_code = "0220" THEN "Repurchase"
             WHEN t1.movement_code = "0870" AND t1.net_amount < 0 THEN "Interest Expense"
             WHEN t1.movement_code = "0870" AND t1.net_amount > 0 THEN "Interest Income"
             WHEN t1.movement_code = "1100" THEN "Purchase"
             WHEN t1.movement_code = "1104" THEN "Maturity"
             WHEN t1.movement_code = "1105" THEN "Drawdown"
             WHEN t1.movement_code = "1120" THEN "Maturity"
             WHEN t1.movement_code = "1200" AND t1.net_amount < 0 THEN "Interest Expense"
             WHEN t1.movement_code = "1200" AND t1.net_amount > 0 THEN "Interest Income"
             WHEN t1.movement_code = "5001" THEN "Interest Income"
             WHEN t1.movement_code = "5014" THEN "Interest Income"
             WHEN t1.movement_code = "7003" THEN "Interest Expense"
             WHEN t1.movement_code = "7100" THEN "Maturity"
             ELSE "Unclassified"
          END) FORMAT = $30. LABEL = "Transaction Type" AS txn_type,
          (CASE
             WHEN (UPPER(SUBSTR(t1.product_category, 1, 1)) IN ('W','N','S')
                   OR t1.product_category = 'TM')
                  AND t1.movement_code IN ('1104','1120')                     THEN 'g01'

             WHEN (UPPER(SUBSTR(t1.product_category, 1, 2)) IN ('RM','RV')
                   OR t1.product_category = 'ES')
                  AND t1.movement_code IN ('1120','7100')                     THEN 'g02'

             WHEN UPPER(SUBSTR(t1.product_category, 1, 1)) IN ('W','N','S')
                  AND t1.movement_code IN ('0200')                            THEN 'g03'

             WHEN t1.product_category = 'IF'
                  AND t1.movement_code IN ('0200')                            THEN 'g04'

             WHEN t1.product_category = 'ES'
                  AND t1.movement_code IN ('0220')                            THEN 'g05'

             WHEN (UPPER(SUBSTR(t1.product_category, 1, 1)) IN ('W','N','S')
                   OR t1.product_category = 'TM')
                  AND t1.movement_code IN ('0100','1100')                     THEN 'g06'

             WHEN t1.product_category = 'IF'
                  AND t1.movement_code IN ('0100')                            THEN 'g07'

             WHEN (UPPER(SUBSTR(t1.product_category, 1, 2)) IN ('RM','RV')
                   OR t1.product_category IN ('ES'))
                  AND t1.movement_code IN ('0120','1105')                     THEN 'g08'

             WHEN t1.product_category IN ('RG')
                  AND t1.movement_code IN ('1120')                            THEN 'g11'

             WHEN t1.product_category IN ('RG')
                  AND t1.movement_code IN ('1105','1110')                     THEN 'g12'

             WHEN (UPPER(SUBSTR(t1.product_category, 1, 1)) IN ('W','N','S','R','T')
                   OR t1.product_category IN ('IF','ES'))
               THEN
                 CASE
                   WHEN t1.movement_code IN ('0870','1200') AND t1.net_amount > 0 THEN 'g09'
                   WHEN t1.movement_code IN ('0870','1200') AND t1.net_amount < 0 THEN 'g10'
                   WHEN t1.movement_code IN ('5001','5014')                        THEN 'g09'
                   WHEN t1.movement_code IN ('7003')                               THEN 'g10'
                   ELSE 'g99'
                 END

             ELSE 'g99'
          END) AS txn_group,
          (INTNX('MONTH', t1.booking_date, 0, 'END'))
              FORMAT = DDMMYYP10. LABEL = "Period" AS period_end,
          (IFN(
               INTNX('MONTH', &gReportingDate., 0, 'END') <  t1.booking_date
           AND t1.booking_date <= INTNX('MONTH', "&SYSDATE."d, 0, 'END'),
               t1.booking_date,
               INTNX('MONTH', t1.booking_date, 0, 'END')
          )) FORMAT = DDMMYYP10. LABEL = "Effective Period" AS period_effective
      FROM source.transactions t1
      WHERE SUBSTR(t1.product_category, 1, 1) NE 'F'
        AND t1.booking_date >  INTNX('MONTH', &gReportingDate., 0, 'B')
        AND t1.booking_date <= INTNX('MONTH', &gReportingDate., 999, 'END')
      ORDER BY txn_group, period_end;
QUIT;

%_eg_conditional_dropds(WORK.TXN_GROUP_TOTALS);

PROC SQL;
   CREATE TABLE WORK.TXN_GROUP_TOTALS AS
   SELECT t1.txn_group,
          t1.period_end,
          (ABS(SUM(t1.net_amount))) FORMAT = COMMAX20.3 AS group_total
      FROM WORK.TXN_CLASSIFIED t1
      GROUP BY t1.txn_group,
               t1.period_end;
QUIT;

%_eg_conditional_dropds(WORK.TXN_PIVOT_STRUCTURE);

PROC SQL;
   CREATE TABLE WORK.TXN_PIVOT_STRUCTURE AS
   SELECT
          (YEAR(&gReportingDate.))  LABEL = "Report Year"  AS report_year,
          (MONTH(&gReportingDate.)) LABEL = "Report Month" AS report_month,
          t1.i,
          t1.period_label,
          t1.period_end,
          (ROUND(COALESCE(
              (SELECT SUM(group_total)
               FROM WORK.TXN_GROUP_TOTALS
               WHERE txn_group = 'g01'
                 AND period_end = t1.period_end), 0)
           / 1000000, 0.001)) FORMAT = COMMAX20.3 LABEL = "N*,S*,W*,TM" AS g01,
          (ROUND(COALESCE(
              (SELECT SUM(group_total)
               FROM WORK.TXN_GROUP_TOTALS
               WHERE txn_group = 'g02'
                 AND period_end = t1.period_end), 0)
           / 1000000, 0.001)) FORMAT = COMMAX20.3 LABEL = "RM*,RV,ES" AS g02,
          (ROUND(COALESCE(
              (SELECT SUM(group_total)
               FROM WORK.TXN_GROUP_TOTALS
               WHERE txn_group = 'g03'
                 AND period_end = t1.period_end), 0)
           / 1000000, 0.001)) FORMAT = COMMAX20.3 LABEL = "N*,S*,W* Sales" AS g03,
          (ROUND(COALESCE(
              (SELECT SUM(group_total)
               FROM WORK.TXN_GROUP_TOTALS
               WHERE txn_group = 'g06'
                 AND period_end = t1.period_end), 0)
           / 1000000, 0.001)) FORMAT = COMMAX20.3 LABEL = "N*,S*,W*,TM Purchases" AS g06,
          (ROUND(COALESCE(
              (SELECT SUM(group_total)
               FROM WORK.TXN_GROUP_TOTALS
               WHERE txn_group = 'g09'
                 AND period_end = t1.period_end), 0)
           / 1000000, 0.001)) FORMAT = COMMAX20.3 LABEL = "Interest Income" AS g09,
          (ROUND(COALESCE(
              (SELECT SUM(group_total)
               FROM WORK.TXN_GROUP_TOTALS
               WHERE txn_group = 'g10'
                 AND period_end = t1.period_end), 0)
           / 1000000, 0.001)) FORMAT = COMMAX20.3 LABEL = "Interest Expense" AS g10,
          (ROUND(COALESCE(
              (SELECT SUM(group_total)
               FROM WORK.TXN_GROUP_TOTALS
               WHERE txn_group = 'g11'
                 AND period_end = t1.period_end), 0)
           / 1000000, 0.001)) FORMAT = COMMAX20.3 LABEL = "Refi Overnight" AS g11
      FROM WORK.BASIS_PERIOD_GRID t1
      ORDER BY t1.i;
QUIT;

%_eg_conditional_dropds(WORK.TXN_TRANSPOSED, WORK.SORT_TEMP);

PROC SQL;
    CREATE VIEW WORK.SORT_TEMP AS
        SELECT g01, g02, g03, g06, g09, g10, g11, i, period_label
        FROM WORK.TXN_PIVOT_STRUCTURE;
QUIT;

PROC TRANSPOSE DATA  = WORK.SORT_TEMP
               OUT   = WORK.TXN_TRANSPOSED (LABEL = "Transposed pivot result")
               PREFIX = col
               NAME   = grp_code
               LABEL  = grp_label;
    ID i;
    IDLABEL period_label;
    VAR g01 g02 g03 g06 g09 g10 g11;
RUN;

%_eg_conditional_dropds(WORK.SORT_TEMP);

%_eg_conditional_dropds(WORK.RELEVANT_ACCOUNTS);

DATA WORK.RELEVANT_ACCOUNTS (KEEP = account_code reporting_position);
    SET staging.account_groupings END = eof_flag;
    WHERE NOT MISSING(reporting_position)
      AND reporting_position NE 'EX';
    OUTPUT;
    IF eof_flag THEN DO;
        account_code = '0020122320'; reporting_position = '12'; OUTPUT;
        account_code = '0020122320'; reporting_position = '42'; OUTPUT;
    END;
RUN;

%_eg_conditional_dropds(WORK.ACCOUNT_POSITIONS);

PROC SQL;
   CREATE TABLE WORK.ACCOUNT_POSITIONS AS
   SELECT t1.reporting_position,
          t1.account_code,
          (CASE
             WHEN TRIM(t1.reporting_position) IN ('11','12','43') THEN
                 COALESCE(t2.period_credit_balance, 0)
             WHEN TRIM(t1.reporting_position) IN ('20','41','42') THEN
                 COALESCE(t2.period_debit_balance, 0)
             WHEN TRIM(t1.reporting_position) IN ('50','51') THEN
                 COALESCE(t2.ytd_debit_balance,  0)
               - COALESCE(t2.ytd_credit_balance, 0)
          END) FORMAT = COMMAX20.2 AS position_value
      FROM WORK.RELEVANT_ACCOUNTS t1
           LEFT JOIN dwh.ledger_account_balances t2
               ON t1.account_code = t2.account_code
      WHERE t2.period_key = SUBSTR("&gReportingDate.", 1, 6)
         OR t2.period_key IS NULL
      ORDER BY t1.reporting_position;
QUIT;

%_eg_conditional_dropds(WORK.POSITION_TOTALS);

PROC SQL;
   CREATE TABLE WORK.POSITION_TOTALS AS
   SELECT
          (SUM(t1.position_value)) FORMAT = COMMAX20.2 AS position_value,
          (INPUT(t1.reporting_position, BEST.)) AS reporting_position_nr
      FROM WORK.ACCOUNT_POSITIONS t1
      GROUP BY t1.reporting_position;
QUIT;

%_eg_conditional_dropds(WORK.POSITION_RESULT);

PROC SQL;
   CREATE TABLE WORK.POSITION_RESULT AS
   SELECT
          (YEAR(&gReportingDate.))  LABEL = "Report Year"  AS report_year,
          (MONTH(&gReportingDate.)) LABEL = "Report Month" AS report_month,
          t1.position_nr,
          t1.position_name,
          ROUND(COALESCE(SUM(t2.position_value) / 1000000, 0), 0.001)
              FORMAT = COMMAX20.3 LABEL = "Value (Mio. EUR)" AS position_value_mio
      FROM WORK.BASIS_POSITION_STRUCTURE t1
           LEFT JOIN WORK.POSITION_TOTALS t2
               ON t1.position_nr = t2.reporting_position_nr
      GROUP BY (CALCULATED report_year),
               (CALCULATED report_month),
               t1.position_nr,
               t1.position_name;
QUIT;

%_eg_conditional_dropds(WORK.COMBINED_OUTPUT);

PROC SQL;
   CREATE TABLE WORK.COMBINED_OUTPUT AS
   SELECT report_year,
          report_month,
          position_nr,
          position_name,
          position_value_mio,
          '' AS txn_group
      FROM WORK.POSITION_RESULT

   OUTER UNION CORR

   SELECT
          (YEAR(&gReportingDate.))  AS report_year,
          (MONTH(&gReportingDate.)) AS report_month,
          .                         AS position_nr,
          ''                        AS position_name,
          ROUND(SUM(group_total) / 1000000, 0.001) AS position_value_mio,
          txn_group
      FROM WORK.TXN_GROUP_TOTALS
      GROUP BY txn_group;
QUIT;
