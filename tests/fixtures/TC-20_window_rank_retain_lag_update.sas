/*******************************************************************************
 * FILE: TC-20_window_rank_retain_lag_update.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - %LET / %GLOBAL macro variable declarations
 *   - &macro_var. references → ${prop_variable} Pentaho parameters
 *   - %SYSFUNC(INTNX(...)) in %LET → pre-computed Pentaho parameter
 *   - PROC SORT DATA= OUT= NODUPKEY → ROW_NUMBER() OVER (PARTITION BY BY-keys
 *       ORDER BY first sort col) = 1 in a subquery (keep first occurrence)
 *   - DATA step RETAIN with FIRST.var reset → Oracle running SUM() OVER
 *       (PARTITION BY group ORDER BY sort ROWS UNBOUNDED PRECEDING)
 *   - DATA step LAG() with FIRST.var null-reset → Oracle LAG() OVER
 *       (PARTITION BY group ORDER BY sort) — reset at group boundary
 *   - FIRST.by_variable in DATA step → ROW_NUMBER() = 1 in PARTITION BY
 *   - PROC RANK DESCENDING TIES=DENSE OUT= → DENSE_RANK() OVER (PARTITION BY ...
 *       ORDER BY ... DESC)
 *   - PROC RANK GROUPS=10 OUT= → NTILE(10) OVER (PARTITION BY ... ORDER BY ...)
 *   - PROC APPEND BASE= DATA= → INSERT INTO base SELECT ... FROM data
 *   - PROC SQL UPDATE ... SET col = (subquery) → Oracle UPDATE (same syntax;
 *       Oracle raises ORA-01427 if subquery returns more than one row)
 *   - PROC SQL DELETE FROM ... WHERE → Oracle DELETE FROM (compatible)
 *   - PROC SQL INSERT INTO ... SELECT → Oracle INSERT INTO ... SELECT (compatible)
 *   - CALCULATED keyword in SELECT / GROUP BY → repeat full expression
 *   - MISSING() / IS MISSING → IS NULL
 *   - IFN() → CASE WHEN ... THEN ... ELSE ... END
 *   - SUM(a, b) multi-argument (NULL-safe add) → COALESCE(a,0) + COALESCE(b,0)
 *   - INTNX('MONTH', date, n, 'B') → TRUNC(ADD_MONTHS(date, n), 'MM')
 *   - YEAR() / MONTH() → EXTRACT(YEAR FROM ...) / EXTRACT(MONTH FROM ...)
 *   - FORMAT=COMMAX15.2 / LABEL= on column aliases → stripped in Oracle
 *   - Double-quoted string literals → single-quoted
 *   - NULLIF() → identical in Oracle
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. DATA step RETAIN is not a SQL concept. The SAS pattern
 *        RETAIN cumcol 0;
 *        IF FIRST.grp THEN cumcol = 0;
 *        cumcol = SUM(cumcol, new_col);
 *      becomes in Oracle:
 *        SUM(new_col) OVER (PARTITION BY grp ORDER BY sort_col
 *                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
 *      The ROWS UNBOUNDED PRECEDING frame is mandatory — the default is
 *      RANGE UNBOUNDED PRECEDING which can collapse ties into one bucket.
 *   2. SAS LAG(col) in a DATA step returns the previous row's value within the
 *      current BY group, and automatically resets to missing at each group
 *      boundary (enforced by the FIRST.var null-reset pattern). Oracle LAG()
 *      does not reset automatically — the PARTITION BY clause does the grouping:
 *        LAG(col, 1) OVER (PARTITION BY grp ORDER BY sort_col)
 *      The first row of each partition naturally returns NULL in Oracle LAG(),
 *      matching the SAS reset logic exactly.
 *   3. PROC SORT NODUPKEY keeps the first observation per BY key as encountered
 *      in the input (i.e., whatever came first in sort order). Oracle equivalent:
 *        SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER
 *          (PARTITION BY key_cols ORDER BY tie_breaker_col) AS rn
 *         FROM src) WHERE rn = 1
 *      The ORDER BY inside the window determines which row is "first".
 *   4. PROC RANK TIES=DENSE → DENSE_RANK(); TIES=LOW → RANK(); TIES=HIGH →
 *      no direct Oracle equivalent (Oracle RANK() uses TIES=LOW semantics);
 *      TIES=MEAN → AVG(RANK()) via a subquery, which is expensive.
 *   5. PROC RANK GROUPS=n produces 0-based n-tile bins (0 to n-1). Oracle
 *      NTILE(n) produces 1-based bins (1 to n). Subtract 1 if you need
 *      to preserve the 0-based SAS output exactly.
 *   6. PROC APPEND BASE=X DATA=Y silently appends even if schemas differ
 *      (SAS aligns by name). Oracle INSERT INTO ... SELECT requires exact
 *      column list agreement — emit an explicit column list, never SELECT *.
 *   7. PROC SQL UPDATE in SAS tolerates multiple rows returned by the SET
 *      subquery and silently uses the last one. Oracle raises ORA-01427 if the
 *      subquery returns more than one row. The correlated WHERE on the subquery
 *      must be tight enough to guarantee a single row.
 *   8. SUM(a, b) in SAS is a NULL-safe two-argument addition (returns b if a is
 *      missing). In Oracle there is no two-argument SUM() — use
 *      COALESCE(a, 0) + COALESCE(b, 0), or NVL(a,0) + NVL(b,0).
 ******************************************************************************/

LIBNAME STAGING META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Staging Tables']";
LIBNAME DWH     META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Data Warehouse']";
LIBNAME DM      META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Datamart']";

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


%GLOBAL gReportDate;
%GLOBAL gPrevMonthStart;
%GLOBAL gMandant;

%LET gReportDate     = &DWH_REPORTING_DATE.;
%LET gPrevMonthStart = %SYSFUNC(INTNX(MONTH, &gReportDate., -1, B));
%LET gMandant        = &DWH_MANDANT.;


%_eg_conditional_dropds(WORK.LOAN_BASE);

PROC SQL;
    CREATE TABLE WORK.LOAN_BASE AS
    SELECT
        t1.LOAN_ID,
        t1.MANAGER_ID,
        t1.PRODUCT_TYPE,
        t1.APPROVAL_DATE,
        t1.APPROVED_AMOUNT,
        t1.OUTSTANDING_BALANCE,
        t1.LOAN_STATUS,
        t1.DEFAULT_FLAG,
        t1.IDWH_OSP_MANDANT
    FROM staging.LOAN_PORTFOLIO t1
    WHERE t1.IDWH_OSP_MANDANT = "&gMandant."
      AND t1.APPROVAL_DATE BETWEEN "&gPrevMonthStart."d AND "&gReportDate."d
      AND t1.LOAN_STATUS NOT IN ('CANCELLED', 'REJECTED');
QUIT;


%_eg_conditional_dropds(WORK.LOAN_NODUPS);

PROC SORT DATA=WORK.LOAN_BASE OUT=WORK.LOAN_NODUPS NODUPKEY;
    BY LOAN_ID;
RUN;


%_eg_conditional_dropds(WORK.MANAGER_MONTHLY);

PROC SQL;
    CREATE TABLE WORK.MANAGER_MONTHLY AS
    SELECT
        t1.MANAGER_ID,
        YEAR(t1.APPROVAL_DATE)                                            AS APPROVAL_YEAR,
        MONTH(t1.APPROVAL_DATE)                                           AS APPROVAL_MONTH,
        COUNT(*)                                                          AS LOAN_COUNT,
        SUM(t1.APPROVED_AMOUNT)                                           AS TOTAL_APPROVED,
        SUM(t1.OUTSTANDING_BALANCE)                                       AS TOTAL_OUTSTANDING,
        SUM(IFN(t1.DEFAULT_FLAG = 1, t1.APPROVED_AMOUNT, 0))             AS DEFAULTED_AMOUNT,
        CALCULATED DEFAULTED_AMOUNT / NULLIF(CALCULATED TOTAL_APPROVED, 0) AS DEFAULT_RATE
    FROM WORK.LOAN_NODUPS t1
    GROUP BY
        t1.MANAGER_ID,
        YEAR(t1.APPROVAL_DATE),
        MONTH(t1.APPROVAL_DATE);
QUIT;


%_eg_conditional_dropds(WORK.MANAGER_RUNNING);

DATA WORK.MANAGER_RUNNING;
    SET WORK.MANAGER_MONTHLY;
    BY MANAGER_ID APPROVAL_YEAR APPROVAL_MONTH;
    RETAIN CUMULATIVE_APPROVED CUMULATIVE_LOANS 0;
    IF FIRST.MANAGER_ID THEN DO;
        CUMULATIVE_APPROVED = 0;
        CUMULATIVE_LOANS    = 0;
    END;
    CUMULATIVE_APPROVED = SUM(CUMULATIVE_APPROVED, TOTAL_APPROVED);
    CUMULATIVE_LOANS    = SUM(CUMULATIVE_LOANS,    LOAN_COUNT);
RUN;


%_eg_conditional_dropds(WORK.MANAGER_LAG);

DATA WORK.MANAGER_LAG;
    SET WORK.MANAGER_RUNNING;
    BY MANAGER_ID;
    PREV_TOTAL_APPROVED    = LAG(TOTAL_APPROVED);
    PREV_TOTAL_OUTSTANDING = LAG(TOTAL_OUTSTANDING);
    IF FIRST.MANAGER_ID THEN DO;
        PREV_TOTAL_APPROVED    = .;
        PREV_TOTAL_OUTSTANDING = .;
    END;
    MOM_APPROVED_CHANGE    = TOTAL_APPROVED    - PREV_TOTAL_APPROVED;
    MOM_OUTSTANDING_CHANGE = TOTAL_OUTSTANDING - PREV_TOTAL_OUTSTANDING;
RUN;


%_eg_conditional_dropds(WORK.MANAGER_RANKED);

PROC RANK DATA=WORK.MANAGER_LAG OUT=WORK.MANAGER_RANKED DESCENDING TIES=DENSE;
    BY APPROVAL_YEAR APPROVAL_MONTH;
    VAR TOTAL_APPROVED;
    RANKS APPROVED_RANK;
RUN;


%_eg_conditional_dropds(WORK.MANAGER_DECILE);

PROC RANK DATA=WORK.MANAGER_RANKED OUT=WORK.MANAGER_DECILE GROUPS=10;
    BY APPROVAL_YEAR APPROVAL_MONTH;
    VAR DEFAULT_RATE;
    RANKS DEFAULT_DECILE;
RUN;


%_eg_conditional_dropds(WORK.MANAGER_ENRICHED);

PROC SQL;
    CREATE TABLE WORK.MANAGER_ENRICHED AS
    SELECT
        t1.MANAGER_ID,
        t2.MANAGER_NAME,
        t2.REGION_CODE,
        t2.BRANCH_ID,
        t1.APPROVAL_YEAR,
        t1.APPROVAL_MONTH,
        t1.LOAN_COUNT,
        t1.TOTAL_APPROVED        FORMAT=COMMAX15.2  LABEL="Total Approved Amount",
        t1.TOTAL_OUTSTANDING     FORMAT=COMMAX15.2  LABEL="Total Outstanding",
        t1.DEFAULTED_AMOUNT      FORMAT=COMMAX15.2  LABEL="Defaulted Amount",
        t1.DEFAULT_RATE,
        t1.CUMULATIVE_APPROVED,
        t1.CUMULATIVE_LOANS,
        t1.PREV_TOTAL_APPROVED,
        t1.MOM_APPROVED_CHANGE,
        t1.MOM_OUTSTANDING_CHANGE,
        t1.APPROVED_RANK,
        t1.DEFAULT_DECILE
    FROM WORK.MANAGER_DECILE t1
        LEFT JOIN dwh.MANAGER_REFERENCE t2
            ON  t1.MANAGER_ID  = t2.MANAGER_ID
            AND t2.VALID_FROM  <= "&gReportDate."d
            AND t2.VALID_TO     > "&gReportDate."d;
QUIT;


PROC SQL;
    UPDATE WORK.MANAGER_ENRICHED t1
    SET t1.REGION_CODE = (
        SELECT t2.REGION_CODE
        FROM dwh.BRANCH_REGION_MAP t2
        WHERE t2.BRANCH_ID    = t1.BRANCH_ID
          AND t2.ACTIVE_FLAG  = 1
    )
    WHERE MISSING(t1.REGION_CODE);
QUIT;


%_eg_conditional_dropds(WORK.TOP_PERFORMERS);

PROC SQL;
    CREATE TABLE WORK.TOP_PERFORMERS AS
    SELECT
        t1.MANAGER_ID,
        t1.MANAGER_NAME,
        t1.REGION_CODE,
        t1.APPROVAL_YEAR,
        t1.APPROVAL_MONTH,
        t1.TOTAL_APPROVED,
        t1.APPROVED_RANK,
        t1.DEFAULT_RATE,
        t1.DEFAULT_DECILE,
        t1.MOM_APPROVED_CHANGE,
        "TOP10" AS PERFORMANCE_TIER
    FROM WORK.MANAGER_ENRICHED t1
    WHERE t1.APPROVED_RANK <= 10
      AND t1.DEFAULT_DECILE <= 2;
QUIT;


%_eg_conditional_dropds(WORK.LOW_PERFORMERS);

PROC SQL;
    CREATE TABLE WORK.LOW_PERFORMERS AS
    SELECT
        t1.MANAGER_ID,
        t1.MANAGER_NAME,
        t1.REGION_CODE,
        t1.APPROVAL_YEAR,
        t1.APPROVAL_MONTH,
        t1.TOTAL_APPROVED,
        t1.APPROVED_RANK,
        t1.DEFAULT_RATE,
        t1.DEFAULT_DECILE,
        t1.MOM_APPROVED_CHANGE,
        "REVIEW" AS PERFORMANCE_TIER
    FROM WORK.MANAGER_ENRICHED t1
    WHERE t1.APPROVED_RANK > (
            SELECT COUNT(DISTINCT t2.MANAGER_ID) * 0.8
            FROM WORK.MANAGER_ENRICHED t2
            WHERE t2.APPROVAL_YEAR  = t1.APPROVAL_YEAR
              AND t2.APPROVAL_MONTH = t1.APPROVAL_MONTH
          )
       OR t1.DEFAULT_DECILE >= 9;
QUIT;


%_eg_conditional_dropds(WORK.PERFORMANCE_COMBINED);

PROC SQL;
    CREATE TABLE WORK.PERFORMANCE_COMBINED AS
    SELECT
        MANAGER_ID,
        MANAGER_NAME,
        REGION_CODE,
        APPROVAL_YEAR,
        APPROVAL_MONTH,
        TOTAL_APPROVED,
        APPROVED_RANK,
        DEFAULT_RATE,
        DEFAULT_DECILE,
        MOM_APPROVED_CHANGE,
        PERFORMANCE_TIER
    FROM WORK.TOP_PERFORMERS

    OUTER UNION CORR

    SELECT
        MANAGER_ID,
        MANAGER_NAME,
        REGION_CODE,
        APPROVAL_YEAR,
        APPROVAL_MONTH,
        TOTAL_APPROVED,
        APPROVED_RANK,
        DEFAULT_RATE,
        DEFAULT_DECILE,
        MOM_APPROVED_CHANGE,
        PERFORMANCE_TIER
    FROM WORK.LOW_PERFORMERS;
QUIT;


PROC APPEND BASE=DM.MANAGER_PERFORMANCE_HIST DATA=WORK.PERFORMANCE_COMBINED;
RUN;


PROC SQL;
    INSERT INTO DM.MANAGER_PERFORMANCE_SNAPSHOT
        (MANAGER_ID, APPROVAL_YEAR, APPROVAL_MONTH,
         TOTAL_APPROVED, DEFAULT_RATE, APPROVED_RANK, LOAD_DATE)
    SELECT
        t1.MANAGER_ID,
        t1.APPROVAL_YEAR,
        t1.APPROVAL_MONTH,
        t1.TOTAL_APPROVED,
        t1.DEFAULT_RATE,
        t1.APPROVED_RANK,
        TODAY()
    FROM WORK.PERFORMANCE_COMBINED t1
    WHERE t1.APPROVED_RANK <= 5;
QUIT;


PROC SQL;
    DELETE FROM WORK.LOAN_BASE
    WHERE LOAN_ID IN (
        SELECT LOAN_ID
        FROM WORK.LOAN_NODUPS
        WHERE MISSING(APPROVED_AMOUNT)
           OR MISSING(MANAGER_ID)
    );
QUIT;
