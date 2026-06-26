/*******************************************************************************
 * FILE: TC-18_insurance_additions_exits_quarterly.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - DATA step CSV import (unusual delimiter: ASCII 127 DEL, WLATIN1 encoding,
 *     COMMAX informat for European decimal notation)
 *   - INNER JOIN between two external DB tables
 *   - LEFT JOIN to external DB table (deceased-partner enrichment)
 *   - LEFT JOIN to external DB table (status code lookup)
 *   - CASE WHEN multi-branch status code decode (13 values)
 *   - CASE WHEN single-condition flag columns (MM_1 through MM_7)
 *   - INTNX('QTR', &macro., 0, 'B') — quarter-start date calculation
 *   - INTNX('QTR', &macro., 0, 'B') - 30 — date arithmetic on INTNX result
 *   - Macro variable &DWH_REPORTING_DATE. used as reference date
 *   - LABEL= on column aliases (display-only, must be stripped)
 *   - FORMAT= on column aliases (display-only, must be stripped)
 *   - FORMAT=COMMAX20.2 (European number format with dot thousands, comma decimal)
 *   - CALCULATED keyword in GROUP BY and HAVING
 *   - SAS multi-argument SUM(a, b) as NULL-safe addition (not aggregate)
 *   - Nested SUM(SUM(col1, col2)) — inner is NULL-safe add, outer is aggregate
 *   - OUTER UNION CORR for combining tables with matching columns
 *   - IFC(condition, 'true_val', 'false_val') — conditional string function
 *   - Nested IFC(cond, val, IFC(cond2, val2, val3)) — chained conditional
 *   - ORDER BY inside CREATE TABLE AS SELECT (forbidden in Oracle)
 *   - NOT IS MISSING — SAS null check (reversed English word order)
 *   - COUNT(DISTINCT(...)) — distinct count aggregate
 *   - COUNT(col) - COUNT(DISTINCT col) — duplicate count formula
 *   - TRIM() in GROUP BY expression (dynamic label concatenation)
 *   - COALESCE used for NULL-safe subtraction in computed column
 *   - Empty string '' as placeholder column value
 *   - %_eg_conditional_dropds macro (SAS EG auto-generated drop pattern)
 *   - %LET _CLIENTTASKLABEL boilerplate (SAS EG metadata — ignored)
 *   - Commented-out CASE branch left in source (NETTO_AUSGEGLICHEN table
 *     created but downstream JOIN is commented out — orphaned table)
 *
 * COMPLEXITY: High
 *
 * EDGE CASES / TRICKY PARTS:
 *   - Primary source is an EXTERNAL CSV FILE, not a DWH table. Pentaho needs
 *     Text File Input + Table Output, not ExecSQL. Delimiter = ASCII 127 (DEL).
 *   - INTNX('QTR', date, 0, 'B') = first day of current quarter =
 *     TRUNC(date, 'Q') in Oracle. Not INTNX('MONTH',...) — quarter-specific.
 *   - SAS SUM(a, b) with two arguments is a NULL-safe addition function, NOT
 *     a GROUP BY aggregate. SUM(SUM(a, b)) = one function wrapping the other.
 *   - OUTER UNION CORR: SAS stacks tables by matching column names, padding
 *     missing columns with NULL. Oracle UNION ALL requires explicit column
 *     lists when structures differ; here all sub-tables share the same 3 columns
 *     so a straight UNION ALL works.
 *   - CALCULATED keyword appears in both GROUP BY and HAVING — must be removed
 *     and the full expression repeated in Oracle.
 *   - PLAUSIBILISIERUNG_SORTED has ORDER BY in the CTAS — forbidden in Oracle.
 *     ORDER BY belongs in the TableInput feeding the export step instead.
 *   - NOT IS MISSING = IS NOT NULL. Easy to misread — the SAS phrasing is
 *     reversed from natural English.
 *   - The NETTO_AUSGEGLICHEN table is created but the ROHDATEN_3 CASE branch
 *     that was supposed to use it is commented out. Migrate the table anyway
 *     but flag it as orphaned.
 *   - Six PLAUSIBILISIERUNG tables all use CALCULATED inside GROUP BY with a
 *     string literal as the group key — Oracle does not allow grouping by a
 *     literal; drop it and group by PROVIDER_ID only.
 *   - IFC nested two levels deep maps to nested CASE WHEN in Oracle.
 ******************************************************************************/

LIBNAME SOURCE  META REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Source Data']";

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


%_eg_conditional_dropds(WORK.RAW_DATA);

DATA WORK.RAW_DATA;
    LENGTH
        PROVIDER_ID              $ 7
        CONTRACT_NR                8
        POLICYHOLDER_NAME        $ 45
        STREET_ADDRESS           $ 28
        ZIP_CITY                 $ 31
        POLICY_NR                  8
        POLICY_START_DT            8
        INSURED_AMOUNT             8
        INTEREST_RATE_PCT          8
        HEALTH_SUPPLEMENT          8
        GROSS_PREMIUM              8
        NET_PREMIUM                8
        ACQUISITION_FEE            8
        ACQUISITION_FEE_BANK       8
        ADMIN_FEE                  8
        ADMIN_FEE_BANK             8
        COLLECTION_FEE             8
        INSURED_PERSON_1_NAME    $ 27
        INSURED_PERSON_1_GENDER  $ 1
        INSURED_PERSON_1_DOB       8
        INSURED_PERSON_2_NAME    $ 13
        INSURED_PERSON_2_GENDER  $ 1
        INSURED_PERSON_2_DOB       8
        PROCESSING_RUN_DT          8 ;
    LABEL
        CONTRACT_NR              = "CONTRACT-NR"
        POLICYHOLDER_NAME        = "NAME OF POLICYHOLDER"
        ZIP_CITY                 = "ZIP    CITY"
        POLICY_NR                = "POLICY NR."
        POLICY_START_DT          = "POLICY START DATE"
        INSURED_AMOUNT           = "INSURED AMOUNT"
        INTEREST_RATE_PCT        = "INTEREST RATE %"
        ACQUISITION_FEE_BANK     = "ACQUISITION FEE BANK"
        ADMIN_FEE_BANK           = "ADMIN FEE BANK"
        INSURED_PERSON_1_NAME    = "NAME INSURED PERSON 1"
        INSURED_PERSON_1_GENDER  = "GENDER CODE INSURED PERSON 1"
        INSURED_PERSON_1_DOB     = "DOB INSURED PERSON 1"
        INSURED_PERSON_2_NAME    = "NAME INSURED PERSON 2"
        INSURED_PERSON_2_GENDER  = "GENDER CODE INSURED PERSON 2"
        INSURED_PERSON_2_DOB     = "DOB INSURED PERSON 2"
        PROCESSING_RUN_DT        = "PROCESSING RUN DATE" ;
    FORMAT
        PROVIDER_ID              $CHAR7.
        CONTRACT_NR              BEST10.
        POLICYHOLDER_NAME        $CHAR45.
        STREET_ADDRESS           $CHAR28.
        ZIP_CITY                 $CHAR31.
        POLICY_NR                BEST10.
        POLICY_START_DT          DDMMYYP10.
        INSURED_AMOUNT           COMMAX20.2
        INTEREST_RATE_PCT        COMMAX20.2
        HEALTH_SUPPLEMENT        COMMAX20.2
        GROSS_PREMIUM            COMMAX20.2
        NET_PREMIUM              COMMAX20.2
        ACQUISITION_FEE          COMMAX20.2
        ACQUISITION_FEE_BANK     COMMAX20.2
        ADMIN_FEE                COMMAX20.2
        ADMIN_FEE_BANK           COMMAX20.2
        COLLECTION_FEE           COMMAX20.2
        INSURED_PERSON_1_NAME    $CHAR27.
        INSURED_PERSON_1_GENDER  $CHAR1.
        INSURED_PERSON_1_DOB     DDMMYYP10.
        INSURED_PERSON_2_NAME    $CHAR13.
        INSURED_PERSON_2_GENDER  $CHAR1.
        INSURED_PERSON_2_DOB     DDMMYYP10.
        PROCESSING_RUN_DT        DDMMYYP10. ;
    INFORMAT
        PROVIDER_ID              $CHAR7.
        CONTRACT_NR              BEST10.
        POLICYHOLDER_NAME        $CHAR45.
        STREET_ADDRESS           $CHAR28.
        ZIP_CITY                 $CHAR31.
        POLICY_NR                BEST10.
        POLICY_START_DT          DDMMYY10.
        INSURED_AMOUNT           BEST11.
        INTEREST_RATE_PCT        BEST4.
        HEALTH_SUPPLEMENT        BEST4.
        GROSS_PREMIUM            BEST7.
        NET_PREMIUM              BEST7.
        ACQUISITION_FEE          BEST6.
        ACQUISITION_FEE_BANK     BEST6.
        ADMIN_FEE                BEST7.
        ADMIN_FEE_BANK           BEST6.
        COLLECTION_FEE           BEST6.
        INSURED_PERSON_1_NAME    $CHAR27.
        INSURED_PERSON_1_GENDER  $CHAR1.
        INSURED_PERSON_1_DOB     DDMMYY10.
        INSURED_PERSON_2_NAME    $CHAR13.
        INSURED_PERSON_2_GENDER  $CHAR1.
        INSURED_PERSON_2_DOB     DDMMYY10.
        PROCESSING_RUN_DT        DDMMYY10. ;
    INFILE 'C:\TRANSFER_IN\INSURANCE_MOVEMENTS_Q.csv'
        LRECL=235
        ENCODING="WLATIN1"
        TERMSTR=CRLF
        DLM='7F'x
        MISSOVER
        DSD ;
    INPUT
        PROVIDER_ID              : $CHAR7.
        CONTRACT_NR              : ?? BEST10.
        POLICYHOLDER_NAME        : $CHAR45.
        STREET_ADDRESS           : $CHAR28.
        ZIP_CITY                 : $CHAR31.
        POLICY_NR                : ?? BEST10.
        POLICY_START_DT          : ?? DDMMYY10.
        INSURED_AMOUNT           : ?? COMMAX11.
        INTEREST_RATE_PCT        : ?? COMMAX4.
        HEALTH_SUPPLEMENT        : ?? COMMAX4.
        GROSS_PREMIUM            : ?? COMMAX7.
        NET_PREMIUM              : ?? COMMAX7.
        ACQUISITION_FEE          : ?? COMMAX6.
        ACQUISITION_FEE_BANK     : ?? COMMAX6.
        ADMIN_FEE                : ?? COMMAX7.
        ADMIN_FEE_BANK           : ?? COMMAX6.
        COLLECTION_FEE           : ?? COMMAX6.
        INSURED_PERSON_1_NAME    : $CHAR27.
        INSURED_PERSON_1_GENDER  : $CHAR1.
        INSURED_PERSON_1_DOB     : ?? DDMMYY10.
        INSURED_PERSON_2_NAME    : $CHAR13.
        INSURED_PERSON_2_GENDER  : $CHAR1.
        INSURED_PERSON_2_DOB     : ?? DDMMYY10.
        PROCESSING_RUN_DT        : ?? DDMMYY10. ;
RUN;


%_eg_conditional_dropds(WORK.DECEASED_PARTNERS);

PROC SQL;
   CREATE TABLE WORK.DECEASED_PARTNERS AS
   SELECT t2.CONTRACT_NR,
          t1.PARTNER_NR,
          t1.DECEASED_FLAG,
          t1.PARTNER_NAME,
          t1.FIRST_NAME_ADD,
          t1.DEATH_DT
      FROM SOURCE.PARTNER_MASTER t1
           INNER JOIN SOURCE.CONTRACT_MASTER t2 ON (t1.PARTNER_NR = t2.CUSTOMER_NR)
      WHERE t1.DECEASED_FLAG = 'X';
QUIT;


%_eg_conditional_dropds(WORK.RAW_DATA_1);

PROC SQL;
   CREATE TABLE WORK.RAW_DATA_1 AS
   SELECT t1.PROVIDER_ID,
          t1.CONTRACT_NR,
          t1.POLICYHOLDER_NAME,
          t2.DECEASED_FLAG,
          t2.PARTNER_NAME,
          t2.FIRST_NAME_ADD,
          t2.DEATH_DT FORMAT=DDMMYYP10. AS DEATH_DT,
          t1.STREET_ADDRESS,
          t1.ZIP_CITY,
          t1.POLICY_NR,
          t1.POLICY_START_DT,
          t1.INSURED_AMOUNT,
          t1.INTEREST_RATE_PCT,
          t1.HEALTH_SUPPLEMENT,
          t1.GROSS_PREMIUM,
          t1.NET_PREMIUM,
          t1.ACQUISITION_FEE,
          t1.ACQUISITION_FEE_BANK,
          t1.ADMIN_FEE,
          t1.ADMIN_FEE_BANK,
          t1.COLLECTION_FEE,
          t1.INSURED_PERSON_1_NAME,
          t1.INSURED_PERSON_1_GENDER,
          t1.INSURED_PERSON_1_DOB,
          t1.INSURED_PERSON_2_NAME,
          t1.INSURED_PERSON_2_GENDER,
          t1.INSURED_PERSON_2_DOB,
          t1.PROCESSING_RUN_DT
      FROM WORK.RAW_DATA t1
           LEFT JOIN WORK.DECEASED_PARTNERS t2 ON (t1.CONTRACT_NR = t2.CONTRACT_NR);
QUIT;


%_eg_conditional_dropds(WORK.RAW_DATA_2);

PROC SQL;
   CREATE TABLE WORK.RAW_DATA_2 AS
   SELECT t1.PROVIDER_ID,
          t1.CONTRACT_NR,
          t1.POLICYHOLDER_NAME,
          t1.DECEASED_FLAG,
          t1.PARTNER_NAME,
          t1.FIRST_NAME_ADD,
          t1.DEATH_DT,
          t1.STREET_ADDRESS,
          t1.ZIP_CITY,
          t1.POLICY_NR,
          t1.POLICY_START_DT,
          t1.INSURED_AMOUNT,
          t1.INTEREST_RATE_PCT,
          t1.HEALTH_SUPPLEMENT,
          t1.GROSS_PREMIUM,
          t1.NET_PREMIUM,
          t1.ACQUISITION_FEE,
          t1.ACQUISITION_FEE_BANK,
          t1.ADMIN_FEE,
          t1.ADMIN_FEE_BANK,
          t1.COLLECTION_FEE,
          t1.INSURED_PERSON_1_NAME,
          t1.INSURED_PERSON_1_GENDER,
          t1.INSURED_PERSON_1_DOB,
          t1.INSURED_PERSON_2_NAME,
          t1.INSURED_PERSON_2_GENDER,
          t1.INSURED_PERSON_2_DOB,
          t1.PROCESSING_RUN_DT,
          t2.STATUS_CODE,
            (CASE t2.STATUS_CODE
               WHEN '00' THEN 'not insured'
               WHEN '01' THEN 'pending registration'
               WHEN '02' THEN 'increase pending'
               WHEN '03' THEN 'replacement policy'
               WHEN '06' THEN 'health review in progress'
               WHEN '07' THEN 'cancelled'
               WHEN '08' THEN 'death claim'
               WHEN '09' THEN 'revoked/disputed'
               WHEN '10' THEN 'active'
               WHEN '12' THEN 'increased coverage'
               WHEN '13' THEN 'reversed'
            END
              ) AS INSURANCE_STATUS_DESC
      FROM WORK.RAW_DATA_1 t1
           LEFT JOIN SOURCE.INSURANCE_RISK_PRODUCT t2 ON (t1.CONTRACT_NR = t2.CONTRACT_NR);
QUIT;


%_eg_conditional_dropds(WORK.NET_ZERO_MOVEMENTS);

PROC SQL;
   CREATE TABLE WORK.NET_ZERO_MOVEMENTS AS
   SELECT t1.CONTRACT_NR,
            (SUM(t1.INSURED_AMOUNT)) FORMAT=COMMAX20.2 AS SUM_of_INSURED_AMOUNT,
            (SUM(t1.NET_PREMIUM)) FORMAT=COMMAX20.2 AS SUM_of_NET_PREMIUM,
            (COUNT(t1.CONTRACT_NR)) AS COUNT_of_CONTRACT_NR
      FROM WORK.RAW_DATA_2 t1
      GROUP BY t1.CONTRACT_NR
      HAVING (CALCULATED SUM_of_NET_PREMIUM) = 0;
QUIT;


%_eg_conditional_dropds(WORK.MULTI_ADDITIONS);

PROC SQL;
   CREATE TABLE WORK.MULTI_ADDITIONS AS
   SELECT t1.CONTRACT_NR,
            (COUNT(t1.CONTRACT_NR)) AS COUNT_of_CONTRACT_NR
      FROM WORK.RAW_DATA_2 t1
      WHERE t1.INSURED_AMOUNT > 0
      GROUP BY t1.CONTRACT_NR
      HAVING (CALCULATED COUNT_of_CONTRACT_NR) > 1;
QUIT;


%_eg_conditional_dropds(WORK.RAW_DATA_3);

PROC SQL;
   CREATE TABLE WORK.RAW_DATA_3 AS
   SELECT 
            (CASE
              /* WHEN t2.CONTRACT_NR NOT IS MISSING THEN "01 - Net zero" */
              WHEN t1.DECEASED_FLAG = "X" THEN "Exclusion - policyholder deceased"
              WHEN t1.INSURED_AMOUNT = 0 THEN "Exclusion - insured amount zero"
              WHEN t1.POLICY_NR = 0 THEN "Policy number zero"
              WHEN t1.GROSS_PREMIUM > 0 AND t1.POLICY_START_DT < INTNX('QTR', &DWH_REPORTING_DATE., 0, 'B') THEN
            "Addition with start date before quarter"
              ELSE ""
            END) AS EXCLUSION_REASON,
          t1.DECEASED_FLAG LABEL="FLAG_1" AS FLAG_1,
            (CASE WHEN t1.INSURED_AMOUNT = 0 THEN 'X' END) LABEL="FLAG_2" AS FLAG_2,
            (CASE WHEN t1.POLICY_NR = 0 THEN 'X' END) LABEL="FLAG_3" AS FLAG_3,
            (CASE WHEN t1.INSURED_AMOUNT > 0 AND t1.POLICY_START_DT < INTNX('QTR', &DWH_REPORTING_DATE., 0, 'B') THEN 'X' END)
            LABEL="FLAG_4" AS FLAG_4,
            (CASE WHEN t1.STATUS_CODE = '09' AND t1.POLICY_START_DT < INTNX('QTR', &DWH_REPORTING_DATE., 0, 'B') THEN 'X' END)
            LABEL="FLAG_5" AS FLAG_5,
            (CASE WHEN t2.CONTRACT_NR NOT IS MISSING THEN 'X' END) LABEL="FLAG_6" AS FLAG_6,
            ('') LABEL="Comment" AS COMMENT_FIELD,
          t1.PROVIDER_ID,
          t1.CONTRACT_NR,
          t1.POLICYHOLDER_NAME,
          t1.STREET_ADDRESS,
          t1.ZIP_CITY,
          t1.POLICY_NR,
          t1.INSURANCE_STATUS_DESC,
          t1.POLICY_START_DT,
          t1.INSURED_AMOUNT,
          t1.INTEREST_RATE_PCT,
          t1.HEALTH_SUPPLEMENT,
          t1.GROSS_PREMIUM,
          t1.NET_PREMIUM,
          t1.ACQUISITION_FEE,
          t1.ACQUISITION_FEE_BANK,
          t1.ADMIN_FEE,
          t1.ADMIN_FEE_BANK,
          t1.COLLECTION_FEE,
          t1.INSURED_PERSON_1_NAME,
          t1.INSURED_PERSON_1_GENDER,
          t1.INSURED_PERSON_1_DOB,
          t1.INSURED_PERSON_2_NAME,
          t1.INSURED_PERSON_2_GENDER,
          t1.INSURED_PERSON_2_DOB,
          t1.PROCESSING_RUN_DT,
          t1.DECEASED_FLAG,
          t1.PARTNER_NAME,
          t1.FIRST_NAME_ADD,
            (CASE
              WHEN t1.DECEASED_FLAG = "X" THEN 2
              WHEN t1.INSURED_AMOUNT = 0 THEN 3
              ELSE 0
            END) AS EXCLUSION_CODE,
          t1.STATUS_CODE
      FROM WORK.RAW_DATA_2 t1
           LEFT JOIN WORK.MULTI_ADDITIONS t2 ON (t1.CONTRACT_NR = t2.CONTRACT_NR);
QUIT;


%_eg_conditional_dropds(WORK.MULTI_EXITS);

PROC SQL;
   CREATE TABLE WORK.MULTI_EXITS AS
   SELECT t1.CONTRACT_NR,
            (COUNT(t1.CONTRACT_NR)) AS COUNT_of_CONTRACT_NR
      FROM WORK.RAW_DATA_2 t1
      WHERE t1.INSURED_AMOUNT < 0
      GROUP BY t1.CONTRACT_NR
      HAVING (CALCULATED COUNT_of_CONTRACT_NR) > 1;
QUIT;


%_eg_conditional_dropds(WORK.RAW_DATA_FLAGGED);

PROC SQL;
   CREATE TABLE WORK.RAW_DATA_FLAGGED AS
   SELECT t1.EXCLUSION_REASON,
          t1.FLAG_1,
          t1.FLAG_2,
          t1.FLAG_3,
          t1.FLAG_4,
          t1.FLAG_5,
          t1.FLAG_6,
            (CASE WHEN t2.CONTRACT_NR NOT IS MISSING THEN 'X' END) LABEL="FLAG_7" AS FLAG_7,
          t1.COMMENT_FIELD,
          t1.PROVIDER_ID,
          t1.CONTRACT_NR,
          t1.POLICYHOLDER_NAME,
          t1.STREET_ADDRESS,
          t1.ZIP_CITY,
          t1.POLICY_NR,
          t1.INSURANCE_STATUS_DESC,
          t1.POLICY_START_DT,
          t1.INSURED_AMOUNT,
          t1.INTEREST_RATE_PCT,
          t1.HEALTH_SUPPLEMENT,
          t1.GROSS_PREMIUM,
          t1.NET_PREMIUM,
          t1.ACQUISITION_FEE,
          t1.ACQUISITION_FEE_BANK,
          t1.ADMIN_FEE,
          t1.ADMIN_FEE_BANK,
          t1.COLLECTION_FEE,
          t1.INSURED_PERSON_1_NAME,
          t1.INSURED_PERSON_1_GENDER,
          t1.INSURED_PERSON_1_DOB,
          t1.INSURED_PERSON_2_NAME,
          t1.INSURED_PERSON_2_GENDER,
          t1.INSURED_PERSON_2_DOB,
          t1.PROCESSING_RUN_DT,
          t1.DECEASED_FLAG,
          t1.PARTNER_NAME,
          t1.FIRST_NAME_ADD,
          t1.EXCLUSION_CODE,
          t1.STATUS_CODE
      FROM WORK.RAW_DATA_3 t1
           LEFT JOIN WORK.MULTI_EXITS t2 ON (t1.CONTRACT_NR = t2.CONTRACT_NR);
QUIT;


%_eg_conditional_dropds(WORK.ADDITIONS);

PROC SQL;
   CREATE TABLE WORK.ADDITIONS AS
   SELECT t1.PROVIDER_ID,
          t1.CONTRACT_NR,
          t1.POLICYHOLDER_NAME,
          t1.STREET_ADDRESS,
          t1.ZIP_CITY,
          t1.POLICY_NR,
          t1.INSURANCE_STATUS_DESC,
          t1.POLICY_START_DT,
          t1.INSURED_AMOUNT,
          t1.INTEREST_RATE_PCT,
          t1.HEALTH_SUPPLEMENT,
          t1.GROSS_PREMIUM,
          t1.NET_PREMIUM,
          t1.ACQUISITION_FEE,
          t1.ACQUISITION_FEE_BANK,
          t1.ADMIN_FEE,
          t1.ADMIN_FEE_BANK,
          t1.COLLECTION_FEE,
          t1.INSURED_PERSON_1_NAME,
          t1.INSURED_PERSON_1_GENDER,
          t1.INSURED_PERSON_1_DOB,
          t1.INSURED_PERSON_2_NAME,
          t1.INSURED_PERSON_2_GENDER,
          t1.INSURED_PERSON_2_DOB,
          t1.PROCESSING_RUN_DT
      FROM WORK.RAW_DATA_FLAGGED t1
      WHERE t1.INSURED_AMOUNT > 0 AND t1.EXCLUSION_CODE <= 1;
QUIT;


%_eg_conditional_dropds(WORK.EXITS);

PROC SQL;
   CREATE TABLE WORK.EXITS AS
   SELECT t1.PROVIDER_ID,
          t1.CONTRACT_NR,
          t1.POLICYHOLDER_NAME,
          t1.STREET_ADDRESS,
          t1.ZIP_CITY,
          t1.POLICY_NR,
          t1.INSURANCE_STATUS_DESC,
          t1.POLICY_START_DT,
          t1.INSURED_AMOUNT,
          t1.INTEREST_RATE_PCT,
          t1.HEALTH_SUPPLEMENT,
          t1.GROSS_PREMIUM,
          t1.NET_PREMIUM,
          t1.ACQUISITION_FEE,
          t1.ACQUISITION_FEE_BANK,
          t1.ADMIN_FEE,
          t1.ADMIN_FEE_BANK,
          t1.COLLECTION_FEE,
          t1.INSURED_PERSON_1_NAME,
          t1.INSURED_PERSON_1_GENDER,
          t1.INSURED_PERSON_1_DOB,
          t1.INSURED_PERSON_2_NAME,
          t1.INSURED_PERSON_2_GENDER,
          t1.INSURED_PERSON_2_DOB,
          t1.PROCESSING_RUN_DT
      FROM WORK.RAW_DATA_FLAGGED t1
      WHERE t1.INSURED_AMOUNT < 0 AND t1.EXCLUSION_CODE <= 1;
QUIT;


%_eg_conditional_dropds(WORK.CHECK_POLICY_NR_ZERO);

PROC SQL;
   CREATE TABLE WORK.CHECK_POLICY_NR_ZERO AS
   SELECT t1.PROVIDER_ID LABEL="Provider" AS PROVIDER_ID,
            ('CHECK1: Policy number = 0') LABEL="Check Description" AS CHECK_DESC,
            (COUNT(DISTINCT(t1.CONTRACT_NR))) LABEL="Contract Count" AS CONTRACT_COUNT
      FROM WORK.RAW_DATA_FLAGGED t1
      WHERE t1.POLICY_NR = 0
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED CHECK_DESC);
QUIT;


%_eg_conditional_dropds(WORK.CHECK_AMOUNT_ZERO);

PROC SQL;
   CREATE TABLE WORK.CHECK_AMOUNT_ZERO AS
   SELECT t1.PROVIDER_ID,
            ('CHECK2: Insured amount = 0') AS CHECK_DESC,
            (COUNT(DISTINCT(t1.CONTRACT_NR))) LABEL="COUNT" AS CONTRACT_COUNT
      FROM WORK.RAW_DATA_FLAGGED t1
      WHERE t1.INSURED_AMOUNT = 0
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED CHECK_DESC);
QUIT;


%_eg_conditional_dropds(WORK.CHECK_START_BEFORE_QUARTER);

PROC SQL;
   CREATE TABLE WORK.CHECK_START_BEFORE_QUARTER AS
   SELECT t1.PROVIDER_ID,
            ('CHECK3: Addition start date before quarter start') AS CHECK_DESC,
            (COUNT(DISTINCT(t1.CONTRACT_NR))) AS CONTRACT_COUNT
      FROM WORK.ADDITIONS t1
      WHERE t1.POLICY_START_DT < INTNX('QTR', &DWH_REPORTING_DATE., 0, 'B')
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED CHECK_DESC);
QUIT;


%_eg_conditional_dropds(WORK.CHECK_REVOKED_BEFORE_QUARTER_MINUS30);

PROC SQL;
   CREATE TABLE WORK.CHECK_REVOKED_BEFORE_QUARTER_MINUS30 AS
   SELECT t1.PROVIDER_ID,
            ('CHECK4: Revoked contract start 30 days before quarter start') AS CHECK_DESC,
            (COUNT(DISTINCT(t1.CONTRACT_NR))) LABEL="COUNT" AS CONTRACT_COUNT
      FROM WORK.RAW_DATA_FLAGGED t1
      WHERE t1.STATUS_CODE = '09' AND t1.POLICY_START_DT < INTNX('QTR', &DWH_REPORTING_DATE., 0, 'B') - 30
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED CHECK_DESC);
QUIT;


%_eg_conditional_dropds(WORK.CHECK_MULTIPLE_ADDITIONS);

PROC SQL;
   CREATE TABLE WORK.CHECK_MULTIPLE_ADDITIONS AS
   SELECT t1.PROVIDER_ID,
            ('CHECK5: Contracts with multiple additions') AS CHECK_DESC,
            ((COUNT(t1.CONTRACT_NR)) - (COUNT(DISTINCT(t1.CONTRACT_NR)))) AS CONTRACT_COUNT
      FROM WORK.ADDITIONS t1
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED CHECK_DESC);
QUIT;


%_eg_conditional_dropds(WORK.CHECK_MULTIPLE_EXITS);

PROC SQL;
   CREATE TABLE WORK.CHECK_MULTIPLE_EXITS AS
   SELECT t1.PROVIDER_ID,
            ('CHECK6: Contracts with multiple exits') AS CHECK_DESC,
            ((COUNT(t1.CONTRACT_NR)) - (COUNT(DISTINCT(t1.CONTRACT_NR)))) AS CONTRACT_COUNT
      FROM WORK.EXITS t1
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED CHECK_DESC);
QUIT;


%_eg_conditional_dropds(WORK.ALL_CHECKS);
PROC SQL;
CREATE TABLE WORK.ALL_CHECKS AS
SELECT * FROM WORK.CHECK_POLICY_NR_ZERO
 OUTER UNION CORR
SELECT * FROM WORK.CHECK_AMOUNT_ZERO
 OUTER UNION CORR
SELECT * FROM WORK.CHECK_START_BEFORE_QUARTER
 OUTER UNION CORR
SELECT * FROM WORK.CHECK_REVOKED_BEFORE_QUARTER_MINUS30
 OUTER UNION CORR
SELECT * FROM WORK.CHECK_MULTIPLE_ADDITIONS
 OUTER UNION CORR
SELECT * FROM WORK.CHECK_MULTIPLE_EXITS
;
Quit;


%_eg_conditional_dropds(WORK.ALL_CHECKS_SORTED);

PROC SQL;
   CREATE TABLE WORK.ALL_CHECKS_SORTED AS
   SELECT t1.PROVIDER_ID,
          t1.CHECK_DESC,
          t1.CONTRACT_COUNT
      FROM WORK.ALL_CHECKS t1
      ORDER BY t1.CHECK_DESC;
QUIT;


%_eg_conditional_dropds(WORK.TOTALS_ADDITIONS);

PROC SQL;
   CREATE TABLE WORK.TOTALS_ADDITIONS AS
   SELECT t1.PROVIDER_ID,
            ("01 - Additions") AS MOVEMENT_TYPE,
            (SUM(t1.INSURED_AMOUNT)) FORMAT=COMMAX20.2 AS SUM_of_INSURED_AMOUNT,
            (SUM(t1.HEALTH_SUPPLEMENT)) FORMAT=COMMAX20.2 AS SUM_of_HEALTH_SUPPLEMENT,
            (SUM(t1.GROSS_PREMIUM)) FORMAT=COMMAX20.2 AS SUM_of_GROSS_PREMIUM,
            (SUM(t1.NET_PREMIUM)) FORMAT=COMMAX20.2 AS SUM_of_NET_PREMIUM,
            (SUM(SUM(t1.ACQUISITION_FEE_BANK, t1.ADMIN_FEE_BANK))) FORMAT=COMMAX20.2 AS BANK_REIMBURSEMENT,
            (COUNT(t1.CONTRACT_NR)) AS CONTRACT_COUNT
      FROM WORK.ADDITIONS t1
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED MOVEMENT_TYPE);
QUIT;


%_eg_conditional_dropds(WORK.TOTALS_EXITS);

PROC SQL;
   CREATE TABLE WORK.TOTALS_EXITS AS
   SELECT t1.PROVIDER_ID,
            ("02 - Exits") AS MOVEMENT_TYPE,
            (SUM(t1.INSURED_AMOUNT)) FORMAT=COMMAX20.2 AS SUM_of_INSURED_AMOUNT,
            (SUM(t1.HEALTH_SUPPLEMENT)) FORMAT=COMMAX20.2 AS SUM_of_HEALTH_SUPPLEMENT,
            (SUM(t1.GROSS_PREMIUM)) FORMAT=COMMAX20.2 AS SUM_of_GROSS_PREMIUM,
            (SUM(t1.NET_PREMIUM)) FORMAT=COMMAX20.2 AS SUM_of_NET_PREMIUM,
            (SUM(SUM(t1.ACQUISITION_FEE_BANK, t1.ADMIN_FEE_BANK))) AS BANK_REIMBURSEMENT,
            (COUNT(t1.CONTRACT_NR)) AS CONTRACT_COUNT
      FROM WORK.EXITS t1
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED MOVEMENT_TYPE);
QUIT;


%_eg_conditional_dropds(WORK.TOTALS_OVERALL);

PROC SQL;
   CREATE TABLE WORK.TOTALS_OVERALL AS
   SELECT t1.PROVIDER_ID,
            ("00 - Total") AS MOVEMENT_TYPE,
            (SUM(t1.INSURED_AMOUNT)) FORMAT=COMMAX20.2 AS SUM_of_INSURED_AMOUNT,
            (SUM(t1.HEALTH_SUPPLEMENT)) FORMAT=COMMAX20.2 AS SUM_of_HEALTH_SUPPLEMENT,
            (SUM(t1.GROSS_PREMIUM)) FORMAT=COMMAX20.2 AS SUM_of_GROSS_PREMIUM,
            (SUM(t1.NET_PREMIUM)) FORMAT=COMMAX20.2 AS SUM_of_NET_PREMIUM,
            (SUM(SUM(t1.ACQUISITION_FEE_BANK, t1.ADMIN_FEE_BANK))) AS BANK_REIMBURSEMENT,
            (COUNT(t1.CONTRACT_NR)) AS CONTRACT_COUNT
      FROM WORK.RAW_DATA_FLAGGED t1
      WHERE t1.EXCLUSION_CODE <= 1
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED MOVEMENT_TYPE);
QUIT;


%_eg_conditional_dropds(WORK.TOTALS_COMBINED);
PROC SQL;
CREATE TABLE WORK.TOTALS_COMBINED AS
SELECT * FROM WORK.TOTALS_ADDITIONS
 OUTER UNION CORR
SELECT * FROM WORK.TOTALS_EXITS
 OUTER UNION CORR
SELECT * FROM WORK.TOTALS_OVERALL
;
Quit;


%_eg_conditional_dropds(WORK.SUMMARY_FINAL);

PROC SQL;
   CREATE TABLE WORK.SUMMARY_FINAL AS
   SELECT t1.PROVIDER_ID LABEL="Provider" AS PROVIDER_ID,
          t1.MOVEMENT_TYPE LABEL="Type" AS MOVEMENT_TYPE,
          t1.CONTRACT_COUNT LABEL="Count" AS CONTRACT_COUNT,
          t1.SUM_of_INSURED_AMOUNT FORMAT=COMMAX20.2 LABEL="Insured Amount" AS INSURED_AMOUNT,
          t1.SUM_of_HEALTH_SUPPLEMENT FORMAT=COMMAX20.2 LABEL="Health Supplement" AS HEALTH_SUPPLEMENT,
          t1.SUM_of_GROSS_PREMIUM FORMAT=COMMAX20.2 LABEL="Gross Premium" AS GROSS_PREMIUM,
          t1.SUM_of_NET_PREMIUM FORMAT=COMMAX20.2 LABEL="Net Premium" AS NET_PREMIUM,
          t1.BANK_REIMBURSEMENT FORMAT=COMMAX20.2 LABEL="Bank Reimbursement" AS BANK_REIMBURSEMENT,
            (COALESCE(t1.SUM_of_NET_PREMIUM,0) - COALESCE(t1.BANK_REIMBURSEMENT,0)) FORMAT=COMMAX20.2
            LABEL="Net Contribution" AS NET_CONTRIBUTION
      FROM WORK.TOTALS_COMBINED t1
      ORDER BY t1.PROVIDER_ID,
               t1.MOVEMENT_TYPE;
QUIT;


%_eg_conditional_dropds(WORK.EXCLUDED_RECORDS);

PROC SQL;
   CREATE TABLE WORK.EXCLUDED_RECORDS AS
   SELECT t1.EXCLUSION_REASON,
          t1.PROVIDER_ID,
          t1.CONTRACT_NR,
          t1.POLICYHOLDER_NAME,
          t1.DECEASED_FLAG,
          t1.PARTNER_NAME,
          t1.FIRST_NAME_ADD,
          t1.STREET_ADDRESS,
          t1.ZIP_CITY,
          t1.POLICY_NR,
          t1.INSURANCE_STATUS_DESC,
          t1.POLICY_START_DT,
          t1.INSURED_AMOUNT,
          t1.INTEREST_RATE_PCT,
          t1.HEALTH_SUPPLEMENT,
          t1.GROSS_PREMIUM,
          t1.NET_PREMIUM,
          t1.ACQUISITION_FEE,
          t1.ACQUISITION_FEE_BANK,
          t1.ADMIN_FEE,
          t1.ADMIN_FEE_BANK,
          t1.COLLECTION_FEE,
          t1.INSURED_PERSON_1_NAME,
          t1.INSURED_PERSON_1_GENDER,
          t1.INSURED_PERSON_1_DOB,
          t1.INSURED_PERSON_2_NAME,
          t1.INSURED_PERSON_2_GENDER,
          t1.INSURED_PERSON_2_DOB,
          t1.PROCESSING_RUN_DT,
            (IFC(t1.INSURED_AMOUNT>0,"ADDITION",IFC(t1.INSURED_AMOUNT<0,"EXIT","---"))) AS DIRECTION
      FROM WORK.RAW_DATA_FLAGGED t1
      WHERE t1.EXCLUSION_CODE > 1
      ORDER BY t1.EXCLUSION_REASON,
               t1.CONTRACT_NR;
QUIT;


%_eg_conditional_dropds(WORK.TOTALS_EXCLUDED);

PROC SQL;
   CREATE TABLE WORK.TOTALS_EXCLUDED AS
   SELECT t1.PROVIDER_ID LABEL="Provider" AS PROVIDER_ID,
            (TRIM(t1.EXCLUSION_REASON) || " (" || TRIM(t1.DIRECTION) || ")") LABEL="Exclusion Reason" AS EXCLUSION_LABEL,
            (COUNT(t1.CONTRACT_NR)) LABEL="Count" AS CONTRACT_COUNT,
            (SUM(t1.INSURED_AMOUNT)) FORMAT=COMMAX20.2 LABEL="Insured Amount" AS SUM_of_INSURED_AMOUNT,
            (SUM(t1.HEALTH_SUPPLEMENT)) FORMAT=COMMAX20.2 LABEL="Health Supplement" AS SUM_of_HEALTH_SUPPLEMENT,
            (SUM(t1.GROSS_PREMIUM)) FORMAT=COMMAX20.2 LABEL="Gross Premium" AS SUM_of_GROSS_PREMIUM,
            (SUM(t1.NET_PREMIUM)) FORMAT=COMMAX20.2 LABEL="Net Premium" AS SUM_of_NET_PREMIUM,
            (SUM(SUM(t1.ACQUISITION_FEE_BANK, t1.ADMIN_FEE_BANK))) FORMAT=COMMAX20.2 LABEL="Bank Reimbursement " AS
            BANK_REIMBURSEMENT,
            (COALESCE((SUM(t1.NET_PREMIUM)),0) - COALESCE(SUM(SUM(t1.ACQUISITION_FEE_BANK, t1.ADMIN_FEE_BANK)),0)) FORMAT=COMMAX20.2
            LABEL="Net Contribution" AS NET_CONTRIBUTION
      FROM WORK.EXCLUDED_RECORDS t1
      GROUP BY t1.PROVIDER_ID,
               (CALCULATED EXCLUSION_LABEL);
QUIT;
