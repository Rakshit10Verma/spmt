/*******************************************************************************
 * FILE: TC-08_linkages_contains_multijoin.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - SAS date literals ('01JAN2025'd) → TO_DATE()
 *   - Named columns with special characters ('Column Name'n) → Standard identifier
 *   - CONTAINS operator → LIKE '%...%'
 *   - NOT = operator → <>
 *   - CASE WHEN with FORMAT=$30. specification → CASE WHEN (format removed)
 *   - FORMAT= on column output → Removed in Oracle
 *   - FORMAT=EURDFDD10. on date columns → Removed or TO_CHAR
 *   - WORK. tables → Schema.PREFIX_ tables
 *   - Multiple LEFT JOIN / INNER JOIN combinations (5 joins in one query)
 *   - Subqueries in FROM clause for row filtering
 *   - MAX() subquery for temporal validity
 *   - Table split pattern (single source → UNION of active/inactive)
 *   - PK_STAND format handling (YYYYMM for monthly tables)
 *   - Validity period filters (valid_from <= date AND valid_to > date)
 *   - Macro variable references (&var.) → Pentaho ${var}
 *   - ORDER BY clause preservation
 *   - LIBNAME META statements → ignored in conversion
 *   - %include external macro file → ignored
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. 'Linkage Type'n contains space — must rename to LINKAGE_TYPE everywhere
 *   2. CONTAINS is SAS-specific, no direct Oracle equivalent → LIKE '%X%'
 *   3. FORMAT=$30. and FORMAT=EURDFDD10. must be stripped
 *   4. Single VC_ACCOUNTS table splits into ACCOUNT_ACTIVE + ACCOUNT_INACTIVE
 *   5. Source accounts may be closed → need UNION with inactive
 *   6. PK_STAND uses 6-digit YYYYMM derived from 8-digit date parameter
 *   7. CASE WHEN with 14 branches for linkage type mapping
 *   8. Final query has 5 JOINs including both LEFT and INNER
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

%GLOBAL reporting_date;
%LET reporting_date = &DWH_REPORTING_DATE.;

%_eg_conditional_dropds(WORK.CONTRACT_LINKS_ALL);

PROC SQL;
   CREATE TABLE WORK.CONTRACT_LINKS_ALL AS 
   SELECT t1.SOURCE_CONTRACT_ID, 
          t1.TARGET_CONTRACT_ID, 
          t1.LINK_TYPE_CODE, 
            (CASE 
               WHEN '01' = t1.LINK_TYPE_CODE THEN 'Partial Contract Creation'
               WHEN '02' = t1.LINK_TYPE_CODE THEN 'Contract Split'
               WHEN '03' = t1.LINK_TYPE_CODE THEN 'Disentanglement'
               WHEN '04' = t1.LINK_TYPE_CODE THEN 'Transfer Without Binding'
               WHEN '05' = t1.LINK_TYPE_CODE THEN 'Savings Consolidation'
               WHEN '06' = t1.LINK_TYPE_CODE THEN 'Loan Transfer'
               WHEN '07' = t1.LINK_TYPE_CODE THEN 'Loan Consolidation'
               WHEN '08' = t1.LINK_TYPE_CODE THEN 'Reinstatement'
               WHEN '09' = t1.LINK_TYPE_CODE THEN 'Increase'
               WHEN '10' = t1.LINK_TYPE_CODE THEN 'Transfer With Binding'
               WHEN '11' = t1.LINK_TYPE_CODE THEN 'Death Transfer'
               WHEN '12' = t1.LINK_TYPE_CODE THEN 'Mixed Chain'
               WHEN '13' = t1.LINK_TYPE_CODE THEN 'Reduction'
               WHEN '14' = t1.LINK_TYPE_CODE THEN 'Agent Compensation Follow-up'
               ELSE t1.LINK_TYPE_CODE
            END) FORMAT=$30. AS 'Linkage Type'n, 
          t1.LINKAGE_DATE, 
          t1.RIGHTS_START_DATE
      FROM SOURCE.CONTRACT_LINKAGE t1
      WHERE t1.LINK_TYPE_CODE NOT = '09';
QUIT;

%_eg_conditional_dropds(WORK.CONTRACT_SPLITS);

PROC SQL;
   CREATE TABLE WORK.CONTRACT_SPLITS AS 
   SELECT t1.SOURCE_CONTRACT_ID, 
          t1.TARGET_CONTRACT_ID, 
          t1.LINK_TYPE_CODE, 
          t1.'Linkage Type'n, 
          t1.LINKAGE_DATE, 
          t1.RIGHTS_START_DATE
      FROM WORK.CONTRACT_LINKS_ALL t1
      WHERE t1.'Linkage Type'n IN 
           (
           'Partial Contract Creation',
           'Contract Split'
           ) AND t1.LINKAGE_DATE BETWEEN '1Jan2025'd AND '31Dec2025'd
      ORDER BY t1.LINKAGE_DATE;
QUIT;

%_eg_conditional_dropds(WORK.REGIONAL_OFFICES);

PROC SQL;
   CREATE TABLE WORK.REGIONAL_OFFICES AS 
   SELECT t1.OFFICE_KEY, 
          t1.OFFICE_FUSION_KEY, 
          t1.AGENT_NUMBER, 
          t1.DISTRICT_NUMBER, 
          t1.OFFICE_NUMBER, 
          t1.FUSION_COUNTER, 
          t1.FUSION_DATE, 
          t1.OFFICE_NAME, 
          t1.OFFICE_SHORT_NAME, 
          t1.COOPERATION_FLAG, 
          t1.VALID_FROM_DATE, 
          t1.VALID_TO_DATE
      FROM STAGING.REGIONAL_OFFICE t1
      WHERE t1.OFFICE_NAME CONTAINS 'Mountain';
QUIT;

%_eg_conditional_dropds(WORK.SPLITS_WITH_DETAILS);

PROC SQL;
   CREATE TABLE WORK.SPLITS_WITH_DETAILS AS 
   SELECT t1.SOURCE_CONTRACT_ID, 
          Source_Acct.ACCOUNT_TYPE_CODE AS ACCOUNT_TYPE_SOURCE, 
          Source_Cust.CUSTOMER_OFFICE_ID AS CUSTOMER_ID_SOURCE, 
          Source_Acct.ORIGINATING_OFFICE_NR AS ORIG_OFFICE_NR1, 
          Source_Acct.ORIGINATING_OFFICE_BRANCH, 
          Source_Acct.CONTRACT_AMOUNT, 
          Source_Acct.TARIFF_DESCRIPTION, 
          Source_Acct.START_DATE, 
          Source_Acct.ACCOUNT_CLOSE_DATE, 
          t1.TARGET_CONTRACT_ID, 
          Target_Acct.ACCOUNT_TYPE_CODE AS ACCOUNT_TYPE_TARGET, 
          Target_Cust.CUSTOMER_OFFICE_ID AS CUSTOMER_ID_TARGET, 
          Target_Acct.CONTRACT_AMOUNT AS CONTRACT_AMOUNT1, 
          Target_Acct.TARIFF_DESCRIPTION AS TARIFF_DESCRIPTION1, 
          Target_Acct.ORIGINATING_OFFICE_NR, 
          Target_Acct.ORIGINATING_OFFICE_BRANCH AS ORIG_OFFICE_BRANCH1, 
          Target_Acct.START_DATE AS START_DATE1, 
          Target_Acct.ACCOUNT_CLOSE_DATE AS ACCOUNT_CLOSE_DATE1, 
          t1.LINK_TYPE_CODE, 
          t1.'Linkage Type'n, 
          t1.LINKAGE_DATE FORMAT=EURDFDD10. AS LINKAGE_DATE, 
          t1.RIGHTS_START_DATE FORMAT=EURDFDD10. AS RIGHTS_START_DATE
      FROM WORK.CONTRACT_SPLITS t1
           LEFT JOIN DWH.VC_ACCOUNTS Source_Acct ON (t1.SOURCE_CONTRACT_ID = Source_Acct.CONTRACT_NUMBER)
           LEFT JOIN DWH.VC_ACCOUNTS Target_Acct ON (t1.TARGET_CONTRACT_ID = Target_Acct.CONTRACT_NUMBER)
           INNER JOIN WORK.REGIONAL_OFFICES t2 ON (Source_Acct.ORIGINATING_OFFICE_NR = t2.OFFICE_NUMBER)
           INNER JOIN DWH.VC_CUSTOMERS Source_Cust ON (Source_Acct.PARTNER_NUMBER = Source_Cust.PARTNER_NUMBER)
           INNER JOIN DWH.VC_CUSTOMERS Target_Cust ON (Target_Acct.PARTNER_NUMBER = Target_Cust.PARTNER_NUMBER)
      ORDER BY t1.LINKAGE_DATE,
               t1.SOURCE_CONTRACT_ID;
QUIT;

%GLOBAL gReportDate;
%LET gReportDate = &DWH_REPORTING_DATE.;
%include "\\server\scripts\macros\standard_export.sas";
%EXPORT_TO_DIRECTORY;
