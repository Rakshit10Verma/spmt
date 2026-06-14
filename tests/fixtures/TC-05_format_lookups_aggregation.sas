/* ============================================================
   FILE: test_04_format_lookups_and_aggregation.sas
   
   CONVERSION PATTERNS PRESENT:
   - PUT() with custom numeric format → CASE WHEN inline lookup
   - PUT() with custom character format ($) → CASE WHEN inline lookup
   - PUT() with standard format → JOIN to lookup table
   - SAS name literal ('column name'n) → renamed column identifier
   - LABEL= attribute on column → removed (display-only in SAS)
   - FORMAT= attribute on column → removed (display-only in SAS)
   - today() → TRUNC(SYSDATE)
   - &macro_variable. → ${prop_variable} Pentaho substitution
   - %LET / %GLOBAL macro declarations → Pentaho parameters
   - CALCULATED keyword in HAVING → direct aggregate function
   - ORDER BY inside CREATE TABLE AS SELECT → removed (performance)
   - Dynamic period-based table suffix → parameterized source table
   - T_SVZ-style inline VALUES subquery replacing unavailable lookup table
   - Pre-materialized lookup table replacing repeated subquery scan
   - RIGHT JOIN behaviour with NULL semantics
   
   COMPLEXITY: Medium
============================================================ */


%GLOBAL gReferenceDate;      %LET gReferenceDate = %SYSFUNC(INTNX(QTR,"&SYSDATE."d,-1,END));
%GLOBAL gReferenceDateStr;   %LET gReferenceDateStr = %SYSFUNC(PUTN(&gReferenceDate., DDMMYYP10.));
%GLOBAL gPeriode;            %LET gPeriode = %SYSFUNC(PUTN(&gReferenceDate., YYMMDD10.));
%GLOBAL gPeriodeTable;       %LET gPeriodeTable = %SYSFUNC(YEAR(&gReferenceDate.))_%SYSFUNC(PUTN(%SYSFUNC(MONTH(&gReferenceDate.)), Z2.));


PROC SQL;
    CREATE VIEW WORK.CUSTOMERS_PERIOD AS
        SELECT * FROM source.CUSTOMERS_&gPeriodeTable.;
QUIT;


%_eg_conditional_dropds(WORK.CUSTOMERS_BASE);

PROC SQL;
    CREATE TABLE WORK.CUSTOMERS_BASE AS
    SELECT DISTINCT
        t1.customer_id,
        t1.customer_type_code,
        (PUT(t1.customer_type_code, $CUSTTYPE.)) AS customer_type_label,
        t1.occupation_code,
        (PUT(t1.occupation_code, OCCUPGRP.)) AS occupation_label,
        t1.nationality_code,
        (CASE t1.nationality_code
            WHEN 1 THEN 'Domestic'
            ELSE 'Foreign'
        END) AS 'Nationality Category'n,
        t1.region_code LABEL='' AS region_code,
        t1.score FORMAT=COMMAX20. AS score,
        t1.status_flag
    FROM WORK.CUSTOMERS_PERIOD t1
    WHERE t1.status_flag = 'A'
    ORDER BY t1.customer_id;
QUIT;


%_eg_conditional_dropds(WORK.CUSTOMERS_WITH_LABELS);

PROC SQL;
    CREATE TABLE WORK.CUSTOMERS_WITH_LABELS AS
    SELECT
        t1.occupation_code,
        t1.occupation_label,
        t1.customer_id,
        t1.customer_type_code,
        t1.nationality_code,
        (PUT(t1.nationality_code, NATIONALITY.)) AS nationality_label,
        t1.region_code,
        t1.score,
        t2.CATEGORY_DESC AS customer_category_desc,
        t1.'Nationality Category'n,
        t1.status_flag
    FROM WORK.CUSTOMERS_BASE t1
        INNER JOIN source.CATEGORY_LOOKUP t2
            ON (t1.customer_type_code = t2.CATEGORY_CODE
            AND (t2.LOOKUP_GROUP = 'CT'));
QUIT;


%_eg_conditional_dropds(WORK.DUPLICATE_CHECK);

PROC SQL;
    CREATE TABLE WORK.DUPLICATE_CHECK AS
    SELECT
        t1.nationality_code,
        (COUNT(t1.nationality_code)) AS count_of_nationality
    FROM WORK.CUSTOMERS_WITH_LABELS t1
    GROUP BY t1.nationality_code
    HAVING (CALCULATED count_of_nationality) > 1
    ORDER BY count_of_nationality DESC;
QUIT;


%_eg_conditional_dropds(WORK.VALID_CATEGORIES);

PROC SQL;
    CREATE TABLE WORK.VALID_CATEGORIES AS
    SELECT
        t1.category_code,
        t1.category_label,
        t1.valid_from,
        t1.valid_to
    FROM source.CATEGORY_REFERENCE t1
    WHERE t1.valid_from <= today()
      AND t1.valid_to   >= today();
QUIT;


%_eg_conditional_dropds(WORK.CONTRACTS_WITH_RISK);

PROC SQL;
    CREATE TABLE WORK.CONTRACTS_WITH_RISK AS
    SELECT
        t3.contract_id,
        t3.customer_id,
        t3.customer_name,
        t3.nationality_code,
        t2.risk_country_label,
        t2.country_code,
        t2.risk_flag,
        t3.residential_country_code AS residential_country
    FROM WORK.RISK_COUNTRIES t2
        RIGHT JOIN WORK.CUSTOMER_CONTRACTS t3
            ON (t2.country_code = t3.residential_country_code);
QUIT;
