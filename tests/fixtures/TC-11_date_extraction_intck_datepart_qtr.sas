/*******************************************************************************
 * FILE: TC-11_date_extraction_intck_datepart_qtr.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - YEAR(date)      → EXTRACT(YEAR FROM date)
 *   - MONTH(date)     → EXTRACT(MONTH FROM date)
 *   - DAY(date)       → EXTRACT(DAY FROM date)
 *   - QTR(date)       → TO_CHAR(date, 'Q') or CEIL(EXTRACT(MONTH FROM date)/3)
 *   - WEEKDAY(date)   → TO_CHAR(date, 'D')   (1=Sunday in SAS, 1=Sunday in Oracle NLS)
 *   - DATEPART(dtm)   → TRUNC(dtm)           (datetime → date component)
 *   - TIMEPART(dtm)   → MOD(dtm, 86400) or TO_CHAR(dtm,'HH24:MI:SS')
 *   - INTCK('DAY',  d1, d2) → (d2 - d1)
 *   - INTCK('MONTH',d1, d2) → MONTHS_BETWEEN(d2, d1) truncated to integer
 *   - INTCK('YEAR', d1, d2) → FLOOR(MONTHS_BETWEEN(d2, d1) / 12)
 *   - INTCK('WEEK', d1, d2) → FLOOR((d2 - d1) / 7)
 *   - INTCK('QTR',  d1, d2) → FLOOR(MONTHS_BETWEEN(d2, d1) / 3)
 *   - INTNX('YEAR', date, n)         → ADD_MONTHS(TRUNC(date,'YYYY'), n*12)
 *   - INTNX('QTR',  date, n, 'B')    → ADD_MONTHS(TRUNC(date,'Q'), n*3)
 *   - INTNX('WEEK', date, 0, 'B')    → TRUNC(date, 'IW')  (start of ISO week)
 *   - DATDIF(d1, d2, 'ACT/365')      → (d2 - d1) / 365
 *   - DHMS(date, h, m, s)            → date + (h/24 + m/1440 + s/86400)
 *   - TODAY()                        → TRUNC(SYSDATE)
 *   - DATE()                         → TRUNC(SYSDATE)  (synonym for TODAY())
 *   - DATETIME()                     → SYSDATE
 *   - YRDIF(d1, d2, 'AGE')           → FLOOR(MONTHS_BETWEEN(d2,d1)/12)
 *   - MDY(m, d, y) with expressions  → TO_DATE(y||LPAD(m,2,'0')||LPAD(d,2,'0'),'YYYYMMDD')
 *   - SAS comparison operators (ge, le, gt, lt, eq, ne) → >=, <=, >, <, =, <>
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. QTR() has no direct Oracle function — two valid expansions, tool must pick one.
 *   2. WEEKDAY() numbering: SAS 1=Sunday; Oracle TO_CHAR(date,'D') is NLS-dependent —
 *      flag as a potential discrepancy for reviewer.
 *   3. INTCK counts complete intervals (boundary-based), MONTHS_BETWEEN is fractional —
 *      must truncate/floor MONTHS_BETWEEN to match SAS INTCK semantics.
 *   4. DATDIF with day-count convention ACT/365 is not built into Oracle; formula needed.
 *   5. INTNX 'WEEK' with 'B' alignment → TRUNC(date,'IW') uses ISO week (Mon start).
 *      SAS WEEK starts Sunday — subtle mismatch, must document.
 *   6. DATE() and TODAY() are synonyms in SAS — both map to TRUNC(SYSDATE).
 *   7. DATETIME() returns a SAS datetime (seconds since 1960-01-01), not a SQL timestamp —
 *      Oracle SYSDATE is the closest equivalent but epoch is different; flag for QA.
 *   8. YRDIF with 'AGE' convention follows birthday logic (no partial year rounding).
 *   9. MDY with macro-variable expressions: mdy(&month., 1, &year.) must be
 *      resolved before passing to TO_DATE, or kept as Pentaho expression.
 ******************************************************************************/

%LET report_date    = 20250531;
%LET report_month   = 05;
%LET report_year    = 2025;
%LET start_of_year  = 20250101;
%LET client_code    = WEST;

PROC SQL;
   CREATE TABLE WORK.DATE_COMPONENTS AS
   SELECT t1.CUSTOMER_ID,
          t1.BIRTH_DATE,
          t1.ONBOARDING_DATE,
          t1.LAST_ACTIVITY_DATE,
          YEAR(t1.BIRTH_DATE)             AS BIRTH_YEAR,
          MONTH(t1.BIRTH_DATE)            AS BIRTH_MONTH,
          DAY(t1.BIRTH_DATE)              AS BIRTH_DAY,
          QTR(t1.ONBOARDING_DATE)         AS ONBOARD_QUARTER,
          WEEKDAY(t1.LAST_ACTIVITY_DATE)  AS LAST_ACTIVITY_WEEKDAY,
          YEAR(TODAY())                   AS CURRENT_YEAR,
          MONTH(DATE())                   AS CURRENT_MONTH,
          QTR(DATE())                     AS CURRENT_QUARTER
   FROM SOURCE.CUSTOMER_MASTER t1
   WHERE t1.CLIENT_CODE = "&client_code."
     AND t1.STATUS_FLAG eq 'A'
     AND t1.BIRTH_DATE IS NOT MISSING;
QUIT;

PROC SQL;
   CREATE TABLE WORK.CUSTOMER_AGE_TENURE AS
   SELECT t1.CUSTOMER_ID,
          t1.BIRTH_DATE,
          t1.ONBOARDING_DATE,
          YRDIF(t1.BIRTH_DATE, TODAY(), 'AGE')                         AS AGE_YEARS,
          INTCK('YEAR',  t1.BIRTH_DATE, DATE())                        AS AGE_COMPLETE_YEARS,
          INTCK('MONTH', t1.ONBOARDING_DATE, TODAY())                  AS TENURE_MONTHS,
          INTCK('DAY',   t1.ONBOARDING_DATE, TODAY())                  AS TENURE_DAYS,
          INTCK('WEEK',  t1.ONBOARDING_DATE, TODAY())                  AS TENURE_WEEKS,
          INTCK('QTR',   t1.ONBOARDING_DATE, DATE())                   AS TENURE_QUARTERS,
          DATDIF(t1.ONBOARDING_DATE, TODAY(), 'ACT/365')               AS TENURE_FRACTION_YR,
          INTCK('MONTH', mdy(&report_month., 1, &report_year.),
                         TODAY())                                       AS MONTHS_SINCE_REPORT
   FROM WORK.DATE_COMPONENTS t1
   WHERE INTCK('YEAR', t1.BIRTH_DATE, TODAY()) ge 18
     AND INTCK('YEAR', t1.BIRTH_DATE, TODAY()) le 100;
QUIT;

PROC SQL;
   CREATE TABLE WORK.PERIOD_BOUNDARIES AS
   SELECT t1.CUSTOMER_ID,
          t1.ONBOARDING_DATE,
          INTNX('MONTH', t1.ONBOARDING_DATE, 0, 'B')    AS MONTH_START,
          INTNX('MONTH', t1.ONBOARDING_DATE, 0, 'E')    AS MONTH_END,
          INTNX('QTR',   t1.ONBOARDING_DATE, 0, 'B')    AS QUARTER_START,
          INTNX('QTR',   t1.ONBOARDING_DATE, 0, 'E')    AS QUARTER_END,
          INTNX('YEAR',  t1.ONBOARDING_DATE, 0, 'B')    AS YEAR_START,
          INTNX('YEAR',  t1.ONBOARDING_DATE, 0, 'E')    AS YEAR_END,
          INTNX('WEEK',  t1.ONBOARDING_DATE, 0, 'B')    AS WEEK_START,
          INTNX('YEAR',  TODAY(), -1, 'B')               AS PREV_YEAR_START,
          INTNX('QTR',   TODAY(), -1, 'B')               AS PREV_QTR_START,
          INTNX('MONTH', TODAY(), -3)                    AS THREE_MONTHS_AGO
   FROM WORK.CUSTOMER_AGE_TENURE t1;
QUIT;

PROC SQL;
   CREATE TABLE WORK.DATETIME_CONVERSIONS AS
   SELECT t1.EVENT_ID,
          t1.EVENT_TIMESTAMP,
          DATEPART(t1.EVENT_TIMESTAMP)                   AS EVENT_DATE,
          TIMEPART(t1.EVENT_TIMESTAMP)                   AS EVENT_TIME_SECS,
          DHMS(DATEPART(t1.EVENT_TIMESTAMP), 0, 0, 0)   AS EVENT_DAY_START_DTM,
          DHMS(DATEPART(t1.EVENT_TIMESTAMP), 23, 59, 59) AS EVENT_DAY_END_DTM,
          (DATETIME() - t1.EVENT_TIMESTAMP) / 3600       AS HOURS_AGO,
          INTCK('DAY',
                DATEPART(t1.EVENT_TIMESTAMP),
                TODAY())                                  AS DAYS_SINCE_EVENT,
          PUT(DATEPART(t1.EVENT_TIMESTAMP), YYMMDD10.)   AS EVENT_DATE_ISO,
          PUT(DATEPART(t1.EVENT_TIMESTAMP), DDMMYY10.)   AS EVENT_DATE_EU
   FROM SOURCE.SYSTEM_EVENTS t1
   WHERE DATEPART(t1.EVENT_TIMESTAMP) ge mdy(&report_month., 1, &report_year.)
     AND DATEPART(t1.EVENT_TIMESTAMP) le "&report_date."d
     AND t1.EVENT_TYPE NOT = 'HEARTBEAT';
QUIT;

PROC SQL;
   CREATE TABLE WORK.CUSTOMER_TIMELINE_REPORT AS
   SELECT c.CUSTOMER_ID,
          c.BIRTH_DATE,
          c.ONBOARDING_DATE,
          a.AGE_YEARS,
          a.TENURE_MONTHS,
          a.TENURE_FRACTION_YR,
          p.MONTH_START,
          p.MONTH_END,
          p.QUARTER_START,
          p.PREV_YEAR_START,
          d.EVENT_DATE,
          d.HOURS_AGO,
          d.DAYS_SINCE_EVENT,
          d.EVENT_DATE_ISO,
          CASE
            WHEN a.AGE_YEARS lt 25                      THEN 'YOUNG_ADULT'
            WHEN a.AGE_YEARS ge 25 AND a.AGE_YEARS lt 40 THEN 'ADULT'
            WHEN a.AGE_YEARS ge 40 AND a.AGE_YEARS lt 60 THEN 'MATURE'
            ELSE                                              'SENIOR'
          END                                            AS AGE_BAND,
          CASE
            WHEN a.TENURE_MONTHS lt 12                  THEN 'NEW'
            WHEN a.TENURE_MONTHS ge 12
             AND a.TENURE_MONTHS lt 36                  THEN 'ESTABLISHED'
            ELSE                                              'LOYAL'
          END                                            AS TENURE_BAND
   FROM WORK.DATE_COMPONENTS     c
        INNER JOIN WORK.CUSTOMER_AGE_TENURE a ON a.CUSTOMER_ID = c.CUSTOMER_ID
        INNER JOIN WORK.PERIOD_BOUNDARIES   p ON p.CUSTOMER_ID = c.CUSTOMER_ID
        LEFT  JOIN WORK.DATETIME_CONVERSIONS d ON d.EVENT_ID   = c.CUSTOMER_ID
   WHERE a.AGE_YEARS ge 18
     AND c.ONBOARDING_DATE ge INTNX('YEAR', TODAY(), -5, 'B')
   ORDER BY a.AGE_YEARS DESC,
            a.TENURE_MONTHS DESC;
QUIT;
