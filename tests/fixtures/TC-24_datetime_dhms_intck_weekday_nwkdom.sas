/*============================================================================
 * FILE: TC-24_datetime_dhms_intck_weekday_nwkdom.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - DHMS(date, h, m, s) → TO_DATE(TO_CHAR(date,'YYYY-MM-DD')||' '||h||':'||m||':'||s,
 *                            'YYYY-MM-DD HH24:MI:SS')  or CAST(date AS TIMESTAMP) tricks
 *   - HMS(h, m, s) → NUMTODSINTERVAL or interval literal — no clean Oracle equivalent
 *   - DATEPART(datetime_val) → TRUNC(datetime_val)
 *   - TIMEPART(datetime_val) → datetime_val - TRUNC(datetime_val)
 *   - INTCK('WEEKDAY', d1, d2) → complex Oracle business-day formula using TRUNC/MOD
 *   - INTCK('QTR', d1, d2) → FLOOR(MONTHS_BETWEEN(d2, d1) / 3)
 *   - INTCK('WEEK', d1, d2) → FLOOR((d2 - d1) / 7)
 *   - INTCK('YEAR', d1, d2) → FLOOR(MONTHS_BETWEEN(d2, d1) / 12)
 *   - INTNX('QTR', date, 0, 'B') → TRUNC(date, 'Q')
 *   - INTNX('QTR', date, 0, 'E') → ADD_MONTHS(TRUNC(date,'Q'), 3) - 1
 *   - INTNX('WEEK.2', date, 0, 'B') → Monday of the current week (specific start day)
 *   - WEEKDAY(date) → TO_NUMBER(TO_CHAR(date, 'D'))  (1=Sunday in both, be careful)
 *   - NWKDOM(n, dow, month, year) → nth weekday of a month; no Oracle native
 *   - DATETIME() → SYSDATE (current datetime)
 *   - TODAY() → TRUNC(SYSDATE)
 *   - TIME() → (SYSDATE - TRUNC(SYSDATE)) * 86400  (seconds since midnight)
 *   - SAS datetime format PUT(dt, DATETIME20.) → TO_CHAR(dt, 'DD-MON-YYYY HH24:MI:SS')
 *   - &macro_var. → ${prop_varname}
 *   - SAS date literals → TO_DATE()
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - INTCK('WEEKDAY',...) counts Mon-Fri only; there is no Oracle built-in for this;
 *     the formula is (d2-d1) - 2*FLOOR((d2-d1+WEEKDAY_OFFSET)/7) — LLMs almost always
 *     emit (d2 - d1) which includes weekends and is wrong
 *   - INTNX with the alignment 'B'/'E'/'M' ('Beginning','End','Middle') must map to
 *     different Oracle TRUNC formats or ADD_MONTHS expressions; 'M' (midpoint) has
 *     no Oracle native and needs (start + end) / 2
 *   - NWKDOM(2, 3, 3, 2025) = 2nd Wednesday of March 2025; the Oracle equivalent is
 *     a formula using NEXT_DAY() that LLMs consistently get wrong
 *   - DHMS builds a SAS datetime (seconds since 1/1/1960); converting to Oracle DATE
 *     requires reconstructing from the date and time components separately
 *   - WEEKDAY() returns 1=Sunday in SAS; TO_CHAR(date,'D') also returns 1=Sunday in
 *     Oracle default NLS but depends on NLS_TERRITORY — a subtle trap
 *============================================================================*/

%LET report_date = 20250531;
%LET report_datetime = %SYSFUNC(DATETIME(), DATETIME20.);
%LET client_code = NORTH;
%LET sla_days = 5;
%LET quarter_start = %SYSFUNC(INTNX(QTR, %SYSFUNC(TODAY()), 0, B), DATE9.);

PROC SQL;
   CREATE TABLE work.transaction_timing AS
   SELECT
       t1.transaction_id,
       t1.transaction_datetime,
       DATEPART(t1.transaction_datetime) AS transaction_date,
       TIMEPART(t1.transaction_datetime) AS transaction_time_seconds,
       t1.booking_datetime,
       DATEPART(t1.booking_datetime) AS booking_date,
       DHMS(DATEPART(t1.booking_datetime), 0, 0, 0) AS booking_day_start_dt,
       DHMS(DATEPART(t1.booking_datetime), 23, 59, 59) AS booking_day_end_dt,
       INTCK('WEEKDAY', DATEPART(t1.transaction_datetime),
             DATEPART(t1.booking_datetime)) AS business_days_to_book,
       INTCK('WEEK', DATEPART(t1.transaction_datetime),
             DATEPART(t1.booking_datetime)) AS calendar_weeks_to_book,
       WEEKDAY(DATEPART(t1.transaction_datetime)) AS transaction_weekday_num,
       CASE WEEKDAY(DATEPART(t1.transaction_datetime))
           WHEN 1 THEN 'Sunday'
           WHEN 2 THEN 'Monday'
           WHEN 3 THEN 'Tuesday'
           WHEN 4 THEN 'Wednesday'
           WHEN 5 THEN 'Thursday'
           WHEN 6 THEN 'Friday'
           WHEN 7 THEN 'Saturday'
       END AS transaction_weekday_name,
       t1.amount,
       t1.client_code
   FROM source_data.transaction_log t1
   WHERE t1.client_code = "&client_code."
     AND DATEPART(t1.transaction_datetime) ge "&quarter_start."d
     AND DATEPART(t1.transaction_datetime) le "&report_date."d;
QUIT;

PROC SQL;
   CREATE TABLE work.transaction_sla_assessment AS
   SELECT
       t1.transaction_id,
       t1.transaction_date,
       t1.booking_date,
       t1.business_days_to_book,
       t1.transaction_weekday_name,
       t1.amount,
       CASE
           WHEN t1.business_days_to_book le &sla_days. THEN 'WITHIN_SLA'
           WHEN t1.business_days_to_book le %EVAL(&sla_days. + 2) THEN 'MINOR_BREACH'
           ELSE 'MAJOR_BREACH'
       END AS sla_status,
       INTNX('QTR', t1.transaction_date, 0, 'B') AS quarter_start_date,
       INTNX('QTR', t1.transaction_date, 0, 'E') AS quarter_end_date,
       INTCK('QTR', INTNX('QTR', t1.transaction_date, 0, 'B'),
             t1.transaction_date) AS days_into_quarter,
       PUT(t1.transaction_datetime, DATETIME20.) AS transaction_datetime_str
   FROM work.transaction_timing t1;
QUIT;

PROC SQL;
   CREATE TABLE work.monthly_settlement_schedule AS
   SELECT
       t1.contract_id,
       t1.settlement_period,
       t1.settlement_due_date,
       NWKDOM(1, 2, MONTH(t1.settlement_due_date), YEAR(t1.settlement_due_date))
           AS first_monday_of_month,
       NWKDOM(3, 4, MONTH(t1.settlement_due_date), YEAR(t1.settlement_due_date))
           AS third_wednesday_of_month,
       INTNX('WEEK.2', t1.settlement_due_date, 0, 'B') AS week_start_monday,
       INTNX('WEEK.2', t1.settlement_due_date, 0, 'E') AS week_end_sunday,
       INTCK('WEEKDAY', TODAY(), t1.settlement_due_date) AS business_days_until_due,
       INTCK('YEAR', '01JAN2020'd, t1.settlement_due_date) AS years_since_epoch,
       CASE
           WHEN INTCK('WEEKDAY', TODAY(), t1.settlement_due_date) < 0
               THEN 'OVERDUE'
           WHEN INTCK('WEEKDAY', TODAY(), t1.settlement_due_date) le 5
               THEN 'DUE_SOON'
           ELSE 'PENDING'
       END AS due_status,
       DHMS(t1.settlement_due_date, 17, 0, 0) AS settlement_cutoff_datetime
   FROM source_data.settlement_schedule t1
   WHERE t1.client_code = "&client_code."
     AND t1.settlement_due_date ge INTNX('QTR', "&report_date."d, -1, 'B')
     AND t1.settlement_due_date le "&report_date."d;
QUIT;

PROC SQL;
   CREATE TABLE work.sla_quarterly_summary AS
   SELECT
       INTNX('QTR', transaction_date, 0, 'B') AS quarter_start,
       INTCK('QTR', '01JAN2020'd, INTNX('QTR', transaction_date, 0, 'B'))
           AS quarter_sequence_number,
       sla_status,
       transaction_weekday_name,
       COUNT(*) AS transaction_count,
       SUM(amount) AS total_amount,
       SUM(CASE WHEN sla_status = 'WITHIN_SLA' THEN amount ELSE 0 END)
           AS within_sla_amount,
       SUM(business_days_to_book) / COUNT(*) AS avg_business_days_to_book
   FROM work.transaction_sla_assessment
   GROUP BY
       CALCULATED quarter_start,
       CALCULATED quarter_sequence_number,
       sla_status,
       transaction_weekday_name
   ORDER BY
       CALCULATED quarter_start,
       sla_status;
QUIT;
