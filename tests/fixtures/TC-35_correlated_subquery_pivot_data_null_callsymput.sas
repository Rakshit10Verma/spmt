/*============================================================================
 * FILE: TC-35_correlated_subquery_pivot_data_null_callsymput.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * BASED ON:    LBS Cashflows_V01.03 and Auszahlungskontrolle patterns
 *
 * CONVERSION PATTERNS PRESENT:
 *   - 8+ correlated scalar subqueries in one SELECT (manual pivot via subquery)
 *     → Oracle PIVOT clause or conditional SUM(CASE WHEN group='X' THEN val END)
 *   - DATA _NULL_; SET table(OBS=1); CALL SYMPUT('var', PUT(col, fmt)); RUN;
 *     → extract scalar value from table into Pentaho parameter; no SQL equivalent
 *   - PUT(date, YYMMD7.) → TO_CHAR(date, 'YYYYMM')  [7-char YYYYMM format]
 *   - PUT(date, DDMMYYP10.) → TO_CHAR(date, 'DD.MM.YYYY')  [German dot-separator]
 *   - PROC SQL NOPRINT; SELECT COUNT(*) INTO :macro TRIMMED → scalar fetch; remove
 *   - INSERT INTO ... SET col=val for single-row metadata sentinel → VALUES() in Oracle
 *   - CATS(YEAR(date), '-Q', IFC(MONTH(date)<=3,'1', IFC(...))) → quarter label
 *   - SUM(col1 + col2 + col3) → SUM(NVL(col1,0) + NVL(col2,0) + NVL(col3,0))
 *   - COALESCE(scalar_subquery, 0) → Oracle compatible
 *   - ROUND(x / 1000000, 0.001) → ROUND(x / 1000000, 3)  [SAS uses decimal unit; Oracle uses precision]
 *   - IFN(SUM(col) > 0, 1, 0) → CASE WHEN SUM(col) > 0 THEN 1 ELSE 0 END
 *   - CALCULATED alias in GROUP BY → repeat expression
 *   - &macro_var. → ${prop_varname}
 *   - FORMAT=COMMAX20.3 LABEL="..." on column → stripped in Oracle
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - 8 correlated scalar subqueries per SELECT row is an O(n×8) plan; Oracle PIVOT
 *     or conditional aggregation is O(n); semantics are identical but the correlated
 *     form is what SAS PROC SQL generates; LLMs should prefer PIVOT but both are valid
 *   - DATA _NULL_ with CALL SYMPUT is a SAS-only mechanism to extract a scalar value
 *     from a dataset into a macro variable; it has no Oracle SQL equivalent; the value
 *     it extracts (e.g. a reporting date) becomes a Pentaho parameter in the KTR; LLMs
 *     often try to emit a SELECT INTO :var which is PL/SQL, not standard Oracle SQL
 *   - ROUND(x, 0.001) — SAS ROUND takes the rounding unit (0.001 = round to 3 decimal
 *     places); Oracle ROUND takes the decimal precision (ROUND(x, 3)); LLMs occasionally
 *     emit ROUND(x, 0.001) verbatim, which in Oracle rounds to the nearest 0.001 ≠ 3 d.p.
 *   - CATS(YEAR(date), '-Q', IFC(MONTH<=3,'1',IFC(MONTH<=6,'2',IFC(MONTH<=9,'3','4'))))
 *     — building a "YYYY-Q1" string; Oracle: TO_CHAR(date,'YYYY') || '-Q' || TO_CHAR(date,'Q')
 *     is far simpler; LLMs rarely find this and instead translate the nested IFC literally
 *   - INSERT INTO ... SET syntax (SAS extension) must become INSERT INTO ... VALUES
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;

DATA _NULL_;
    SET source_data.reporting_params(OBS=1);
    CALL SYMPUT('gStichtag',     PUT(DWH_MW_BUCHUNG_DTM,    8.));
    CALL SYMPUT('gStichtagFmt',  PUT(DWH_MW_BUCHUNG_DTM,    DDMMYYP10.));
    CALL SYMPUT('gPeriode',      PUT(DWH_MW_BUCHUNG_DTM,    YYMMD7.));
    CALL SYMPUT('gQuartal',
        CATS(YEAR(DWH_MW_BUCHUNG_DTM), '-Q',
             IFC(MONTH(DWH_MW_BUCHUNG_DTM) le 3, '1',
             IFC(MONTH(DWH_MW_BUCHUNG_DTM) le 6, '2',
             IFC(MONTH(DWH_MW_BUCHUNG_DTM) le 9, '3', '4')))));
RUN;

PROC SQL NOPRINT;
    SELECT COUNT(*)
    INTO :gCashflowRows TRIMMED
    FROM source_data.cashflow_transactions
    WHERE client_code = "&client_code."
      AND period_key  = &gStichtag.;
QUIT;

PROC SQL;
    CREATE TABLE work.cashflow_by_group AS
    SELECT
        t1.period_key,
        t1.org_unit_id,
        ROUND(COALESCE(
            (SELECT SUM(t2.cashflow_amount) / 1000000
             FROM source_data.cashflow_transactions t2
             WHERE t2.cashflow_group = 'G01'
               AND t2.period_key     = t1.period_key
               AND t2.org_unit_id    = t1.org_unit_id), 0), 0.001)
            FORMAT=COMMAX20.3 LABEL="Gruppe 01 (Mio.)" AS g01_mio,
        ROUND(COALESCE(
            (SELECT SUM(t2.cashflow_amount) / 1000000
             FROM source_data.cashflow_transactions t2
             WHERE t2.cashflow_group = 'G02'
               AND t2.period_key     = t1.period_key
               AND t2.org_unit_id    = t1.org_unit_id), 0), 0.001)
            FORMAT=COMMAX20.3 LABEL="Gruppe 02 (Mio.)" AS g02_mio,
        ROUND(COALESCE(
            (SELECT SUM(t2.cashflow_amount) / 1000000
             FROM source_data.cashflow_transactions t2
             WHERE t2.cashflow_group = 'G03'
               AND t2.period_key     = t1.period_key
               AND t2.org_unit_id    = t1.org_unit_id), 0), 0.001)
            FORMAT=COMMAX20.3 LABEL="Gruppe 03 (Mio.)" AS g03_mio,
        ROUND(COALESCE(
            (SELECT SUM(t2.cashflow_amount) / 1000000
             FROM source_data.cashflow_transactions t2
             WHERE t2.cashflow_group = 'G04'
               AND t2.period_key     = t1.period_key
               AND t2.org_unit_id    = t1.org_unit_id), 0), 0.001)
            FORMAT=COMMAX20.3 LABEL="Gruppe 04 (Mio.)" AS g04_mio,
        ROUND(COALESCE(
            (SELECT SUM(t2.cashflow_amount) / 1000000
             FROM source_data.cashflow_transactions t2
             WHERE t2.cashflow_group = 'G05'
               AND t2.period_key     = t1.period_key
               AND t2.org_unit_id    = t1.org_unit_id), 0), 0.001)
            FORMAT=COMMAX20.3 LABEL="Gruppe 05 (Mio.)" AS g05_mio,
        ROUND(COALESCE(
            (SELECT SUM(t2.cashflow_amount) / 1000000
             FROM source_data.cashflow_transactions t2
             WHERE t2.cashflow_group = 'G06'
               AND t2.period_key     = t1.period_key
               AND t2.org_unit_id    = t1.org_unit_id), 0), 0.001)
            FORMAT=COMMAX20.3 LABEL="Gruppe 06 (Mio.)" AS g06_mio,
        ROUND(COALESCE(
            (SELECT SUM(t2.cashflow_amount) / 1000000
             FROM source_data.cashflow_transactions t2
             WHERE t2.cashflow_group = 'G07'
               AND t2.period_key     = t1.period_key
               AND t2.org_unit_id    = t1.org_unit_id), 0), 0.001)
            FORMAT=COMMAX20.3 LABEL="Gruppe 07 (Mio.)" AS g07_mio,
        ROUND(COALESCE(
            (SELECT SUM(t2.cashflow_amount) / 1000000
             FROM source_data.cashflow_transactions t2
             WHERE t2.cashflow_group = 'G08'
               AND t2.period_key     = t1.period_key
               AND t2.org_unit_id    = t1.org_unit_id), 0), 0.001)
            FORMAT=COMMAX20.3 LABEL="Gruppe 08 (Mio.)" AS g08_mio,
        IFN(SUM(t1.cashflow_amount) > 0, 1, 0)           AS has_positive_flow,
        CATS(YEAR(t1.period_key), '-Q',
             IFC(MONTH(t1.period_key) le 3, '1',
             IFC(MONTH(t1.period_key) le 6, '2',
             IFC(MONTH(t1.period_key) le 9, '3', '4'))))  AS quarter_label
    FROM source_data.cashflow_transactions t1
    WHERE t1.client_code = "&client_code."
      AND t1.period_key  = &gStichtag.
    GROUP BY t1.period_key,
             t1.org_unit_id,
             CALCULATED quarter_label;
QUIT;

PROC SQL;
    CREATE TABLE work.cashflow_totals AS
    SELECT
        period_key,
        quarter_label,
        SUM(g01_mio + g02_mio + g03_mio + g04_mio) FORMAT=COMMAX20.3 AS inflow_total_mio,
        SUM(g05_mio + g06_mio + g07_mio + g08_mio) FORMAT=COMMAX20.3 AS outflow_total_mio,
        SUM(g01_mio + g02_mio + g03_mio + g04_mio)
            - SUM(g05_mio + g06_mio + g07_mio + g08_mio)
            FORMAT=COMMAX20.3                                           AS net_cashflow_mio,
        COUNT(DISTINCT org_unit_id)                                    AS org_units_active,
        SUM(has_positive_flow)                                         AS units_with_inflow
    FROM work.cashflow_by_group
    GROUP BY period_key,
             quarter_label
    ORDER BY period_key;
QUIT;
