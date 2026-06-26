/*============================================================================
 * FILE: TC-33_name_literals_proc_view_dictionary_tables.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * BASED ON:    LBS Auszahlungskontrolle and FAEH-Tool patterns
 *
 * CONVERSION PATTERNS PRESENT:
 *   - 'Column Name With Spaces'n SAS name literal → Oracle "Column Name With Spaces"
 *   - 'Laufzeit (Jahre)'n name literal with parentheses → Oracle double-quoted identifier
 *   - 'Anzahl Vertr.'n name literal with period → Oracle double-quoted identifier
 *   - 'Konto-Nr'n name literal with hyphen → Oracle double-quoted identifier
 *   - SELECT 'name'n in SELECT list → SELECT "name" in Oracle
 *   - WHERE 'name'n = value → WHERE "name" = value
 *   - GROUP BY 'name'n → GROUP BY "name"
 *   - PROC SQL; CREATE VIEW work.v AS SELECT T.col FROM work.table AS T → Oracle CREATE VIEW
 *   - FROM SASHELP.VTABLE WHERE libname='WORK' AND memname='TABLE' → Oracle USER_TABLES
 *   - FROM DICTIONARY.COLUMNS WHERE libname='WORK' AND memname='TABLE' → ALL_TAB_COLUMNS
 *   - SELECT INTO :macro TRIMMED from SASHELP → extract metadata into macro variable
 *   - PROC SQL NOPRINT for metadata queries → remove option
 *   - DATEPART(crdate) from SASHELP.VTABLE → CREATED column in USER_OBJECTS
 *   - PUT(col, DDMMYYP10.) in macro context → TO_CHAR(col, 'DD.MM.YYYY')
 *   - &macro_var. → ${prop_varname}
 *   - CALCULATED in GROUP BY → repeat expression
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - SAS name literals use single quotes with trailing 'n': 'col name'n — this is the
 *     ONLY way to reference column names with spaces, hyphens, parentheses, or reserved
 *     words in SAS; Oracle uses double-quoted identifiers: "col name"; LLMs often either
 *     drop the spaces (wrong column name) or use single quotes (string literal, not identifier)
 *   - A name literal in GROUP BY must also become a double-quoted identifier in Oracle,
 *     not a string — LLMs sometimes emit GROUP BY 'col name' (a string constant) instead
 *     of GROUP BY "col name" (a column reference)
 *   - PROC SQL; CREATE VIEW work.v AS SELECT T.col FROM work.table AS T — the SAS work
 *     VIEW is stored in-memory and used by downstream PROC TRANSPOSE or report steps;
 *     in Oracle, CREATE VIEW requires schema + view name; Oracle WORK tables become
 *     DATAMART_SAS_TEMP.PREFIX_ but views also need a schema prefix
 *   - FROM SASHELP.VTABLE is the SAS catalog table listing all datasets; there is no
 *     direct Oracle equivalent — USER_TABLES / ALL_TABLES lists tables but NOT datasets
 *     in the SAS WORK library; LLMs often emit FROM SYS.ALL_TABLES which is different
 *   - FROM DICTIONARY.COLUMNS has no Oracle equivalent accessible in standard SQL;
 *     the closest is SELECT * FROM ALL_TAB_COLUMNS but the column names differ
 *   - SELECT INTO :macro TRIMMED runs a query and stores the scalar result in a SAS macro
 *     variable; this has no Oracle SQL equivalent — it is a SAS engine feature that must
 *     be removed or replaced by bind variable logic in the target ETL tool
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET table_lib = WORK;
%LET table_name = DISBURSEMENT_BASE;

PROC SQL NOPRINT;
    SELECT COUNT(*)
    INTO :table_row_count TRIMMED
    FROM SASHELP.VTABLE
    WHERE libname = "&table_lib."
      AND memname = "&table_name.";
QUIT;

PROC SQL NOPRINT;
    SELECT DATEPART(crdate)
    INTO :table_created_date TRIMMED
    FROM SASHELP.VTABLE
    WHERE libname = "&table_lib."
      AND memname = "&table_name.";
QUIT;

PROC SQL NOPRINT;
    SELECT name
    INTO :col_list SEPARATED BY ','
    FROM DICTIONARY.COLUMNS
    WHERE libname = "&table_lib."
      AND memname = "&table_name."
      AND type = 'num';
QUIT;

PROC SQL;
    CREATE TABLE work.disbursement_base AS
    SELECT
        t1.contract_id,
        t1.partner_id,
        t1.disbursement_amount AS 'Auszahlungsbetrag (EUR)'n,
        t1.processing_fee     AS 'Bearbeitungsgebühr'n,
        t1.net_disbursement   AS 'Netto-Auszahlung'n,
        t1.loan_term_months / 12.0 AS 'Laufzeit (Jahre)'n,
        t1.contract_number    AS 'Konto-Nr'n,
        t1.contract_count_per_partner AS 'Anzahl Vertr.'n,
        t1.disbursement_date,
        t1.error_code,
        t1.error_description
    FROM source_data.disbursement_transactions t1
    WHERE t1.client_code = "&client_code."
      AND t1.disbursement_date le "&report_date."d
      AND 'Auszahlungsbetrag (EUR)'n IS NOT MISSING;
QUIT;

PROC SQL;
    CREATE VIEW work.v_disbursement_errors AS
    SELECT T.'Konto-Nr'n,
           T.partner_id,
           T.'Auszahlungsbetrag (EUR)'n,
           T.'Laufzeit (Jahre)'n,
           T.error_code,
           T.error_description,
           T.disbursement_date
    FROM work.disbursement_base AS T
    WHERE T.error_code IS NOT MISSING
      AND T.error_code NOT IN (0, 999);
QUIT;

PROC SQL;
    CREATE TABLE work.error_summary_by_code AS
    SELECT
        t1.error_code,
        t1.error_description,
        COUNT(*)                                       AS 'Anzahl Fälle'n,
        SUM(t1.'Auszahlungsbetrag (EUR)'n)
            FORMAT=COMMAX20.2                          AS 'Gesamtbetrag'n,
        AVG(t1.'Laufzeit (Jahre)'n) FORMAT=8.2         AS 'Ø Laufzeit (Jahre)'n,
        MIN(t1.disbursement_date)                      AS 'Erstes Datum'n,
        MAX(t1.disbursement_date)                      AS 'Letztes Datum'n
    FROM work.v_disbursement_errors t1
    WHERE t1.'Konto-Nr'n IS NOT MISSING
    GROUP BY t1.error_code,
             t1.error_description
    HAVING COUNT(*) >= 3
    ORDER BY CALCULATED 'Gesamtbetrag'n DESC;
QUIT;

PROC SQL;
    CREATE TABLE work.partner_disbursement_flags AS
    SELECT
        t1.partner_id,
        COUNT(DISTINCT t1.'Konto-Nr'n)              AS 'Anzahl Vertr.'n,
        SUM(t1.'Auszahlungsbetrag (EUR)'n)
            FORMAT=COMMAX20.2                        AS 'Gesamtbetrag'n,
        SUM(t1.'Bearbeitungsgebühr'n)
            FORMAT=COMMAX15.2                        AS 'Gesamtgebühren'n,
        SUM(CASE WHEN t1.error_code IS NOT MISSING
                 AND t1.error_code NOT IN (0, 999)
                 THEN 1 ELSE 0 END)                  AS 'Fehleranzahl'n,
        SUM(CASE WHEN t1.error_code IS NOT MISSING
                 AND t1.error_code NOT IN (0, 999)
                 THEN t1.'Auszahlungsbetrag (EUR)'n
                 ELSE 0 END)
            FORMAT=COMMAX20.2                        AS 'Fehlerbetrag'n,
        'Fehlerbetrag'n / NULLIF('Gesamtbetrag'n, 0)
            FORMAT=PERCENTN20.2                      AS 'Fehlerquote'n
    FROM work.disbursement_base t1
    GROUP BY t1.partner_id
    HAVING CALCULATED 'Anzahl Vertr.'n >= 1
    ORDER BY CALCULATED 'Fehlerbetrag'n DESC;
QUIT;
