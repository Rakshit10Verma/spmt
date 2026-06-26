/*============================================================================
 * FILE: TC-27_passthrough_libname_schema_remap.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - LIBNAME lib ORACLE SCHEMA=X → strip lib prefix; prepend schema when referencing tables
 *   - lib.table_name → schema.table_name (LIBNAME-resolved table reference)
 *   - LIBNAME lib CLEAR → no Oracle equivalent; remove
 *   - PROC SQL CONNECT TO oracle AS alias (...) → infrastructure; strip from output SQL
 *   - EXECUTE (...) BY alias → DDL passthrough; preserve as standalone statement or drop
 *   - SELECT * FROM CONNECTION TO alias (inner_oracle_sql) → extract inner SQL as-is
 *   - inner_oracle_sql is ALREADY Oracle syntax — must NOT be re-converted
 *   - DISCONNECT FROM alias → no Oracle equivalent; remove
 *   - WORK. prefix on tables inside library queries → DATAMART_SAS_TEMP.PREFIX_ mapping
 *   - Multiple LIBNAME references to different schemas in same query
 *   - TGL time-slice filter (valid_from <= date AND valid_to > date)
 *   - IS MISSING / IS NOT MISSING → IS NULL / IS NOT NULL
 *   - &macro_var. → ${prop_varname}
 *   - SAS comparison operators (le, ge) → <=, >=
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - The SQL inside CONNECTION TO con (...) is ALREADY valid Oracle SQL — it contains
 *     TO_CHAR(), TO_DATE(), ROW_NUMBER() OVER(), NVL(), NULLIF() etc.; an LLM must extract
 *     it verbatim; re-applying SAS conversion rules to it BREAKS valid Oracle code
 *   - LIBNAME resolution: dwh.contract_header is NOT a schema.table reference yet — 'dwh'
 *     is a SAS library alias that maps to SCHEMA=DATAMART; the output must be
 *     DATAMART.contract_header, not dwh.contract_header
 *   - Three different LIBNAME aliases (dwh, stg, ref) each map to a different Oracle schema;
 *     LLMs often either drop all prefixes (wrong) or keep the SAS alias (also wrong)
 *   - LIBNAME CLEAR generates no SQL; LLMs sometimes emit a DROP or REVOKE statement
 *   - EXECUTE (...) BY con runs DDL against the remote database; it is infrastructure and
 *     should not appear in the converted query SQL; LLMs sometimes try to convert it
 *   - CONNECTION=GLOBAL / CONNECTION=SHARED are LIBNAME options with no SQL equivalent;
 *     READBUFF= is a performance hint — both must be silently dropped
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET dsn = PROD_ORACLE;
%LET sas_user = SAS_ETL;
%LET schema_main = DATAMART;
%LET schema_stage = STAGE_DATA;
%LET schema_ref = REFERENCE;

LIBNAME dwh ORACLE USER="&sas_user." PASSWORD="{SAS002}xyz789ABC"
    PATH="&dsn." SCHEMA="&schema_main." CONNECTION=GLOBAL READBUFF=5000;
LIBNAME stg ORACLE USER="&sas_user." PASSWORD="{SAS002}xyz789ABC"
    PATH="&dsn." SCHEMA="&schema_stage." CONNECTION=GLOBAL;
LIBNAME ref ORACLE USER="&sas_user." PASSWORD="{SAS002}xyz789ABC"
    PATH="&dsn." SCHEMA="&schema_ref." CONNECTION=SHARED;

PROC SQL;
    CREATE TABLE work.customer_dim_current AS
    SELECT
        c.partner_id,
        c.customer_segment,
        c.regulatory_classification,
        c.pep_flag,
        c.aml_risk_level,
        c.domicile_country,
        r.country_name,
        r.fatf_risk_category,
        r.sanctions_list_flag,
        r.tax_treaty_flag
    FROM dwh.customer_master c
    LEFT JOIN ref.country_risk_reference r
        ON c.domicile_country = r.country_code
       AND r.valid_from le "&report_date."d
       AND r.valid_to gt "&report_date."d
    WHERE c.client_code = "&client_code."
      AND c.valid_from le "&report_date."d
      AND c.valid_to gt "&report_date."d;
QUIT;

PROC SQL;
    CREATE TABLE work.product_dim_current AS
    SELECT
        p.product_code,
        p.product_group,
        p.product_subtype,
        p.regulatory_asset_class,
        p.risk_weight_pct,
        p.margin_floor,
        s.segment_name,
        s.segment_priority
    FROM dwh.product_catalog p
    LEFT JOIN ref.product_segment_map s
        ON p.product_group = s.product_group
       AND s.effective_from le "&report_date."d
       AND s.effective_to gt "&report_date."d
    WHERE p.valid_from le "&report_date."d
      AND p.valid_to gt "&report_date."d;
QUIT;

PROC SQL;
    CONNECT TO oracle AS con (USER="&sas_user." PASSWORD="{SAS002}xyz789ABC"
        PATH="&dsn." PRESERVE_COMMENTS=NO);

    EXECUTE (ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDD') BY con;
    EXECUTE (ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,') BY con;

    CREATE TABLE work.contract_enriched_raw AS
    SELECT * FROM CONNECTION TO con (
        SELECT
            h.contract_id,
            h.partner_id,
            h.product_code,
            h.org_unit_id,
            h.approved_amount,
            h.booking_date,
            h.interest_rate,
            h.contract_status,
            TO_CHAR(h.booking_date, 'YYYYMM')             AS period_key_str,
            TO_CHAR(h.booking_date, 'YYYY')                AS booking_year,
            TO_CHAR(h.booking_date, 'Q')                   AS booking_quarter,
            h.approved_amount * p.risk_weight_pct / 100    AS risk_weighted_amount,
            ROW_NUMBER() OVER (
                PARTITION BY h.partner_id
                ORDER BY h.booking_date DESC, h.contract_id
            )                                              AS partner_contract_rank,
            SUM(h.approved_amount) OVER (
                PARTITION BY h.org_unit_id, TO_CHAR(h.booking_date,'YYYYMM')
            )                                              AS org_period_total,
            h.approved_amount / NULLIF(
                SUM(h.approved_amount) OVER (
                    PARTITION BY h.product_code, TO_CHAR(h.booking_date,'YYYYMM')
                ), 0
            )                                              AS product_share_ratio
        FROM DATAMART.contract_header h
        INNER JOIN REFERENCE.product_catalog p
            ON h.product_code = p.product_code
           AND p.valid_from <= TO_DATE('20250531','YYYYMMDD')
           AND p.valid_to    >  TO_DATE('20250531','YYYYMMDD')
        WHERE h.client_code     = 'NORTH'
          AND h.booking_date   <= TO_DATE('20250531','YYYYMMDD')
          AND h.contract_status NOT IN ('CANCELLED', 'REJECTED')
    );

    CREATE TABLE work.feed_audit_latest AS
    SELECT * FROM CONNECTION TO con (
        SELECT
            f.feed_id,
            f.source_system,
            f.record_count,
            f.error_count,
            f.load_timestamp,
            f.status,
            NVL(f.error_count, 0)
                / NULLIF(f.record_count, 0)        AS error_rate,
            RANK() OVER (
                PARTITION BY f.source_system
                ORDER BY f.load_timestamp DESC
            )                                      AS load_rank
        FROM STAGE_DATA.feed_audit_log f
        WHERE TRUNC(f.load_timestamp) = TO_DATE('20250531','YYYYMMDD')
          AND f.status IN ('COMPLETED', 'PARTIAL')
    );

    DISCONNECT FROM con;
QUIT;

PROC SQL;
    CREATE TABLE work.portfolio_enriched_final AS
    SELECT
        ce.contract_id,
        ce.partner_id,
        ce.product_code,
        ce.org_unit_id,
        ce.approved_amount,
        ce.risk_weighted_amount,
        ce.product_share_ratio,
        ce.partner_contract_rank,
        cd.customer_segment,
        cd.aml_risk_level,
        cd.country_name,
        cd.fatf_risk_category,
        pd.product_group,
        pd.regulatory_asset_class,
        pd.risk_weight_pct,
        fa.error_rate AS feed_error_rate
    FROM work.contract_enriched_raw ce
    LEFT JOIN work.customer_dim_current cd
        ON ce.partner_id = cd.partner_id
    LEFT JOIN work.product_dim_current pd
        ON ce.product_code = pd.product_code
    LEFT JOIN work.feed_audit_latest fa
        ON fa.load_rank = 1
       AND fa.source_system = 'CONTRACT_HEADER'
    WHERE ce.partner_contract_rank = 1;
QUIT;

LIBNAME dwh CLEAR;
LIBNAME stg CLEAR;
LIBNAME ref CLEAR;
