/*============================================================================
 * FILE: TC-29_scd2_temporal_joins_date_edge_cases.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - TGL exclusive end date: valid_from <= date AND valid_to > date (NOT >=)
 *   - BETWEEN inclusive both ends: date BETWEEN effective_start AND effective_end
 *   - '31DEC9999'd sentinel for current-record marker → TO_DATE('99991231','YYYYMMDD')
 *   - CASE WHEN col = '31DEC9999'd → CASE WHEN col = TO_DATE('99991231','YYYYMMDD')
 *   - Multiple effective-dated dimension joins (3 temporal joins in one SELECT)
 *   - NOT EXISTS correlated subquery for SCD gap detection
 *   - PROC SQL EXCEPT → MINUS in Oracle
 *   - Self-join for SCD Type 2 attribute change detection (consecutive version pairs)
 *   - SAS DATE literal vs DATETIME literal: '01JAN2025'd vs '01JAN2025:00:00:00'dt
 *   - MDY(m, d, y) for constructing effective dates → TO_DATE(...)
 *   - INTNX used inside WHERE clause for temporal window
 *   - INTCK('MONTH', d1, d2) → MONTHS_BETWEEN truncated
 *   - COALESCE chain for multi-version attribute fallback
 *   - IS MISSING / IS NOT MISSING → IS NULL / IS NOT NULL
 *   - NE operator → <> in Oracle
 *   - PROC SQL DELETE FROM WHERE NOT EXISTS → Oracle DELETE same syntax (compatible)
 *   - &macro_var. → ${prop_varname}
 *   - SAS comparison operators (le, ge, gt, lt) → <=, >=, >, <
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - TGL uses EXCLUSIVE end date (valid_to > date); BETWEEN uses INCLUSIVE both ends;
 *     both patterns appear in the same query on different tables; LLMs normalise all joins
 *     to one pattern and produce incorrect boundary results on the other table
 *   - '31DEC9999'd is NOT a SAS missing value — it is a valid DATE value used as a sentinel
 *     meaning "still active"; LLMs confuse it with IS MISSING/IS NULL and emit the wrong check
 *   - The EXCEPT block in TC-29 compares TGL-filtered vs BETWEEN-filtered sets; changing
 *     either filter breaks the intended set difference; LLMs often drop the EXCEPT or
 *     emit MINUS with an incorrect ORDER BY position
 *   - SAS DATE literal '01JAN2025'd uses a SAS date value (days since 1960-01-01); the
 *     DATETIME literal '01JAN2025:00:00:00'dt is a different type (seconds since 1960-01-01);
 *     LLMs sometimes swap them or apply the same TO_DATE() call to both
 *   - NOT EXISTS with a three-condition correlated subquery: all three conditions must
 *     reference the outer table alias; missing any one changes the semantics entirely
 *   - PROC SQL EXCEPT followed by ORDER BY: in Oracle the ORDER BY must come after the
 *     last SELECT in the set operation and cannot appear in either branch individually
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET lookback_months = 12;
%LET sentinel_date = 31DEC9999;

PROC SQL;
    CREATE TABLE work.customer_product_snapshot AS
    SELECT
        c.partner_id,
        c.customer_segment,
        c.regulatory_classification,
        c.aml_risk_level,
        c.domicile_country,
        c.valid_from AS cust_valid_from,
        c.valid_to AS cust_valid_to,
        CASE WHEN c.valid_to = "&sentinel_date."d THEN 'CURRENT' ELSE 'HISTORICAL' END
            AS customer_record_type,
        p.product_group,
        p.product_subtype,
        p.regulatory_asset_class,
        p.risk_weight_pct,
        p.valid_from AS prod_valid_from,
        p.valid_to AS prod_valid_to,
        r.country_name,
        r.fatf_risk_category,
        r.sanctions_list_flag,
        COALESCE(c.aml_risk_level, r.fatf_risk_category, 'UNKNOWN') AS effective_risk_level,
        INTCK('MONTH', c.valid_from,
              CASE WHEN c.valid_to = "&sentinel_date."d
                   THEN "&report_date."d
                   ELSE c.valid_to END) AS version_active_months
    FROM source_data.customer_master c
    INNER JOIN source_data.product_catalog p
        ON c.product_code = p.product_code
       AND p.valid_from le "&report_date."d
       AND p.valid_to gt "&report_date."d
    LEFT JOIN source_data.country_risk_reference r
        ON c.domicile_country = r.country_code
       AND "&report_date."d BETWEEN r.effective_start AND r.effective_end
    WHERE c.client_code = "&client_code."
      AND c.valid_from le "&report_date."d
      AND c.valid_to gt "&report_date."d;
QUIT;

PROC SQL;
    CREATE TABLE work.customer_version_changes AS
    SELECT
        t1.partner_id,
        t1.customer_segment AS new_segment,
        t2.customer_segment AS old_segment,
        t1.aml_risk_level AS new_aml,
        t2.aml_risk_level AS old_aml,
        t1.valid_from AS change_effective_date,
        t2.valid_to AS prior_version_end,
        CASE
            WHEN t1.customer_segment NE t2.customer_segment
              OR t1.aml_risk_level NE t2.aml_risk_level
              OR t1.regulatory_classification NE t2.regulatory_classification
            THEN 'ATTRIBUTE_CHANGE'
            ELSE 'RENEWAL_ONLY'
        END AS change_type,
        INTCK('MONTH', t2.valid_from, t2.valid_to) AS prior_version_duration_months,
        MDY(MONTH(t1.valid_from), 1, YEAR(t1.valid_from)) AS change_month_start
    FROM source_data.customer_master t1
    INNER JOIN source_data.customer_master t2
        ON t1.partner_id = t2.partner_id
       AND t2.valid_to = t1.valid_from
       AND t2.client_code = "&client_code."
    WHERE t1.client_code = "&client_code."
      AND t1.valid_from BETWEEN INTNX('YEAR', "&report_date."d, -2, 'B')
                             AND "&report_date."d
      AND t2.valid_to NE "&sentinel_date."d;
QUIT;

PROC SQL;
    CREATE TABLE work.contracts_without_customer_tgl AS
    SELECT DISTINCT
        c.contract_id,
        c.partner_id,
        c.booking_date,
        c.contract_status,
        c.product_code
    FROM source_data.contract_header c
    WHERE c.client_code = "&client_code."
      AND c.period_key = &report_date.
      AND NOT EXISTS (
          SELECT 1
          FROM source_data.customer_master cm
          WHERE cm.partner_id  = c.partner_id
            AND cm.client_code = "&client_code."
            AND cm.valid_from le c.booking_date
            AND cm.valid_to gt c.booking_date
      )
    EXCEPT
    SELECT DISTINCT
        c2.contract_id,
        c2.partner_id,
        c2.booking_date,
        c2.contract_status,
        c2.product_code
    FROM source_data.contract_header c2
    INNER JOIN source_data.customer_master cm2
        ON c2.partner_id  = cm2.partner_id
       AND cm2.client_code = "&client_code."
       AND c2.booking_date BETWEEN cm2.valid_from AND cm2.valid_to
    WHERE c2.client_code = "&client_code."
      AND c2.period_key = &report_date.
    ORDER BY partner_id,
             booking_date;
QUIT;

PROC SQL;
    CREATE TABLE work.full_history_coverage AS
    SELECT
        c.partner_id,
        c.customer_segment,
        c.valid_from,
        CASE WHEN c.valid_to = "&sentinel_date."d
             THEN "&report_date."d
             ELSE c.valid_to END AS effective_end,
        MDY(MONTH(c.valid_from), 1, YEAR(c.valid_from)) AS version_period_start,
        CASE WHEN c.valid_to = "&sentinel_date."d
             THEN MDY(MONTH("&report_date."d), 1, YEAR("&report_date."d))
             ELSE MDY(MONTH(c.valid_to), 1, YEAR(c.valid_to))
        END AS version_period_end,
        p.product_group,
        p.product_subtype,
        COALESCE(p.pricing_tier, 'STANDARD') AS effective_pricing_tier,
        r.fatf_risk_category AS country_risk_at_version_start
    FROM source_data.customer_master c
    LEFT JOIN source_data.product_catalog p
        ON c.product_code = p.product_code
       AND p.valid_from le c.valid_from
       AND p.valid_to gt c.valid_from
    LEFT JOIN source_data.country_risk_reference r
        ON c.domicile_country = r.country_code
       AND c.valid_from BETWEEN r.effective_start AND r.effective_end
    WHERE c.client_code = "&client_code."
      AND c.valid_from ge INTNX('YEAR', "&report_date."d, -3, 'B')
    ORDER BY c.partner_id,
             c.valid_from;
QUIT;

PROC SQL;
    DELETE FROM work.contracts_without_customer_tgl
    WHERE NOT EXISTS (
        SELECT 1
        FROM source_data.contract_header ch
        WHERE ch.contract_id = work.contracts_without_customer_tgl.contract_id
          AND ch.contract_status NOT IN ('CANCELLED', 'REJECTED', 'VOID')
    );
QUIT;
