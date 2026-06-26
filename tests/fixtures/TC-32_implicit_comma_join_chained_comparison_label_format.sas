/*============================================================================
 * FILE: TC-32_implicit_comma_join_chained_comparison_label_format.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 * BASED ON:    LBS Risikoanalyse_Laender patterns
 *
 * CONVERSION PATTERNS PRESENT:
 *   - FROM t1, t2 implicit comma join (cross join — safe when t2 is 1-row aggregate)
 *   - FROM t1 LEFT JOIN t3 ON (...), t2 → mixed explicit + implicit comma JOIN syntax
 *   - WHEN 0 le score le 30 → Oracle: WHEN 0 <= score AND score <= 30  (SAS chains)
 *   - WHEN 30 lt score le 60 → Oracle: WHEN score > 30 AND score <= 60
 *   - col NOT IS MISSING → col IS NOT NULL  (SAS reversed-word-order null check)
 *   - FORMAT=$REGION. LABEL="Ländername" on column alias → stripped in Oracle
 *   - FORMAT=PERCENTN20.3 → TO_CHAR(val * 100, 'FM990.000') || '%'
 *   - FORMAT=COMMAX20.2 LABEL="Betrag" → stripped in Oracle
 *   - PUT(col, $REGION.) user-defined character format in SELECT → CASE WHEN lookup
 *   - COUNT(DISTINCT(col)) with redundant parentheses → COUNT(DISTINCT col)
 *   - FORMAT=10. LABEL="KundenNr" on numeric column → stripped in Oracle
 *   - SUM(col) / grand_total_col for share-of-portfolio via 1-row cross join
 *   - CALCULATED alias in GROUP BY / ORDER BY → repeat expression
 *   - &macro_var. → ${prop_varname}
 *   - SAS comparison operators (le, ge, lt, gt, ne) → <=, >=, <, >, <>
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - FROM t1 LEFT JOIN t3 ON (...), t2 — mixing implicit comma join with explicit JOIN
 *     in the same FROM clause; Oracle allows it but the comma has lower precedence than
 *     JOIN; the explicit JOIN binds tighter, so t1 LEFT JOIN t3 forms a unit first, then
 *     the comma cross-joins that result with t2; LLMs frequently mis-parse the join order
 *   - The comma join with a 1-row grand-total table is the SAS PROC SQL idiom for
 *     ratio-to-report WITHOUT triggering a REMERGE warning; in Oracle the equivalent is
 *     SUM() OVER () window function; keeping the correlated subquery form is also valid
 *   - SAS chained comparison: WHEN 0 le score le 30 — the chaining is NOT standard SQL;
 *     Oracle evaluates left-to-right and produces wrong results; must split with AND
 *   - NOT IS MISSING has reversed word order compared to IS NOT NULL/IS NOT MISSING;
 *     LLMs sometimes emit IS NOT MISSING (invalid Oracle) rather than IS NOT NULL
 *   - FORMAT=PERCENTN20.3 is a SAS format for percentages (value * 100 with 3 decimals);
 *     the value stored in the column is the raw decimal (e.g. 0.045 = 4.5%); Oracle
 *     TO_CHAR must multiply by 100; LLMs often apply TO_CHAR to the raw value
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET concentration_threshold = 0.05;

PROC SQL;
    CREATE TABLE work.portfolio_grand_total AS
    SELECT
        SUM(approved_amount)    AS portfolio_total,
        COUNT(*)                AS total_contracts,
        AVG(credit_score)       AS portfolio_avg_score,
        COUNT(DISTINCT partner_id) AS distinct_partners
    FROM source_data.contract_header
    WHERE client_code = "&client_code."
      AND period_key = &report_date.
      AND contract_status NOT IS MISSING;
QUIT;

PROC SQL;
    CREATE TABLE work.country_risk_detail AS
    SELECT
        t1.contract_id,
        t1.partner_id FORMAT=10.   LABEL="Partnernummer"    AS partner_id,
        t1.domicile_country,
        t2.country_name FORMAT=$25. LABEL="Ländername"      AS country_name,
        t2.fatf_category FORMAT=$10. LABEL="FATF-Kategorie" AS fatf_category,
        t2.risk_weight FORMAT=8.4   LABEL="Risikogewicht"   AS risk_weight,
        t1.approved_amount FORMAT=COMMAX20.2 LABEL="Bewilligungsbetrag"
            AS approved_amount,
        t1.credit_score FORMAT=8.2 LABEL="Kreditrating"
            AS credit_score,
        t1.approved_amount / t3.portfolio_total
            FORMAT=PERCENTN20.3 LABEL="Portfolioanteil"     AS share_of_portfolio,
        t1.credit_score / t3.portfolio_avg_score
            FORMAT=10.4 LABEL="Relativer Score"             AS relative_score,
        CASE
            WHEN 0   le t1.credit_score le 20 THEN 'VERY_HIGH_RISK'
            WHEN 20  lt t1.credit_score le 40 THEN 'HIGH_RISK'
            WHEN 40  lt t1.credit_score le 65 THEN 'MEDIUM_RISK'
            WHEN 65  lt t1.credit_score le 85 THEN 'LOW_RISK'
            ELSE 'MINIMAL_RISK'
        END LABEL="Risikostufe" AS risk_category
    FROM source_data.contract_header t1
         LEFT JOIN source_data.country_reference t2
             ON t1.domicile_country = t2.country_code
            AND t2.valid_from le "&report_date."d
            AND t2.valid_to gt "&report_date."d,
         work.portfolio_grand_total t3
    WHERE t1.client_code = "&client_code."
      AND t1.period_key  = &report_date.
      AND t1.contract_status NOT IS MISSING
      AND t1.domicile_country NOT IS MISSING;
QUIT;

PROC SQL;
    CREATE TABLE work.country_risk_aggregated AS
    SELECT
        t1.domicile_country,
        MIN(t2.country_name) FORMAT=$25.     AS country_name,
        MIN(t2.fatf_category) FORMAT=$10.    AS fatf_category,
        MIN(t2.risk_weight)  FORMAT=8.4      AS country_risk_weight,
        COUNT(DISTINCT(t1.partner_id))       AS distinct_partners,
        COUNT(DISTINCT(t1.contract_id))      AS contract_count,
        SUM(t1.approved_amount) FORMAT=COMMAX20.2 AS total_exposure,
        AVG(t1.credit_score)    FORMAT=8.2   AS avg_score,
        SUM(t1.approved_amount) / t3.portfolio_total
            FORMAT=PERCENTN20.3              AS country_portfolio_share,
        CASE
            WHEN SUM(t1.approved_amount) / t3.portfolio_total gt &concentration_threshold.
                THEN 'CONCENTRATION_ALERT'
            WHEN SUM(t1.approved_amount) / t3.portfolio_total gt 0.02
                THEN 'ELEVATED'
            ELSE 'NORMAL'
        END AS concentration_flag
    FROM source_data.contract_header t1
         LEFT JOIN source_data.country_reference t2
             ON t1.domicile_country = t2.country_code
            AND t2.valid_from le "&report_date."d
            AND t2.valid_to gt "&report_date."d,
         work.portfolio_grand_total t3
    WHERE t1.client_code = "&client_code."
      AND t1.period_key  = &report_date.
      AND t1.contract_status NOT IS MISSING
    GROUP BY t1.domicile_country,
             t3.portfolio_total
    HAVING COUNT(DISTINCT(t1.contract_id)) ge 3
    ORDER BY CALCULATED total_exposure DESC;
QUIT;

PROC SQL;
    CREATE TABLE work.country_score_bands AS
    SELECT
        t1.fatf_category,
        t1.risk_category,
        COUNT(*)                                   AS contract_count,
        SUM(t1.approved_amount)
            FORMAT=COMMAX20.2                      AS band_total_exposure,
        AVG(t1.credit_score) FORMAT=8.2            AS avg_score_in_band,
        SUM(t1.approved_amount) / t2.portfolio_total
            FORMAT=PERCENTN20.3                    AS band_portfolio_share,
        MIN(t1.credit_score) FORMAT=8.2            AS min_score,
        MAX(t1.credit_score) FORMAT=8.2            AS max_score
    FROM work.country_risk_detail t1,
         work.portfolio_grand_total t2
    WHERE t1.fatf_category NOT IS MISSING
      AND t1.risk_category NOT IS MISSING
    GROUP BY t1.fatf_category,
             t1.risk_category,
             t2.portfolio_total
    ORDER BY CALCULATED band_total_exposure DESC;
QUIT;
