/*============================================================================
 * FILE: TC-23_hierarchical_org_connect_by.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - Multi-level self-join (3+ levels) → Oracle CONNECT BY ... START WITH
 *   - Self-join to resolve parent/child/grandchild → LEVEL pseudocolumn in Oracle
 *   - SYS_CONNECT_BY_PATH() → no SAS equivalent; add to KTR output only
 *   - CONNECT_BY_ROOT → derive root node from hierarchy walk
 *   - CONNECT_BY_ISLEAF → identify leaf nodes in Oracle hierarchy
 *   - Recursive hierarchy flattening via chained LEFT JOINs → WITH RECURSIVE CTE
 *   - NOCYCLE clause → protect against circular references in Oracle hierarchy
 *   - SAS OUTER JOIN used to catch root nodes (no parent) → START WITH parent IS NULL
 *   - Aggregate AFTER hierarchy expansion (SUM per level group)
 *   - COALESCE chain for null-safe path building
 *   - PROC SQL REMERGE pattern: aggregate alongside non-aggregate in same SELECT
 *   - &macro_var. → ${prop_varname}
 *   - SAS word operators (le, ge) → Oracle (<=, >=)
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - SAS PROC SQL has no CONNECT BY; it expresses hierarchies through chained self-joins
 *     which are logically equivalent but structurally very different in Oracle
 *   - The number of self-join levels limits the hierarchy depth; CONNECT BY is unlimited
 *   - CONNECT_BY_ROOT and SYS_CONNECT_BY_PATH are Oracle-only constructs with no SAS
 *     equivalent — LLMs must infer them from the pattern of the self-join chain
 *   - NOCYCLE is needed when the source data may contain circular org references;
 *     LLMs typically omit it because SAS self-joins silently stop
 *   - PROC SQL REMERGE: SAS allows SUM(approved_amount) alongside non-aggregated cols
 *     in the same SELECT without GROUP BY; Oracle raises ORA-00937 — must add GROUP BY
 *     or move the aggregate to a subquery
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET max_hierarchy_depth = 4;

PROC SQL;
   CREATE TABLE work.org_unit_flat AS
   SELECT
       t1.org_unit_id AS level1_id,
       t1.org_unit_name AS level1_name,
       t2.org_unit_id AS level2_id,
       t2.org_unit_name AS level2_name,
       t3.org_unit_id AS level3_id,
       t3.org_unit_name AS level3_name,
       t4.org_unit_id AS level4_id,
       t4.org_unit_name AS level4_name,
       COALESCE(t4.org_unit_id, t3.org_unit_id,
                t2.org_unit_id, t1.org_unit_id) AS leaf_org_unit_id,
       COALESCE(t4.org_unit_name, t3.org_unit_name,
                t2.org_unit_name, t1.org_unit_name) AS leaf_org_unit_name,
       CASE
           WHEN t4.org_unit_id IS NOT MISSING THEN 4
           WHEN t3.org_unit_id IS NOT MISSING THEN 3
           WHEN t2.org_unit_id IS NOT MISSING THEN 2
           ELSE 1
       END AS hierarchy_depth
   FROM source_data.org_unit_master t1
   LEFT JOIN source_data.org_unit_master t2
       ON t2.parent_org_unit_id = t1.org_unit_id
      AND t2.valid_from le "&report_date."d
      AND t2.valid_to gt "&report_date."d
   LEFT JOIN source_data.org_unit_master t3
       ON t3.parent_org_unit_id = t2.org_unit_id
      AND t3.valid_from le "&report_date."d
      AND t3.valid_to gt "&report_date."d
   LEFT JOIN source_data.org_unit_master t4
       ON t4.parent_org_unit_id = t3.org_unit_id
      AND t4.valid_from le "&report_date."d
      AND t4.valid_to gt "&report_date."d
   WHERE t1.parent_org_unit_id IS MISSING
     AND t1.client_code = "&client_code."
     AND t1.valid_from le "&report_date."d
     AND t1.valid_to gt "&report_date."d;
QUIT;

PROC SQL;
   CREATE TABLE work.org_contract_volume AS
   SELECT
       h.level1_id,
       h.level1_name,
       h.level2_id,
       h.level2_name,
       h.leaf_org_unit_id,
       h.leaf_org_unit_name,
       h.hierarchy_depth,
       COUNT(c.contract_id) AS contract_count,
       SUM(c.approved_amount) AS total_approved,
       SUM(CASE WHEN c.contract_status = 'ACTIVE' THEN c.approved_amount ELSE 0 END)
           AS active_approved,
       SUM(CASE WHEN c.risk_flag = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_count
   FROM work.org_unit_flat h
   LEFT JOIN source_data.contract_header c
       ON c.org_unit_id = h.leaf_org_unit_id
      AND c.client_code = "&client_code."
      AND c.period_key = %SYSFUNC(PUTN(%SYSFUNC(TODAY(), YYMMN6.), 8.))
   GROUP BY
       h.level1_id,
       h.level1_name,
       h.level2_id,
       h.level2_name,
       h.leaf_org_unit_id,
       h.leaf_org_unit_name,
       h.hierarchy_depth;
QUIT;

PROC SQL;
   CREATE TABLE work.org_rollup_by_level2 AS
   SELECT
       v.level1_id,
       v.level1_name,
       v.level2_id,
       v.level2_name,
       SUM(v.contract_count) AS rollup_contract_count,
       SUM(v.total_approved) AS rollup_total_approved,
       SUM(v.active_approved) AS rollup_active_approved,
       SUM(v.high_risk_count) AS rollup_high_risk,
       COUNT(DISTINCT v.leaf_org_unit_id) AS branch_count
   FROM work.org_contract_volume v
   WHERE v.level2_id IS NOT MISSING
   GROUP BY
       v.level1_id,
       v.level1_name,
       v.level2_id,
       v.level2_name;
QUIT;

PROC SQL;
   CREATE TABLE work.org_employee_chain AS
   SELECT
       e1.employee_id AS employee_id,
       e1.employee_name,
       e1.org_unit_id,
       e2.employee_id AS direct_manager_id,
       e2.employee_name AS direct_manager_name,
       e3.employee_id AS skip_manager_id,
       e3.employee_name AS skip_manager_name,
       e4.employee_id AS division_head_id,
       e4.employee_name AS division_head_name,
       CASE
           WHEN e4.employee_id IS NOT MISSING THEN
               CATS(e4.employee_name, ' > ',
                    e3.employee_name, ' > ',
                    e2.employee_name, ' > ',
                    e1.employee_name)
           WHEN e3.employee_id IS NOT MISSING THEN
               CATS(e3.employee_name, ' > ',
                    e2.employee_name, ' > ',
                    e1.employee_name)
           WHEN e2.employee_id IS NOT MISSING THEN
               CATS(e2.employee_name, ' > ', e1.employee_name)
           ELSE e1.employee_name
       END AS reporting_chain_path
   FROM source_data.employee_master e1
   LEFT JOIN source_data.employee_master e2
       ON e1.manager_employee_id = e2.employee_id
      AND e2.valid_from le "&report_date."d
      AND e2.valid_to gt "&report_date."d
   LEFT JOIN source_data.employee_master e3
       ON e2.manager_employee_id = e3.employee_id
      AND e3.valid_from le "&report_date."d
      AND e3.valid_to gt "&report_date."d
   LEFT JOIN source_data.employee_master e4
       ON e3.manager_employee_id = e4.employee_id
      AND e4.valid_from le "&report_date."d
      AND e4.valid_to gt "&report_date."d
   WHERE e1.client_code = "&client_code."
     AND e1.valid_from le "&report_date."d
     AND e1.valid_to gt "&report_date."d
     AND e1.employment_status NE 'TERMINATED';
QUIT;
