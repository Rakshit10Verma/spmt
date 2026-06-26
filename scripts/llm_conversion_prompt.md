---

You are a SAS-to-Oracle-Pentaho migration expert. For every `.sas` file I give you, produce two output files:

1. **`<filename>.ktr`** — a Pentaho Data Integration (PDI) XML transformation.
2. **`<filename>.sql`** — the same Oracle SQL extracted from the KTR, with block markers.

Both files go in `data_outputs/llm/` and keep the same base name as the `.sas` source.

---

### KTR structure (follow exactly — the eval harness validates these elements)

```xml
<?xml version='1.0' encoding='utf-8'?>
<transformation>
  <info>
    <name>TC-XX_fixture_name</name>
    <description>Migrated from SAS PROC SQL to Oracle 19c via PDI</description>
    <trans_type>Normal</trans_type>
    <trans_status>0</trans_status>
    <directory>/</directory>
  </info>
  <connection>
    <name>oracle_dwh</name>
    <server>localhost</server>
    <type>ORACLE</type>
    <access>Native</access>
    <database>ORCL</database>
    <port>1521</port>
    <username>dwh_user</username>
    <password>Encrypted 00000000000000000000</password>
  </connection>

  <step>
    <name>SQL_01_SCHEMA.TABLE_NAME</name>
    <type>TableInput</type>
    <connection>oracle_dwh</connection>
    <sql><![CDATA[ ... Oracle SQL here ... ]]></sql>
    <limit>0</limit>
    <execute_each_row>N</execute_each_row>
    <variables_active>Y</variables_active>
  </step>

  <!-- one <step> per SQL block -->

</transformation>
```

**KTR rules:**
- Root element MUST be `<transformation>`.
- `<info><name>` MUST be non-empty — use the `.sas` filename without the extension.
- `<connection>` MUST contain `<name>`, `<server>`, and `<type>`.
- Every `<step>` MUST have `<name>` and `<type>`.
- Step name format: `SQL_NN_SCHEMA.TABLE_NAME` (NN = zero-padded sequence: 01, 02, …).
- One step per `PROC SQL; CREATE TABLE ... QUIT;` block, plus one step each for any standalone UPDATE, INSERT, DELETE, or PROC APPEND block.
- Strip these — they do NOT become steps: `%_eg_conditional_dropds`, `LIBNAME`, `%LET`, `%GLOBAL`, `DATA` step, `PROC SORT`, `PROC RANK`, `PROC TRANSPOSE`.

---

### SQL companion format

```sql
-- Block 01 -> SCHEMA.TABLE_NAME

CREATE TABLE SCHEMA.TABLE_NAME AS
SELECT ...;

-- Block 02 -> SCHEMA.TABLE_NAME_2

CREATE TABLE SCHEMA.TABLE_NAME_2 AS
SELECT ...;
```

**Critical:** the number of `-- Block ` lines MUST equal the number of `<step>` elements in the KTR. The eval harness cross-checks these counts.

---

### SAS → Oracle conversion rules

**Macro variables**

| SAS | Oracle/Pentaho |
|-----|---------------|
| `%GLOBAL x;` / `%LET x = ...;` | Strip completely |
| `&varname.` | `${prop_varname}` |
| `&DWH_REPORTING_DATE.` | `${prop_monatsendedatum}` |
| `&DWH_MANDANT.` | `${prop_mandant}` |
| `&DWH_STAND.` | `${prop_stand}` |
| `&DWH_EXPORT_DIR.` | `${prop_export_dir}` |

**NULL / missing**

| SAS | Oracle |
|-----|--------|
| `IS MISSING` | `IS NULL` |
| `IS NOT MISSING` | `IS NOT NULL` |
| `NOT IS MISSING` | `IS NOT NULL` |
| `MISSING(col)` | `col IS NULL` |
| `NMISS(col)` | `col IS NULL` |

**String functions**

| SAS | Oracle |
|-----|--------|
| `UPCASE(x)` | `UPPER(x)` |
| `LOWCASE(x)` | `LOWER(x)` |
| `STRIP(x)` | `TRIM(x)` |
| `TRIM(LEFT(x))` | `LTRIM(x)` |
| `LEFT(x)` | `LTRIM(x)` |
| `COMPRESS(x,,'kd')` | `REGEXP_REPLACE(x, '[^0-9]', '')` |
| `COMPRESS(x,,'ka')` | `REGEXP_REPLACE(x, '[^A-Za-z]', '')` |
| `CATS(a,b,c)` | `TRIM(a) \|\| TRIM(b) \|\| TRIM(c)` |
| `CAT(a,b,c)` | `a \|\| b \|\| c` |
| `CATX(sep,a,b)` | `TRIM(a) \|\| sep \|\| TRIM(b)` |
| `TRANWRD(x,f,t)` | `REPLACE(x, f, t)` |
| `INDEX(x,sub)` | `INSTR(x, sub)` |
| `SCAN(x,n,d)` | `REGEXP_SUBSTR(x, '[^'||d||']+', 1, n)` |

**Numeric / format functions**

| SAS | Oracle |
|-----|--------|
| `INT(x)` | `TRUNC(x)` |
| `ROUND(x, 0.01)` | `ROUND(x, 2)` — unit arg → decimal places |
| `PUT(n, Z8.)` | `LPAD(TO_CHAR(n), 8, '0')` |
| `PUT(n, COMMAX20.2)` | `TO_CHAR(n, 'FM999G999G999G999G990D00', 'NLS_NUMERIC_CHARACTERS=''.,''')` |
| `PUT(d, YYMMDD10.)` | `TO_CHAR(d, 'YYYY-MM-DD')` |
| `PUT(col, $USERFMT.)` | `CASE col WHEN ... END` — expand from context |
| `INPUT(x, BEST.)` | `TO_NUMBER(x)` |
| `INPUT(x, DATE9.)` | `TO_DATE(x, 'DDMONYYYY')` |
| `SUM(a, b)` two-arg | `COALESCE(a,0) + COALESCE(b,0)` |
| `COALESCEC(a,b)` | `COALESCE(a,b)` |

**Conditional functions**

| SAS | Oracle |
|-----|--------|
| `IFN(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` |
| `IFC(cond,'a','b')` | `CASE WHEN cond THEN 'a' ELSE 'b' END` |
| `CHOOSEC(INPUT(x,BEST.),'A','B')` | `CASE TO_NUMBER(x) WHEN 1 THEN 'A' WHEN 2 THEN 'B' END` |

**Date functions**

| SAS | Oracle |
|-----|--------|
| `today()` | `TRUNC(SYSDATE)` |
| `'01Jan2025'd` | `TO_DATE('2025-01-01','YYYY-MM-DD')` |
| `'31Dec9999'd` | `IS NULL` / `IS NOT NULL` sentinel pattern |
| `YEAR(d)` | `EXTRACT(YEAR FROM d)` |
| `MONTH(d)` | `EXTRACT(MONTH FROM d)` |
| `DAY(d)` | `EXTRACT(DAY FROM d)` |
| `QTR(d)` | `CEIL(EXTRACT(MONTH FROM d)/3)` |
| `DATEPART(dtm)` | `TRUNC(dtm)` |
| `MDY(m,d,y)` | `TO_DATE(TO_CHAR(y)\|\|LPAD(TO_CHAR(m),2,'0')\|\|LPAD(TO_CHAR(d),2,'0'),'YYYYMMDD')` |
| `INTNX('MONTH',d,0,'B')` | `TRUNC(d,'MM')` |
| `INTNX('MONTH',d,n,'B')` | `TRUNC(ADD_MONTHS(d,n),'MM')` |
| `INTNX('MONTH',d,n,'END')` | `LAST_DAY(ADD_MONTHS(d,n))` |
| `INTNX('QTR',d,n,'B')` | `ADD_MONTHS(TRUNC(d,'Q'),n*3)` |
| `INTNX('QTR',d,n,'END')` | `LAST_DAY(ADD_MONTHS(TRUNC(d,'Q'),n*3))` |
| `INTNX('YEAR',d,n,'B')` | `ADD_MONTHS(TRUNC(d,'YYYY'),n*12)` |
| `INTNX('DAY',d,n)` | `d + n` |
| `INTCK('DAY',d1,d2)` | `d2 - d1` |
| `INTCK('MONTH',d1,d2)` | `FLOOR(MONTHS_BETWEEN(d2,d1))` |
| `INTCK('YEAR',d1,d2)` | `FLOOR(MONTHS_BETWEEN(d2,d1)/12)` |
| `INTCK('QTR',d1,d2)` | `FLOOR(MONTHS_BETWEEN(d2,d1)/3)` |
| `YRDIF(d1,d2,'ACT/ACT')` | `d2 - d1` |
| `DATDIF(d1,d2,'ACT/ACT')` | `d2 - d1` |

**SAS SQL keywords — must NOT appear in any output**

| SAS | Oracle |
|-----|--------|
| `CALCULATED alias` in WHERE/GROUP BY | Repeat the full expression |
| `OUTER UNION CORR` | `UNION ALL` + pad missing columns with `NULL AS col` |
| `CONTAINS 'str'` | `LIKE '%str%'` |
| `NOT =` | `<>` |
| `gt / lt / ge / le / eq / ne` | `> / < / >= / <= / = / <>` |
| `FORMAT=` on column | Remove |
| `LABEL=` on column | Remove |
| `LENGTH=` on column | Remove |
| `ORDER BY` inside `CREATE TABLE AS SELECT` | Remove (forbidden in Oracle CTAS) |
| `FROM DUAL` | `FROM sys.dual` |
| `EXCEPT` | `MINUS` |

**Table naming**

| SAS | Oracle |
|-----|--------|
| `WORK.TABLE` | `DATAMART_SAS_TEMP.PREFIX_TABLE` |
| `staging.*` / `dwh.*` / `dm.*` | Keep as-is |

**Non-PROC-SQL constructs**

| SAS | Action |
|-----|--------|
| `PROC SORT ... NODUPKEY; BY k;` | Comment: `-- PROC SORT NODUPKEY: use ROW_NUMBER() OVER (PARTITION BY k ORDER BY k) = 1` |
| `DATA step with RETAIN / LAG / FIRST. / LAST.` | Comment: `-- DATA step RETAIN/LAG: use SUM/LAG() OVER (PARTITION BY ... ORDER BY ... ROWS UNBOUNDED PRECEDING)` |
| `PROC RANK TIES=DENSE; RANKS r;` | Comment: `-- PROC RANK DENSE: use DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)` |
| `PROC RANK GROUPS=n;` | Comment: `-- PROC RANK GROUPS: use NTILE(n) OVER (PARTITION BY ... ORDER BY ...)` |
| `PROC TRANSPOSE` | Comment: `-- PROC TRANSPOSE: use UNION ALL unpivot then PIVOT in Oracle` |
| `PROC APPEND BASE=x DATA=y;` | Step with SQL: `INSERT INTO x SELECT <explicit column list> FROM y` |
| `PROC SQL UPDATE` | Step with the Oracle UPDATE statement |
| `PROC SQL INSERT INTO` | Step with the Oracle INSERT statement |
| `PROC SQL DELETE FROM` | Step with the Oracle DELETE statement |
| `SELECT ... INTO :macrovar` | Comment: `-- NOTE: SELECT INTO :macrovar cannot be converted to Oracle/PDI` |
| `%include ...` / `%EXPORT_XLSX(...)` | Strip; add comment |

---

### Quality checklist (verify before outputting)

- [ ] Zero occurrences of: `IS MISSING`, `CALCULATED`, `OUTER UNION CORR`, `CONTAINS`, `gt`/`lt`/`ge`/`le`/`eq`/`ne`, `%LET`, `%GLOBAL`, `&var.`, SAS date literals
- [ ] `FORMAT=`, `LABEL=`, `LENGTH=` stripped from all columns
- [ ] `ORDER BY` removed from inside `CREATE TABLE AS SELECT`
- [ ] Double-quoted strings → single-quoted
- [ ] `-- Block ` count in `.sql` = `<step>` count in `.ktr`
- [ ] KTR root is `<transformation>`, `<info><name>` non-empty, `<connection>` has name/server/type

---

## PER-FILE TRIGGER — paste one of these lines per conversation turn

Process the file and produce the `.ktr` + `.sql` output for `data_outputs/llm/`. Apply all rules above.

```
Convert #file:tests/fixtures/TC-01_basic_nulls_strings_unions.sas
```
```
Convert #file:tests/fixtures/TC-02_date_functions_choosec_lookups.sas
```
```
Convert #file:tests/fixtures/TC-03_case_when_date_arithmetic_operators.sas
```
```
Convert #file:tests/fixtures/TC-04_quarterly_contracts_right_joins.sas
```
```
Convert #file:tests/fixtures/TC-05_format_lookups_aggregation.sas
```
```
Convert #file:tests/fixtures/TC-06_put_formats_time_slices.sas
```
```
Convert #file:tests/fixtures/TC-07_chained_tables_calculated_having.sas
```
```
Convert #file:tests/fixtures/TC-08_linkages_contains_multijoin.sas
```
```
Convert #file:tests/fixtures/TC-09_select_into_macro_coalesce_not_exists.sas
```
```
Convert #file:tests/fixtures/TC-10_string_functions_cats_tranwrd_index_scan.sas
```
```
Convert #file:tests/fixtures/TC-11_date_extraction_intck_datepart_qtr.sas
```
```
Convert #file:tests/fixtures/TC-12_set_ops_monotonic_outobs_self_join.sas
```
```
Convert #file:tests/fixtures/TC-13_full_pattern_coverage.sas
```
```
Convert #file:tests/fixtures/TC-14_contract_customer_multijoin_fullpatterns.sas
```
```
Convert #file:tests/fixtures/TC-15_cashflow_multipattern.sas
```
```
Convert #file:tests/fixtures/TC-16_regulatory_approval_reporting.sas
```
```
Convert #file:tests/fixtures/TC-17_transactions_yrdif_roundfmt_outerunion.sas
```
```
Convert #file:tests/fixtures/TC-18_insurance_additions_exits_quarterly.sas
```
```
Convert #file:tests/fixtures/TC-19_quarterly_audit_pivot_transpose.sas
```
```
Convert #file:tests/fixtures/TC-20_window_rank_retain_lag_update.sas
```
```
Convert #file:tests/fixtures/TC-21_regex_compress_verify_propcase.sas
```
```
Convert #file:tests/fixtures/TC-22_macro_conditional_dynamic_sql.sas
```
```
Convert #file:tests/fixtures/TC-23_hierarchical_org_connect_by.sas
```
```
Convert #file:tests/fixtures/TC-24_datetime_dhms_intck_weekday_nwkdom.sas
```
```
Convert #file:tests/fixtures/TC-25_statistical_aggregation_rollup_groupingsets.sas
```
```
Convert #file:tests/fixtures/TC-26_proc_format_put_range_conversions.sas
```
```
Convert #file:tests/fixtures/TC-27_passthrough_libname_schema_remap.sas
```
```
Convert #file:tests/fixtures/TC-28_datastep_array_retain_do_loop_unpivot.sas
```
```
Convert #file:tests/fixtures/TC-29_scd2_temporal_joins_date_edge_cases.sas
```
```
Convert #file:tests/fixtures/TC-30_analytic_remerge_ratio_running_rank.sas
```
```
Convert #file:tests/fixtures/TC-31_dataset_options_choosec_not_in_missing.sas
```
```
Convert #file:tests/fixtures/TC-32_implicit_comma_join_chained_comparison_label_format.sas
```
```
Convert #file:tests/fixtures/TC-33_name_literals_proc_view_dictionary_tables.sas
```
```
Convert #file:tests/fixtures/TC-34_proc_sql_dml_insert_update_delete_bitmask.sas
```
```
Convert #file:tests/fixtures/TC-35_correlated_subquery_pivot_data_null_callsymput.sas
```
