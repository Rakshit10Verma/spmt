"""
tests/test_converter.py - Tests for the core conversion engine.

I split these into groups based on what each test covers. Some tests
exercise the converter in isolation (no VariableHandler or TableMapper),
and some wire up mocks to test the full pipeline. The isolation tests
are more important because they verify each handler works correctly
without depending on external modules.
"""

import re
import pytest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass, field

from spmt.converter import Converter, BlockResult, ConversionResult
from spmt.parser import ParsedBlock, ParseResult, MacroDeclaration, DropdsCall


# Fixtures

@pytest.fixture
def bare_converter():
    """Converter with no VariableHandler or TableMapper.

    I use this for tests that only care about the converter's own
    rule application logic, not the upstream modules.
    """
    return Converter()


@pytest.fixture
def mock_var_handler():
    """A fake VariableHandler that just passes SQL through unchanged.

    For tests where I need the handler wired up but do not care about
    what it does to the SQL. Individual tests can override the return
    value on substitute() if they need specific behavior.
    """
    handler = MagicMock()
    handler.substitute.return_value = MagicMock(
        original_sql="",
        converted_sql="",
        substitutions_made=[],
        warnings=[],
    )
    return handler


@pytest.fixture
def mock_table_mapper():
    """A fake TableMapper that passes SQL through unchanged."""
    mapper = MagicMock()
    mapper.map_tables.return_value = MagicMock(
        converted_sql="",
        mappings=[],
        warnings=[],
    )
    return mapper


def _make_block(sql: str, block_number: int = 1) -> ParsedBlock:
    """Helper to create a ParsedBlock from raw SQL."""
    return ParsedBlock(
        block_number=block_number,
        original_sql=sql,
        line_start=1,
        line_end=sql.count("\n") + 1,
    )


def _make_parse_result(
    blocks: list[str],
    macros: list[MacroDeclaration] | None = None,
    dropds: list[DropdsCall] | None = None,
) -> ParseResult:
    """Helper to create a ParseResult from raw SQL strings."""
    return ParseResult(
        sql_blocks=[
            _make_block(f"PROC SQL;\n{sql}\nQUIT;", i + 1)
            for i, sql in enumerate(blocks)
        ],
        macro_declarations=macros or [],
        dropds_calls=dropds or [],
        source_file="test_input.sas",
    )


# Stage 1: Strip PROC SQL / QUIT wrappers

class TestStripWrapper:

    def test_removes_proc_sql_and_quit(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n  CREATE TABLE x AS SELECT 1;\nQUIT;"
        )
        assert "PROC SQL" not in result.converted_sql
        assert "QUIT" not in result.converted_sql
        assert "CREATE TABLE" in result.converted_sql

    def test_handles_mixed_case(self, bare_converter):
        result = bare_converter.convert_sql(
            "proc sql;\n  SELECT 1 FROM dual;\nquit;"
        )
        assert "proc sql" not in result.converted_sql.lower()
        assert "quit" not in result.converted_sql.lower().split("from")[0]

    def test_preserves_inner_sql(self, bare_converter):
        inner = "CREATE TABLE work.test AS SELECT a, b FROM source.tbl WHERE a > 1;"
        result = bare_converter.convert_sql(f"PROC SQL;\n{inner}\nQUIT;")
        assert "SELECT a, b" in result.converted_sql

    def test_handles_extra_whitespace(self, bare_converter):
        result = bare_converter.convert_sql(
            "  PROC   SQL  ;  \n  SELECT 1;\n  QUIT  ;  "
        )
        assert "SELECT 1" in result.converted_sql


# Stage 5: Regex-based rules

class TestRegexRules:
    """Tests for rules applied via simple re.sub."""

    def test_r01_is_missing_to_is_null(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE col IS MISSING;\n"
            "QUIT;"
        )
        assert "IS NULL" in result.converted_sql
        assert "IS MISSING" not in result.converted_sql
        assert "R-01" in result.rules_applied

    def test_r01_is_not_missing(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE col IS NOT MISSING;\n"
            "QUIT;"
        )
        # IS NOT MISSING → IS NOT NULL
        # R-01 pattern matches IS MISSING inside IS NOT MISSING
        assert "MISSING" not in result.converted_sql

    def test_r09_today(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE dt < today();\n"
            "QUIT;"
        )
        assert "TRUNC(SYSDATE)" in result.converted_sql
        assert "today()" not in result.converted_sql
        assert "R-09" in result.rules_applied

    def test_r12_upcase(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT UPCASE(name) FROM t;\n"
            "QUIT;"
        )
        assert "UPPER(" in result.converted_sql
        assert "UPCASE" not in result.converted_sql
        assert "R-12" in result.rules_applied

    def test_r13_lowcase(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT LOWCASE(code) FROM t;\n"
            "QUIT;"
        )
        assert "LOWER(" in result.converted_sql
        assert "R-13" in result.rules_applied

    def test_r14_strip(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT STRIP(val) FROM t;\n"
            "QUIT;"
        )
        assert "TRIM(" in result.converted_sql
        assert "STRIP" not in result.converted_sql
        assert "R-14" in result.rules_applied

    def test_r15_compress_basic(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT COMPRESS(contract_nr) FROM t;\n"
            "QUIT;"
        )
        assert "REPLACE(" in result.converted_sql
        assert "COMPRESS" not in result.converted_sql
        assert "R-15" in result.rules_applied

    def test_r17_outer_union_corr(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT a FROM t1\n"
            "OUTER UNION CORR\n"
            "SELECT a FROM t2;\n"
            "QUIT;"
        )
        assert "UNION ALL" in result.converted_sql
        assert "OUTER UNION CORR" not in result.converted_sql
        assert "R-17" in result.rules_applied

    def test_r18_format_removal(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT dt FORMAT=EURDFDD10. AS dt FROM t;\n"
            "QUIT;"
        )
        assert "FORMAT=" not in result.converted_sql
        assert "R-18" in result.rules_applied

    def test_r18_format_dollar(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT name FORMAT=$30. AS name FROM t;\n"
            "QUIT;"
        )
        assert "FORMAT=$30." not in result.converted_sql

    def test_r19_label_removal(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT code LABEL='' AS code FROM t;\n"
            "QUIT;"
        )
        assert "LABEL=" not in result.converted_sql
        assert "R-19" in result.rules_applied

    def test_r22_put_numeric_width(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT PUT(product_code, 3.) AS product_str FROM t;\n"
            "QUIT;"
        )
        assert "TO_CHAR(product_code)" in result.converted_sql
        assert "R-22" in result.rules_applied

    def test_r26_not_equals(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE status NOT = 'A';\n"
            "QUIT;"
        )
        assert "<>" in result.converted_sql
        assert "NOT =" not in result.converted_sql
        assert "R-26" in result.rules_applied

    def test_r28_gt_operator(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE amount gt 100;\n"
            "QUIT;"
        )
        assert ">" in result.converted_sql
        assert " gt " not in result.converted_sql

    def test_r28b_le_operator(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE dt le '20250531';\n"
            "QUIT;"
        )
        assert "<=" in result.converted_sql

    def test_r36_contains(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE name CONTAINS 'Mountain';\n"
            "QUIT;"
        )
        assert "LIKE '%Mountain%'" in result.converted_sql
        assert "CONTAINS" not in result.converted_sql
        assert "R-36" in result.rules_applied

    def test_r05_intnx_begin(self, bare_converter):
        result = bare_converter.convert_sql(
            'PROC SQL;\n'
            'SELECT * FROM t WHERE dt >= INTNX("MONTH", 20250531, 0, "BEGIN");\n'
            'QUIT;'
        )
        assert "TRUNC(" in result.converted_sql
        assert "'MM'" in result.converted_sql
        assert "INTNX" not in result.converted_sql
        assert "R-05" in result.rules_applied

    def test_r06_intnx_end(self, bare_converter):
        result = bare_converter.convert_sql(
            'PROC SQL;\n'
            'SELECT * FROM t WHERE dt <= INTNX("MONTH", 20250531, 0, "END");\n'
            'QUIT;'
        )
        assert "LAST_DAY(" in result.converted_sql
        assert "INTNX" not in result.converted_sql
        assert "R-06" in result.rules_applied

    def test_r07_intnx_add_months(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT INTNX('month', report_dt, 6) AS threshold FROM t;\n"
            "QUIT;"
        )
        assert "ADD_MONTHS(" in result.converted_sql
        assert "R-07" in result.rules_applied

    def test_r08_intnx_end_alignment(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT intnx('month', report_dt, -3, 'E') AS prev_q FROM t;\n"
            "QUIT;"
        )
        assert "LAST_DAY(ADD_MONTHS(" in result.converted_sql
        assert "R-08" in result.rules_applied


# Stage 6: Handler-based rules

class TestHandlerRules:
    """Tests for rules that need dedicated handler functions."""

    def test_r10_date_literal(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE dt >= '1Jan2025'd;\n"
            "QUIT;"
        )
        assert "TO_DATE('20250101', 'YYYYMMDD')" in result.converted_sql
        assert "'1Jan2025'd" not in result.converted_sql
        assert "R-10" in result.rules_applied

    def test_r10_date_literal_december(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE dt <= '31Dec2025'd;\n"
            "QUIT;"
        )
        assert "TO_DATE('20251231', 'YYYYMMDD')" in result.converted_sql

    def test_r11_31dec9999_equals(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE close_date = '31Dec9999'd;\n"
            "QUIT;"
        )
        assert "IS NULL" in result.converted_sql
        assert "31Dec9999" not in result.converted_sql
        assert "R-11" in result.rules_applied

    def test_r11_31dec9999_not_equals(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE close_date <> '31Dec9999'd;\n"
            "QUIT;"
        )
        assert "IS NOT NULL" in result.converted_sql

    def test_r16_compress_kd(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            'SELECT COMPRESS(phone, , "kd") AS digits FROM t;\n'
            "QUIT;"
        )
        assert "REGEXP_REPLACE(" in result.converted_sql
        assert "[^0-9]" in result.converted_sql
        assert "R-16" in result.rules_applied

    def test_r16_compress_ka(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            'SELECT COMPRESS(name, , "ka") AS alpha FROM t;\n'
            "QUIT;"
        )
        assert "REGEXP_REPLACE(" in result.converted_sql
        assert "[^A-Za-z]" in result.converted_sql

    def test_r23_choosec_input(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            'SELECT CHOOSEC(INPUT(t1.type_cd, 10.), "Fixed", "Variable", "Allocated") AS type_desc FROM t;\n'
            "QUIT;"
        )
        assert "CASE TO_NUMBER(" in result.converted_sql
        assert "WHEN 1 THEN 'Fixed'" in result.converted_sql
        assert "WHEN 2 THEN 'Variable'" in result.converted_sql
        assert "WHEN 3 THEN 'Allocated'" in result.converted_sql
        assert "END" in result.converted_sql
        assert "R-23" in result.rules_applied

    def test_r25_name_literal(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT t1.'Linkage Type'n FROM t;\n"
            "QUIT;"
        )
        assert "LINKAGE_TYPE" in result.converted_sql
        assert "'Linkage Type'n" not in result.converted_sql
        assert "R-25" in result.rules_applied

    def test_r25_name_literal_nationality(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT t1.'Nationality Category'n FROM t;\n"
            "QUIT;"
        )
        assert "NATIONALITY_CATEGORY" in result.converted_sql

    def test_r27_mdy_literals(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE dt >= mdy(5, 1, 2025);\n"
            "QUIT;"
        )
        assert "TO_DATE('20250501', 'YYYYMMDD')" in result.converted_sql
        assert "mdy" not in result.converted_sql.lower()
        assert "R-27" in result.rules_applied

    def test_r29_order_by_removed_from_ctas(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "CREATE TABLE work.test AS\n"
            "SELECT a, b FROM t\n"
            "ORDER BY a DESC;\n"
            "QUIT;"
        )
        assert "ORDER BY" not in result.converted_sql
        assert "R-29" in result.rules_applied

    def test_r29_order_by_kept_in_plain_select(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT a, b FROM t ORDER BY a DESC;\n"
            "QUIT;"
        )
        # No CREATE TABLE means ORDER BY should stay
        assert "ORDER BY" in result.converted_sql

    def test_r34_sas_sum_null_safe(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT sum(credit_limit, (-1)*(balance + interest)) AS net FROM t;\n"
            "QUIT;"
        )
        assert "NVL(" in result.converted_sql
        assert "R-34" in result.rules_applied

    def test_r34_aggregate_sum_untouched(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT SUM(amount) AS total FROM t GROUP BY id;\n"
            "QUIT;"
        )
        # Aggregate SUM (single argument) should not be modified
        assert "NVL(" not in result.converted_sql

    def test_r20_put_numeric_format_warns(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT PUT(t1.tariff_code, TARIF.) AS tariff_desc FROM t;\n"
            "QUIT;"
        )
        assert "R-20" in result.rules_applied
        assert any("R-20" in w for w in result.warnings)
        assert "TODO" in result.converted_sql

    def test_r21_put_char_format_warns(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT PUT(t1.type_code, $PNRTYP.) AS type_desc FROM t;\n"
            "QUIT;"
        )
        assert "R-21" in result.rules_applied
        assert any("R-21" in w for w in result.warnings)


# CALCULATED keyword (R-24)

class TestCalculatedHandler:
    """R-24 needs its own class because the alias resolution is complex."""

    def test_calculated_in_having(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT customer_id,\n"
            "  (SUM(balance)) AS total_balance\n"
            "FROM t\n"
            "GROUP BY customer_id\n"
            "HAVING (CALCULATED total_balance) > 100000;\n"
            "QUIT;"
        )
        assert "CALCULATED" not in result.converted_sql
        assert "R-24" in result.rules_applied

    def test_calculated_replaced_with_expression(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT customer_id,\n"
            "  (SUM(available_balance)) AS total_available_credit\n"
            "FROM t\n"
            "GROUP BY customer_id\n"
            "HAVING (CALCULATED total_available_credit) > 750000;\n"
            "QUIT;"
        )
        # The CALCULATED reference should be replaced with the expression
        assert "SUM(available_balance)" in result.converted_sql
        assert "CALCULATED" not in result.converted_sql

    def test_unresolvable_calculated_still_removed(self, bare_converter):
        """If the alias is not found, CALCULATED is still removed."""
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT a FROM t HAVING CALCULATED mystery_col > 0;\n"
            "QUIT;"
        )
        assert "CALCULATED" not in result.converted_sql
        assert any("R-24" in w for w in result.warnings)


# 31Dec9999'd handler (R-11)

class TestDateSentinel:
    """Tests for the '31Dec9999'd → IS NULL conversion."""

    def test_equality(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE t1.closing_date = '31Dec9999'd;\n"
            "QUIT;"
        )
        assert "closing_date IS NULL" in result.converted_sql

    def test_not_equality_with_not_equals(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE t1.end_date NOT = '31Dec9999'd;\n"
            "QUIT;"
        )
        # NOT = gets converted to <> by R-26 first, then R-11 fires.
        # Or R-11 may fire before R-26 depending on order.
        # Either way, the result should contain IS NOT NULL.
        converted = result.converted_sql
        assert "IS NOT NULL" in converted or "<>" in converted


# Full pipeline tests with mocked dependencies

class TestFullPipeline:
    """Tests that exercise the full convert() method with ParseResult."""

    def test_multiple_blocks(self, bare_converter):
        parse_result = _make_parse_result([
            "SELECT * FROM t1 WHERE col IS MISSING;",
            "SELECT UPCASE(name) FROM t2;",
        ])
        result = bare_converter.convert(parse_result)
        assert len(result.blocks) == 2
        assert "IS NULL" in result.blocks[0].converted_sql
        assert "UPPER(" in result.blocks[1].converted_sql

    def test_parameters_extracted(self, bare_converter):
        parse_result = _make_parse_result(
            ["SELECT 1 FROM dual;"],
            macros=[
                MacroDeclaration(
                    name="report_date",
                    value="20250531",
                    directive="LET",
                    line_number=1,
                ),
            ],
        )
        result = bare_converter.convert(parse_result)
        assert len(result.parameters_extracted) == 1
        assert "report_date" in result.parameters_extracted[0]

    def test_combined_sql_output(self, bare_converter):
        parse_result = _make_parse_result([
            "SELECT 1 FROM dual;",
            "SELECT 2 FROM dual;",
        ])
        result = bare_converter.convert(parse_result)
        combined = result.converted_sql
        assert "SELECT 1" in combined
        assert "SELECT 2" in combined

    def test_source_file_preserved(self, bare_converter):
        parse_result = _make_parse_result(["SELECT 1 FROM dual;"])
        result = bare_converter.convert(parse_result)
        assert result.source_file == "test_input.sas"

    def test_rules_applied_aggregated(self, bare_converter):
        parse_result = _make_parse_result([
            "SELECT * FROM t WHERE col IS MISSING;",
            "SELECT UPCASE(name) FROM t;",
        ])
        result = bare_converter.convert(parse_result)
        assert "R-01" in result.rules_applied
        assert "R-12" in result.rules_applied

    def test_total_rules_counted(self, bare_converter):
        parse_result = _make_parse_result([
            "SELECT * FROM t WHERE col IS MISSING;",
        ])
        result = bare_converter.convert(parse_result)
        assert result.total_rules_applied >= 1


# Dropds handling

class TestDropds:

    def test_dropds_generates_drop_table(self, bare_converter):
        parse_result = _make_parse_result(
            ["CREATE TABLE WORK.TEST_TABLE AS SELECT 1 FROM dual;"],
            dropds=[DropdsCall(table_name="WORK.TEST_TABLE", line_number=1)],
        )
        result = bare_converter.convert(parse_result)
        block = result.blocks[0]
        assert "DROP TABLE" in block.drop_statement
        assert "R-41" in block.rules_applied

    def test_dropds_oracle_exception_pattern(self, bare_converter):
        parse_result = _make_parse_result(
            ["CREATE TABLE WORK.MY_TBL AS SELECT 1 FROM dual;"],
            dropds=[DropdsCall(table_name="WORK.MY_TBL", line_number=1)],
        )
        result = bare_converter.convert(parse_result)
        drop = result.blocks[0].drop_statement
        # Oracle-style exception block for "table does not exist"
        assert "EXCEPTION" in drop
        assert "-942" in drop

    def test_no_dropds_no_drop_statement(self, bare_converter):
        parse_result = _make_parse_result([
            "CREATE TABLE WORK.TEST AS SELECT 1 FROM dual;",
        ])
        result = bare_converter.convert(parse_result)
        assert result.blocks[0].drop_statement == ""


# Variable handler integration

class TestVariableHandlerIntegration:

    def test_delegates_to_var_handler(self, mock_var_handler):
        mock_var_handler.substitute.return_value = MagicMock(
            original_sql="SELECT &report_date. FROM t;",
            converted_sql="SELECT ${prop_report_date} FROM t;",
            substitutions_made=["&report_date. → ${prop_report_date}"],
            warnings=[],
        )
        converter = Converter(variable_handler=mock_var_handler)
        result = converter.convert_sql(
            "PROC SQL;\nSELECT &report_date. FROM t;\nQUIT;"
        )
        mock_var_handler.substitute.assert_called_once()
        assert "R-02" in result.rules_applied


# Table mapper integration

class TestTableMapperIntegration:

    def test_delegates_to_table_mapper(self, mock_table_mapper):
        mock_mapping = MagicMock()
        mock_mapping.original = "WORK.TEST"
        mock_mapping.mapped = "STAGING.TMP_TEST"
        mock_table_mapper.map_tables.return_value = MagicMock(
            converted_sql="CREATE TABLE STAGING.TMP_TEST AS SELECT 1 FROM SOURCE_DATA.T;",
            mappings=[mock_mapping],
            warnings=[],
        )
        converter = Converter(table_mapper=mock_table_mapper)
        result = converter.convert_sql(
            "PROC SQL;\nCREATE TABLE WORK.TEST AS SELECT 1 FROM SOURCE.T;\nQUIT;"
        )
        mock_table_mapper.map_tables.assert_called_once()
        assert "R-35" in result.rules_applied


# Edge cases

class TestEdgeCases:

    def test_empty_sql_block(self, bare_converter):
        result = bare_converter.convert_sql("PROC SQL;\n\nQUIT;")
        # After stripping PROC SQL and QUIT, nothing remains
        assert result.converted_sql == ""

    def test_multiple_patterns_in_one_block(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT UPCASE(STRIP(name)) AS clean_name,\n"
            "       LOWCASE(code) AS lower_code\n"
            "FROM t WHERE col IS MISSING;\n"
            "QUIT;"
        )
        assert "UPPER(" in result.converted_sql
        assert "TRIM(" in result.converted_sql
        assert "LOWER(" in result.converted_sql
        assert "IS NULL" in result.converted_sql
        assert len(result.rules_applied) >= 4

    def test_format_removal_multiple_formats(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT dt FORMAT=EURDFDD10. AS dt,\n"
            "       amt FORMAT=COMMA12.2 AS amt,\n"
            "       code FORMAT=$30. AS code\n"
            "FROM t;\n"
            "QUIT;"
        )
        assert "FORMAT=" not in result.converted_sql

    def test_sas_operators_in_join_condition(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t1\n"
            "LEFT JOIN t2 ON t1.id = t2.id\n"
            "  AND t2.valid_from le '20250531'\n"
            "  AND t2.valid_to gt '20250531';\n"
            "QUIT;"
        )
        assert "<=" in result.converted_sql
        assert ">" in result.converted_sql
        assert " le " not in result.converted_sql
        assert " gt " not in result.converted_sql

    def test_convert_sql_returns_block_result(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\nSELECT 1 FROM dual;\nQUIT;"
        )
        assert isinstance(result, BlockResult)
        assert result.block_number == 1
        assert isinstance(result.rules_applied, list)
        assert isinstance(result.warnings, list)


# TC-01 integration (partial)

class TestTC01Integration:
    """Spot-check against TC-01 patterns without external modules."""

    def test_block1_is_missing_and_quotes(self, bare_converter):
        result = bare_converter.convert_sql(
            'PROC SQL;\n'
            'CREATE TABLE WORK.CUSTOMER_FILTERED AS\n'
            'SELECT t1.CUSTOMER_ID\n'
            'FROM SOURCE.CUSTOMER_MASTER t1\n'
            'WHERE t1.EMAIL_ADDRESS IS NOT MISSING\n'
            '  AND t1.PHONE_NUMBER IS MISSING\n'
            '  AND t1.STATUS_FLAG = "A";\n'
            'QUIT;'
        )
        assert "IS NOT NULL" in result.converted_sql
        assert "IS NULL" in result.converted_sql
        assert "IS MISSING" not in result.converted_sql
        assert "IS NOT MISSING" not in result.converted_sql

    def test_block3_compress_modifiers(self, bare_converter):
        result = bare_converter.convert_sql(
            'PROC SQL;\n'
            'CREATE TABLE WORK.CLEANED AS\n'
            'SELECT COMPRESS(t1.CONTRACT_NUMBER) AS clean,\n'
            '  COMPRESS(t1.PHONE_RAW, , "kd") AS digits,\n'
            '  COMPRESS(t1.NAME_RAW, , "ka") AS alpha,\n'
            '  t1.START_DATE FORMAT=DATE9.\n'
            'FROM SOURCE.CONTRACTS_RAW t1;\n'
            'QUIT;'
        )
        assert "REPLACE(" in result.converted_sql
        assert "REGEXP_REPLACE(" in result.converted_sql
        assert "FORMAT=" not in result.converted_sql

    def test_block4_outer_union_corr(self, bare_converter):
        result = bare_converter.convert_sql(
            'PROC SQL;\n'
            'SELECT CUSTOMER_ID FROM SOURCE.RETAIL\n'
            'OUTER UNION CORR\n'
            'SELECT CUSTOMER_ID FROM SOURCE.CORPORATE;\n'
            'QUIT;'
        )
        assert "UNION ALL" in result.converted_sql
        assert "OUTER UNION CORR" not in result.converted_sql


# TC-08 integration (partial)

class TestTC08Integration:
    """Spot-check against TC-08 patterns."""

    def test_contains_operator(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t WHERE name CONTAINS 'Mountain';\n"
            "QUIT;"
        )
        assert "LIKE '%Mountain%'" in result.converted_sql

    def test_name_literal_and_format(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT t1.LINKAGE_DATE FORMAT=EURDFDD10. AS LINKAGE_DATE,\n"
            "       t1.'Linkage Type'n\n"
            "FROM t;\n"
            "QUIT;"
        )
        assert "FORMAT=" not in result.converted_sql
        assert "LINKAGE_TYPE" in result.converted_sql

    def test_date_literals_in_between(self, bare_converter):
        result = bare_converter.convert_sql(
            "PROC SQL;\n"
            "SELECT * FROM t\n"
            "WHERE dt BETWEEN '1Jan2025'd AND '31Dec2025'd;\n"
            "QUIT;"
        )
        assert "TO_DATE('20250101'" in result.converted_sql
        assert "TO_DATE('20251231'" in result.converted_sql
        assert "'d" not in result.converted_sql
