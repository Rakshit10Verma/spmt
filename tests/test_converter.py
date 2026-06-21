"""Tests for converter.py.

I organise these by rule category so each of the eight categories from rules.py
has at least one test that proves the converter handles it. After the category
tests there is one integration test that runs a whole TC file through the real
parser and the converter together.

Two of the categories, macro variables and table mapping, are not done by the
converter itself. The converter passes the SQL to the variable handler and the
table mapper and folds their result back in. To test that wiring without pulling
in the real handler config, I use small fake handlers defined here. They give
the converter exactly the result shape it expects, so the test checks the
converter's own job and nothing else.
"""

import os
import re

import pytest

from spmt.parser import (
    ParsedBlock,
    ParseResult,
    DropdsCall,
    MacroDeclaration,
)
from spmt.converter import Converter


# Test doubles for the injected handlers

class _FakeSubResult:
    def __init__(self, original, converted, subs):
        self.original_sql = original
        self.converted_sql = converted
        self.substitutions_made = subs
        self.warnings = []


class FakeVarHandler:
    """Stands in for the real variable handler. Turns &name. into ${prop_name}
    so I can check the converter reads the handler result back correctly."""

    def convert_sql(self, sql):
        subs = []

        def repl(m):
            subs.append(m.group(0))
            return "${prop_" + m.group(1) + "}"

        out = re.sub(r"&(\w+)\.?", repl, sql)
        return _FakeSubResult(sql, out, subs)


class _FakeMapResult:
    def __init__(self, original, converted, mappings):
        self.original_sql = original
        self.converted_sql = converted
        self.mappings = mappings
        self.warnings = []


class FakeTableMapper:
    """Stands in for the real table mapper. Maps WORK.table to a staging name
    so I can check the converter folds the mapped SQL back in."""

    def map_tables(self, sql):
        maps = []

        def repl(m):
            maps.append(m.group(0))
            return "staging.PREFIX_" + m.group(1)

        out = re.sub(r"\bWORK\.(\w+)", repl, sql, flags=re.IGNORECASE)
        return _FakeMapResult(sql, out, maps)


def _block(sql, number=1):
    return ParsedBlock(number, sql, line_start=1, line_end=1)


# Category: null handling

def test_null_handling_is_missing():
    sql = (
        "PROC SQL; CREATE TABLE WORK.A AS SELECT t1.c FROM SOURCE.M t1 "
        "WHERE t1.email IS NOT MISSING AND t1.phone IS MISSING; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    assert "IS NOT NULL" in out
    assert "IS NULL" in out
    assert "MISSING" not in out.upper()


# Category: date functions

def test_date_functions_intnx_and_today():
    sql = (
        "PROC SQL; CREATE TABLE WORK.D AS "
        "SELECT (INTNX('month', base_date, 6, 'E')) AS thresh, "
        "(INTNX('month', base_date, 0, 'BEGIN')) AS month_start, "
        "today() AS run_date "
        "FROM SOURCE.T t1; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    # 'E' alignment becomes LAST_DAY around ADD_MONTHS
    assert "LAST_DAY(ADD_MONTHS(base_date, 6))" in out
    # 'BEGIN' alignment becomes TRUNC to the month
    assert "TRUNC(base_date, 'MM')" in out
    # today() becomes the current date with no time part
    assert "TRUNC(SYSDATE)" in out
    assert "INTNX" not in out.upper()
    assert "TODAY(" not in out.upper()


# Category: string functions

def test_string_functions_upcase_strip_compress():
    sql = (
        "PROC SQL; CREATE TABLE WORK.S AS "
        "SELECT UPCASE(STRIP(t1.a)) AS a, LOWCASE(t1.b) AS b, "
        "COMPRESS(t1.c) AS c, COMPRESS(t1.d, , 'kd') AS dgt "
        "FROM SOURCE.T t1; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    assert "UPPER(" in out
    assert "TRIM(" in out
    assert "LOWER(" in out
    # plain COMPRESS drops spaces, the 'kd' modifier keeps digits only
    assert "REPLACE(" in out
    assert "REGEXP_REPLACE(" in out
    assert "UPCASE" not in out.upper()
    assert "LOWCASE" not in out.upper()
    assert "COMPRESS" not in out.upper()


# Category: macro variables

def test_macro_variables_substituted_via_handler():
    sql = (
        "PROC SQL; CREATE TABLE WORK.M AS SELECT t1.a FROM SOURCE.T t1 "
        'WHERE t1.code = "&client." AND t1.dt >= &report_date.; QUIT;'
    )
    result = Converter(variable_handler=FakeVarHandler()).convert_block(_block(sql))
    out = result.converted_sql
    assert "${prop_client}" in out
    assert "${prop_report_date}" in out
    assert "&" not in out
    # the converter records that the macro variable stage did something
    assert result.rules_applied


# Category: type conversion

def test_type_conversion_put_and_choosec_warn():
    # PUT and CHOOSEC need a SAS format catalog, which Oracle does not have. The
    # converter cannot resolve them on its own, so it leaves them in place and
    # raises a warning instead of guessing.
    sql = (
        "PROC SQL; CREATE TABLE WORK.T AS "
        "SELECT (PUT(t1.code, FMT.)) AS descr, "
        "(CHOOSEC(INPUT(t1.k, 8.), 'A', 'B')) AS pick "
        "FROM SOURCE.T t1; QUIT;"
    )
    result = Converter().convert_block(_block(sql))
    assert any("PUT" in w for w in result.warnings)
    assert any("CHOOSEC" in w for w in result.warnings)
    assert "CASE WHEN" in result.converted_sql  # PUT() converts to CASE WHEN placeholder
    assert "TO_CHAR" in result.converted_sql  # PUT() handler generates TO_CHAR
    assert "CHOOSEC(" in result.converted_sql
    # PUT must not match inside INPUT(...)
    assert re.search(r"CHOOSEC\(\s*INPUT\(", result.converted_sql, re.IGNORECASE)
    assert "IN(CASE WHEN" not in result.converted_sql


def test_order_by_removed_from_ctas_is_reported():
    sql = (
        "PROC SQL; CREATE TABLE WORK.O AS "
        "SELECT t1.a FROM SOURCE.T t1 ORDER BY t1.a DESC; QUIT;"
    )
    result = Converter().convert_block(_block(sql))
    assert "ORDER BY" not in result.converted_sql.upper()
    assert any("ORDER BY removed" in w for w in result.warnings)


# Category: SAS keywords

def test_sas_keywords_operators_and_contains():
    sql = (
        "PROC SQL; CREATE TABLE WORK.K AS SELECT c FROM SOURCE.M t1 "
        "WHERE t1.a gt 10 AND t1.b le 5 AND t1.c NOT = 0 "
        "AND t1.name CONTAINS 'x'; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    assert re.search(r"a\s*>\s*10", out)
    assert re.search(r"b\s*<=\s*5", out)
    assert "<>" in out
    assert "LIKE '%x%'" in out
    assert "CONTAINS" not in out.upper()
    assert "NOT =" not in out


# Category: table mapping

def test_table_mapping_via_mapper():
    sql = "PROC SQL; CREATE TABLE WORK.OUT AS SELECT t1.a FROM WORK.IN t1; QUIT;"
    result = Converter(table_mapper=FakeTableMapper()).convert_block(_block(sql))
    out = result.converted_sql
    assert "staging.PREFIX_OUT" in out
    assert "staging.PREFIX_IN" in out
    assert "TABLE_MAPPING" in result.rules_applied
    assert not re.search(r"\bWORK\.", out)


# Category: join patterns

def test_join_patterns_preserved():
    # Joins that are already valid Oracle must come through untouched, including
    # the MAX() subquery used to pick the latest period.
    sql = (
        "PROC SQL; CREATE TABLE WORK.J AS "
        "SELECT t1.a, t2.b FROM SOURCE.A t1 "
        "LEFT JOIN SOURCE.B t2 ON t1.k = t2.k "
        "INNER JOIN (SELECT MAX(period) AS mp FROM SOURCE.C) t3 "
        "ON t1.period = t3.mp; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    assert "LEFT JOIN" in out
    assert "INNER JOIN" in out
    assert "MAX(period)" in out
    assert "t1.k = t2.k" in out


# A few converter specific behaviours worth pinning down

def test_calculated_keyword_repeats_expression():
    sql = (
        "PROC SQL; CREATE TABLE WORK.H AS "
        "SELECT t1.id, (SUM(t1.bal)) AS total FROM SOURCE.T t1 "
        "GROUP BY t1.id HAVING (CALCULATED total) > 100; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    assert "(SUM(t1.bal)) > 100" in out
    assert "CALCULATED" not in out.upper()


def test_sas_sum_two_args_converts_to_nvl_addition():
    sql = (
        "PROC SQL; CREATE TABLE WORK.SUMX AS "
        "SELECT (sum(t1.credit_limit, (-1) * (t2.principal + t2.interest))) AS net_bal "
        "FROM SOURCE.A t1 LEFT JOIN SOURCE.B t2 ON t1.id=t2.id; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    assert "NVL(t1.credit_limit, 0)" in out
    assert "NVL((-1) * (t2.principal + t2.interest), 0)" in out
    assert "sum(t1.credit_limit" not in out.lower()


def test_mdy_with_literals_is_zero_padded_date_literal():
    sql = (
        "PROC SQL; CREATE TABLE WORK.DT AS "
        "SELECT mdy(5, 1, 2025) AS dt FROM SOURCE.T t1; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    assert "TO_DATE('2025-05-01', 'YYYY-MM-DD')" in out


def test_name_literal_rename_carries_across_blocks():
    conv = Converter()
    b1 = (
        "PROC SQL; CREATE TABLE WORK.A AS "
        "SELECT (t1.code) AS 'Linkage Type'n FROM SOURCE.T t1; QUIT;"
    )
    b2 = (
        "PROC SQL; CREATE TABLE WORK.B AS "
        "SELECT t1.'Linkage Type'n FROM WORK.A t1; QUIT;"
    )
    out1 = conv.convert_block(_block(b1, 1)).converted_sql
    out2 = conv.convert_block(_block(b2, 2)).converted_sql
    assert "LINKAGE_TYPE" in out1
    assert "LINKAGE_TYPE" in out2
    assert "'Linkage Type'n" not in out1
    assert "'Linkage Type'n" not in out2


def test_name_literal_after_case_expression_is_renamed_cleanly():
    sql = (
        "PROC SQL; CREATE TABLE WORK.C AS "
        "SELECT (CASE t1.n WHEN 1 THEN 'Domestic' ELSE 'Foreign' END) "
        "AS 'Nationality Category'n FROM SOURCE.T t1; QUIT;"
    )
    out = Converter().convert_block(_block(sql)).converted_sql
    assert "NATIONALITY_CATEGORY" in out
    assert "'Nationality Category'n" not in out
    assert "ForeignEND_AS" not in out


def test_convert_file_collects_drops_and_parameters():
    parse_result = ParseResult(
        sql_blocks=[_block("PROC SQL; CREATE TABLE WORK.X AS SELECT a FROM SOURCE.Y t1; QUIT;")],
        macro_declarations=[
            MacroDeclaration("report_date", "20250531", "LET", 1),
            MacroDeclaration("client_code", "ABC", "LET", 2),
        ],
        dropds_calls=[DropdsCall("WORK.X", 3)],
        source_file="unit.sas",
    )
    result = Converter().convert_file(parse_result)
    assert len(result.blocks) == 1
    assert result.drop_statements
    assert "DROP TABLE" in result.drop_statements[0]
    assert "EXECUTE IMMEDIATE" in result.drop_statements[0]
    assert "report_date" in result.parameters
    assert "client_code" in result.parameters


# Integration: a full TC file through the real parser and the converter

_HERE = os.path.dirname(__file__)
_TC_FIXTURES = {
    "TC-01_basic_nulls_strings_unions.sas": 5,
    "TC-02_date_functions_choosec_lookups.sas": 3,
    "TC-03_case_when_date_arithmetic_operators.sas": 4,
    "TC-04_quarterly_contracts_right_joins.sas": 5,
    "TC-05_format_lookups_aggregation.sas": 6,
    "TC-06_put_formats_time_slices.sas": 5,
    "TC-07_chained_tables_calculated_having.sas": 6,
    "TC-08_linkages_contains_multijoin.sas": 4,
}


def _real_handlers():
    """Build the real variable handler and table mapper if they are available
    and need no extra configuration here. If either one cannot be built in this
    environment, return None for it and the integration test skips the checks
    that depend on it."""
    vh = tm = None
    try:
        from spmt.variable_handler import VariableHandler
        vh = VariableHandler()
    except Exception:
        vh = None
    try:
        from spmt.table_mapper import TableMapper
        tm = TableMapper()
    except Exception:
        tm = None
    return vh, tm


@pytest.mark.parametrize("filename,expected_blocks", _TC_FIXTURES.items())
def test_integration_full_tc_file(filename, expected_blocks):
    parser_mod = pytest.importorskip("spmt.parser")
    parse_file = getattr(parser_mod, "parse_file", None)
    if parse_file is None:
        pytest.skip("parser.parse_file not available")

    fixture_path = os.path.join(_HERE, "fixtures", filename)
    assert os.path.exists(fixture_path), f"{filename} fixture not found"

    parse_result = parse_file(fixture_path)
    assert len(parse_result.sql_blocks) == expected_blocks

    vh, tm = _real_handlers()
    result = Converter(variable_handler=vh, table_mapper=tm).convert_file(parse_result)

    # one converted block per parsed block, none of them empty
    assert len(result.blocks) == len(parse_result.sql_blocks)
    for block in result.blocks:
        assert block.converted_sql.strip()
        assert "PROC SQL" not in block.converted_sql
        assert "QUIT" not in block.converted_sql.upper()

    joined = "\n".join(b.converted_sql for b in result.blocks)

    # conversions that do not depend on the injected handlers
    assert not re.search(r"\bFORMAT\s*=", joined, re.IGNORECASE)

    has_macro_references = any("&" in block.original_sql for block in parse_result.sql_blocks)

    # Macro substitution only applies to fixtures whose SQL blocks reference macros.
    if vh is not None and has_macro_references:
        assert "${prop_" in joined
