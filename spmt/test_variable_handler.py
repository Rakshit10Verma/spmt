"""
tests/test_variable_handler.py

Tests for the variable handler module. I organized these by substitution
pattern (A through E) since that's how the handler processes things, plus
some config tests at the top and integration tests at the bottom using
real snippets from the TC files.
"""

import json
import os
import tempfile
from pathlib import Path

import pytest

from spmt.variable_handler import MacroDeclaration, VariableHandler


# Fixtures 

@pytest.fixture
def config_path(tmp_path: Path) -> str:
    """Write a minimal variable_mappings.json for tests."""
    cfg = {
        "date_variables": {
            "report_date": {
                "pentaho_name": "prop_report_date",
                "type": "date",
                "date_format": "YYYYMMDD",
            },
            "report_end_date": {
                "pentaho_name": "prop_report_end_date",
                "type": "date",
                "date_format": "YYYYMMDD",
            },
            "dwh_month_end_date": {
                "pentaho_name": "prop_DWH_MONTH_END_DATE",
                "type": "date",
                "date_format": "YYYYMMDD",
            },
        },
        "period_variables": {
            "period_key": {
                "pentaho_name": "prop_period_key",
                "type": "numeric",
            },
            "report_period": {
                "pentaho_name": "prop_report_period",
                "type": "numeric",
            },
            "gperiodetable": {
                "pentaho_name": "prop_gPeriodeTable",
                "type": "string",
            },
        },
        "string_variables": {
            "mandant_code": {
                "pentaho_name": "prop_mandant_code",
                "type": "string",
            },
            "client_code": {
                "pentaho_name": "prop_client_code",
                "type": "string",
            },
            "org_unit": {
                "pentaho_name": "prop_org_unit",
                "type": "string",
            },
        },
        "numeric_variables": {
            "report_month": {
                "pentaho_name": "prop_report_month",
                "type": "numeric",
            },
            "report_year": {
                "pentaho_name": "prop_report_year",
                "type": "numeric",
            },
        },
        "default_date_format": "YYYYMMDD",
        "default_pentaho_prefix": "prop_",
    }
    p = tmp_path / "variable_mappings.json"
    p.write_text(json.dumps(cfg))
    return str(p)


@pytest.fixture
def handler(config_path: str) -> VariableHandler:
    """Handler loaded with the test config above."""
    return VariableHandler(config_path=config_path)


@pytest.fixture
def handler_no_config() -> VariableHandler:
    """Handler with no config at all. everything gets auto-generated names."""
    return VariableHandler()



# 1. Config loading 
class TestConfigLoading:

    def test_loads_all_sections(self, handler: VariableHandler):
        # Make sure every section in the JSON got flattened into _mappings
        assert "report_date" in handler._mappings
        assert "period_key" in handler._mappings
        assert "mandant_code" in handler._mappings
        assert "report_month" in handler._mappings

    def test_missing_config_raises(self):
        with pytest.raises(FileNotFoundError):
            VariableHandler(config_path="/nonexistent/path.json")

    def test_default_prefix(self, handler: VariableHandler):
        assert handler._default_prefix == "prop_"

    def test_lookup_mapped_variable(self, handler: VariableHandler):
        pname, vtype, dfmt = handler._lookup("report_date")
        assert pname == "prop_report_date"
        assert vtype == "date"
        assert dfmt == "YYYYMMDD"

    def test_lookup_unmapped_variable(self, handler: VariableHandler):
        # Should get a made-up name, not crash
        pname, vtype, _ = handler._lookup("totally_unknown_var")
        assert pname == "prop_totally_unknown_var"
        assert vtype == "string"

    def test_lookup_case_insensitive(self, handler: VariableHandler):
        # SAS doesn't care about case, so neither should we
        pname1, _, _ = handler._lookup("REPORT_DATE")
        pname2, _, _ = handler._lookup("report_date")
        assert pname1 == pname2


# 2. %LET / %GLOBAL extraction 

class TestExtractDeclarations:

    def test_simple_let(self, handler: VariableHandler):
        sas = "%LET report_date = 20250531;"
        decls = handler.extract_declarations(sas)
        assert len(decls) == 1
        assert decls[0].name == "report_date"
        assert decls[0].value == "20250531"
        assert decls[0].pentaho_name == "prop_report_date"

    def test_global_then_let(self, handler: VariableHandler):
        # This is the standard EG pattern: declare global, then assign
        sas = "%GLOBAL report_date;\n%LET report_date = 20250531;"
        decls = handler.extract_declarations(sas)
        assert len(decls) == 1
        d = decls[0]
        assert d.name == "report_date"
        assert d.value == "20250531"
        assert d.is_global is True

    def test_global_same_line_as_let(self, handler: VariableHandler):
        # EG sometimes jams both on the same line with lots of whitespace
        sas = "%GLOBAL gRef;      %LET gRef = somevalue;"
        decls = handler.extract_declarations(sas)
        assert any(d.name == "gRef" and d.value == "somevalue" for d in decls)

    def test_bare_global_no_value(self, handler: VariableHandler):
        sas = "%GLOBAL myvar;"
        decls = handler.extract_declarations(sas)
        assert len(decls) == 1
        assert decls[0].value is None
        assert decls[0].is_global is True

    def test_multiple_lets(self, handler: VariableHandler):
        sas = (
            "%LET report_date = 20250531;\n"
            "%LET report_month = 05;\n"
            "%LET report_year = 2025;\n"
            "%LET client_code = ABC;\n"
        )
        decls = handler.extract_declarations(sas)
        names = {d.name.lower() for d in decls}
        assert names == {"report_date", "report_month", "report_year", "client_code"}

    def test_let_with_macro_value(self, handler: VariableHandler):
        # Sometimes a %LET assigns one macro to another, like in TC-01
        sas = "%LET report_date = &DWH_MONTH_END_DATE.;"
        decls = handler.extract_declarations(sas)
        assert decls[0].value == "&DWH_MONTH_END_DATE."

    def test_case_insensitive_let(self, handler: VariableHandler):
        sas = "%let MY_VAR = hello;"
        decls = handler.extract_declarations(sas)
        assert len(decls) == 1
        assert decls[0].name == "MY_VAR"


# 3. Pattern A: date literal "&var."d -> TO_DATE(...) 
# SAS has this weird thing where you can put a macro var inside a date
# literal. Like "&report_end_date."d means "resolve the variable, then
# treat the whole thing as a date." Oracle needs TO_DATE() instead.

class TestPatternDateLiteral:

    def test_date_literal_with_dot(self, handler: VariableHandler):
        sql = 'WHERE t2.valid_from le "&report_end_date."d'
        result = handler.substitute(sql)
        assert "TO_DATE('${prop_report_end_date}', 'YYYYMMDD')" in result.converted_sql
        # The whole "&report_end_date."d should be gone
        assert '"d' not in result.converted_sql

    def test_date_literal_without_dot(self, handler: VariableHandler):
        sql = 'WHERE t1.close_dt = "&report_date"d'
        result = handler.substitute(sql)
        assert "TO_DATE(" in result.converted_sql


# 4. Pattern B: quoted string "&var." -> '${prop_var}' 
# SAS uses double quotes when it wants macro resolution inside strings.
# Oracle only does single quotes. So I need to swap the quotes and
# replace the variable at the same time.

class TestPatternQuotedString:

    def test_quoted_var_with_dot(self, handler: VariableHandler):
        sql = 'WHERE t1.MANDANT = "&mandant_code."'
        result = handler.substitute(sql)
        assert "'${prop_mandant_code}'" in result.converted_sql
        # No double quotes should survive
        assert '"' not in result.converted_sql or result.converted_sql.count('"') == 0

    def test_quoted_var_without_dot(self, handler: VariableHandler):
        sql = 'AND t2.client_code = "&client_code"'
        result = handler.substitute(sql)
        assert "'${prop_client_code}'" in result.converted_sql

    def test_quoted_var_preserves_surrounding_sql(self, handler: VariableHandler):
        sql = 'WHERE x = 1 AND t1.code = "&org_unit." AND y = 2'
        result = handler.substitute(sql)
        assert "WHERE x = 1" in result.converted_sql
        assert "AND y = 2" in result.converted_sql
        assert "'${prop_org_unit}'" in result.converted_sql


# 5. Pattern C: double dot &schema..table -> ${prop_schema}.table 
# This is the confusing one. In SAS &var.. means "the variable ends here
# (first dot) and there's a real dot after it (second dot)." It shows up
# in dynamic table names like source.CUSTOMERS_&gPeriodeTable.

class TestPatternDoubleDot:

    def test_double_dot_table_ref(self, handler: VariableHandler):
        sql = "SELECT * FROM source.CUSTOMERS_&gPeriodeTable.;"
        result = handler.substitute(sql)
        assert "${prop_gPeriodeTable}" in result.converted_sql


# 6. Pattern D: bare var with dot &var. -> ${prop_var} 
# The bread and butter case. Dot just means "this is where the
# variable name ends."

class TestPatternBareVarDot:

    def test_bare_numeric_var(self, handler: VariableHandler):
        sql = "WHERE t1.PERIOD_KEY = &period_key."
        result = handler.substitute(sql)
        assert "${prop_period_key}" in result.converted_sql
        # Trailing dot should be consumed, not left dangling
        assert "&period_key." not in result.converted_sql

    def test_bare_in_expression(self, handler: VariableHandler):
        sql = "AND t2.snapshot_period = &report_period."
        result = handler.substitute(sql)
        assert "${prop_report_period}" in result.converted_sql

    def test_skips_sas_builtin_vars(self, handler: VariableHandler):
        # These live inside the %_eg_conditional_dropds macro and shouldn't
        # be converted - they're SAS internal plumbing, not user parameters
        sql = "%let dsname=%qscan(&syspbuff,&num);"
        result = handler.substitute(sql)
        assert "&syspbuff" in result.converted_sql
        assert "&num" in result.converted_sql


# 7. Pattern E: bare var no dot &var -> ${prop_var} 
# Sometimes people just leave the dot off, usually inside function calls
# where commas or parens make the boundary clear to SAS.

class TestPatternBareVarNoDot:

    def test_bare_no_dot(self, handler: VariableHandler):
        sql = "INTNX('month', &report_date, 6)"
        result = handler.substitute(sql)
        assert "${prop_report_date}" in result.converted_sql

    def test_bare_multiple_vars(self, handler: VariableHandler):
        # mdy() from TC-07 - three bare vars in one function call
        sql = "mdy(&report_month, 1, &report_year)"
        result = handler.substitute(sql)
        assert "${prop_report_month}" in result.converted_sql
        assert "${prop_report_year}" in result.converted_sql


# 8. Unmapped variables 
# If a variable isn't in the config, I still convert it (using an
# auto-generated name) but I also emit a warning. Had a bug earlier
# where I was scanning the already-converted output for warnings
# instead of tracking them during substitution.

class TestUnmappedVariables:

    def test_auto_name(self, handler: VariableHandler):
        sql = "WHERE x = &unknown_var."
        result = handler.substitute(sql)
        assert "${prop_unknown_var}" in result.converted_sql

    def test_warning_for_unmapped(self, handler: VariableHandler):
        sql = "WHERE x = &unknown_var."
        result = handler.substitute(sql)
        assert any("unknown_var" in w for w in result.warnings)

    def test_no_config_handler(self, handler_no_config: VariableHandler):
        # Even with zero config, substitution should still work
        sql = "WHERE x = &foo."
        result = handler_no_config.substitute(sql)
        assert "${prop_foo}" in result.converted_sql


# 9. Integration tests 
# These use actual SQL snippets from the test case files to make sure
# everything works together and not just in isolation.

class TestIntegration:

    def test_tc01_snippet(self, handler: VariableHandler):
        """TC-01 has quoted strings and bare vars mixed together."""
        sas = (
            'WHERE t1.ORG_UNIT_CODE = "&org_unit."\n'
            "  AND t1.TRANSACTION_DATE >= INTNX('MONTH', &report_date., 0, 'BEGIN')\n"
            '  AND t1.MANDANT = "&mandant_code."'
        )
        result = handler.substitute(sas)
        assert "'${prop_org_unit}'" in result.converted_sql
        assert "${prop_report_date}" in result.converted_sql
        assert "'${prop_mandant_code}'" in result.converted_sql
        assert len(result.substitutions_made) >= 3

    def test_tc07_snippet(self, handler: VariableHandler):
        """TC-07 has numeric vars inside mdy() and a quoted client_code."""
        sas = (
            'AND t2.client_code = "&client_code."\n'
            "WHERE t1.approval_date >= mdy(&report_month., 1, &report_year.)"
        )
        result = handler.substitute(sas)
        assert "'${prop_client_code}'" in result.converted_sql
        assert "${prop_report_month}" in result.converted_sql
        assert "${prop_report_year}" in result.converted_sql

    def test_tc06_date_literal(self, handler: VariableHandler):
        """TC-06 has the "&var."d date literal pattern twice in a row."""
        sas = (
            'AND t2.valid_from le "&report_end_date."d\n'
            'AND t2.valid_to gt "&report_end_date."d'
        )
        result = handler.substitute(sas)
        assert result.converted_sql.count("TO_DATE(") == 2
        assert "prop_report_end_date" in result.converted_sql

    def test_extract_and_substitute_pipeline(self, handler: VariableHandler):
        """Make sure process() runs both extraction and substitution."""
        sas = (
            "%LET report_date = 20250531;\n"
            "%LET mandant_code = NOS;\n"
            "\n"
            "PROC SQL;\n"
            "CREATE TABLE WORK.T AS\n"
            'SELECT * FROM SOURCE.X WHERE code = "&mandant_code."\n'
            "  AND dt >= &report_date.;\n"
            "QUIT;\n"
        )
        decls, result = handler.process(sas)

        # Should find both declarations
        assert len(decls) == 2
        names = {d.name.lower() for d in decls}
        assert "report_date" in names
        assert "mandant_code" in names

        # And both should be substituted
        assert "'${prop_mandant_code}'" in result.converted_sql
        assert "${prop_report_date}" in result.converted_sql

    def test_substitution_tracks_count(self, handler: VariableHandler):
        sql = 'WHERE a = &report_date. AND b = "&mandant_code."'
        result = handler.substitute(sql)
        assert len(result.substitutions_made) >= 2

    def test_original_sql_preserved(self, handler: VariableHandler):
        # I keep the original around for diffing in the migration report
        sql = "WHERE x = &report_date."
        result = handler.substitute(sql)
        assert result.original_sql == sql
        assert result.original_sql != result.converted_sql
