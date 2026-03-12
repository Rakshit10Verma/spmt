"""
Test suite for spmt/parser.py covering:
  1. Single PROC SQL block
  2. Multiple blocks in one file
  3. File with %LET declarations
  4. File with comments between blocks
  5. Empty file
  6. File with no PROC SQL
  7. Edge cases from the real test suite (macro defs, dropds, %GLOBAL, mixed casing)
  8. Integration tests against TC-01 through TC-08
"""

import pytest
from pathlib import Path
from spmt.parser import parse_string, parse_file, ParsedBlock, MacroDeclaration


TC_DIR = Path("/mnt/project")

def tc_path(name: str) -> Path:
    p = TC_DIR / name
    if not p.exists():
        pytest.skip(f"{name} not found in {TC_DIR}")
    return p


# 1. Single PROC SQL block

class TestSingleBlock:

    def test_extracts_one_block(self):
        sas = "PROC SQL;\n  CREATE TABLE x AS SELECT 1;\nQUIT;"
        result = parse_string(sas)
        assert len(result.sql_blocks) == 1

    def test_block_number_is_one(self):
        sas = "PROC SQL;\n  CREATE TABLE x AS SELECT 1;\nQUIT;"
        result = parse_string(sas)
        assert result.sql_blocks[0].block_number == 1

    def test_original_sql_contains_proc_and_quit(self):
        sas = "PROC SQL;\n  CREATE TABLE x AS SELECT 1;\nQUIT;"
        result = parse_string(sas)
        sql = result.sql_blocks[0].original_sql
        assert sql.startswith("PROC SQL;")
        assert sql.rstrip().endswith("QUIT;")

    def test_line_start_and_end(self):
        sas = "PROC SQL;\n  CREATE TABLE x AS SELECT 1;\nQUIT;"
        result = parse_string(sas)
        assert result.sql_blocks[0].line_start == 1
        assert result.sql_blocks[0].line_end == 3

    def test_line_numbers_account_for_leading_blank_lines(self):
        sas = "\n\n\nPROC SQL;\n  SELECT 1;\nQUIT;"
        result = parse_string(sas)
        assert result.sql_blocks[0].line_start == 4
        assert result.sql_blocks[0].line_end == 6

    def test_case_insensitive_proc_sql(self):
        sas = "proc sql;\n  select 1;\nquit;"
        result = parse_string(sas)
        assert len(result.sql_blocks) == 1

    def test_mixed_case(self):
        sas = "Proc Sql;\n  SELECT 1;\nQuIt;"
        result = parse_string(sas)
        assert len(result.sql_blocks) == 1


# 2. Multiple blocks in one file

class TestMultipleBlocks:

    MULTI = (
        "PROC SQL;\n  CREATE TABLE a AS SELECT 1;\nQUIT;\n\n"
        "PROC SQL;\n  CREATE TABLE b AS SELECT 2;\nQUIT;\n\n"
        "PROC SQL;\n  CREATE TABLE c AS SELECT 3;\nQUIT;\n"
    )

    def test_extracts_three_blocks(self):
        result = parse_string(self.MULTI)
        assert len(result.sql_blocks) == 3

    def test_block_numbers_are_sequential(self):
        result = parse_string(self.MULTI)
        numbers = [b.block_number for b in result.sql_blocks]
        assert numbers == [1, 2, 3]

    def test_each_block_contains_its_own_sql(self):
        result = parse_string(self.MULTI)
        assert "table a" in result.sql_blocks[0].original_sql.lower()
        assert "table b" in result.sql_blocks[1].original_sql.lower()
        assert "table c" in result.sql_blocks[2].original_sql.lower()

    def test_line_ranges_dont_overlap(self):
        result = parse_string(self.MULTI)
        for i in range(len(result.sql_blocks) - 1):
            assert result.sql_blocks[i].line_end < result.sql_blocks[i + 1].line_start


# 3. File with %LET declarations

class TestLetDeclarations:

    SAS_WITH_LETS = (
        "%LET report_date = 20250531;\n"
        "%LET period_key = 202505;\n"
        "%LET client_code = ABC;\n"
        "\n"
        "PROC SQL;\n"
        "  CREATE TABLE work.t1 AS SELECT 1;\n"
        "QUIT;\n"
    )

    def test_extracts_let_declarations(self):
        result = parse_string(self.SAS_WITH_LETS)
        lets = [m for m in result.macro_declarations if m.directive == "LET"]
        assert len(lets) == 3

    def test_let_names_and_values(self):
        result = parse_string(self.SAS_WITH_LETS)
        lets = {m.name: m.value for m in result.macro_declarations if m.directive == "LET"}
        assert lets["report_date"] == "20250531"
        assert lets["period_key"] == "202505"
        assert lets["client_code"] == "ABC"

    def test_let_line_numbers(self):
        result = parse_string(self.SAS_WITH_LETS)
        lets = [m for m in result.macro_declarations if m.directive == "LET"]
        assert lets[0].line_number == 1
        assert lets[1].line_number == 2
        assert lets[2].line_number == 3

    def test_let_with_macro_reference_in_value(self):
        sas = "%LET x = &SOME_OTHER_VAR.;\nPROC SQL; SELECT 1; QUIT;"
        result = parse_string(sas)
        lets = [m for m in result.macro_declarations if m.directive == "LET"]
        assert len(lets) == 1
        assert lets[0].value == "&SOME_OTHER_VAR."

    def test_global_declaration_without_value(self):
        sas = "%GLOBAL myvar;\nPROC SQL; SELECT 1; QUIT;"
        result = parse_string(sas)
        globs = [m for m in result.macro_declarations if m.directive == "GLOBAL"]
        assert len(globs) == 1
        assert globs[0].name == "myvar"
        assert globs[0].value == ""

    def test_global_and_let_on_same_variable(self):
        sas = (
            "%GLOBAL report_date;\n"
            "%LET report_date = 20250531;\n"
            "PROC SQL; SELECT 1; QUIT;\n"
        )
        result = parse_string(sas)
        names = [m.name for m in result.macro_declarations]
        assert names.count("report_date") == 2

    def test_declarations_sorted_by_line_number(self):
        sas = (
            "%GLOBAL b_var;\n"
            "%LET a_var = 1;\n"
            "%GLOBAL c_var;\n"
            "PROC SQL; SELECT 1; QUIT;\n"
        )
        result = parse_string(sas)
        lines = [m.line_number for m in result.macro_declarations]
        assert lines == sorted(lines)


# 4. File with comments between blocks

class TestCommentsAndWhitespace:

    SAS_WITH_COMMENTS = (
        "/* Header comment\n"
        "   spanning multiple lines */\n"
        "\n"
        "PROC SQL;\n"
        "  CREATE TABLE a AS SELECT 1;\n"
        "QUIT;\n"
        "\n"
        "/* Another comment between blocks */\n"
        "\n"
        "PROC SQL;\n"
        "  CREATE TABLE b AS SELECT 2;\n"
        "QUIT;\n"
    )

    def test_comments_dont_prevent_extraction(self):
        result = parse_string(self.SAS_WITH_COMMENTS)
        assert len(result.sql_blocks) == 2

    def test_comments_not_included_in_sql(self):
        result = parse_string(self.SAS_WITH_COMMENTS)
        for block in result.sql_blocks:
            assert "Header comment" not in block.original_sql
            assert "Another comment" not in block.original_sql

    def test_line_numbers_account_for_comments(self):
        result = parse_string(self.SAS_WITH_COMMENTS)
        assert result.sql_blocks[0].line_start == 4
        assert result.sql_blocks[1].line_start == 10

    def test_inline_comment_inside_sql_preserved(self):
        sas = "PROC SQL;\n  /* inside */ CREATE TABLE x AS SELECT 1;\nQUIT;"
        result = parse_string(sas)
        assert "/* inside */" in result.sql_blocks[0].original_sql


# 5. Empty file

class TestEmptyFile:

    def test_no_blocks(self):
        result = parse_string("")
        assert len(result.sql_blocks) == 0

    def test_no_declarations(self):
        result = parse_string("")
        assert len(result.macro_declarations) == 0

    def test_no_dropds(self):
        result = parse_string("")
        assert len(result.dropds_calls) == 0

    def test_whitespace_only(self):
        result = parse_string("   \n\n  \n   ")
        assert len(result.sql_blocks) == 0


# 6. File with no PROC SQL

class TestNoProcSql:

    def test_only_lets_no_blocks(self):
        sas = "%LET x = 10;\n%LET y = 20;\nDATA work.out; SET work.in; RUN;\n"
        result = parse_string(sas)
        assert len(result.sql_blocks) == 0
        lets = [m for m in result.macro_declarations if m.directive == "LET"]
        assert len(lets) == 2

    def test_data_step_not_captured(self):
        sas = "DATA work.out;\n  SET work.in;\nRUN;\n"
        result = parse_string(sas)
        assert len(result.sql_blocks) == 0

    def test_comment_only_file(self):
        sas = "/* This file has only comments */\n/* Nothing else */\n"
        result = parse_string(sas)
        assert len(result.sql_blocks) == 0


# 7. Edge cases: macro defs, dropds, nested proc sql

class TestMacroDefinitionSkipping:
    """The %macro...%mend block in TC-08 has a nested proc sql; drop table.
    The parser must not pick that up as a real PROC SQL block."""

    MACRO_WITH_NESTED_SQL = (
        "%macro cleanup /parmbuff;\n"
        "  %let num=1;\n"
        "  proc sql; drop table &dsname; quit;\n"
        "%mend cleanup;\n"
        "\n"
        "PROC SQL;\n"
        "  CREATE TABLE real_table AS SELECT 1;\n"
        "QUIT;\n"
    )

    def test_skips_nested_proc_sql_in_macro(self):
        result = parse_string(self.MACRO_WITH_NESTED_SQL)
        assert len(result.sql_blocks) == 1
        assert "real_table" in result.sql_blocks[0].original_sql

    def test_skips_let_inside_macro_def(self):
        result = parse_string(self.MACRO_WITH_NESTED_SQL)
        lets = [m for m in result.macro_declarations if m.directive == "LET"]
        assert len(lets) == 0

    def test_line_numbers_stable_after_macro_stripping(self):
        result = parse_string(self.MACRO_WITH_NESTED_SQL)
        assert result.sql_blocks[0].line_start == 6


class TestDropdsCalls:

    def test_extracts_dropds(self):
        sas = (
            "%_eg_conditional_dropds(WORK.MY_TABLE);\n"
            "PROC SQL;\n  CREATE TABLE WORK.MY_TABLE AS SELECT 1;\nQUIT;\n"
        )
        result = parse_string(sas)
        assert len(result.dropds_calls) == 1
        assert result.dropds_calls[0].table_name == "WORK.MY_TABLE"

    def test_multiple_dropds(self):
        sas = (
            "%_eg_conditional_dropds(WORK.TABLE_A);\n"
            "PROC SQL; SELECT 1; QUIT;\n"
            "%_eg_conditional_dropds(WORK.TABLE_B);\n"
            "PROC SQL; SELECT 2; QUIT;\n"
        )
        result = parse_string(sas)
        assert len(result.dropds_calls) == 2
        names = [d.table_name for d in result.dropds_calls]
        assert "WORK.TABLE_A" in names
        assert "WORK.TABLE_B" in names

    def test_dropds_line_numbers(self):
        sas = (
            "\n\n%_eg_conditional_dropds(WORK.X);\n"
            "PROC SQL; SELECT 1; QUIT;\n"
        )
        result = parse_string(sas)
        assert result.dropds_calls[0].line_number == 3


class TestLetWithComplexValues:

    def test_sysfunc_in_value(self):
        sas = (
            "%LET end_date = %SYSFUNC(INTNX(QTR,today(),-1,END));\n"
            "PROC SQL; SELECT 1; QUIT;\n"
        )
        result = parse_string(sas)
        lets = [m for m in result.macro_declarations if m.directive == "LET"]
        assert len(lets) == 1
        assert "INTNX" in lets[0].value

    def test_multiple_globals_and_lets_on_same_line(self):
        sas = (
            "%GLOBAL gRef;      %LET gRef = 20250531;\n"
            "PROC SQL; SELECT 1; QUIT;\n"
        )
        result = parse_string(sas)
        globs = [m for m in result.macro_declarations if m.directive == "GLOBAL"]
        lets = [m for m in result.macro_declarations if m.directive == "LET"]
        assert len(globs) == 1
        assert len(lets) == 1
        assert globs[0].name == "gRef"
        assert lets[0].name == "gRef"


# 8. File I/O edge cases

class TestFileIO:

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            parse_file(tmp_path / "nonexistent.sas")

    def test_parse_string_source_name(self):
        result = parse_string("PROC SQL; SELECT 1; QUIT;", source_name="test_input")
        assert result.source_file == "test_input"

    def test_parse_file_records_filepath(self, tmp_path):
        f = tmp_path / "sample.sas"
        f.write_text("PROC SQL; SELECT 1; QUIT;")
        result = parse_file(f)
        assert "sample.sas" in result.source_file


# 9. Integration tests against the real test case files

# Hardcoded block counts from MASTER_INDEX.md
EXPECTED_BLOCK_COUNTS = {
    "TC-01_basic_nulls_strings_unions.txt": 5,
    "TC-02_date_functions_choosec_lookups.txt": 3,
    "TC-03_case_when_date_arithmetic_operators.txt": 4,
    "TC-04_quarterly_contracts_right_joins.txt": 5,
    "TC-05_format_lookups_aggregation.txt": 6,
    "TC-06_put_formats_time_slices.txt": 5,
    "TC-07_chained_tables_calculated_having.txt": 6,
    "TC-08_linkages_contains_multijoin.txt": 4,
}


class TestIntegrationWithTestCases:

    @pytest.mark.parametrize("filename,expected_blocks", EXPECTED_BLOCK_COUNTS.items())
    def test_block_count_matches_master_index(self, filename, expected_blocks):
        path = tc_path(filename)
        result = parse_file(path)
        assert len(result.sql_blocks) == expected_blocks, (
            f"{filename}: expected {expected_blocks} blocks, got {len(result.sql_blocks)}"
        )

    @pytest.mark.parametrize("filename", EXPECTED_BLOCK_COUNTS.keys())
    def test_every_block_starts_with_proc_sql(self, filename):
        path = tc_path(filename)
        result = parse_file(path)
        for block in result.sql_blocks:
            assert block.original_sql.strip().upper().startswith("PROC"), (
                f"{filename} block {block.block_number} doesn't start with PROC"
            )

    @pytest.mark.parametrize("filename", EXPECTED_BLOCK_COUNTS.keys())
    def test_every_block_ends_with_quit(self, filename):
        path = tc_path(filename)
        result = parse_file(path)
        for block in result.sql_blocks:
            assert block.original_sql.strip().upper().endswith("QUIT;"), (
                f"{filename} block {block.block_number} doesn't end with QUIT;"
            )

    @pytest.mark.parametrize("filename", EXPECTED_BLOCK_COUNTS.keys())
    def test_block_numbers_are_sequential(self, filename):
        path = tc_path(filename)
        result = parse_file(path)
        numbers = [b.block_number for b in result.sql_blocks]
        assert numbers == list(range(1, len(result.sql_blocks) + 1))

    def test_tc08_skips_macro_definition(self):
        # TC-08 has a macro that drops tables. Make sure we didn't scrape it.
        path = tc_path("TC-08_linkages_contains_multijoin.txt")
        result = parse_file(path)
        assert len(result.sql_blocks) == 4
        for block in result.sql_blocks:
            assert "drop table &dsname" not in block.original_sql.lower()

    def test_tc08_macro_internal_lets_excluded(self):
        """The %let num=1 and %let dsname=... inside the macro def
        should not appear in macro_declarations."""
        path = tc_path("TC-08_linkages_contains_multijoin.txt")
        result = parse_file(path)
        let_names = [m.name for m in result.macro_declarations if m.directive == "LET"]
        assert "num" not in let_names
        assert "dsname" not in let_names

    def test_tc05_has_globals_and_lets(self):
        # TC-05 has lines like: %GLOBAL gRef;      %LET gRef = value;
        path = tc_path("TC-05_format_lookups_aggregation.txt")
        result = parse_file(path)
        globs = [m for m in result.macro_declarations if m.directive == "GLOBAL"]
        lets = [m for m in result.macro_declarations if m.directive == "LET"]
        assert len(globs) == 4
        assert len(lets) == 4

    def test_tc03_has_dropds_calls(self):
        """TC-03 uses %_eg_conditional_dropds before each block."""
        path = tc_path("TC-03_case_when_date_arithmetic_operators.txt")
        result = parse_file(path)
        assert len(result.dropds_calls) == 4
        # Each dropds should reference a WORK. table
        for call in result.dropds_calls:
            assert call.table_name.startswith("WORK.")
