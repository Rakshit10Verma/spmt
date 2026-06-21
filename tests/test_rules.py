"""
Unit tests for the SPMT rule registry.
"""

import re
import pytest

from spmt.rules import (
    ALL_RULES,
    ConversionRule,
    Complexity,
    RuleCategory,
    get_handler_rules,
    get_keep_rules,
    get_regex_rules,
    get_rule_by_id,
    get_rules_by_category,
    rule_summary,
)


# Structural integrity

class TestRuleRegistry:
    """Make sure the registry loads correctly without errors."""

    def test_total_rule_count(self):
        # We should have 46 rules total (42 primary + 4 sub-rules for operators).
        # The master index groups R-28 into one pattern, but I split them up into 5 here 
        # for better precision. Change this if we decide to collapse them back down.
        assert len(ALL_RULES) >= 42, (
            f"Expected at least 42 rules, got {len(ALL_RULES)}"
        )

    def test_no_duplicate_ids(self):
        ids = [r.rule_id for r in ALL_RULES]
        duplicates = {rid for rid in ids if ids.count(rid) > 1}
        assert not duplicates, f"Duplicate rule IDs: {duplicates}"

    def test_all_rules_are_dataclass_instances(self):
        for rule in ALL_RULES:
            assert isinstance(rule, ConversionRule)

    def test_all_patterns_are_compiled_regex(self):
        for rule in ALL_RULES:
            assert isinstance(rule.sas_pattern, re.Pattern), (
                f"{rule.rule_id} pattern is not a compiled regex"
            )

    def test_all_categories_are_valid(self):
        for rule in ALL_RULES:
            assert isinstance(rule.category, RuleCategory), (
                f"{rule.rule_id} has invalid category: {rule.category}"
            )

    def test_all_complexities_are_valid(self):
        for rule in ALL_RULES:
            assert isinstance(rule.complexity, Complexity), (
                f"{rule.rule_id} has invalid complexity: {rule.complexity}"
            )

    def test_replacement_is_string(self):
        for rule in ALL_RULES:
            assert isinstance(rule.oracle_replacement, str), (
                f"{rule.rule_id} replacement is not a string"
            )

    def test_description_not_empty(self):
        for rule in ALL_RULES:
            assert rule.description.strip(), (
                f"{rule.rule_id} has empty description"
            )


# Category coverage

class TestCategoryCoverage:
    """Check that we don't have any empty categories."""

    def test_all_categories_populated(self):
        grouped = get_rules_by_category()
        for cat in RuleCategory:
            assert len(grouped[cat]) > 0, f"Category {cat.value} has no rules"

    def test_category_counts(self):
        grouped = get_rules_by_category()
        assert len(grouped[RuleCategory.NULL_HANDLING]) >= 4
        assert len(grouped[RuleCategory.DATE_FUNCTIONS]) >= 7
        assert len(grouped[RuleCategory.STRING_FUNCTIONS]) >= 5
        assert len(grouped[RuleCategory.MACRO_VARIABLES]) >= 3
        assert len(grouped[RuleCategory.TYPE_CONVERSION]) >= 4
        assert len(grouped[RuleCategory.SAS_KEYWORDS]) >= 11
        assert len(grouped[RuleCategory.TABLE_MAPPING]) >= 5
        assert len(grouped[RuleCategory.JOIN_PATTERNS]) >= 3


# Lookup helpers

class TestLookupHelpers:

    def test_get_rule_by_id_found(self):
        rule = get_rule_by_id("R-01")
        assert rule is not None
        assert rule.rule_id == "R-01"

    def test_get_rule_by_id_not_found(self):
        assert get_rule_by_id("R-99") is None

    def test_handler_rules_all_have_handler_sentinel(self):
        for rule in get_handler_rules():
            assert rule.oracle_replacement == "__HANDLER__"

    def test_regex_rules_have_no_handler_sentinel(self):
        for rule in get_regex_rules():
            assert rule.oracle_replacement not in ("__HANDLER__", "__KEEP__")

    def test_keep_rules_have_keep_sentinel(self):
        for rule in get_keep_rules():
            assert rule.oracle_replacement == "__KEEP__"

    def test_handler_regex_keep_partition_is_complete(self):
        total = len(get_handler_rules()) + len(get_regex_rules()) + len(get_keep_rules())
        assert total == len(ALL_RULES)


# Pattern matching — verify each regex fires on representative SAS input

class TestPatternMatching:
    """Spot check that the regex actually catches the right SAS code."""

    # --- NULL handling ---
    def test_r01_is_missing(self):
        assert get_rule_by_id("R-01").sas_pattern.search("WHERE x IS MISSING")

    def test_r01_is_not_missing(self):
        assert get_rule_by_id("R-01").sas_pattern.search("AND col IS NOT MISSING")

    def test_r11_31dec9999(self):
        assert get_rule_by_id("R-11").sas_pattern.search("t1.close_date = '31Dec9999'd")

    def test_r33_nvl(self):
        assert get_rule_by_id("R-33").sas_pattern.search("NVL(t1.amount, 0)")

    def test_r34_sas_sum(self):
        assert get_rule_by_id("R-34").sas_pattern.search("sum(t1.credit_limit, (-1)*val)")

    # --- Date functions ---
    def test_r05_intnx_begin(self):
        rule = get_rule_by_id("R-05")
        m = rule.sas_pattern.search('INTNX("MONTH", &report_date., 0, "BEGIN")')
        assert m is not None

    def test_r06_intnx_end(self):
        rule = get_rule_by_id("R-06")
        m = rule.sas_pattern.search('INTNX("MONTH", &report_date., 0, "END")')
        assert m is not None

    def test_r07_intnx_n_months(self):
        rule = get_rule_by_id("R-07")
        m = rule.sas_pattern.search("INTNX('month', &report_date, 6)")
        assert m is not None

    def test_r08_intnx_end_alignment(self):
        rule = get_rule_by_id("R-08")
        m = rule.sas_pattern.search("intnx('month', &report_date, -3, 'E')")
        assert m is not None

    def test_r09_today(self):
        assert get_rule_by_id("R-09").sas_pattern.search("WHERE t1.dt < today()")

    def test_r10_date_literal(self):
        rule = get_rule_by_id("R-10")
        assert rule.sas_pattern.search("'1Jan2025'd")
        assert rule.sas_pattern.search("'31Dec2025'd")

    def test_r27_mdy(self):
        rule = get_rule_by_id("R-27")
        assert rule.sas_pattern.search("mdy(&report_month., 1, &report_year.)")

    # --- String functions ---
    def test_r12_upcase(self):
        assert get_rule_by_id("R-12").sas_pattern.search("UPCASE(t1.name)")

    def test_r13_lowcase(self):
        assert get_rule_by_id("R-13").sas_pattern.search("LOWCASE(t1.code)")

    def test_r14_strip(self):
        assert get_rule_by_id("R-14").sas_pattern.search("STRIP(t1.val)")

    def test_r15_compress_basic(self):
        assert get_rule_by_id("R-15").sas_pattern.search("COMPRESS(t1.CONTRACT_NUMBER)")

    def test_r16_compress_kd(self):
        rule = get_rule_by_id("R-16")
        assert rule.sas_pattern.search('COMPRESS(t1.PHONE_RAW, , "kd")')

    # --- Macro variables ---
    def test_r02_macro_var_with_dot(self):
        rule = get_rule_by_id("R-02")
        m = rule.sas_pattern.search("&report_date.")
        assert m is not None
        assert m.group(1) == "report_date"

    def test_r02_macro_var_without_dot(self):
        rule = get_rule_by_id("R-02")
        m = rule.sas_pattern.search("&PERIOD_KEY")
        assert m is not None

    def test_r03_let(self):
        assert get_rule_by_id("R-03").sas_pattern.search("%LET report_date = 20250531;")

    def test_r03_global(self):
        assert get_rule_by_id("R-03").sas_pattern.search("%GLOBAL gReportDate;")

    def test_r37_libname(self):
        rule = get_rule_by_id("R-37")
        assert rule.sas_pattern.search(
            "LIBNAME SOURCE META REPNAME='Foundation' LIBURI=\"SASLibrary\";"
        )

    def test_r37_include(self):
        rule = get_rule_by_id("R-37")
        assert rule.sas_pattern.search('%include "\\\\server\\scripts\\macros\\std.sas";')

    # --- Type conversion ---
    def test_r20_put_numeric_format(self):
        rule = get_rule_by_id("R-20")
        assert rule.sas_pattern.search("PUT(t1.tariff_code, TARIF.)")

    def test_r21_put_char_format(self):
        rule = get_rule_by_id("R-21")
        assert rule.sas_pattern.search("PUT(t1.partner_type_code, $PNRTYP.)")

    def test_r22_put_numeric_width(self):
        rule = get_rule_by_id("R-22")
        assert rule.sas_pattern.search("PUT(t1.PRODUCT_TYPE_CD, 3.)")

    def test_r23_choosec_input(self):
        rule = get_rule_by_id("R-23")
        assert rule.sas_pattern.search(
            'CHOOSEC(INPUT(t1.INTEREST_LOCK_TYPE_CD, 10.), "Fixed", "Variable")'
        )

    # --- SAS keywords ---
    def test_r17_outer_union_corr(self):
        assert get_rule_by_id("R-17").sas_pattern.search("OUTER UNION CORR")

    def test_r18_format_dollar(self):
        assert get_rule_by_id("R-18").sas_pattern.search(" FORMAT=$30.")

    def test_r18_format_eurdfdd(self):
        assert get_rule_by_id("R-18").sas_pattern.search(" FORMAT=EURDFDD10.")

    def test_r19_label(self):
        assert get_rule_by_id("R-19").sas_pattern.search(" LABEL=''")

    def test_r24_calculated(self):
        rule = get_rule_by_id("R-24")
        m = rule.sas_pattern.search("HAVING (CALCULATED total_available_credit) > 750000")
        assert m is not None
        assert m.group(1) == "total_available_credit"

    def test_r25_name_literal(self):
        rule = get_rule_by_id("R-25")
        m = rule.sas_pattern.search("t1.'Linkage Type'n")
        assert m is not None
        assert m.group(1) == "Linkage Type"

    def test_r26_not_equals(self):
        assert get_rule_by_id("R-26").sas_pattern.search("NOT = '09'")

    def test_r28_gt(self):
        assert get_rule_by_id("R-28").sas_pattern.search("t2.valid_to gt")

    def test_r28b_le(self):
        assert get_rule_by_id("R-28b").sas_pattern.search("t2.valid_from le")

    def test_r36_contains(self):
        rule = get_rule_by_id("R-36")
        m = rule.sas_pattern.search("t1.OFFICE_NAME CONTAINS 'Mountain'")
        assert m is not None
        assert m.group(1) == "Mountain"

    def test_r41_conditional_dropds(self):
        rule = get_rule_by_id("R-41")
        m = rule.sas_pattern.search("%_eg_conditional_dropds(WORK.CONTRACT_SPLITS);")
        assert m is not None
        assert m.group(1) == "WORK.CONTRACT_SPLITS"

    # --- Table mapping ---
    def test_r35_work_table(self):
        rule = get_rule_by_id("R-35")
        m = rule.sas_pattern.search("FROM WORK.ACTIVE_CONTRACTS t1")
        assert m is not None
        assert m.group(1) == "ACTIVE_CONTRACTS"

    # --- Join patterns ---
    def test_r32_right_join(self):
        assert get_rule_by_id("R-32").sas_pattern.search("RIGHT JOIN work.contracts t1")

    def test_r42_max_subquery(self):
        assert get_rule_by_id("R-42").sas_pattern.search(
            "INNER JOIN ( SELECT MAX(PERIOD_KEY)"
        )


# ---------------------------------------------------------------------------
# Regex replacement — verify simple (non-handler) rules produce correct output
# ---------------------------------------------------------------------------

class TestRegexReplacements:
    """For rules with direct regex replacements, verify the substitution output."""

    def test_r01_is_missing_to_is_null(self):
        rule = get_rule_by_id("R-01")
        # R-01 is now handler-based; verify pattern matches
        assert rule.sas_pattern.search("WHERE x IS MISSING")
        assert rule.oracle_replacement == "__HANDLER__"

    def test_r09_today_to_sysdate(self):
        rule = get_rule_by_id("R-09")
        result = rule.sas_pattern.sub(rule.oracle_replacement, "WHERE dt < today()")
        assert result == "WHERE dt < TRUNC(SYSDATE)"

    def test_r12_upcase_to_upper(self):
        rule = get_rule_by_id("R-12")
        result = rule.sas_pattern.sub(rule.oracle_replacement, "UPCASE(t1.name)")
        assert result == "UPPER(t1.name)"

    def test_r13_lowcase_to_lower(self):
        rule = get_rule_by_id("R-13")
        result = rule.sas_pattern.sub(rule.oracle_replacement, "LOWCASE(t1.code)")
        assert result == "LOWER(t1.code)"

    def test_r14_strip_to_trim(self):
        rule = get_rule_by_id("R-14")
        result = rule.sas_pattern.sub(rule.oracle_replacement, "STRIP(t1.val)")
        assert result == "TRIM(t1.val)"

    def test_r17_outer_union_corr_to_union_all(self):
        rule = get_rule_by_id("R-17")
        result = rule.sas_pattern.sub(rule.oracle_replacement, "OUTER UNION CORR")
        assert result == "UNION ALL"

    def test_r26_not_equals(self):
        rule = get_rule_by_id("R-26")
        result = rule.sas_pattern.sub(rule.oracle_replacement, "WHERE x NOT = 0")
        assert result == "WHERE x <> 0"

    def test_r28_gt_to_greater_than(self):
        rule = get_rule_by_id("R-28")
        result = rule.sas_pattern.sub(rule.oracle_replacement, "t1.amount gt 10")
        assert result == "t1.amount > 10"

    def test_r28b_le_to_less_equal(self):
        rule = get_rule_by_id("R-28b")
        result = rule.sas_pattern.sub(rule.oracle_replacement, "t2.valid_from le sysdate")
        assert result == "t2.valid_from <= sysdate"

    def test_r36_contains_to_like(self):
        rule = get_rule_by_id("R-36")
        result = rule.sas_pattern.sub(
            rule.oracle_replacement, "t1.NAME CONTAINS 'Mountain'"
        )
        assert result == "t1.NAME LIKE '%Mountain%'"

    def test_r05_intnx_begin_to_trunc(self):
        rule = get_rule_by_id("R-05")
        result = rule.sas_pattern.sub(
            rule.oracle_replacement,
            'INTNX("MONTH", &report_date., 0, "BEGIN")',
        )
        assert "TRUNC(" in result
        assert "'MM'" in result

    def test_r06_intnx_end_to_last_day(self):
        rule = get_rule_by_id("R-06")
        result = rule.sas_pattern.sub(
            rule.oracle_replacement,
            'INTNX("MONTH", &report_date., 0, "END")',
        )
        assert "LAST_DAY(" in result

    def test_r08_intnx_e_alignment(self):
        rule = get_rule_by_id("R-08")
        result = rule.sas_pattern.sub(
            rule.oracle_replacement,
            "intnx('month', &report_date, -3, 'E')",
        )
        assert "LAST_DAY(ADD_MONTHS(" in result

    def test_r22_put_width_to_to_char(self):
        rule = get_rule_by_id("R-22")
        result = rule.sas_pattern.sub(
            rule.oracle_replacement,
            "PUT(t1.PRODUCT_TYPE_CD, 3.)",
        )
        assert result == "TO_CHAR(t1.PRODUCT_TYPE_CD)"

    def test_r18_format_removed(self):
        rule = get_rule_by_id("R-18")
        result = rule.sas_pattern.sub(
            rule.oracle_replacement,
            "t1.score FORMAT=COMMAX20. AS score",
        )
        assert "FORMAT" not in result
        assert "AS score" in result


# ---------------------------------------------------------------------------
# Summary output
# ---------------------------------------------------------------------------

class TestSummary:

    def test_summary_returns_string(self):
        s = rule_summary()
        assert isinstance(s, str)
        assert "Total rules:" in s

    def test_summary_mentions_all_categories(self):
        s = rule_summary()
        for cat in RuleCategory:
            assert cat.value in s
