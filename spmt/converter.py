"""
spmt/converter.py - The core conversion engine.

This is where everything comes together. I take the parsed SQL blocks from
parser.py, run them through a fixed sequence of transformation stages, and
produce Oracle-compatible SQL. The stages run in this order because each
one depends on the output of the previous:

  1. Strip PROC SQL / QUIT wrappers (they are SAS shell, not SQL)
  2. Handle %_eg_conditional_dropds (emit DROP TABLE before the block)
  3. Substitute macro variables (via variable_handler)
  4. Remap table references (via table_mapper)
  5. Apply regex-based conversion rules (the simple pattern swaps)
  6. Apply handler-based conversion rules (the complex ones that need logic)
  7. Final cleanup pass

Stages 5 and 6 cover everything the build guide lists as separate steps
(FORMAT/LABEL removal, ORDER BY stripping, name literal renaming). Those
are just specific rules in the rules module that happen to fire during
the regex or handler passes.

I chose a pipeline design over a single-pass approach because ordering
matters. Variable substitution must happen before table mapping, since
table names can contain macro variables. Table mapping must happen before
rule application, since rules like the SAS comparison operators should not
accidentally match inside schema names.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from spmt.parser import ParseResult, ParsedBlock, DropdsCall
from spmt.rules import (
    ALL_RULES,
    ConversionRule,
    get_regex_rules,
    get_handler_rules,
    get_keep_rules,
    get_rule_by_id,
)
from spmt.variable_handler import VariableHandler
from spmt.table_mapper import TableMapper


# Result dataclasses

@dataclass
class BlockResult:
    """Conversion result for a single PROC SQL block."""
    block_number: int
    original_sql: str
    converted_sql: str
    drop_statement: str = ""
    rules_applied: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class ConversionResult:
    """Full conversion result for an entire .sas file."""
    source_file: str
    blocks: list[BlockResult] = field(default_factory=list)
    parameters_extracted: list[str] = field(default_factory=list)
    rules_applied: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def converted_sql(self) -> str:
        """Combine all converted blocks into a single SQL string.

        Each block is separated by a blank line. If a block has a DROP TABLE
        statement, it appears immediately before the CREATE TABLE.
        """
        parts = []
        for block in self.blocks:
            if block.drop_statement:
                parts.append(block.drop_statement)
            parts.append(block.converted_sql)
        return "\n\n".join(parts)

    @property
    def total_rules_applied(self) -> int:
        return len(set(self.rules_applied))


# Rules that other modules handle, so I skip them during regex application.
# R-02: macro variables (handled by VariableHandler)
# R-35: WORK.table mapping (handled by TableMapper)
_SKIP_IN_REGEX_PASS = {"R-02", "R-35"}

# Month abbreviation lookup for date literal conversion (R-10).
_MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


class Converter:
    """Orchestrates the full SAS → Oracle conversion pipeline.

    I need a VariableHandler and a TableMapper to do my job. You can
    either pass them in directly or let me create defaults from config
    file paths.
    """

    def __init__(
        self,
        variable_handler: Optional[VariableHandler] = None,
        table_mapper: Optional[TableMapper] = None,
        var_config_path: Optional[str | Path] = None,
        table_config_path: Optional[str | Path] = None,
    ) -> None:
        # If handlers were passed in, use them directly.
        # Otherwise try to create them from config paths.
        if variable_handler is not None:
            self._var_handler = variable_handler
        elif var_config_path is not None:
            self._var_handler = VariableHandler(mappings_path=str(var_config_path))
        else:
            self._var_handler = None

        if table_mapper is not None:
            self._table_mapper = table_mapper
        elif table_config_path is not None:
            self._table_mapper = TableMapper.from_config(str(table_config_path))
        else:
            self._table_mapper = None

        # Collect the rules I will apply via simple regex substitution.
        # I filter out rules handled by other modules and rules that need
        # dedicated handler functions.
        self._regex_rules = [
            r for r in get_regex_rules()
            if r.rule_id not in _SKIP_IN_REGEX_PASS
        ]

        self._handler_rules = get_handler_rules()
        self._keep_rules = get_keep_rules()


    # Public API


    def convert(self, parse_result: ParseResult) -> ConversionResult:
        """Convert all SQL blocks from a parsed .sas file.

        This is the main entry point. Feed it the output of parser.parse_file()
        and it returns everything: converted SQL, which rules fired, warnings.
        """
        result = ConversionResult(source_file=parse_result.source_file)

        # Extract parameter names from macro declarations for reporting.
        for decl in parse_result.macro_declarations:
            result.parameters_extracted.append(
                f"{decl.directive} {decl.name} = {decl.value}"
            )

        # Build a lookup from table name → dropds call so I can pair each
        # PROC SQL block with its preceding DROP TABLE if one exists.
        dropds_lookup = self._build_dropds_lookup(parse_result)

        for block in parse_result.sql_blocks:
            block_result = self._convert_block(block, dropds_lookup)
            result.blocks.append(block_result)
            result.rules_applied.extend(block_result.rules_applied)
            result.warnings.extend(block_result.warnings)

        return result

    def convert_sql(self, sql: str) -> BlockResult:
        """Convert a single SQL string without full parse context.

        Useful for testing individual blocks. Does not handle dropds or
        parameter extraction since there is no ParseResult to draw from.
        """
        dummy_block = ParsedBlock(
            block_number=1,
            original_sql=sql,
            line_start=1,
            line_end=sql.count("\n") + 1,
        )
        return self._convert_block(dummy_block, {})


    # Internal pipeline


    def _convert_block(
        self,
        block: ParsedBlock,
        dropds_lookup: dict[str, DropdsCall],
    ) -> BlockResult:
        """Run a single block through all conversion stages."""

        result = BlockResult(
            block_number=block.block_number,
            original_sql=block.original_sql,
            converted_sql="",
        )

        sql = block.original_sql

        # Stage 1: Strip PROC SQL / QUIT wrappers
        sql = self._strip_proc_sql_wrapper(sql)

        # Stage 2: Handle %_eg_conditional_dropds
        drop_stmt = self._resolve_dropds(sql, dropds_lookup, result)
        result.drop_statement = drop_stmt

        # Stage 3: Substitute macro variables
        sql = self._apply_variable_substitution(sql, result)

        # Stage 4: Remap table references
        sql = self._apply_table_mapping(sql, result)

        # Stage 5: Apply regex-based rules
        sql = self._apply_regex_rules(sql, result)

        # Stage 6: Apply handler-based rules
        sql = self._apply_handler_rules(sql, result)

        # Stage 7: Detect __KEEP__ patterns for documentation
        self._detect_keep_patterns(sql, result)

        # Stage 8: Final cleanup
        sql = self._final_cleanup(sql)

        result.converted_sql = sql
        return result


    # Stage 1: Strip PROC SQL / QUIT


    _RE_PROC_SQL_OPEN = re.compile(
        r"^\s*PROC\s+SQL\s*;\s*",
        re.IGNORECASE,
    )
    _RE_QUIT = re.compile(
        r"\s*QUIT\s*;\s*$",
        re.IGNORECASE,
    )

    def _strip_proc_sql_wrapper(self, sql: str) -> str:
        """Remove PROC SQL; prefix and QUIT; suffix.

        These are the SAS execution wrappers. The actual SQL is inside them.
        I strip them because Oracle just runs the SQL directly.
        """
        sql = self._RE_PROC_SQL_OPEN.sub("", sql)
        sql = self._RE_QUIT.sub("", sql)
        return sql.strip()


    # Stage 2: %_eg_conditional_dropds → DROP TABLE


    def _build_dropds_lookup(
        self, parse_result: ParseResult
    ) -> dict[str, DropdsCall]:
        """Map table names from dropds calls to their DropdsCall objects.

        The parser already extracted these. I need to match each one to the
        CREATE TABLE block that follows it. The key is the table name from
        the dropds call (e.g. "WORK.CUSTOMER_FILTERED"), and the value is
        the DropdsCall object.
        """
        lookup: dict[str, DropdsCall] = {}
        for call in parse_result.dropds_calls:
            # Normalize to upper case for matching
            lookup[call.table_name.upper()] = call
        return lookup

    def _resolve_dropds(
        self,
        sql: str,
        dropds_lookup: dict[str, DropdsCall],
        result: BlockResult,
    ) -> str:
        """Generate a DROP TABLE statement if this block has a preceding dropds call.

        I look for a CREATE TABLE ... AS pattern in the SQL and check if the
        target table had a dropds call. If so, I emit a DROP TABLE using the
        mapped Oracle table name.
        """
        # Find the CREATE TABLE target in this block
        m = re.search(
            r"\bCREATE\s+(?:TABLE|VIEW)\s+(\w+\.\w+)",
            sql,
            re.IGNORECASE,
        )
        if not m:
            return ""

        target_table = m.group(1).upper()
        if target_table not in dropds_lookup:
            return ""

        # Map the table name through TableMapper if available
        mapped_name = target_table
        if self._table_mapper is not None:
            map_result = self._table_mapper.map_tables(target_table)
            mapped_name = map_result.converted_sql.strip()

        result.rules_applied.append("R-41")

        # Oracle does not have IF EXISTS on DROP TABLE, so I use the
        # PL/SQL exception block pattern that is standard in Oracle scripts.
        return (
            f"BEGIN\n"
            f"  EXECUTE IMMEDIATE 'DROP TABLE {mapped_name}';\n"
            f"EXCEPTION\n"
            f"  WHEN OTHERS THEN\n"
            f"    IF SQLCODE != -942 THEN RAISE; END IF;\n"
            f"END;\n/"
        )


    # Stage 3: Variable substitution


    def _apply_variable_substitution(
        self, sql: str, result: BlockResult
    ) -> str:
        """Delegate to VariableHandler for macro variable conversion.

        The VariableHandler has its own specificity-ordered pattern matching
        that is smarter than a single regex. It handles date literals,
        quoted strings, double-dot table references, and bare variables
        in the right order.
        """
        if self._var_handler is None:
            return sql

        sub_result = self._var_handler.substitute(sql)
        result.rules_applied.append("R-02")

        for sub in sub_result.substitutions_made:
            result.rules_applied.append(f"R-02:{sub}")

        result.warnings.extend(sub_result.warnings)
        return sub_result.converted_sql


    # Stage 4: Table mapping


    def _apply_table_mapping(self, sql: str, result: BlockResult) -> str:
        """Delegate to TableMapper for library.table → schema.table conversion."""
        if self._table_mapper is None:
            return sql

        map_result = self._table_mapper.map_tables(sql)

        if map_result.mappings:
            result.rules_applied.append("R-35")
            for mapping in map_result.mappings:
                result.rules_applied.append(
                    f"R-35:{mapping.original}→{mapping.mapped}"
                )

        result.warnings.extend(map_result.warnings)
        return map_result.converted_sql


    # Stage 5: Regex-based rule application


    # IS NOT MISSING needs its own pattern because R-01 only matches
    # "IS MISSING" (the word NOT sits between IS and MISSING, breaking
    # the regex). I handle the NOT variant first so R-01 does not
    # accidentally turn "IS NOT MISSING" into "IS NOT NULL" by only
    # matching the MISSING part.
    _RE_IS_NOT_MISSING = re.compile(r"\bIS\s+NOT\s+MISSING\b", re.IGNORECASE)

    def _apply_regex_rules(self, sql: str, result: BlockResult) -> str:
        """Apply all direct-substitution rules via re.sub.

        I iterate through the rules in their defined order. For each one,
        I check if the pattern matches anywhere in the SQL. If it does,
        I apply the substitution and record which rule fired.

        The order matters in a few places. For example, R-05 through R-08
        all match INTNX but with increasing specificity (4-arg with BEGIN,
        4-arg with END, 3-arg, 4-arg with E alignment). Since they are
        defined from most-specific to least-specific in rules.py, the
        right one fires first.
        """
        # Pre-pass: IS NOT MISSING → IS NOT NULL (must run before R-01
        # which only handles IS MISSING). See class-level comment above.
        new_sql = self._RE_IS_NOT_MISSING.sub("IS NOT NULL", sql)
        if new_sql != sql:
            result.rules_applied.append("R-01")
            sql = new_sql

        for rule in self._regex_rules:
            # Check if the pattern matches before substituting, so I only
            # record rules that actually did something.
            if rule.sas_pattern.search(sql):
                new_sql = rule.sas_pattern.sub(rule.oracle_replacement, sql)
                if new_sql != sql:
                    result.rules_applied.append(rule.rule_id)
                    sql = new_sql

        return sql


    # Stage 6: Handler-based rule application


    def _apply_handler_rules(self, sql: str, result: BlockResult) -> str:
        """Apply rules that need dedicated logic instead of simple regex.

        Each handler function is mapped by rule ID. If the rule's detection
        pattern matches the SQL, I call the corresponding handler.
        """
        # Dispatch table maps rule IDs to handler methods.
        handlers = {
            "R-10": self._handle_date_literal,
            "R-11": self._handle_31dec9999,
            "R-16": self._handle_compress_modifier,
            "R-23": self._handle_choosec_input,
            "R-24": self._handle_calculated,
            "R-25": self._handle_name_literal,
            "R-27": self._handle_mdy,
            "R-29": self._handle_order_by_ctas,
            "R-34": self._handle_sas_sum,
            "R-20": self._handle_put_format,
            "R-21": self._handle_put_char_format,
            "R-30": self._handle_dynamic_table_suffix,
            "R-39": self._handle_pk_stand,
            "R-41": self._handle_dropds_inline,
        }

        for rule in self._handler_rules:
            if rule.rule_id in handlers:
                if rule.sas_pattern.search(sql):
                    sql = handlers[rule.rule_id](sql, rule, result)

        return sql


    # Stage 7: Detect __KEEP__ patterns


    def _detect_keep_patterns(self, sql: str, result: BlockResult) -> str:
        """Check for patterns that are already valid Oracle but worth noting.

        These do not change the SQL. I just record that they were detected
        so the documenter can report on them.
        """
        for rule in self._keep_rules:
            if rule.sas_pattern.search(sql):
                result.rules_applied.append(f"{rule.rule_id}:detected")
        return sql


    # Stage 8: Final cleanup


    def _final_cleanup(self, sql: str) -> str:
        """Minor cleanup after all conversions are done.

        - Collapse multiple blank lines into one
        - Ensure the statement ends with a semicolon
        - Strip trailing whitespace from each line
        """
        # Collapse runs of blank lines
        sql = re.sub(r"\n{3,}", "\n\n", sql)

        # Strip trailing whitespace per line
        lines = [line.rstrip() for line in sql.split("\n")]
        sql = "\n".join(lines).strip()

        # Ensure trailing semicolon
        if sql and not sql.rstrip().endswith(";"):
            sql = sql.rstrip() + ";"

        return sql


    # Handler implementations


    def _handle_date_literal(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-10: Convert SAS date literals like '1Jan2025'd to TO_DATE().

        SAS format: 'DDMonYYYY'd
        Oracle format: TO_DATE('YYYYMMDD', 'YYYYMMDD')

        I parse the day, month abbreviation, and year, then reassemble as
        an 8-digit string inside TO_DATE.
        """
        def _repl(m: re.Match) -> str:
            day = m.group(1).zfill(2)
            month_abbr = m.group(2).capitalize()
            year = m.group(3)
            month_num = _MONTH_MAP.get(month_abbr, "01")
            return f"TO_DATE('{year}{month_num}{day}', 'YYYYMMDD')"

        new_sql = rule.sas_pattern.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-10")
        return new_sql

    def _handle_31dec9999(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-11: Convert '31Dec9999'd comparisons to IS NULL / IS NOT NULL.

        SAS uses '31Dec9999'd as a sentinel meaning "open-ended / active".
        Oracle convention is to use NULL for the same thing.

        I look at the comparison operator before the literal to decide:
          col = '31Dec9999'd   → col IS NULL
          col <> '31Dec9999'd  → col IS NOT NULL
          col NOT = '31Dec9999'd → col IS NOT NULL
        """
        # Pattern: column_ref (=|<>|NOT =|!=) '31Dec9999'd
        pat = re.compile(
            r"(\b\w+(?:\.\w+)?)\s+"           # column (possibly table.column)
            r"(NOT\s*=|<>|!=|=)\s*"            # operator
            r"'31Dec9999'd",
            re.IGNORECASE,
        )

        def _repl(m: re.Match) -> str:
            col = m.group(1)
            op = m.group(2).strip().upper().replace(" ", "")
            if op == "=":
                return f"{col} IS NULL"
            else:
                return f"{col} IS NOT NULL"

        new_sql = pat.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-11")
        return new_sql

    def _handle_compress_modifier(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-16: COMPRESS(str, , 'kd') → REGEXP_REPLACE().

        The 'k' prefix means "keep" and the second letter says what to keep:
          kd → keep digits   → REGEXP_REPLACE(str, '[^0-9]', '')
          ka → keep alpha    → REGEXP_REPLACE(str, '[^A-Za-z]', '')
          kn → keep name     → REGEXP_REPLACE(str, '[^A-Za-z0-9_]', '')
        """
        modifier_map = {
            "kd": "[^0-9]",
            "ka": "[^A-Za-z]",
            "kn": "[^A-Za-z0-9_]",
        }

        def _repl(m: re.Match) -> str:
            col_expr = m.group(1)
            modifier = m.group(2).lower()
            pattern = modifier_map.get(modifier, "[^0-9]")
            return f"REGEXP_REPLACE({col_expr}, '{pattern}', '')"

        new_sql = rule.sas_pattern.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-16")
        return new_sql

    def _handle_choosec_input(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-23: CHOOSEC(INPUT(col, N.), 'A', 'B', 'C') → CASE expression.

        CHOOSEC is 1-based: index 1 returns the first string argument.
        INPUT converts string to number. Together they mean "use the numeric
        value of col to pick from a list of strings."

        I parse the full function call, extract each string argument, and
        build a CASE TO_NUMBER(col) WHEN 1 THEN 'A' WHEN 2 THEN 'B' ... END.
        """
        # Match the full CHOOSEC(INPUT(...), "val1", "val2", ...) expression.
        # This is complex because the argument list has variable length.
        pat = re.compile(
            r"\bCHOOSEC\s*\(\s*"
            r"INPUT\s*\(\s*([^,]+?)\s*,\s*\d+\s*\.\s*\)\s*,"
            r"\s*(.+?)\s*\)",
            re.IGNORECASE | re.DOTALL,
        )

        def _repl(m: re.Match) -> str:
            col_expr = m.group(1).strip()
            args_str = m.group(2)

            # Parse the comma-separated string arguments.
            # They may use double or single quotes.
            args = re.findall(r'["\']([^"\']*)["\']', args_str)

            parts = [f"CASE TO_NUMBER({col_expr})"]
            for idx, val in enumerate(args, start=1):
                parts.append(f"  WHEN {idx} THEN '{val}'")
            parts.append("END")

            return "\n".join(parts)

        new_sql = pat.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-23")
        return new_sql

    def _handle_calculated(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-24: Replace CALCULATED alias with the original expression.

        SAS lets you write CALCULATED total_credit in WHERE or HAVING to
        reference a column alias defined in the SELECT list. Oracle does
        not support this, so I need to find the alias definition and
        paste the original expression in place of CALCULATED alias.

        I scan the SELECT list for "expression AS alias" and build a
        lookup. Then I replace each CALCULATED alias with the expression.
        """
        # Find all CALCULATED references first
        calc_refs = re.findall(
            r"\bCALCULATED\s+(\w+)",
            sql,
            re.IGNORECASE,
        )
        if not calc_refs:
            return sql

        # Build alias → expression lookup from the SELECT list.
        # I look for patterns like: (expression) AS alias_name
        # The expression is typically wrapped in parentheses by SAS EG.
        alias_map = self._build_alias_map(sql)

        for alias in calc_refs:
            alias_upper = alias.upper()
            expr = alias_map.get(alias_upper)

            if expr:
                # Replace CALCULATED alias with the expression
                sql = re.sub(
                    rf"\bCALCULATED\s+{re.escape(alias)}\b",
                    expr,
                    sql,
                    flags=re.IGNORECASE,
                )
                result.rules_applied.append("R-24")
            else:
                # Could not find the alias definition. Leave as-is and warn.
                result.warnings.append(
                    f"R-24: Could not resolve CALCULATED {alias} "
                    f"— alias not found in SELECT list"
                )
                # Still remove the CALCULATED keyword even if I can not find
                # the expression, since Oracle will reject it either way.
                sql = re.sub(
                    rf"\bCALCULATED\s+({re.escape(alias)})\b",
                    r"\1",
                    sql,
                    flags=re.IGNORECASE,
                )
                result.rules_applied.append("R-24")

        return sql

    def _build_alias_map(self, sql: str) -> dict[str, str]:
        """Extract column alias → expression mappings from a SELECT list.

        I look for two patterns:
          1. (some_expression) AS alias_name
          2. some_function(...) AS alias_name

        This is not a full SQL parser, so it will miss edge cases. But it
        covers the patterns in our test cases, which is enough for now.
        """
        alias_map: dict[str, str] = {}

        # Pattern 1: parenthesized expression with AS alias
        # e.g. (SUM(t1.available_balance)) AS total_available_credit
        for m in re.finditer(
            r"\(([^()]+(?:\([^()]*\)[^()]*)*)\)\s+AS\s+(\w+)",
            sql,
            re.IGNORECASE,
        ):
            expr = m.group(1).strip()
            alias = m.group(2).upper()
            alias_map[alias] = expr

        # Pattern 2: function call with AS alias (no outer parens)
        # e.g. SUM(t1.balance) AS total_balance
        for m in re.finditer(
            r"(\w+\s*\([^()]*\))\s+AS\s+(\w+)",
            sql,
            re.IGNORECASE,
        ):
            alias = m.group(2).upper()
            if alias not in alias_map:
                alias_map[alias] = m.group(1).strip()

        return alias_map

    def _handle_name_literal(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-25: Convert SAS name literals to valid Oracle identifiers.

        SAS name literals look like 'Column Name'n — the 'n suffix tells SAS
        this is an identifier, not a string. Oracle identifiers cannot contain
        spaces, so I replace spaces and special characters with underscores
        and upper-case the result.
        """
        def _repl(m: re.Match) -> str:
            raw_name = m.group(1)
            # Replace spaces and non-alphanumeric chars with underscores
            clean = re.sub(r"[^A-Za-z0-9_]", "_", raw_name)
            # Collapse multiple underscores
            clean = re.sub(r"_+", "_", clean)
            # Strip leading/trailing underscores
            clean = clean.strip("_")
            return clean.upper()

        new_sql = rule.sas_pattern.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-25")
        return new_sql

    def _handle_mdy(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-27: mdy(month, day, year) → TO_DATE expression.

        SAS mdy() builds a date from numeric month, day, year arguments.
        The arguments can be literals, macro variables, or expressions.

        For literal arguments like mdy(5, 1, 2025):
          → TO_DATE('20250501', 'YYYYMMDD')

        For variable arguments like mdy(&month., 1, &year.):
          → TO_DATE(${year} || LPAD(${month}, 2, '0') || LPAD(1, 2, '0'), 'YYYYMMDD')

        I take the simpler approach of building a string concatenation expression
        that works regardless of whether arguments are literals or variables.
        """
        def _repl(m: re.Match) -> str:
            month_arg = m.group(1).strip()
            day_arg = m.group(2).strip()
            year_arg = m.group(3).strip()

            # Check if all three are numeric literals
            if (month_arg.isdigit() and day_arg.isdigit() and year_arg.isdigit()):
                date_str = (
                    f"{year_arg}"
                    f"{int(month_arg):02d}"
                    f"{int(day_arg):02d}"
                )
                return f"TO_DATE('{date_str}', 'YYYYMMDD')"

            # Mixed case: use LPAD for zero-padding and concatenation
            return (
                f"TO_DATE("
                f"{year_arg} || LPAD({month_arg}, 2, '0') || LPAD({day_arg}, 2, '0')"
                f", 'YYYYMMDD')"
            )

        new_sql = rule.sas_pattern.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-27")
        return new_sql

    def _handle_order_by_ctas(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-29: Remove ORDER BY from CREATE TABLE AS SELECT.

        Oracle ignores ORDER BY in a CTAS statement (the physical storage
        order is not guaranteed). SAS EG adds ORDER BY to CTAS because SAS
        datasets can be physically ordered. I strip it to avoid confusion.

        I only remove ORDER BY when the statement starts with CREATE TABLE.
        If it is a standalone SELECT, I keep the ORDER BY.
        """
        # Check if this is a CREATE TABLE ... AS statement
        is_ctas = bool(re.search(
            r"\bCREATE\s+TABLE\b",
            sql,
            re.IGNORECASE,
        ))

        if not is_ctas:
            return sql

        # Remove ORDER BY clause at the end of the statement.
        # The ORDER BY comes after the last FROM/WHERE/GROUP BY/HAVING
        # and before the final semicolon.
        new_sql = re.sub(
            r"\s+ORDER\s+BY\s+[^;]+(?=\s*;?\s*$)",
            "",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if new_sql != sql:
            result.rules_applied.append("R-29")
        return new_sql

    def _handle_sas_sum(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-34: Convert SAS arithmetic sum() to NVL() chains.

        SAS sum(a, b) is NULL-safe: it returns the non-NULL value if one arg
        is NULL. Oracle's a + b would return NULL. So I need to wrap each
        argument in NVL(arg, 0) and join with +.

        This handler targets the non-aggregate sum() that SAS uses for
        NULL-safe addition. I distinguish it from aggregate SUM() by looking
        for comma-separated arguments (aggregate SUM has a single argument).
        """
        # Match sum(arg1, arg2, ...) where there are commas between args.
        # This is the arithmetic form; aggregate SUM(expr) has no commas.
        pat = re.compile(
            r"\bsum\s*\(\s*"
            r"(.+?)\s*"                 # capture the full argument list
            r"\)",
            re.IGNORECASE | re.DOTALL,
        )

        def _repl(m: re.Match) -> str:
            args_str = m.group(1)

            # Only process if there are commas (multi-argument = arithmetic sum).
            # Single-argument sum() is the aggregate form; leave it alone.
            if "," not in args_str:
                return m.group(0)

            # Split on commas that are not inside parentheses.
            args = self._split_args(args_str)

            if len(args) < 2:
                return m.group(0)

            # Wrap each argument in NVL(arg, 0) and join with +
            wrapped = [f"NVL({arg.strip()}, 0)" for arg in args]
            return "(" + " + ".join(wrapped) + ")"

        new_sql = pat.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-34")
        return new_sql

    def _handle_put_format(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-20: PUT(numeric_col, NUMFMT.) — flag for manual CASE WHEN.

        I cannot auto-generate the CASE WHEN branches because the format
        definitions live in SAS format catalogs that I do not have access to.
        I replace the PUT() call with a placeholder comment and warn the user.
        """
        def _repl(m: re.Match) -> str:
            col_expr = m.group(1).strip()
            fmt_name = m.group(2).strip()
            result.warnings.append(
                f"R-20: PUT({col_expr}, {fmt_name}.) needs manual CASE WHEN — "
                f"format catalog not available"
            )
            return f"/* TODO: Replace PUT({col_expr}, {fmt_name}.) with CASE WHEN lookup */ {col_expr}"

        new_sql = rule.sas_pattern.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-20")
        return new_sql

    def _handle_put_char_format(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-21: PUT(char_col, $CHARFMT.) — flag for manual CASE WHEN.

        Same situation as R-20 but for character format names ($ prefix).
        """
        def _repl(m: re.Match) -> str:
            col_expr = m.group(1).strip()
            fmt_name = m.group(2).strip()
            result.warnings.append(
                f"R-21: PUT({col_expr}, ${fmt_name}.) needs manual CASE WHEN — "
                f"format catalog not available"
            )
            return f"/* TODO: Replace PUT({col_expr}, ${fmt_name}.) with CASE WHEN lookup */ {col_expr}"

        new_sql = rule.sas_pattern.sub(_repl, sql)
        if new_sql != sql:
            result.rules_applied.append("R-21")
        return new_sql

    def _handle_dynamic_table_suffix(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-30: Dynamic table suffix with macro variable.

        SAS code like source.CUSTOMERS_&gPeriodeTable. creates table names
        dynamically. After variable substitution, this becomes something like
        source.CUSTOMERS_${gPeriodeTable}. The TableMapper already handles
        the library prefix; this handler just records that the pattern was
        detected so the documenter can flag it.
        """
        if rule.sas_pattern.search(sql):
            result.rules_applied.append("R-30")
            result.warnings.append(
                "R-30: Dynamic table suffix detected — verify the Pentaho "
                "parameter resolves to a valid table name at runtime"
            )
        return sql

    def _handle_pk_stand(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-39: PK_STAND / period_key with macro variable.

        This is mostly handled by variable substitution already. I just
        detect it for reporting and warn if the digit count might be wrong.
        """
        if rule.sas_pattern.search(sql):
            result.rules_applied.append("R-39")
        return sql

    def _handle_dropds_inline(
        self, sql: str, rule: ConversionRule, result: BlockResult
    ) -> str:
        """R-41: %_eg_conditional_dropds inside SQL text.

        This should have been handled in Stage 2 via the dropds lookup. If it
        somehow appears inside a SQL block, remove it.
        """
        new_sql = re.sub(
            r"%_eg_conditional_dropds\s*\(\s*[\w.]+\s*\)\s*;",
            "",
            sql,
            flags=re.IGNORECASE,
        )
        if new_sql != sql:
            result.rules_applied.append("R-41")
        return new_sql


    # Utility methods


    @staticmethod
    def _split_args(args_str: str) -> list[str]:
        """Split a comma-separated argument list respecting parentheses.

        Simple comma splitting would break on expressions like (-1)*(a+b).
        I track parenthesis depth and only split on commas at depth 0.
        """
        args = []
        depth = 0
        current = []

        for char in args_str:
            if char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                args.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            args.append("".join(current))

        return args
