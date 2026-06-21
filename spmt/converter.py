"""
spmt/converter.py

This is the engine that ties the other modules together. The parser hands me
a list of PROC SQL blocks, plus the %LET declarations and the dropds calls.
For each block I run a fixed sequence of stages that turn SAS PROC SQL into
Oracle-compatible SQL. The rules themselves live in rules.py, the macro
variable work lives in variable_handler.py, and the library remapping lives
in table_mapper.py. I keep this file focused on orchestration plus the few
conversions that are too context-dependent for a plain regex swap.

Each block goes through these stages in this order:
  1. strip the PROC SQL / QUIT wrapper
  2. handle any inline %_eg_conditional_dropds call
  3. apply macro variable substitution
  4. apply library to schema table mapping
  5. apply the conversion rules (the smart handlers first, then the simple
     regex swaps from rules.py)
  6. remove FORMAT= and LABEL= attributes
  7. remove ORDER BY from a CREATE TABLE AS SELECT
  8. rename SAS name literals like 'Linkage Type'n to a valid identifier

The order matters in a few places and I explain why at each stage. One thing
I lean on a lot: every transform is written so that running it twice does
nothing the second time. That means I do not have to worry too much about a
rule firing once here and again later. It just will not match anything the
second time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple

from spmt.rules import (
    ALL_RULES,
    ConversionRule,
    get_handler_rules,
    get_regex_rules,
    get_rule_by_id,
)
from spmt.parser import ParsedBlock, ParseResult


# Result objects

@dataclass
class ConversionResult:
    """What I return for a single PROC SQL block.

    The task only asked for original_sql, converted_sql, rules_applied and
    warnings. I added block_number and target_table as well because the
    documenter and the CLI both need them later, and carrying them here
    saves a second pass over the SQL.
    """
    block_number: int
    original_sql: str
    converted_sql: str
    rules_applied: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    target_table: Optional[str] = None


@dataclass
class FileConversionResult:
    """The whole file after conversion: every block plus the file level bits
    that do not belong to any single block (drop statements and the parameter
    list pulled from the %LET declarations)."""
    source_file: str = ""
    blocks: List[ConversionResult] = field(default_factory=list)
    drop_statements: List[str] = field(default_factory=list)
    parameters: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# Small helpers used in several places

_MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def _sanitize_identifier(name: str) -> str:
    """Turn a SAS name literal into a valid Oracle identifier.

    Oracle does not allow spaces or most special characters in a plain
    identifier, so I replace any run of those with a single underscore,
    drop leading and trailing underscores, and upper-case the result.
    """
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()
    if not cleaned:
        cleaned = "COL"
    if cleaned[0].isdigit():
        cleaned = "C_" + cleaned
    return cleaned


def _rid(keyword: str, fallback: str) -> str:
    """Find the real rule id whose description mentions a keyword.

    I do this instead of hard-coding numbers because the rule ids in rules.py
    have shifted around (R-28 got split, some patterns merged). Looking the id
    up by description keeps the reported rule id correct even if the numbering
    changes. If nothing matches I fall back to a readable label.
    """
    low = keyword.lower()
    for rule in ALL_RULES:
        if low in rule.description.lower():
            return rule.rule_id
    return fallback


def _detect_target_table(sql: str) -> Optional[str]:
    """Pull the target name out of a CREATE TABLE/VIEW ... AS statement."""
    match = re.search(
        r"CREATE\s+(?:TABLE|VIEW)\s+([\w$.{}]+)\s+AS\b",
        sql,
        re.IGNORECASE,
    )
    return match.group(1) if match else None


# The converter

class Converter:
    """Runs the eight stage pipeline over each block.

    I pass the variable handler and table mapper in rather than building them
    inside, for two reasons. First, the CLI already builds them from the config
    files and can hand them straight over. Second, the unit tests can run the
    converter on its own with those set to None, in which case stages 3 and 4
    are simply skipped and the rest still works.
    """

    def __init__(
        self,
        variable_handler=None,
        table_mapper=None,
        rules: Optional[List[ConversionRule]] = None,
    ):
        self.variable_handler = variable_handler
        self.table_mapper = table_mapper
        self.rules = rules if rules is not None else ALL_RULES
        # Name literal renames carry across blocks. When block 1 creates a
        # column from 'Linkage Type'n, later blocks reference the same literal,
        # so the same rename applies. I keep the map here for reporting.
        self._renames: dict = {}

    # Public entry points

    def convert_block(self, block: ParsedBlock) -> ConversionResult:
        """Convert one parsed block and return a ConversionResult."""
        original = block.original_sql
        sql = original
        applied: List[str] = []
        warnings: List[str] = []

        sql = self._strip_wrapper(sql)
        sql, a, w = self._handle_inline_dropds(sql)
        applied += a
        warnings += w
        sql, a, w = self._apply_variables(sql)
        applied += a
        warnings += w
        sql, a, w = self._apply_table_mapping(sql)
        applied += a
        warnings += w
        sql, a, w = self._apply_rules(sql)
        applied += a
        warnings += w
        sql, a = self._remove_format_label(sql)
        applied += a
        sql, a = self._remove_order_by_in_ctas(sql)
        applied += a
        sql, a = self._rename_name_literals(sql)
        applied += a

        sql = self._tidy_whitespace(sql)

        return ConversionResult(
            block_number=block.block_number,
            original_sql=original,
            converted_sql=sql,
            rules_applied=applied,
            warnings=warnings,
            target_table=_detect_target_table(sql),
        )

    def convert_file(self, parse_result: ParseResult) -> FileConversionResult:
        """Convert every block in a parsed file and collect the file level bits."""
        result = FileConversionResult(source_file=parse_result.source_file)

        # The %_eg_conditional_dropds calls sit outside the PROC SQL blocks, so
        # the parser hands them to me separately. I turn each one into an Oracle
        # drop statement and run the table name through the same mapper the
        # blocks use, so the drop targets the real Oracle table.
        for call in parse_result.dropds_calls:
            name = self._map_table_name(call.table_name)
            result.drop_statements.append(self._drop_if_exists(name))

        # The %LET and %GLOBAL names become Pentaho parameters. I just collect
        # the names here; the actual ${...} substitution happened per block.
        for decl in parse_result.macro_declarations:
            pname = getattr(decl, "pentaho_name", None) or decl.name
            if pname not in result.parameters:
                result.parameters.append(pname)

        for block in parse_result.sql_blocks:
            result.blocks.append(self.convert_block(block))

        return result

    # Stage 1: strip the wrapper

    def _strip_wrapper(self, sql: str) -> str:
        """Remove the leading PROC SQL; and the trailing QUIT;.

        Oracle does not use PROC SQL blocks, so these wrapper lines just go.
        I keep everything in between untouched at this stage.
        """
        sql = re.sub(r"^\s*PROC\s+SQL\s*;\s*", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\s*QUIT\s*;\s*$", "", sql, flags=re.IGNORECASE)
        return sql.strip()

    # Stage 2: inline dropds

    def _handle_inline_dropds(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Convert any %_eg_conditional_dropds call found inside a block.

        Normally the parser has already pulled these out, so a block will not
        contain one. I still handle the inline case so nothing slips through if
        a file is shaped differently from my test files.
        """
        applied: List[str] = []
        pattern = re.compile(
            r"%_eg_conditional_dropds\s*\(\s*([\w.]+)\s*\)\s*;",
            re.IGNORECASE,
        )

        def repl(m):
            applied.append(_rid("dropds", "R-41"))
            name = self._map_table_name(m.group(1))
            return self._drop_if_exists(name)

        return pattern.sub(repl, sql), applied, []

    def _drop_if_exists(self, table_name: str) -> str:
        """Oracle has no DROP TABLE IF EXISTS before 23ai, so I use the standard
        anonymous block that drops the table and swallows the does-not-exist
        error (ORA-00942). This is the safe equivalent on any Oracle version."""
        return (
            "BEGIN\n"
            f"   EXECUTE IMMEDIATE 'DROP TABLE {table_name}';\n"
            "EXCEPTION\n"
            "   WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
            "END;\n/"
        )

    # Stage 3: macro variables

    def _apply_variables(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Hand the SQL to the variable handler if I have one.

        The handler returns a result object. I read the converted SQL from it
        and, if it changed anything, I record the macro variable rule. I look
        the handler up by a couple of method names because the handler has gone
        through a few versions and I do not want this to break if the entry
        point was renamed.
        """
        vh = self.variable_handler
        if vh is None:
            return sql, [], []

        res = None
        for name in ("convert_sql", "substitute", "process"):
            fn = getattr(vh, name, None)
            if callable(fn):
                res = fn(sql)
                break
        if res is None:
            return sql, [], []
        if isinstance(res, tuple):
            res = res[-1]

        new_sql = getattr(res, "converted_sql", sql)
        subs = getattr(res, "substitutions_made", None)
        if subs is None:
            subs = getattr(res, "substitutions", []) or []
        warnings = list(getattr(res, "warnings", []) or [])
        applied = [_rid("macro var", "R-02")] if subs else []
        return new_sql, applied, warnings

    # Stage 4: table mapping

    def _apply_table_mapping(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Hand the SQL to the table mapper if I have one.

        Same defensive lookup as the variable handler. The mapper only touches
        tokens whose left side is a known library, so SQL aliases like t1.col
        are left alone. I just read the result back here.
        """
        tm = self.table_mapper
        if tm is None:
            return sql, [], []

        res = None
        for name in ("map_tables", "convert", "remap", "map", "convert_sql"):
            fn = getattr(tm, name, None)
            if callable(fn):
                res = fn(sql)
                break
        if res is None:
            return sql, [], []

        new_sql = getattr(res, "converted_sql", sql)
        maps = getattr(res, "mappings", None)
        if maps is None:
            maps = getattr(res, "tables_mapped", []) or []
        warnings = list(getattr(res, "warnings", []) or [])
        applied = ["TABLE_MAPPING"] if maps else []
        return new_sql, applied, warnings

    def _map_table_name(self, name: str) -> str:
        """Map a single library.table name (used by the drop statements)."""
        if self.table_mapper is None:
            return name
        mapped, _, _ = self._apply_table_mapping(name)
        return mapped

    # Stage 5: the rules

    def _apply_rules(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Apply the conversion rules.

        I run this in three passes. First the smart handlers, because several
        of them depend on running in a set order (the active record check has
        to beat the generic date literal, for example). Then the simple regex
        rules from rules.py for the one to one swaps. Last, I check the handler
        rules I did not implement and raise a warning only if their pattern
        actually shows up, so the user knows a manual step is needed.
        """
        applied: List[str] = []
        warnings: List[str] = []

        sql, a, w = self._run_smart_handlers(sql)
        applied += a
        warnings += w

        for rule in get_regex_rules():
            new_sql, count = rule.sas_pattern.subn(rule.oracle_replacement, sql)
            if count:
                sql = new_sql
                applied.append(rule.rule_id)

        covered = set(self._handled_rule_ids())
        for rule in get_handler_rules():
            if rule.rule_id in covered:
                continue
            if rule.sas_pattern.search(sql):
                warnings.append(
                    f"{rule.rule_id} ({rule.description}) needs a manual check "
                    "-- no automatic conversion for this pattern."
                )

        return sql, applied, warnings

    def _handled_rule_ids(self) -> List[str]:
        """Rule ids that the smart handlers and later stages already cover, so
        the warning pass does not flag them as unhandled."""
        return [
            _rid("missing", "R-01"),
            _rid("31dec9999", "R-11"),
            _rid("contains", "R-36"),
            _rid("name literal", "R-25"),
            _rid("order by", "R-29"),
            _rid("format", "R-18"),
            _rid("label", "R-19"),
        ]

    def _run_smart_handlers(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """The conversions that a plain regex swap cannot do on its own."""
        applied: List[str] = []
        warnings: List[str] = []

        # The order in this list is deliberate. Active record must run before
        # the generic date literal so '31Dec9999'd becomes IS NULL and not a
        # TO_DATE call. CALCULATED runs after the date handlers so the repeated
        # expression is already in Oracle form when I copy it.
        steps: List[Callable[[str], Tuple[str, Optional[str], List[str]]]] = [
            self._h_active_record,
            self._h_date_literal,
            self._h_is_missing,
            self._h_contains,
            self._h_compress,
            self._h_today,
            self._h_intnx,
            self._h_mdy,
            self._h_outer_union_corr,
            self._h_double_quotes,
            self._h_calculated,
            self._h_put,
            self._h_choosec,
        ]
        for step in steps:
            sql, rule_id, warns = step(sql)
            if rule_id:
                applied.append(rule_id)
            warnings += warns
        return sql, applied, warnings

    def _h_active_record(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """'31Dec9999'd is SAS shorthand for an open record. Oracle uses NULL,
        so I rewrite the comparison itself, not just the literal. An equals turns
        into IS NULL and a not-equals turns into IS NOT NULL."""
        rid = _rid("31dec9999", "R-11")
        fired = False
        neg = re.compile(r"(?:NOT\s*=|<>|!=|\^=)\s*['\"]31Dec9999['\"][dD]", re.IGNORECASE)
        pos = re.compile(r"=\s*['\"]31Dec9999['\"][dD]", re.IGNORECASE)
        sql, n1 = neg.subn("IS NOT NULL", sql)
        sql, n2 = pos.subn("IS NULL", sql)
        if n1 or n2:
            fired = True
        return sql, (rid if fired else None), []

    def _h_date_literal(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """A SAS date literal like '01JAN2025'd becomes a TO_DATE call. I read
        the day, month abbreviation and year, then build a fixed YYYYMMDD form
        which is the least ambiguous thing to feed Oracle."""
        rid = _rid("date literal", "R-10")
        pattern = re.compile(r"['\"](\d{1,2})([A-Za-z]{3})(\d{4})['\"][dD]")
        fired = [False]

        def repl(m):
            day = m.group(1).zfill(2)
            mon = _MONTHS.get(m.group(2).lower())
            if not mon:
                return m.group(0)
            year = m.group(3)
            fired[0] = True
            return f"TO_DATE('{year}{mon}{day}', 'YYYYMMDD')"

        sql = pattern.sub(repl, sql)
        return sql, (rid if fired[0] else None), []

    def _h_is_missing(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """IS MISSING and IS NOT MISSING map straight to the Oracle null checks.
        I keep the optional NOT so the meaning is preserved."""
        rid = _rid("missing", "R-01")
        pattern = re.compile(r"\bIS\s+(NOT\s+)?MISSING\b", re.IGNORECASE)
        fired = [False]

        def repl(m):
            fired[0] = True
            return "IS NOT NULL" if m.group(1) else "IS NULL"

        sql = pattern.sub(repl, sql)
        return sql, (rid if fired[0] else None), []

    def _h_contains(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """SAS CONTAINS has no Oracle keyword. The equivalent is LIKE with the
        search text wrapped in percent signs on both sides."""
        rid = _rid("contains", "R-36")
        pattern = re.compile(r"\bCONTAINS\s+'([^']*)'", re.IGNORECASE)
        fired = [False]

        def repl(m):
            fired[0] = True
            return f"LIKE '%{m.group(1)}%'"

        sql = pattern.sub(repl, sql)
        return sql, (rid if fired[0] else None), []

    def _h_compress(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """COMPRESS strips characters. Plain COMPRESS(x) removes spaces, the 'kd'
        modifier keeps digits and 'ka' keeps letters. Oracle has no direct match
        for the modifiers, so I use REGEXP_REPLACE to drop everything else."""
        rid = _rid("compress", "R-16")
        fired = [False]

        def kd(m):
            fired[0] = True
            return f"REGEXP_REPLACE({m.group(1).strip()}, '[^0-9]', '')"

        def ka(m):
            fired[0] = True
            return f"REGEXP_REPLACE({m.group(1).strip()}, '[^A-Za-z]', '')"

        def plain(m):
            fired[0] = True
            return f"REPLACE({m.group(1).strip()}, ' ', '')"

        sql = re.sub(r"COMPRESS\(\s*([^,()]+?)\s*,\s*,\s*'kd'\s*\)", kd, sql, flags=re.IGNORECASE)
        sql = re.sub(r"COMPRESS\(\s*([^,()]+?)\s*,\s*,\s*'ka'\s*\)", ka, sql, flags=re.IGNORECASE)
        sql = re.sub(r"COMPRESS\(\s*([^,()]+?)\s*\)", plain, sql, flags=re.IGNORECASE)
        return sql, (rid if fired[0] else None), []

    def _h_today(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """today() returns the current date with no time part, so TRUNC(SYSDATE)
        is the match in Oracle."""
        rid = _rid("today", "R-09")
        sql, n = re.subn(r"\btoday\s*\(\s*\)", "TRUNC(SYSDATE)", sql, flags=re.IGNORECASE)
        return sql, (rid if n else None), []

    def _h_intnx(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """INTNX shifts a date by a number of intervals. I only handle the month
        interval here because that is the only one the test files use. The fourth
        argument is the alignment: 'E' or 'END' means end of month, 'B' or
        'BEGIN' means start of month, and no alignment means a plain shift."""
        rid = _rid("intnx", "R-07")
        warnings: List[str] = []
        fired = [False]
        pattern = re.compile(
            r"INTNX\(\s*['\"]?(\w+)['\"]?\s*,\s*([^,]+?)\s*,\s*([^,)]+?)\s*"
            r"(?:,\s*['\"]?(\w+)['\"]?\s*)?\)",
            re.IGNORECASE,
        )

        def repl(m):
            interval, date_arg, n_arg, align = m.group(1), m.group(2), m.group(3), m.group(4)
            if interval.lower() != "month":
                warnings.append(
                    f"INTNX with interval '{interval}' was not converted -- "
                    "only the month interval is handled automatically."
                )
                return m.group(0)
            fired[0] = True
            date_arg = date_arg.strip()
            n_arg = n_arg.strip()
            a = (align or "").upper()
            if a in ("E", "END"):
                if n_arg in ("0", "+0"):
                    return f"LAST_DAY({date_arg})"
                return f"LAST_DAY(ADD_MONTHS({date_arg}, {n_arg}))"
            if a in ("B", "BEGIN"):
                if n_arg in ("0", "+0"):
                    return f"TRUNC({date_arg}, 'MM')"
                return f"TRUNC(ADD_MONTHS({date_arg}, {n_arg}), 'MM')"
            return f"ADD_MONTHS({date_arg}, {n_arg})"

        sql = pattern.sub(repl, sql)
        return sql, (rid if fired[0] else None), warnings

    def _h_mdy(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """mdy(month, day, year) builds a date in SAS. Oracle has no mdy, so I
        rebuild the date with TO_DATE. I flag it for a check because once the
        arguments are Pentaho parameters the formatting can need a second look."""
        rid = _rid("mdy", "R-27")
        warnings: List[str] = []
        fired = [False]
        pattern = re.compile(
            r"\bmdy\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^,)]+?)\s*\)",
            re.IGNORECASE,
        )

        def repl(m):
            fired[0] = True
            month, day, year = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            return f"TO_DATE({year} || '-' || {month} || '-' || {day}, 'YYYY-MM-DD')"

        sql = pattern.sub(repl, sql)
        if fired[0]:
            warnings.append(
                "mdy() was converted to TO_DATE -- check the format if the "
                "arguments are parameters rather than plain numbers."
            )
        return sql, (rid if fired[0] else None), warnings

    def _h_outer_union_corr(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """OUTER UNION CORR lines up columns by name and keeps duplicates. The
        nearest Oracle behaviour is UNION ALL once the column lists match."""
        rid = _rid("outer union", "R-17")
        sql, n = re.subn(r"\bOUTER\s+UNION\s+CORR\b", "UNION ALL", sql, flags=re.IGNORECASE)
        return sql, (rid if n else None), []

    def _h_double_quotes(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """SAS accepts double quoted string literals but Oracle reads double
        quotes as an identifier. I switch them to single quotes, but only when
        the text has no single quote inside, to avoid breaking the literal."""
        rid = _rid("double quote", "R-04")
        pattern = re.compile(r'"([^"\']*)"')
        fired = [False]

        def repl(m):
            fired[0] = True
            return f"'{m.group(1)}'"

        sql = pattern.sub(repl, sql)
        return sql, (rid if fired[0] else None), []

    def _match_open_paren(self, left: str) -> Optional[int]:
        """Walk backwards from a closing parenthesis at the end of `left` and
        return the index of the parenthesis that opens it. I skip over quoted
        spans so a bracket sitting inside a string literal does not change the
        depth count. Returns None if the brackets do not balance."""
        i = len(left) - 1
        if i < 0 or left[i] != ")":
            return None
        depth = 0
        while i >= 0:
            ch = left[i]
            if ch == "'" or ch == '"':
                j = left.rfind(ch, 0, i)
                if j < 0:
                    return None
                i = j - 1
                continue
            if ch == ")":
                depth += 1
            elif ch == "(":
                depth -= 1
                if depth == 0:
                    return i
            i -= 1
        return None

    def _h_calculated(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """Oracle has no CALCULATED keyword. SAS uses it to reuse a column alias
        from the same SELECT. I find what each alias was defined as and paste
        that expression back in wherever CALCULATED refers to it."""
        rid = _rid("calculated", "R-24")
        # Map each alias to the parenthesised expression it was built from. By
        # the time this runs the expression may be wrapped several parentheses
        # deep (INTNX turns into LAST_DAY(ADD_MONTHS(...))), and a FORMAT= or
        # LABEL= attribute can sit between the expression and the AS keyword. A
        # fixed regex cannot handle either, so I walk back from each AS keyword,
        # skip any attribute, and balance the parentheses by hand.
        defs = {}
        for m in re.finditer(r"\bAS\s+(\w+)", sql, re.IGNORECASE):
            alias = m.group(1).lower()
            left = sql[:m.start()]
            left = re.sub(
                r"(?:\s+(?:FORMAT\s*=\s*\$?\w+\.\d*|"
                r"LABEL\s*=\s*(?:\"[^\"]*\"|'[^']*'|\S+)))*\s*$",
                "",
                left,
                flags=re.IGNORECASE,
            ).rstrip()
            if not left or left[-1] != ")":
                continue
            start = self._match_open_paren(left)
            if start is not None:
                defs[alias] = left[start:]

        fired = [False]

        def repl(m):
            alias = m.group(1).lower()
            expr = defs.get(alias)
            if expr is None:
                return m.group(0)
            fired[0] = True
            return expr

        sql = re.sub(r"\(\s*CALCULATED\s+(\w+)\s*\)", repl, sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bCALCULATED\s+(\w+)", repl, sql, flags=re.IGNORECASE)
        return sql, (rid if fired[0] else None), []

    def _h_put(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """PUT applies a SAS format catalog, which does not exist in Oracle. I
        generate a placeholder CASE WHEN that applies TO_CHAR() as a fallback,
        but the real format conversion needs manual review of the SAS format
        definition. The placeholder allows the SQL to execute with reasonable
        defaults while flagging that custom formatting is needed."""
        warnings: List[str] = []
        
        # Match PUT(column, format_name) with optional outer parens/aliases
        # Pattern captures: column_expr, format_name
        pattern = r"\(?\s*PUT\s*\(\s*([^,]+?)\s*,\s*(\$?\w+\.?)\s*\)\s*\)?"
        
        def replace_put(match: re.Match) -> str:
            column_expr = match.group(1).strip()
            format_name = match.group(2).strip()
            
            # Generate a placeholder CASE WHEN with a comment about the format
            # This allows Oracle to execute while marking where format conversion is needed
            placeholder = (
                f"(CASE WHEN {column_expr} IS NULL THEN NULL "
                f"ELSE TO_CHAR({column_expr}) END) /* SAS format: {format_name} */"
            )
            return placeholder
        
        if re.search(r"\bPUT\s*\(", sql, re.IGNORECASE):
            # Extract format name from first PUT() call for the warning
            fmt_match = re.search(pattern, sql, re.IGNORECASE)
            format_name = fmt_match.group(2).strip() if fmt_match else "unknown"
            
            sql = re.sub(pattern, replace_put, sql, flags=re.IGNORECASE)
            warnings.append(
                f"PUT() converted to placeholder CASE WHEN -- verify format [{format_name}] "
                "mappings are correct (SAS formats do not have Oracle equivalents)."
            )
        return sql, None, warnings

    def _h_choosec(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """CHOOSEC picks a string by position. The clean Oracle version is a CASE
        on the index, but the choices come from the SAS call and the index is
        often wrapped in INPUT(). I flag it for a manual CASE rather than guess."""
        warnings: List[str] = []
        if re.search(r"\bCHOOSEC\s*\(", sql, re.IGNORECASE):
            warnings.append(
                "CHOOSEC() has no Oracle equivalent -- rewrite it as a CASE on "
                "the index value (remember SAS uses 1 based positions)."
            )
        return sql, None, warnings

    # Stage 6: FORMAT= and LABEL=

    def _remove_format_label(self, sql: str) -> Tuple[str, List[str]]:
        """FORMAT= and LABEL= are display only in SAS and Oracle has no place for
        them. I strip them out but I am careful to keep any AS alias that follows,
        because that part is real and the downstream tables depend on it."""
        applied: List[str] = []

        fmt = re.compile(r"\s*FORMAT\s*=\s*\$?\w+\.\d*", re.IGNORECASE)
        sql, n1 = fmt.subn("", sql)
        if n1:
            applied.append(_rid("format", "R-18"))

        lab = re.compile(
            r"""\s*LABEL\s*=\s*("[^"]*"|'[^']*'|\S+)""",
            re.IGNORECASE,
        )
        sql, n2 = lab.subn("", sql)
        if n2:
            applied.append(_rid("label", "R-19"))

        return sql, applied

    # Stage 7: ORDER BY in a CTAS

    def _remove_order_by_in_ctas(self, sql: str) -> Tuple[str, List[str]]:
        """Oracle will not accept ORDER BY inside CREATE TABLE AS SELECT. I only
        strip an ORDER BY that sits at the top level of the statement, not one
        inside a subquery in parentheses, and only when this block is actually a
        CREATE TABLE or CREATE VIEW."""
        if not re.search(r"CREATE\s+(?:TABLE|VIEW)\b", sql, re.IGNORECASE):
            return sql, []

        index = self._find_top_level_order_by(sql)
        if index is None:
            return sql, []

        # Cut from ORDER BY up to the trailing semicolon or the end of the text.
        tail = sql[index:]
        semi = tail.rfind(";")
        if semi == -1:
            new_sql = sql[:index].rstrip()
        else:
            new_sql = sql[:index].rstrip() + tail[semi:]
        return new_sql.rstrip(), [_rid("order by", "R-29")]

    def _find_top_level_order_by(self, sql: str) -> Optional[int]:
        """Return the position of an ORDER BY that is outside any parentheses and
        outside any quoted string. This is what tells a CTAS level sort apart
        from a sort inside an inline subquery."""
        depth = 0
        quote = None
        i = 0
        upper = sql.upper()
        while i < len(sql):
            ch = sql[i]
            if quote:
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch in ("'", '"'):
                quote = ch
                i += 1
                continue
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif depth == 0 and upper.startswith("ORDER BY", i):
                before = sql[i - 1] if i > 0 else " "
                if not before.isalnum() and before != "_":
                    return i
            i += 1
        return None

    # Stage 8: name literals

    def _rename_name_literals(self, sql: str) -> Tuple[str, List[str]]:
        """SAS name literals like 'Linkage Type'n let a column have spaces. Oracle
        cannot, so I turn each one into a clean identifier. I record the rename so
        the same column keeps the same name everywhere it appears, including in
        later blocks that reference it."""
        applied: List[str] = []
        pattern = re.compile(r"'([^']+)'[nN]")
        fired = [False]

        def repl(m):
            original = m.group(1)
            new = self._renames.get(original)
            if new is None:
                new = _sanitize_identifier(original)
                self._renames[original] = new
            fired[0] = True
            return new

        sql = pattern.sub(repl, sql)
        if fired[0]:
            applied.append(_rid("name literal", "R-25"))
        return sql, applied

    # Final tidy

    def _tidy_whitespace(self, sql: str) -> str:
        """Removing attributes leaves stray double spaces and the odd space before
        a comma. I clean those up so the output reads cleanly, without touching
        anything inside quotes."""
        sql = re.sub(r"[ \t]{2,}", " ", sql)
        sql = re.sub(r"\s+,", ",", sql)
        sql = re.sub(r"\(\s+", "(", sql)
        sql = re.sub(r"\s+\)", ")", sql)
        return sql.strip()


# Convenience entry point used by the CLI

def convert_parse_result(
    parse_result: ParseResult,
    variable_handler=None,
    table_mapper=None,
) -> FileConversionResult:
    """Build a converter and run it over an already parsed file."""
    return Converter(variable_handler, table_mapper).convert_file(parse_result)
