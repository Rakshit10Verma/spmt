"""Pipeline converter: turns parsed SAS PROC SQL blocks into Oracle SQL.

Stages per block: strip wrapper → inline dropds → macro vars → table mapping
  → conversion rules → strip FORMAT/LABEL → remove ORDER BY in CTAS → name literals.
All transforms are idempotent.
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


@dataclass
class ConversionResult:
    """Result for one converted PROC SQL block."""
    block_number: int
    original_sql: str
    converted_sql: str
    rules_applied: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    target_table: Optional[str] = None


@dataclass
class FileConversionResult:
    """All blocks plus file-level drop statements and parameter names."""
    source_file: str = ""
    blocks: List[ConversionResult] = field(default_factory=list)
    drop_statements: List[str] = field(default_factory=list)
    parameters: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


_MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def _sanitize_identifier(name: str) -> str:
    """Turn a SAS name literal (e.g. 'My Column'n) into a valid Oracle identifier."""
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()
    if not cleaned:
        cleaned = "COL"
    if cleaned[0].isdigit():
        cleaned = "C_" + cleaned
    return cleaned


def _rid(keyword: str, fallback: str) -> str:
    """Find a rule's ID by matching its description. Avoids hardcoding IDs that shift."""
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


class Converter:
    """Eight-stage SAS→Oracle SQL converter. Pass None for variable_handler/table_mapper to skip those stages."""

    def __init__(
        self,
        variable_handler=None,
        table_mapper=None,
        rules: Optional[List[ConversionRule]] = None,
    ):
        self.variable_handler = variable_handler
        self.table_mapper = table_mapper
        self.rules = rules if rules is not None else ALL_RULES
        # name literal renames carry across blocks — same literal must map consistently
        self._renames: dict = {}

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
        sql, a, w = self._remove_order_by_in_ctas(sql)
        applied += a
        warnings += w
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
        """Convert every block in a parsed file and collect the file-level bits."""
        result = FileConversionResult(source_file=parse_result.source_file)

        # dropds calls sit outside PROC SQL blocks; map table name and emit Oracle DROP block
        for call in parse_result.dropds_calls:
            name = self._map_table_name(call.table_name)
            result.drop_statements.append(self._drop_if_exists(name))

        for decl in parse_result.macro_declarations:
            pname = getattr(decl, "pentaho_name", None) or decl.name
            if pname not in result.parameters:
                result.parameters.append(pname)

        for block in parse_result.sql_blocks:
            result.blocks.append(self.convert_block(block))

        return result

    # Stage 1: strip the wrapper

    def _strip_wrapper(self, sql: str) -> str:
        """Remove PROC SQL; / QUIT; wrappers."""
        sql = re.sub(r"^\s*PROC\s+SQL\s*;\s*", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\s*QUIT\s*;\s*$", "", sql, flags=re.IGNORECASE)
        return sql.strip()

    # Stage 2: inline dropds

    def _handle_inline_dropds(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Convert inline %_eg_conditional_dropds calls (parser normally catches these first)."""
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
        # Oracle pre-23ai has no DROP TABLE IF EXISTS; swallow ORA-00942 (table/view does not exist)
        return (
            "BEGIN\n"
            f"   EXECUTE IMMEDIATE 'DROP TABLE {table_name}';\n"
            "EXCEPTION\n"
            "   WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
            "END;\n/"
        )

    # Stage 3: macro variables

    def _apply_variables(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Run variable handler if present; try multiple entry point names for compatibility."""
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
        """Run table mapper if present."""
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
        """Map a single library.table name (used by drop statements)."""
        if self.table_mapper is None:
            return name
        mapped, _, _ = self._apply_table_mapping(name)
        return mapped

    # Stage 5: the rules

    def _apply_rules(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Three-pass rule application: smart handlers first, then regex rules, then warn on unhandled patterns."""
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
        """Rule IDs covered by smart handlers or later stages — excluded from the warning pass."""
        return [
            _rid("IS [NOT] MISSING", "R-01"),
            _rid("SAS sum() NULL-safe arithmetic", "R-34"),
            _rid("31dec9999", "R-11"),
            _rid("CONTAINS 'value'", "R-36"),
            _rid("SAS name literal", "R-25"),
            _rid("ORDER BY in CREATE TABLE AS SELECT", "R-29"),
            _rid("FORMAT= attribute", "R-18"),
            _rid("LABEL= attribute", "R-19"),
        ]

    def _run_smart_handlers(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Conversions that can't be done with a plain regex swap."""
        applied: List[str] = []
        warnings: List[str] = []

        # Order matters: active_record must run before date_literal so '31Dec9999'd
        # becomes IS NULL, not a TO_DATE call. CALCULATED runs after date handlers
        # so the copied expression is already in Oracle form.
        steps: List[Callable[[str], Tuple[str, Optional[str], List[str]]]] = [
            self._h_active_record,
            self._h_date_literal,
            self._h_is_missing,
            self._h_sum,
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
        """'31Dec9999'd → IS NULL/IS NOT NULL on the surrounding comparison."""
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
        """'01JAN2025'd → TO_DATE('20250101', 'YYYYMMDD')."""
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
        """IS [NOT] MISSING → IS [NOT] NULL."""
        rid = _rid("missing", "R-01")
        pattern = re.compile(r"\bIS\s+(NOT\s+)?MISSING\b", re.IGNORECASE)
        fired = [False]

        def repl(m):
            fired[0] = True
            return "IS NOT NULL" if m.group(1) else "IS NULL"

        sql = pattern.sub(repl, sql)
        return sql, (rid if fired[0] else None), []

    def _split_top_level_args(self, args_text: str) -> List[str]:
        """Split function arguments by commas that are outside parens/quotes."""
        args: List[str] = []
        depth = 0
        quote: Optional[str] = None
        start = 0
        i = 0
        while i < len(args_text):
            ch = args_text[i]
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
            elif ch == ")" and depth > 0:
                depth -= 1
            elif ch == "," and depth == 0:
                args.append(args_text[start:i].strip())
                start = i + 1
            i += 1
        args.append(args_text[start:].strip())
        return args

    def _h_sum(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """SAS sum(a,b,...) NULL-safe arithmetic → (NVL(a,0) + NVL(b,0)). Skips single-arg aggregate SUM."""
        rid = _rid("NULL-safe arithmetic", "R-34")
        fired = False
        out: List[str] = []
        i = 0
        lower = sql.lower()
        while i < len(sql):
            m = re.search(r"\bsum\s*\(", lower[i:])
            if not m:
                out.append(sql[i:])
                break

            start = i + m.start()
            open_paren = i + m.end() - 1
            out.append(sql[i:start])

            depth = 1
            j = open_paren + 1
            quote: Optional[str] = None
            while j < len(sql):
                ch = sql[j]
                if quote:
                    if ch == quote:
                        quote = None
                    j += 1
                    continue
                if ch in ("'", '"'):
                    quote = ch
                elif ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1

            if j >= len(sql) or depth != 0:
                out.append(sql[start:])
                break

            args_text = sql[open_paren + 1 : j]
            args = self._split_top_level_args(args_text)
            # one arg = Oracle aggregate SUM() or scalar pass-through, leave it
            if len(args) <= 1:
                out.append(sql[start : j + 1])
            else:
                fired = True
                nvl_terms = [f"NVL({a}, 0)" for a in args]
                out.append("(" + " + ".join(nvl_terms) + ")")

            i = j + 1

        return "".join(out), (rid if fired else None), []

    def _h_contains(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """CONTAINS 'x' → LIKE '%x%'."""
        rid = _rid("contains", "R-36")
        pattern = re.compile(r"\bCONTAINS\s+'([^']*)'", re.IGNORECASE)
        fired = [False]

        def repl(m):
            fired[0] = True
            return f"LIKE '%{m.group(1)}%'"

        sql = pattern.sub(repl, sql)
        return sql, (rid if fired[0] else None), []

    def _h_compress(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """COMPRESS modifiers: plain → REPLACE spaces, 'kd' → keep digits REGEXP, 'ka' → keep alpha REGEXP."""
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

        sql = re.sub(r"COMPRESS\(\s*([^,()]+?)\s*,\s*,\s*[\"']kd[\"']\s*\)", kd, sql, flags=re.IGNORECASE)
        sql = re.sub(r"COMPRESS\(\s*([^,()]+?)\s*,\s*,\s*[\"']ka[\"']\s*\)", ka, sql, flags=re.IGNORECASE)
        sql = re.sub(r"COMPRESS\(\s*([^,()]+?)\s*\)", plain, sql, flags=re.IGNORECASE)
        return sql, (rid if fired[0] else None), []

    def _h_today(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """today() → TRUNC(SYSDATE)."""
        rid = _rid("today", "R-09")
        sql, n = re.subn(r"\btoday\s*\(\s*\)", "TRUNC(SYSDATE)", sql, flags=re.IGNORECASE)
        return sql, (rid if n else None), []

    def _h_intnx(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """INTNX(MONTH, date, n, align) → ADD_MONTHS/LAST_DAY/TRUNC. Only handles MONTH interval."""
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
        """mdy(m, d, y) → TO_DATE(...)."""
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
            if (
                re.fullmatch(r"\d{1,2}", month)
                and re.fullmatch(r"\d{1,2}", day)
                and re.fullmatch(r"\d{4}", year)
            ):
                month_i = int(month)
                day_i = int(day)
                return f"TO_DATE('{year}-{month_i:02d}-{day_i:02d}', 'YYYY-MM-DD')"
            return f"TO_DATE({year} || '-' || {month} || '-' || {day}, 'YYYY-MM-DD')"

        sql = pattern.sub(repl, sql)
        if fired[0]:
            warnings.append(
                "mdy() was converted to TO_DATE -- check the format if the "
                "arguments are parameters rather than plain numbers."
            )
        return sql, (rid if fired[0] else None), warnings

    def _h_outer_union_corr(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """OUTER UNION CORR → UNION ALL."""
        rid = _rid("outer union", "R-17")
        sql, n = re.subn(r"\bOUTER\s+UNION\s+CORR\b", "UNION ALL", sql, flags=re.IGNORECASE)
        return sql, (rid if n else None), []

    def _h_double_quotes(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """Double-quoted string literals → single-quoted (no embedded single quotes)."""
        rid = _rid("double quote", "R-04")
        pattern = re.compile(r'"([^"\']*)"')
        fired = [False]

        def repl(m):
            fired[0] = True
            return f"'{m.group(1)}'"

        sql = pattern.sub(repl, sql)
        return sql, (rid if fired[0] else None), []

    def _match_open_paren(self, left: str) -> Optional[int]:
        """Walk back to find the opening paren index, skipping quoted spans."""
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
        """CALCULATED alias → inline the original expression."""
        rid = _rid("calculated", "R-24")
        # Build alias→expression map. FORMAT/LABEL attrs can sit between the expression
        # and AS, and expressions nest arbitrarily deep — can't do this with a flat regex.
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
        """PUT(col, FORMAT.) → CASE WHEN placeholder. Flags for manual review since SAS format catalogs have no Oracle equivalent."""
        warnings: List[str] = []

        pattern = re.compile(
            r"\bPUT\s*\(\s*([^,()]+?)\s*,\s*(\$?\w+\.?)\s*\)",
            re.IGNORECASE,
        )
        seen_formats: list[str] = []

        def replace_put(match: re.Match) -> str:
            column_expr = match.group(1).strip()
            format_name = match.group(2).strip()
            seen_formats.append(format_name)
            return (
                f"(CASE WHEN {column_expr} IS NULL THEN NULL "
                f"ELSE TO_CHAR({column_expr}) END) /* SAS format: {format_name} */"
            )

        sql, count = pattern.subn(replace_put, sql)
        if count:
            sample = ", ".join(sorted(set(seen_formats))[:3])
            warnings.append(
                f"PUT() converted to placeholder CASE WHEN -- verify format [{sample}] "
                "mappings are correct (SAS formats do not have Oracle equivalents)."
            )
        return sql, None, warnings

    def _h_choosec(self, sql: str) -> Tuple[str, Optional[str], List[str]]:
        """CHOOSEC has no Oracle equivalent; emit a warning for manual CASE rewrite."""
        warnings: List[str] = []
        if re.search(r"\bCHOOSEC\s*\(", sql, re.IGNORECASE):
            warnings.append(
                "CHOOSEC() has no Oracle equivalent -- rewrite it as a CASE on "
                "the index value (remember SAS uses 1 based positions)."
            )
        return sql, None, warnings

    # Stage 6: FORMAT= and LABEL=

    def _remove_format_label(self, sql: str) -> Tuple[str, List[str]]:
        """Strip FORMAT= and LABEL= display attributes."""
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

    def _remove_order_by_in_ctas(self, sql: str) -> Tuple[str, List[str], List[str]]:
        """Remove top-level ORDER BY from CREATE TABLE AS SELECT (Oracle rejects it)."""
        if not re.search(r"CREATE\s+(?:TABLE|VIEW)\b", sql, re.IGNORECASE):
            return sql, [], []

        index = self._find_top_level_order_by(sql)
        if index is None:
            return sql, [], []

        # cut from ORDER BY to trailing semicolon or end
        tail = sql[index:]
        semi = tail.rfind(";")
        if semi == -1:
            new_sql = sql[:index].rstrip()
        else:
            new_sql = sql[:index].rstrip() + tail[semi:]
        return (
            new_sql.rstrip(),
            [_rid("order by", "R-29")],
            [
                "ORDER BY removed from CTAS/CREATE VIEW block -- Oracle does not "
                "preserve physical row order in table storage."
            ],
        )

    def _find_top_level_order_by(self, sql: str) -> Optional[int]:
        """Return position of ORDER BY that is outside all parens and quotes."""
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
        """Replace 'My Column'n name literals with valid Oracle identifiers; consistent across blocks."""
        applied: List[str] = []
        pattern = re.compile(r"'([^'\r\n]+)'[nN](?![A-Za-z0-9_])")
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

    def _tidy_whitespace(self, sql: str) -> str:
        """Clean up double spaces and space-before-comma artifacts left by attribute removal."""
        sql = re.sub(r"[ \t]{2,}", " ", sql)
        sql = re.sub(r"\s+,", ",", sql)
        sql = re.sub(r"\(\s+", "(", sql)
        sql = re.sub(r"\s+\)", ")", sql)
        return sql.strip()


def convert_parse_result(
    parse_result: ParseResult,
    variable_handler=None,
    table_mapper=None,
) -> FileConversionResult:
    """Build a converter and run it over an already parsed file."""
    return Converter(variable_handler, table_mapper).convert_file(parse_result)
