"""Substitutes SAS &var. macro references with Pentaho ${prop_var} syntax.

Substitution order (most-specific first to avoid partial matches):
  1. "&var."d  → TO_DATE('${prop_var}', 'YYYYMMDD')  (date literal)
  2. "&var."   → '${prop_var}'                        (quoted string)
  3. &var..id  → ${prop_var}.id                       (double-dot)
  4. &var.     → ${prop_var}                          (trailing dot consumed)
  5. &var      → ${prop_var}                          (bare reference)
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class MacroDeclaration:
    name: str
    value: Optional[str]          # None for bare %GLOBAL without assignment
    is_global: bool
    pentaho_name: str             # e.g. prop_report_date
    var_type: str                 # date | numeric | string
    line_number: Optional[int] = None

    def __repr__(self) -> str:
        return (
            f"MacroDeclaration(name={self.name!r}, value={self.value!r}, "
            f"pentaho={self.pentaho_name!r}, type={self.var_type!r})"
        )


@dataclass
class SubstitutionResult:
    original_sql: str
    converted_sql: str
    substitutions_made: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class VariableHandler:
    """Does the actual &var → ${prop_var} conversion work."""

    _RE_LET = re.compile(
        r"^\s*%LET\s+(\w+)\s*=\s*(.+?)\s*;\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    _RE_GLOBAL = re.compile(
        r"^\s*%GLOBAL\s+(\w+)\s*;",
        re.IGNORECASE | re.MULTILINE,
    )
    # SAS Enterprise Guide loves putting %GLOBAL and %LET on the same line
    _RE_GLOBAL_LET = re.compile(
        r"^\s*%GLOBAL\s+(\w+)\s*;\s*%LET\s+\1\s*=\s*(.+?)\s*;",
        re.IGNORECASE | re.MULTILINE,
    )

    # Substitution order matters — match specific patterns first.
    # A: "&var."d  date literal (must beat B or the "d" suffix gets orphaned)
    # B: "&var."   quoted string (process each token independently)
    # C: &var..id  double-dot (first dot = name end, second = SQL dot)
    # D: &var.     trailing dot consumed
    # E: &var      bare ref (word-boundary)

    _RE_DATE_LIT_QUOTED = re.compile(r""""&(\w+)\.?"[dD]""")

    _RE_QUOTED_VAR = re.compile(r"""(?<=")([^"]*?)&(\w+)\.?([^"]*?)(?=")""")

    _RE_DOUBLE_DOT = re.compile(r"&(\w+)\.\.")

    _RE_VAR_DOT = re.compile(r"&(\w+)\.")

    _RE_VAR_BARE = re.compile(r"&(\w+)\b(?!\.)")

    def __init__(self, config_path: Optional[str] = None) -> None:
        self._mappings: Dict[str, dict] = {}
        self._default_prefix: str = "prop_"
        self._default_date_format: str = "YYYYMMDD"

        if config_path:
            self.load_config(config_path)

    def load_config(self, config_path: str) -> None:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)

        self._default_prefix = raw.get("default_pentaho_prefix", "prop_")
        self._default_date_format = raw.get("default_date_format", "YYYYMMDD")

        # flatten the four category sections into one dict for fast lookup
        for section in ("date_variables", "period_variables",
                        "string_variables", "numeric_variables"):
            for var_name, meta in raw.get(section, {}).items():
                self._mappings[var_name.lower()] = meta

    def _lookup(self, sas_name: str) -> Tuple[str, str, str]:
        """Get (pentaho_name, var_type, date_format). Auto-generates a name for unknowns."""
        key = sas_name.lower()
        if key in self._mappings:
            m = self._mappings[key]
            return (
                m["pentaho_name"],
                m.get("type", "string"),
                m.get("date_format", self._default_date_format),
            )
        return (f"{self._default_prefix}{sas_name}", "string", self._default_date_format)

    def extract_declarations(self, sas_source: str) -> List[MacroDeclaration]:
        """Pull all %LET/%GLOBAL declarations from SAS source."""
        seen: Dict[str, MacroDeclaration] = {}

        # Pass 1: bare %GLOBAL (declaration only, no value)
        for match in self._RE_GLOBAL.finditer(sas_source):
            name = match.group(1)
            pname, vtype, _ = self._lookup(name)
            seen[name.lower()] = MacroDeclaration(
                name=name,
                value=None,
                is_global=True,
                pentaho_name=pname,
                var_type=vtype,
            )

        # Pass 2: %GLOBAL x; %LET x = ... crammed onto the same line (EG does this)
        for match in self._RE_GLOBAL_LET.finditer(sas_source):
            name = match.group(1)
            value = match.group(2).strip().rstrip(";")
            pname, vtype, _ = self._lookup(name)
            seen[name.lower()] = MacroDeclaration(
                name=name,
                value=value,
                is_global=True,
                pentaho_name=pname,
                var_type=vtype,
            )

        # Pass 3: standalone %LET lines
        for match in self._RE_LET.finditer(sas_source):
            name = match.group(1)
            value = match.group(2).strip().rstrip(";")
            pname, vtype, _ = self._lookup(name)
            key = name.lower()
            is_global = key in seen and seen[key].is_global
            seen[key] = MacroDeclaration(
                name=name,
                value=value,
                is_global=is_global,
                pentaho_name=pname,
                var_type=vtype,
            )

        return list(seen.values())

    def substitute(self, sql: str) -> SubstitutionResult:
        """Replace all &var references with Pentaho ${...} syntax. Patterns applied A→E."""
        result = sql
        subs: List[str] = []
        warnings: List[str] = []

        # Pattern A: "&var."d → TO_DATE('${...}','YYYYMMDD')
        # Must run before B — otherwise B strips the quotes and the "d" suffix is orphaned
        def _repl_date_lit(m: re.Match) -> str:
            name = m.group(1)
            pname, _, dfmt = self._lookup(name)
            replacement = f"TO_DATE('${{{pname}}}', '{dfmt}')"
            subs.append(f"&{name}.(date-literal) → {replacement}")
            return replacement

        result = self._RE_DATE_LIT_QUOTED.sub(_repl_date_lit, result)

        # Pattern B: double-quoted strings containing &var.
        # Process each quoted token independently — a broad pattern can start at one
        # quote and end at another many lines later, corrupting CASE literals
        def _repl_quoted_token(m: re.Match) -> str:
            inner = m.group(1)
            if "&" not in inner:
                return m.group(0)

            def _inner_var(vm: re.Match) -> str:
                name = vm.group(1)
                pname, _, _ = self._lookup(name)
                subs.append(f"&{name}.(quoted) → ${{{pname}}}")
                return f"${{{pname}}}"

            converted_inner = re.sub(r"&(\w+)\.?", _inner_var, inner)
            return f"'{converted_inner}'"

        result = re.sub(r'"([^"\r\n]*)"', _repl_quoted_token, result)

        # Pattern C: &var..identifier — first dot ends the var name, second is the SQL dot
        def _repl_double_dot(m: re.Match) -> str:
            name = m.group(1)
            pname, _, _ = self._lookup(name)
            subs.append(f"&{name}..(double-dot) → ${{{pname}}}.")
            return f"${{{pname}}}."

        result = self._RE_DOUBLE_DOT.sub(_repl_double_dot, result)

        # Pattern D: &var. (trailing dot consumed — most common case)
        def _repl_var_dot(m: re.Match) -> str:
            name = m.group(1)
            # skip SAS internal macro vars that appear inside %_eg_conditional_dropds
            if name.lower() in ("syspbuff", "num", "dsname", "sysdate"):
                return m.group(0)
            pname, vtype, dfmt = self._lookup(name)
            subs.append(f"&{name}. → ${{{pname}}}")
            if name.lower() not in self._mappings:
                warnings.append(
                    f"Variable &{name} not in mapping config; "
                    f"used auto-generated name {pname}"
                )
            return f"${{{pname}}}"

        result = self._RE_VAR_DOT.sub(_repl_var_dot, result)

        # Pattern E: &var (no trailing dot — common inside function calls)
        def _repl_var_bare(m: re.Match) -> str:
            name = m.group(1)
            if name.lower() in ("syspbuff", "num", "dsname", "sysdate"):
                return m.group(0)
            pname, vtype, dfmt = self._lookup(name)
            subs.append(f"&{name} → ${{{pname}}}")
            if name.lower() not in self._mappings:
                warnings.append(
                    f"Variable &{name} not in mapping config; "
                    f"used auto-generated name {pname}"
                )
            return f"${{{pname}}}"

        result = self._RE_VAR_BARE.sub(_repl_var_bare, result)

        return SubstitutionResult(
            original_sql=sql,
            converted_sql=result,
            substitutions_made=subs,
            warnings=warnings,
        )

    def wrap_date_variables(self, sql: str) -> str:
        """Wrap bare ${date_var} params in TO_DATE(). Not yet called — context detection still needed."""
        # TODO: heuristic for "already wrapped vs. bare" is still fragile; leaving skeleton for later
        result = sql
        for var_name, meta in self._mappings.items():
            if meta.get("type") != "date":
                continue
            pname = meta["pentaho_name"]
            dfmt = meta.get("date_format", self._default_date_format)
            token = f"${{{pname}}}"

            pattern = re.compile(
                r"(?<!TO_DATE\(')(?<!')"
                + re.escape(token)
                + r"(?!'\s*,\s*')"
                + r"(?!')"
            )

            def _wrap(m: re.Match, _pn=pname, _df=dfmt) -> str:
                return f"TO_DATE('{_pn}', '{_df}')"

            result = result  # (keep as-is for now; full wrapping is opt-in)

        return result

    def process(self, sas_source: str) -> Tuple[List[MacroDeclaration], SubstitutionResult]:
        """Extract declarations then substitute variables in the SAS source."""
        declarations = self.extract_declarations(sas_source)
        result = self.substitute(sas_source)
        return declarations, result
