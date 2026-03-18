"""
spmt/variable_handler.py

This module deals with the messiest part of SAS-to-Oracle conversion: macro
variables. SAS uses &var. syntax with a trailing dot that sometimes means
"end of variable name" and sometimes means "there's a SQL dot after this."
Pentaho uses ${prop_var} instead, which is at least consistent.

I handle five substitution patterns here, and the order matters a LOT.
If I run the generic &var. pattern first, it'll eat the dot that was
actually part of a date literal like "&report_date."d, and then the
TO_DATE wrapper never fires. So I go from most-specific to least-specific:

  1. "&var."d            -> TO_DATE('${prop_var}','YYYYMMDD')  (date literal)
  2. "&var."             -> '${prop_var}'                      (quoted string)
  3. &var..identifier    -> ${prop_var}.identifier              (double-dot)
  4. &var.               -> ${prop_var}                         (trailing dot)
  5. &var                -> ${prop_var}                         (bare reference)

Mappings come from config/variable_mappings.json. If a variable isn't in
there, I make up a name using the default prefix and move on (but I do
log a warning so the user knows).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---- Data classes ----

@dataclass
class MacroDeclaration:
    """One %LET or %GLOBAL declaration pulled from SAS source."""

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
    """What you get back after running substitute() on a SQL string."""

    original_sql: str
    converted_sql: str
    substitutions_made: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# ---- Variable Handler ----

class VariableHandler:
    """Does the actual &var -> ${prop_var} conversion work."""

    # Regex for %LET and %GLOBAL lines. Case-insensitive because SAS doesn't
    # care about casing and neither do the people writing these files.
    _RE_LET = re.compile(
        r"^\s*%LET\s+(\w+)\s*=\s*(.+?)\s*;\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    _RE_GLOBAL = re.compile(
        r"^\s*%GLOBAL\s+(\w+)\s*;",
        re.IGNORECASE | re.MULTILINE,
    )
    # SAS Enterprise Guide loves putting %GLOBAL and %LET on the same line.
    # Every single EG-exported file has these.
    _RE_GLOBAL_LET = re.compile(
        r"^\s*%GLOBAL\s+(\w+)\s*;\s*%LET\s+\1\s*=\s*(.+?)\s*;",
        re.IGNORECASE | re.MULTILINE,
    )

    # ---- Macro reference patterns ----
    #
    # The ordering here is the whole trick. SAS overloads the dot character
    # to mean like three different things depending on context, so I have to
    # match the most specific patterns first or everything breaks.
    #
    # Pattern A - date literal:   "&var."d  or  "&var"d
    #   e.g. "&report_end_date."d -> TO_DATE('${prop_report_end_date}','YYYYMMDD')
    #
    # Pattern B - quoted string:  "&var."  or  "&var"
    #   e.g. "&client_code."  -> '${prop_client_code}'
    #   (also swaps double quotes to single quotes, because Oracle)
    #
    # Pattern C - double-dot (table name):  &schema..identifier
    #   The first dot terminates the variable, the second is the SQL dot.
    #   e.g. source.CUSTOMERS_&gPeriodeTable.  (dot consumed, SQL dot kept)
    #
    # Pattern D - bare with dot:   &var.
    # Pattern E - bare no dot:     &var  (word-boundary)

    # A: "&var."d  or  "&var"d  - SAS date literal containing a macro var
    _RE_DATE_LIT_QUOTED = re.compile(
        r""""&(\w+)\.?"[dD]"""
    )

    # B: "&var." or "&var" inside a double-quoted string.
    # I match each &var individually inside the string.
    _RE_QUOTED_VAR = re.compile(
        r"""(?<=")([^"]*?)&(\w+)\.?([^"]*?)(?=")"""
    )

    # C: &var..identifier  - two dots means var-dot plus SQL-dot
    _RE_DOUBLE_DOT = re.compile(r"&(\w+)\.\.")

    # D: &var.  - trailing dot gets consumed
    _RE_VAR_DOT = re.compile(r"&(\w+)\.")

    # E: &var  - no dot, just a word boundary
    _RE_VAR_BARE = re.compile(r"&(\w+)\b(?!\.)")

    def __init__(self, config_path: Optional[str] = None) -> None:
        self._mappings: Dict[str, dict] = {}   # lowercase name -> metadata
        self._default_prefix: str = "prop_"
        self._default_date_format: str = "YYYYMMDD"

        if config_path:
            self.load_config(config_path)

    # ---- Configuration ----

    def load_config(self, config_path: str) -> None:
        """Read variable mappings from JSON config."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)

        self._default_prefix = raw.get("default_pentaho_prefix", "prop_")
        self._default_date_format = raw.get("default_date_format", "YYYYMMDD")

        # Flatten the four category sections into one flat dict for fast lookup.
        # I don't care about the category at lookup time, just the variable name.
        for section in ("date_variables", "period_variables",
                        "string_variables", "numeric_variables"):
            for var_name, meta in raw.get(section, {}).items():
                self._mappings[var_name.lower()] = meta

    def _lookup(self, sas_name: str) -> Tuple[str, str, str]:
        """Get (pentaho_name, var_type, date_format) for a SAS variable name.

        If it's not in the config, I just slap the default prefix on it and
        call it a string. Better than crashing.
        """
        key = sas_name.lower()
        if key in self._mappings:
            m = self._mappings[key]
            return (
                m["pentaho_name"],
                m.get("type", "string"),
                m.get("date_format", self._default_date_format),
            )
        # Not mapped - auto-generate a name
        return (f"{self._default_prefix}{sas_name}", "string", self._default_date_format)

    # ---- %LET / %GLOBAL extraction ----

    def extract_declarations(self, sas_source: str) -> List[MacroDeclaration]:
        """Pull all %LET and %GLOBAL declarations out of SAS source.

        I do three passes because SAS has multiple ways to declare the same
        variable. A bare %GLOBAL just declares it, then a %LET on the same
        or next line assigns a value. Last assignment wins.
        """
        seen: Dict[str, MacroDeclaration] = {}

        # Pass 1: bare %GLOBAL (just a declaration, no value yet)
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

        # Pass 2: %GLOBAL x; %LET x = ... crammed onto the same line
        # (EG does this constantly and it's annoying to parse)
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

    # ---- Substitution engine ----

    def substitute(self, sql: str) -> SubstitutionResult:
        """Replace all &var references in sql with Pentaho ${...} syntax.

        This is the main workhorse. I apply patterns A through E in order
        so that the more specific ones (date literals, quoted strings) get
        matched before the generic bare-variable patterns can eat them.
        """
        result = sql
        subs: List[str] = []
        warnings: List[str] = []

        # Pattern A: "&var."d -> TO_DATE('${...}','YYYYMMDD')
        # This one is SAS-specific: they inline macro vars into date literals.
        # If I don't catch this first, Pattern B will strip the quotes and
        # the "d" suffix gets orphaned.
        def _repl_date_lit(m: re.Match) -> str:
            name = m.group(1)
            pname, _, dfmt = self._lookup(name)
            replacement = f"TO_DATE('${{{pname}}}', '{dfmt}')"
            subs.append(f"&{name}.(date-literal) → {replacement}")
            return replacement

        result = self._RE_DATE_LIT_QUOTED.sub(_repl_date_lit, result)

        # Pattern B: double-quoted strings containing &var
        # SAS uses double quotes for strings with macro resolution, Oracle
        # uses single quotes for everything. So I need to swap the quotes
        # AND replace the variables inside.
        def _repl_quoted_segment(m: re.Match) -> str:
            full = m.group(0)  # includes quotes
            inner = full[1:-1]  # strip quotes

            def _inner_var(vm: re.Match) -> str:
                name = vm.group(1)
                pname, _, _ = self._lookup(name)
                subs.append(f"&{name}.(quoted) → ${{{pname}}}")
                return f"${{{pname}}}"

            converted_inner = re.sub(r"&(\w+)\.?", _inner_var, inner)
            return f"'{converted_inner}'"

        result = re.sub(r'"([^"]*&\w+[^"]*)"', _repl_quoted_segment, result)

        # Pattern C: &var..identifier  (the double-dot situation)
        # In SAS, the first dot ends the variable name and the second dot is
        # the actual SQL dot separator. So &schema..table means "resolve
        # &schema, then put a dot, then table". Kind of wild.
        def _repl_double_dot(m: re.Match) -> str:
            name = m.group(1)
            pname, _, _ = self._lookup(name)
            subs.append(f"&{name}..(double-dot) → ${{{pname}}}.")
            return f"${{{pname}}}."

        result = self._RE_DOUBLE_DOT.sub(_repl_double_dot, result)

        # Pattern D: &var. (trailing dot consumed)
        # The most common case. The dot just means "end of variable name."
        def _repl_var_dot(m: re.Match) -> str:
            name = m.group(1)
            # Skip SAS internal macro vars - these show up inside the
            # %_eg_conditional_dropds macro definition and shouldn't be touched
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

        # Pattern E: &var (no dot at all)
        # Some SAS code just leaves the dot off, especially inside function
        # calls like mdy(&report_month, 1, &report_year) where the comma
        # makes the boundary obvious to SAS but not to my regex.
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

        # (Warnings are populated inline by the replacement callbacks above.)

        return SubstitutionResult(
            original_sql=sql,
            converted_sql=result,
            substitutions_made=subs,
            warnings=warnings,
        )

    # ---- TO_DATE wrapping for bare date variables ----

    def wrap_date_variables(self, sql: str) -> str:
        """Wrap bare date ${...} params with TO_DATE() if they aren't already.

        Only targets variables marked as type "date" in the config, and skips
        ones that are already inside a TO_DATE() call. This runs AFTER
        substitute(), so at this point everything is already in Pentaho syntax.

        TODO: I'm not actually calling this anywhere yet because the heuristic
        for "is this bare or already wrapped" is tricky. Leaving the skeleton
        here for Phase 5 when the converter can make smarter decisions.
        """
        result = sql
        for var_name, meta in self._mappings.items():
            if meta.get("type") != "date":
                continue
            pname = meta["pentaho_name"]
            dfmt = meta.get("date_format", self._default_date_format)
            token = f"${{{pname}}}"

            # Try to only wrap tokens that aren't already inside TO_DATE(...)
            # or inside single quotes. The lookbehind/lookahead approach here
            # is fragile but it's a start.
            pattern = re.compile(
                r"(?<!TO_DATE\(')(?<!')"
                + re.escape(token)
                + r"(?!'\s*,\s*')"
                + r"(?!')"
            )

            def _wrap(m: re.Match, _pn=pname, _df=dfmt) -> str:
                return f"TO_DATE('{_pn}', '{_df}')"

            # Not actually replacing yet - need better context detection
            # before I can safely do this without breaking things
            result = result  # (keep as-is for now; full wrapping is opt-in)

        return result

    # ---- High-level pipeline ----

    def process(self, sas_source: str) -> Tuple[List[MacroDeclaration], SubstitutionResult]:
        """Run the whole thing: extract declarations, then substitute variables.

        Takes the complete SAS source (with %LETs, comments, multiple PROC SQL
        blocks, whatever) and returns both the extracted declarations and the
        substituted result.
        """
        declarations = self.extract_declarations(sas_source)
        result = self.substitute(sas_source)
        return declarations, result
