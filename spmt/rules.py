"""
spmt/rules.py — Conversion rule definitions for SAS PROC SQL to Oracle SQL.

Defines all 42 conversion rules as structured data using dataclasses.
Rules are organized into following 8 categories:
    1. null_handling    — IS MISSING, '31Dec9999'd, NVL, SAS sum()
    2. date_functions   — INTNX, today(), date literals, mdy()
    3. string_functions — UPCASE, LOWCASE, STRIP, COMPRESS
    4. macro_variables  — &var., %LET/%GLOBAL, LIBNAME/%include
    5. type_conversion  — PUT(), CHOOSEC/INPUT
    6. sas_keywords     — CALCULATED, 'Name'n, NOT=, CONTAINS, FORMAT=, etc.
    7. table_mapping    — Dynamic suffixes, chained temps, time slices, PK_STAND
    8. join_patterns    — RIGHT JOIN semantics, inline VALUES, MAX() subquery

Each rule carries a regex pattern for detection and a replacement template
(or special handler flag) for the converter to apply.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# Enums

class RuleCategory(str, Enum):
    """Broad grouping for conversion rules."""
    NULL_HANDLING = "null_handling"
    DATE_FUNCTIONS = "date_functions"
    STRING_FUNCTIONS = "string_functions"
    MACRO_VARIABLES = "macro_variables"
    TYPE_CONVERSION = "type_conversion"
    SAS_KEYWORDS = "sas_keywords"
    TABLE_MAPPING = "table_mapping"
    JOIN_PATTERNS = "join_patterns"


class Complexity(str, Enum):
    """Conversion difficulty level used for reporting."""
    BASIC = "basic"
    MEDIUM = "medium"
    HIGH = "high"


# Core dataclass

@dataclass(frozen=True)
class ConversionRule:
    """A single SAS→Oracle conversion rule.

    Attributes:
        rule_id:             Unique identifier (R-01 … R-42).
        category:            One of the 8 RuleCategory values.
        sas_pattern:         Compiled regex that detects the SAS construct.
        oracle_replacement:  Replacement string (may contain \\1 back-refs)
                             or the sentinel ``"__HANDLER__"`` when a simple
                             regex substitution is not sufficient and the
                             converter must invoke a dedicated handler.
        description:         Human-readable explanation of the conversion.
        complexity:          basic / medium / high.
        notes:               Optional extra context for documentation.
    """
    rule_id: str
    category: RuleCategory
    sas_pattern: re.Pattern
    oracle_replacement: str
    description: str
    complexity: Complexity
    notes: str = ""


# Helper — compiling with IGNORECASE + DOTALL where appropriate

def _pat(pattern: str, flags: int = re.IGNORECASE) -> re.Pattern:
    """Compile *pattern* with default case-insensitive flag."""
    return re.compile(pattern, flags)


# CATEGORY 1 — NULL HANDLING  (4 rules)

R01 = ConversionRule(
    rule_id="R-01",
    category=RuleCategory.NULL_HANDLING,
    sas_pattern=_pat(r"\bIS\s+(NOT\s+)?MISSING\b"),
    oracle_replacement="__HANDLER__",
    description="IS [NOT] MISSING → IS [NOT] NULL",
    complexity=Complexity.BASIC,
    notes=(
        "SAS treats missing numeric (.) and missing char ('') identically via IS MISSING. "
        "Handler checks for optional NOT and emits IS NULL or IS NOT NULL accordingly."
    ),
)

R11 = ConversionRule(
    rule_id="R-11",
    category=RuleCategory.NULL_HANDLING,
    sas_pattern=_pat(r"'31Dec9999'd"),
    oracle_replacement="__HANDLER__",
    description="'31Dec9999'd active-record sentinel → IS NULL comparison",
    complexity=Complexity.MEDIUM,
    notes=(
        "SAS uses '31Dec9999'd to mean 'open/active'. Oracle typically uses "
        "NULL for open-ended dates.  The converter must rewrite the surrounding "
        "comparison: = '31Dec9999'd → IS NULL, <> '31Dec9999'd → IS NOT NULL."
    ),
)

R33 = ConversionRule(
    rule_id="R-33",
    category=RuleCategory.NULL_HANDLING,
    sas_pattern=_pat(r"\bNVL\s*\("),
    oracle_replacement="__KEEP__",
    description="NVL for null-safe arithmetic — already Oracle-compatible, keep as-is",
    complexity=Complexity.BASIC,
    notes="NVL() is valid Oracle SQL. Rule exists for pattern detection and documentation only.",
)

R34 = ConversionRule(
    rule_id="R-34",
    category=RuleCategory.NULL_HANDLING,
    sas_pattern=_pat(
        r"\bsum\s*\("
        r"(?![^)]*\bGROUP\b)"  # negative lookahead: not aggregate SUM with GROUP BY context
    ),
    oracle_replacement="__HANDLER__",
    description="SAS sum() NULL-safe arithmetic → NVL() chains",
    complexity=Complexity.HIGH,
    notes=(
        "SAS sum(a, b) ignores NULLs (returns whichever arg is non-NULL). "
        "Oracle's a + b propagates NULL.  Must wrap: NVL(a,0) + NVL(b,0). "
        "Handler must distinguish arithmetic sum() from aggregate SUM()."
    ),
)


# CATEGORY 2 — DATE FUNCTIONS  (7 rules)

R05 = ConversionRule(
    rule_id="R-05",
    category=RuleCategory.DATE_FUNCTIONS,
    sas_pattern=_pat(
        r"""\bINTNX\s*\(\s*["']MONTH["']\s*,\s*(.+?)\s*,\s*0\s*,\s*["']BEGIN["']\s*\)"""
    ),
    oracle_replacement=r"TRUNC(\1, 'MM')",
    description="INTNX('MONTH', date, 0, 'BEGIN') → TRUNC(date, 'MM')",
    complexity=Complexity.BASIC,
)

R06 = ConversionRule(
    rule_id="R-06",
    category=RuleCategory.DATE_FUNCTIONS,
    sas_pattern=_pat(
        r"""\bINTNX\s*\(\s*["']MONTH["']\s*,\s*(.+?)\s*,\s*0\s*,\s*["']END["']\s*\)"""
    ),
    oracle_replacement=r"LAST_DAY(\1)",
    description="INTNX('MONTH', date, 0, 'END') → LAST_DAY(date)",
    complexity=Complexity.BASIC,
)

R07 = ConversionRule(
    rule_id="R-07",
    category=RuleCategory.DATE_FUNCTIONS,
    sas_pattern=_pat(
        r"""\bINTNX\s*\(\s*["'](?:MONTH|month)["']\s*,\s*(.+?)\s*,\s*(-?\d+)\s*\)"""
    ),
    oracle_replacement=r"ADD_MONTHS(\1, \2)",
    description="INTNX('month', date, n) → ADD_MONTHS(date, n)",
    complexity=Complexity.MEDIUM,
    notes="Only matches the 3-argument form (no alignment parameter).",
)

R08 = ConversionRule(
    rule_id="R-08",
    category=RuleCategory.DATE_FUNCTIONS,
    sas_pattern=_pat(
        r"""\bINTNX\s*\(\s*["'](?:MONTH|month)["']\s*,\s*(.+?)\s*,\s*(-?\d+)\s*,\s*["']E["']\s*\)"""
    ),
    oracle_replacement=r"LAST_DAY(ADD_MONTHS(\1, \2))",
    description="INTNX('month', date, n, 'E') → LAST_DAY(ADD_MONTHS(date, n))",
    complexity=Complexity.MEDIUM,
    notes="'E' alignment means end-of-month.",
)

R09 = ConversionRule(
    rule_id="R-09",
    category=RuleCategory.DATE_FUNCTIONS,
    sas_pattern=_pat(r"\btoday\s*\(\s*\)"),
    oracle_replacement="TRUNC(SYSDATE)",
    description="today() → TRUNC(SYSDATE)",
    complexity=Complexity.BASIC,
)

R10 = ConversionRule(
    rule_id="R-10",
    category=RuleCategory.DATE_FUNCTIONS,
    sas_pattern=_pat(r"'(\d{1,2})(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(\d{4})'d"),
    oracle_replacement="__HANDLER__",
    description="SAS date literal 'DDMonYYYY'd → TO_DATE('YYYYMMDD','YYYYMMDD')",
    complexity=Complexity.MEDIUM,
    notes=(
        "Handler must parse the day/month/year components and emit "
        "TO_DATE('YYYYMMDD','YYYYMMDD'). Month abbreviation must map to number."
    ),
)

R27 = ConversionRule(
    rule_id="R-27",
    category=RuleCategory.DATE_FUNCTIONS,
    sas_pattern=_pat(
        r"\bmdy\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)"
    ),
    oracle_replacement="__HANDLER__",
    description="mdy(month, day, year) → TO_DATE(year||month||day, 'YYYYMMDD')",
    complexity=Complexity.MEDIUM,
    notes=(
        "Argument order is month-day-year.  If args are macro variables or "
        "literals, handler builds a TO_DATE with string concatenation."
    ),
)


# CATEGORY 3 — STRING FUNCTIONS  (5 rules)

R12 = ConversionRule(
    rule_id="R-12",
    category=RuleCategory.STRING_FUNCTIONS,
    sas_pattern=_pat(r"\bUPCASE\s*\("),
    oracle_replacement="UPPER(",
    description="UPCASE() → UPPER()",
    complexity=Complexity.BASIC,
)

R13 = ConversionRule(
    rule_id="R-13",
    category=RuleCategory.STRING_FUNCTIONS,
    sas_pattern=_pat(r"\bLOWCASE\s*\("),
    oracle_replacement="LOWER(",
    description="LOWCASE() → LOWER()",
    complexity=Complexity.BASIC,
)

R14 = ConversionRule(
    rule_id="R-14",
    category=RuleCategory.STRING_FUNCTIONS,
    sas_pattern=_pat(r"\bSTRIP\s*\("),
    oracle_replacement="TRIM(",
    description="STRIP() → TRIM()",
    complexity=Complexity.BASIC,
)

R15 = ConversionRule(
    rule_id="R-15",
    category=RuleCategory.STRING_FUNCTIONS,
    sas_pattern=_pat(r"\bCOMPRESS\s*\(\s*(\S+?)\s*\)"),
    oracle_replacement=r"REPLACE(\1, ' ', '')",
    description="COMPRESS(str) basic (remove spaces) → REPLACE(str, ' ', '')",
    complexity=Complexity.BASIC,
    notes="No-modifier COMPRESS only strips spaces.",
)

R16 = ConversionRule(
    rule_id="R-16",
    category=RuleCategory.STRING_FUNCTIONS,
    sas_pattern=_pat(
        r"""\bCOMPRESS\s*\(\s*(.+?)\s*,\s*,\s*["'](k[adn])["']\s*\)"""
    ),
    oracle_replacement="__HANDLER__",
    description="COMPRESS(str, , 'kd'/'ka') → REGEXP_REPLACE()",
    complexity=Complexity.MEDIUM,
    notes=(
        "'kd' = keep digits → REGEXP_REPLACE(str, '[^0-9]', '').  "
        "'ka' = keep alpha  → REGEXP_REPLACE(str, '[^A-Za-z]', ''). "
        "'kn' = keep name-chars → REGEXP_REPLACE(str, '[^A-Za-z0-9_]', '')."
    ),
)


# CATEGORY 4 — MACRO VARIABLES  (3 rules)

R02 = ConversionRule(
    rule_id="R-02",
    category=RuleCategory.MACRO_VARIABLES,
    sas_pattern=_pat(r"&(\w+)\.?"),
    oracle_replacement=r"${\1}",
    description="&macro_var. → ${pentaho_variable}",
    complexity=Complexity.BASIC,
    notes=(
        "Trailing dot is SAS delimiter and must be consumed.  "
        "Variables inside string literals need special handling by the converter "
        "(detect surrounding quotes, wrap with TO_DATE if date variable)."
    ),
)

R03 = ConversionRule(
    rule_id="R-03",
    category=RuleCategory.MACRO_VARIABLES,
    sas_pattern=_pat(r"^\s*%(LET|GLOBAL)\b[^;]*;", re.IGNORECASE | re.MULTILINE),
    oracle_replacement="",
    description="%LET / %GLOBAL declarations → removed (become Pentaho parameters)",
    complexity=Complexity.BASIC,
    notes="Values are extracted separately by variable_handler and stored as Pentaho properties.",
)

R37 = ConversionRule(
    rule_id="R-37",
    category=RuleCategory.MACRO_VARIABLES,
    sas_pattern=_pat(
        r"^\s*(?:LIBNAME\b[^;]*;|%include\b[^;]*;)",
        re.IGNORECASE | re.MULTILINE,
    ),
    oracle_replacement="",
    description="LIBNAME META / %include statements → ignored (no Oracle equivalent)",
    complexity=Complexity.BASIC,
    notes="LIBNAME defines SAS library connections; %include loads external macros. Both are removed.",
)


# CATEGORY 5 — TYPE CONVERSION  (4 rules)

R20 = ConversionRule(
    rule_id="R-20",
    category=RuleCategory.TYPE_CONVERSION,
    sas_pattern=_pat(r"\bPUT\s*\(\s*(.+?)\s*,\s*([A-Z]\w*)\s*\.\s*\)"),
    oracle_replacement="__HANDLER__",
    description="PUT(numeric_col, NUMFMT.) → CASE WHEN lookup (numeric format)",
    complexity=Complexity.HIGH,
    notes=(
        "Numeric format names have NO $ prefix (e.g. TARIF., CATFMT., OCCUPGRP.). "
        "Handler must generate CASE WHEN branches from the format catalog or "
        "flag as manual if the catalog is not available."
    ),
)

R21 = ConversionRule(
    rule_id="R-21",
    category=RuleCategory.TYPE_CONVERSION,
    sas_pattern=_pat(r"\bPUT\s*\(\s*(.+?)\s*,\s*\$(\w+)\s*\.\s*\)"),
    oracle_replacement="__HANDLER__",
    description="PUT(char_col, $CHARFMT.) → CASE WHEN lookup (character format)",
    complexity=Complexity.HIGH,
    notes=(
        "Character format names have $ prefix (e.g. $CUSTTYPE., $PNRTYP.). "
        "Same handler logic as R-20 but input is already character type."
    ),
)

R22 = ConversionRule(
    rule_id="R-22",
    category=RuleCategory.TYPE_CONVERSION,
    sas_pattern=_pat(r"\bPUT\s*\(\s*(.+?)\s*,\s*(\d+)\s*\.\s*\)"),
    oracle_replacement=r"TO_CHAR(\1)",
    description="PUT(numeric_col, N.) → TO_CHAR(col)  (simple number-to-string)",
    complexity=Complexity.BASIC,
    notes="N. is a width-only numeric format — just converts number to string.",
)

R23 = ConversionRule(
    rule_id="R-23",
    category=RuleCategory.TYPE_CONVERSION,
    sas_pattern=_pat(
        r"\bCHOOSEC\s*\(\s*INPUT\s*\(\s*(.+?)\s*,\s*\d+\s*\.\s*\)\s*,"
    ),
    oracle_replacement="__HANDLER__",
    description="CHOOSEC(INPUT(col, N.), 'A','B','C') → CASE TO_NUMBER(col) WHEN 1 THEN 'A' …",
    complexity=Complexity.HIGH,
    notes=(
        "INPUT converts string→number, CHOOSEC picks the Nth string argument. "
        "Handler must parse the full argument list and emit a CASE expression "
        "with 1-based index mapping."
    ),
)


# CATEGORY 6 — SAS KEYWORDS  (11 rules)

R04 = ConversionRule(
    rule_id="R-04",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(
        r'(?<=\bTHEN\s)'     # preceded by THEN (CASE context)
        r'"([^"]*)"'         # double-quoted string
    ),
    oracle_replacement=r"'\1'",
    description="Double-quoted string literals → single-quoted (Oracle standard)",
    complexity=Complexity.BASIC,
    notes=(
        "Only matches double-quoted strings in SQL value contexts. "
        "Must not touch identifier quoting.  Converter applies broadly; "
        "this pattern targets the most common CASE WHEN context."
    ),
)

R17 = ConversionRule(
    rule_id="R-17",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"\bOUTER\s+UNION\s+CORR\b"),
    oracle_replacement="UNION ALL",
    description="OUTER UNION CORR → UNION ALL",
    complexity=Complexity.BASIC,
    notes="OUTER UNION CORR concatenates by column name; UNION ALL is the Oracle equivalent.",
)

R18 = ConversionRule(
    rule_id="R-18",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(
        r"\s+FORMAT\s*=\s*"
        r"(?:\$?\w+\d*\.?\d*)"  # format spec like $30., EURDFDD10., COMMA12.2, DATE9.
    ),
    oracle_replacement="",
    description="FORMAT= attribute on columns → removed (SAS display-only, no Oracle equivalent)",
    complexity=Complexity.BASIC,
    notes="Includes $30., EURDFDD10., DDMMYYP10., COMMA12.2, DATE9., COMMAX20. etc.",
)

R19 = ConversionRule(
    rule_id="R-19",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"\s+LABEL\s*=\s*(?:'[^']*'|\"[^\"]*\"|'')"),
    oracle_replacement="",
    description="LABEL= attribute on columns → removed (no Oracle equivalent)",
    complexity=Complexity.BASIC,
)

R24 = ConversionRule(
    rule_id="R-24",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"\bCALCULATED\s+(\w+)"),
    oracle_replacement="__HANDLER__",
    description="CALCULATED alias → repeat the original expression inline",
    complexity=Complexity.MEDIUM,
    notes=(
        "SAS allows referencing a column alias in WHERE/HAVING via CALCULATED. "
        "Oracle requires the full expression to be repeated.  Handler must look "
        "up the alias definition from the SELECT list."
    ),
)

R25 = ConversionRule(
    rule_id="R-25",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"'([^']+)'n"),
    oracle_replacement="__HANDLER__",
    description="'Column Name'n (SAS name literal) → COLUMN_NAME (valid Oracle identifier)",
    complexity=Complexity.MEDIUM,
    notes=(
        "Handler replaces spaces and special chars with underscores, "
        "upper-cases the result, and propagates the rename across all "
        "downstream references in the same conversion session."
    ),
)

R26 = ConversionRule(
    rule_id="R-26",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"\bNOT\s*=\s*"),
    oracle_replacement="<> ",
    description="NOT = operator → <> (Oracle standard inequality)",
    complexity=Complexity.BASIC,
)

R28 = ConversionRule(
    rule_id="R-28",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"(?<!\w)\bgt\b(?!\w)"),
    oracle_replacement=">",
    description="SAS word operator 'gt' → '>'",
    complexity=Complexity.BASIC,
    notes="Also need le → <=, ge → >=, lt → <, eq → =, ne → <>. See R-28b–R-28e.",
)

R28b = ConversionRule(
    rule_id="R-28b",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"(?<!\w)\ble\b(?!\w)"),
    oracle_replacement="<=",
    description="SAS word operator 'le' → '<='",
    complexity=Complexity.BASIC,
)

R28c = ConversionRule(
    rule_id="R-28c",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"(?<!\w)\bge\b(?!\w)"),
    oracle_replacement=">=",
    description="SAS word operator 'ge' → '>='",
    complexity=Complexity.BASIC,
)

R28d = ConversionRule(
    rule_id="R-28d",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"(?<!\w)\blt\b(?!\w)"),
    oracle_replacement="<",
    description="SAS word operator 'lt' → '<'",
    complexity=Complexity.BASIC,
)

R28e = ConversionRule(
    rule_id="R-28e",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"(?<!\w)\bne\b(?!\w)"),
    oracle_replacement="<>",
    description="SAS word operator 'ne' → '<>'",
    complexity=Complexity.BASIC,
)

R29 = ConversionRule(
    rule_id="R-29",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(
        r"\bORDER\s+BY\b[^;]+(?=\s*;)",
        re.IGNORECASE | re.DOTALL,
    ),
    oracle_replacement="__HANDLER__",
    description="ORDER BY in CREATE TABLE AS SELECT → removed (Oracle ignores/rejects it)",
    complexity=Complexity.BASIC,
    notes=(
        "Handler checks whether the statement is a CTAS. If so, the ORDER BY "
        "clause is stripped.  If it is a standalone SELECT, ORDER BY is kept."
    ),
)

R36 = ConversionRule(
    rule_id="R-36",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(r"\bCONTAINS\s+'([^']+)'"),
    oracle_replacement=r"LIKE '%\1%'",
    description="CONTAINS 'value' → LIKE '%value%'",
    complexity=Complexity.BASIC,
    notes="SAS CONTAINS is a case-sensitive substring check.",
)

R41 = ConversionRule(
    rule_id="R-41",
    category=RuleCategory.SAS_KEYWORDS,
    sas_pattern=_pat(
        r"%_eg_conditional_dropds\s*\(\s*(\w+\.\w+)\s*\)\s*;",
    ),
    oracle_replacement="__HANDLER__",
    description="%_eg_conditional_dropds(WORK.TABLE) → DROP TABLE schema.PREFIX_TABLE",
    complexity=Complexity.BASIC,
    notes=(
        "SAS EG helper macro drops a table if it exists. "
        "Handler maps the WORK.name to the target schema and emits "
        "a standard DROP TABLE statement (or Oracle BEGIN…EXCEPTION block)."
    ),
)


# CATEGORY 7 — TABLE MAPPING  (5 rules)

R30 = ConversionRule(
    rule_id="R-30",
    category=RuleCategory.TABLE_MAPPING,
    sas_pattern=_pat(r"(\w+)\.(\w+)_&(\w+)\.?"),
    oracle_replacement="__HANDLER__",
    description="Dynamic table suffix (LIB.TABLE_&period.) → parameterized WHERE or fixed name",
    complexity=Complexity.MEDIUM,
    notes=(
        "SAS can embed macro vars in table names to select period-specific tables. "
        "Oracle alternative: single table with period_key column filtered via WHERE."
    ),
)

R35 = ConversionRule(
    rule_id="R-35",
    category=RuleCategory.TABLE_MAPPING,
    sas_pattern=_pat(r"\bWORK\.(\w+)"),
    oracle_replacement="__HANDLER__",
    description="WORK.tablename → schema.PREFIX_tablename (temp table mapping)",
    complexity=Complexity.BASIC,
    notes=(
        "Handler uses table_mapper to resolve WORK references to the target "
        "schema and prefix.  Also tracks chained dependencies so tables are "
        "created in the correct order."
    ),
)

R38 = ConversionRule(
    rule_id="R-38",
    category=RuleCategory.TABLE_MAPPING,
    sas_pattern=_pat(
        r"\bvalid_from\b.+?\bvalid_to\b",
        re.IGNORECASE | re.DOTALL,
    ),
    oracle_replacement="__KEEP__",
    description="Time-slice validity filters (valid_from/valid_to) — keep, but verify date wrapping",
    complexity=Complexity.MEDIUM,
    notes=(
        "Time-slice tables (_TGL) use valid_from <= date AND valid_to > date. "
        "Rule detects the pattern; converter ensures date constants use TO_DATE()."
    ),
)

R39 = ConversionRule(
    rule_id="R-39",
    category=RuleCategory.TABLE_MAPPING,
    sas_pattern=_pat(r"\b(?:PK_STAND|snapshot_period|period_key)\s*=\s*&(\w+)"),
    oracle_replacement="__HANDLER__",
    description="PK_STAND / period_key with macro variable → parameterized filter",
    complexity=Complexity.MEDIUM,
    notes=(
        "Monthly tables use 6-digit YYYYMM period key; daily tables use 8-digit "
        "YYYYMMDD.  Handler ensures the Pentaho variable substitution produces "
        "the correct digit count."
    ),
)

R40 = ConversionRule(
    rule_id="R-40",
    category=RuleCategory.TABLE_MAPPING,
    sas_pattern=_pat(
        r"\(\s*SELECT\b[^)]+\bFROM\b[^)]+\bWHERE\b[^)]+\bBETWEEN\s+valid_from\b",
        re.IGNORECASE | re.DOTALL,
    ),
    oracle_replacement="__KEEP__",
    description="Correlated subquery for time-slice lookup — keep, verify date handling",
    complexity=Complexity.HIGH,
    notes=(
        "Pattern like (SELECT desc FROM lookup WHERE sysdate BETWEEN valid_from AND valid_to "
        "AND key = t1.key).  Valid Oracle; just ensure date expressions are converted."
    ),
)


# CATEGORY 8 — JOIN PATTERNS  (3 rules)

R31 = ConversionRule(
    rule_id="R-31",
    category=RuleCategory.JOIN_PATTERNS,
    sas_pattern=_pat(r"__INLINE_VALUES__"),  # sentinel — detected by structure, not regex
    oracle_replacement="__HANDLER__",
    description="Inline VALUES subquery (T_SVZ pattern) replacing unavailable lookup table",
    complexity=Complexity.HIGH,
    notes=(
        "SAS code sometimes builds an inline table via a VALUES-like construct. "
        "Handler either emits an Oracle WITH clause (CTE) or a UNION ALL of "
        "SELECT … FROM DUAL rows."
    ),
)

R32 = ConversionRule(
    rule_id="R-32",
    category=RuleCategory.JOIN_PATTERNS,
    sas_pattern=_pat(r"\bRIGHT\s+JOIN\b"),
    oracle_replacement="__KEEP__",
    description="RIGHT JOIN — valid Oracle, but check NULL semantics with WHERE on left table",
    complexity=Complexity.MEDIUM,
    notes=(
        "A RIGHT JOIN combined with a WHERE filter on the left (outer) table "
        "effectively becomes an INNER JOIN because NULLs are filtered out. "
        "Converter flags this as a warning for manual review."
    ),
)

R42 = ConversionRule(
    rule_id="R-42",
    category=RuleCategory.JOIN_PATTERNS,
    sas_pattern=_pat(
        r"\bINNER\s+JOIN\s*\(\s*SELECT\s+MAX\s*\(",
        re.IGNORECASE | re.DOTALL,
    ),
    oracle_replacement="__KEEP__",
    description="MAX() subquery for latest period filtering — valid Oracle, keep as-is",
    complexity=Complexity.MEDIUM,
    notes="Pattern: INNER JOIN (SELECT MAX(period) …). Valid Oracle SQL; no conversion needed.",
)


# Rule registry

ALL_RULES: list[ConversionRule] = [
    # Category 1 — NULL handling
    R01, R11, R33, R34,
    # Category 2 — Date functions
    R05, R06, R07, R08, R09, R10, R27,
    # Category 3 — String functions
    R12, R13, R14, R15, R16,
    # Category 4 — Macro variables
    R02, R03, R37,
    # Category 5 — Type conversion
    R20, R21, R22, R23,
    # Category 6 — SAS keywords
    R04, R17, R18, R19, R24, R25, R26, R28, R28b, R28c, R28d, R28e, R29, R36, R41,
    # Category 7 — Table mapping
    R30, R35, R38, R39, R40,
    # Category 8 — Join patterns
    R31, R32, R42,
]
"""Flat list of every ConversionRule, in canonical order."""


def get_rules_by_category() -> dict[RuleCategory, list[ConversionRule]]:
    """Return rules grouped by category."""
    grouped: dict[RuleCategory, list[ConversionRule]] = {cat: [] for cat in RuleCategory}
    for rule in ALL_RULES:
        grouped[rule.category].append(rule)
    return grouped


def get_rule_by_id(rule_id: str) -> Optional[ConversionRule]:
    """Look up a single rule by its ID string (e.g. ``'R-01'``)."""
    for rule in ALL_RULES:
        if rule.rule_id == rule_id:
            return rule
    return None


def get_handler_rules() -> list[ConversionRule]:
    """Return only rules that require a dedicated converter handler."""
    return [r for r in ALL_RULES if r.oracle_replacement == "__HANDLER__"]


def get_regex_rules() -> list[ConversionRule]:
    """Return rules that can be applied via simple regex substitution."""
    return [
        r for r in ALL_RULES
        if r.oracle_replacement not in ("__HANDLER__", "__KEEP__")
    ]


def get_keep_rules() -> list[ConversionRule]:
    """Return detection-only rules (valid Oracle — no conversion needed)."""
    return [r for r in ALL_RULES if r.oracle_replacement == "__KEEP__"]


# Summary helpers for documenter and CLI --verbose help

def rule_summary() -> str:
    """Return a human-readable summary table of all rules."""
    lines = [
        f"{'ID':<7} {'Category':<20} {'Complexity':<10} {'Description'}",
        "-" * 80,
    ]
    for rule in ALL_RULES:
        lines.append(
            f"{rule.rule_id:<7} {rule.category.value:<20} "
            f"{rule.complexity.value:<10} {rule.description}"
        )
    lines.append(f"\nTotal rules: {len(ALL_RULES)}")
    lines.append(f"  Regex-applicable:  {len(get_regex_rules())}")
    lines.append(f"  Handler-required:  {len(get_handler_rules())}")
    lines.append(f"  Detection-only:    {len(get_keep_rules())}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(rule_summary())
    print()
    ids = [r.rule_id for r in ALL_RULES]
    dupes = [rid for rid in ids if ids.count(rid) > 1]
    if dupes:
        print(f"WARNING — duplicate rule IDs: {set(dupes)}")
    else:
        print(f"All {len(ids)} rule IDs are unique.")
    print("All regex patterns compiled successfully.")
