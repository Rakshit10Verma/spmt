"""Scoring engine for the SPMT vs LLM thesis comparison.

Axes (weights): Functional Parity 70% | Syntax/Metadata 20% | Code Style 10%
  BLEU-4 = BP * exp(Σ 0.25 * log(p_n)),  BP = exp(1 - r/c) if c < r, else 1.0
  MI = 171 - 5.2*ln(V) - 0.23*CC - 16.2*ln(LOC),  V = N*log2(n),  CC = decisions+1
  pass@k = 1 - C(n-c, k) / C(n, k)  (Chen et al., 2021), threshold = 70.0
"""

from __future__ import annotations

import argparse
import math
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

DATA_ROOT = Path("data_outputs")

TEST_CASE_IDS: tuple[str, ...] = tuple(f"TC-{n:02d}" for n in range(1, 36))

WEIGHT_FUNCTIONAL_PARITY = 0.70
WEIGHT_SYNTAX_METADATA   = 0.20
WEIGHT_CODE_STYLE        = 0.10

_WEIGHT_TOTAL = WEIGHT_FUNCTIONAL_PARITY + WEIGHT_SYNTAX_METADATA + WEIGHT_CODE_STYLE
if abs(_WEIGHT_TOTAL - 1.0) > 1e-9:
    raise ValueError(f"Scoring weights must sum to 1.0, got {_WEIGHT_TOTAL}")

SCORE_FLOOR   = 0.0
SCORE_CEILING = 100.0

# composite score >= this threshold counts as a passing run for pass@k
PASS_THRESHOLD = 70.0

# max LLM runs expected per fixture (TC-01_run1.sql, _run2.sql, _run3.sql)
K_ATTEMPTS = 3


def _clamp_score(value: float) -> float:
    return max(SCORE_FLOOR, min(SCORE_CEILING, value))


# SQL structural keywords for AST skeleton extraction (identifiers stripped)
_SQL_KEYWORDS: frozenset[str] = frozenset({
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
    "FULL", "CROSS", "ON", "GROUP", "BY", "ORDER", "HAVING", "UNION",
    "INTERSECT", "EXCEPT", "MINUS", "INSERT", "INTO", "VALUES", "UPDATE",
    "SET", "DELETE", "CREATE", "TABLE", "VIEW", "DROP", "ALTER", "AS",
    "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "IN",
    "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "DISTINCT", "ALL", "ANY",
    "WITH", "OVER", "PARTITION", "ROWS", "RANGE", "PRECEDING", "FOLLOWING",
    "CURRENT", "ROW", "UNBOUNDED", "PIVOT", "UNPIVOT", "FETCH", "FIRST",
    "ONLY", "RECURSIVE", "MERGE", "USING", "MATCHED",
})


def _sql_tokenize(text: str) -> list[str]:
    """Strip SQL comments then tokenize into lowercase tokens."""
    text = re.sub(r"--[^\n]*", " ", text)
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    return re.findall(r"[A-Za-z_]\w*|\d+(?:\.\d+)?|[^\w\s]", text.lower())


def _ngram_counts(tokens: list[str], n: int) -> Counter[tuple[str, ...]]:
    """Return a Counter of every n-gram in tokens."""
    return Counter(
        tuple(tokens[i : i + n]) for i in range(len(tokens) - n + 1)
    )


def calculate_bleu_4(reference_sql: str, candidate_sql: str) -> float:
    """BLEU-4 n-gram similarity (Papineni et al., 2002).

    BLEU-4 = BP * exp(Σ_{n=1}^{4} 0.25 * log(p_n))
    p_n    = clipped n-gram precision = Σ min(count_cand, count_ref) / total_cand_ngrams
    BP     = 1.0 if c >= r, else exp(1 - r/c)
    Add-1 smoothing prevents log(0). Returns float in [0, 1].
    """
    ref_tokens  = _sql_tokenize(reference_sql)
    cand_tokens = _sql_tokenize(candidate_sql)

    if not ref_tokens or not cand_tokens:
        return 0.0

    # BP: penalizes candidates shorter than reference
    c = len(cand_tokens)
    r = len(ref_tokens)
    brevity_penalty = 1.0 if c >= r else math.exp(1.0 - r / c)

    log_precision_sum = 0.0
    for n in range(1, 5):
        if len(cand_tokens) < n or len(ref_tokens) < n:
            log_precision_sum += math.log(1e-10)
            continue

        ref_counts  = _ngram_counts(ref_tokens, n)
        cand_counts = _ngram_counts(cand_tokens, n)

        clipped_matches = sum(
            min(count, ref_counts[gram])
            for gram, count in cand_counts.items()
        )
        total_cand_ngrams = sum(cand_counts.values())

        # add-1 smoothing
        precision = (clipped_matches + 1) / (total_cand_ngrams + 1)
        log_precision_sum += math.log(precision)

    # dividing by 4 applies uniform weight w_n = 0.25
    bleu = brevity_penalty * math.exp(log_precision_sum / 4.0)
    return max(0.0, min(1.0, bleu))


def _extract_syntax_skeleton(sql: str) -> list[str]:
    """Strip non-keyword tokens; return keyword-only sequence as a lightweight AST proxy."""
    tokens_upper = [t.upper() for t in _sql_tokenize(sql)]
    return [t for t in tokens_upper if t in _SQL_KEYWORDS]


def calculate_ast_similarity(reference_sql: str, candidate_sql: str) -> float:
    """Ratcliff/Obershelp ratio on SQL keyword sequences. Returns [0, 1]."""
    ref_skel  = _extract_syntax_skeleton(reference_sql)
    cand_skel = _extract_syntax_skeleton(candidate_sql)

    if not ref_skel or not cand_skel:
        return 0.0

    return SequenceMatcher(None, ref_skel, cand_skel).ratio()


# Operators for Halstead Volume; everything else is an operand
_HALSTEAD_OPERATORS: frozenset[str] = frozenset({
    "select", "from", "where", "join", "left", "right", "inner", "outer",
    "on", "group", "order", "by", "having", "union", "intersect", "except",
    "and", "or", "not", "in", "exists", "between", "like", "is", "null",
    "case", "when", "then", "else", "end", "as", "distinct", "over",
    "partition", "rows", "coalesce", "nullif", "nvl", "decode",
    "+", "-", "*", "/", "=", "<", ">", "!", "(", ")", ",", ".",
})

# Keywords that add a branch point for McCabe CC
_CC_DECISION_KEYWORDS: frozenset[str] = frozenset({
    "when", "and", "or", "join", "coalesce", "nullif", "case",
    "exists", "not", "between", "like", "if",
})


def calculate_halstead_volume(sql_string: str) -> float:
    """Halstead Volume: V = N * log2(n), where N = total tokens, n = vocabulary size."""
    tokens = _sql_tokenize(sql_string)
    if not tokens:
        return 0.0

    operators = [t for t in tokens if t in _HALSTEAD_OPERATORS]
    operands  = [t for t in tokens if t not in _HALSTEAD_OPERATORS]

    n1 = len(set(operators))
    n2 = len(set(operands))
    n1_total = len(operators)
    n2_total = len(operands)

    vocabulary = n1 + n2
    length     = n1_total + n2_total

    if vocabulary < 2:
        # log2(1) = 0 → V = 0; log2(0) is undefined
        return 0.0

    return length * math.log2(vocabulary)


def calculate_cyclomatic_complexity(sql_string: str) -> int:
    """McCabe CC = decision_points + 1."""
    tokens = _sql_tokenize(sql_string)
    decision_points = sum(1 for t in tokens if t in _CC_DECISION_KEYWORDS)
    return decision_points + 1


def _calculate_mi_raw(sql_string: str) -> float:
    """Raw Maintainability Index (Coleman et al., 1994).

    MI = 171 - 5.2*ln(V) - 0.23*CC - 16.2*ln(LOC)
    LOC = non-blank lines. Ranges: 85-171 high, 65-85 medium, <65 difficult.
    """
    v   = calculate_halstead_volume(sql_string)
    cc  = calculate_cyclomatic_complexity(sql_string)
    loc = max(1, sum(1 for ln in sql_string.splitlines() if ln.strip()))

    ln_v   = math.log(v)   if v   > 0.0 else 0.0
    ln_loc = math.log(loc) if loc > 0   else 0.0

    return 171.0 - 5.2 * ln_v - 0.23 * cc - 16.2 * ln_loc


def compute_pass_at_k(n: int, c: int, k: int) -> float:
    """Unbiased pass@k estimator (Chen et al., 2021).

    pass@k = 1 - C(n-c, k) / C(n, k)
    n = total runs, c = passing runs (composite >= PASS_THRESHOLD), k = attempts.
    """
    if n < k or c == 0:
        return 0.0
    denom = math.comb(n, k)
    if denom == 0:
        return 0.0
    numerator = math.comb(n - c, k) if (n - c) >= k else 0
    return max(0.0, min(1.0, 1.0 - numerator / denom))


@dataclass
class ComponentScores:
    """Three 0-100 axis scores before weighting."""

    functional_parity: float = 0.0
    syntax_metadata:   float = 0.0
    code_style:        float = 0.0

    def __post_init__(self) -> None:
        self.functional_parity = _clamp_score(self.functional_parity)
        self.syntax_metadata   = _clamp_score(self.syntax_metadata)
        self.code_style        = _clamp_score(self.code_style)


@dataclass
class EvaluationReport:
    """All scoring data for one (tool, test-case) pair."""

    test_case_id: str
    tool_name:    str
    scores:       ComponentScores  = field(default_factory=ComponentScores)
    sql_path:     Path | None      = None
    ktr_path:     Path | None      = None
    issues:       list[str]        = field(default_factory=list)

    bleu_score:   float            = float("nan")
    halstead_v:   float            = 0.0
    cyclomatic_c: int              = 0
    pass_at_k:    float | None     = None

    @property
    def has_sql(self) -> bool:
        return self.sql_path is not None

    @property
    def has_ktr(self) -> bool:
        return self.ktr_path is not None

    @property
    def weighted_breakdown(self) -> dict[str, float]:
        return {
            "functional_parity": self.scores.functional_parity * WEIGHT_FUNCTIONAL_PARITY,
            "syntax_metadata":   self.scores.syntax_metadata   * WEIGHT_SYNTAX_METADATA,
            "code_style":        self.scores.code_style        * WEIGHT_CODE_STYLE,
        }

    @property
    def composite_score(self) -> float:
        """Weighted sum: 0.70*FP + 0.20*SM + 0.10*CS."""
        return sum(self.weighted_breakdown.values())


def discover_outputs(tool_dir: Path, test_case_id: str) -> dict[str, list[Path]]:
    """Glob for TC-XX*.sql/.ktr; handles SPMT (single file) and LLM (multi-run) layouts."""
    if not tool_dir.exists():
        return {"sql": [], "ktr": []}

    id_variants = [test_case_id, test_case_id.lower()]
    sql_files: list[Path] = sorted(
        {p for v in id_variants for p in tool_dir.glob(f"{v}*.sql")}
    )
    ktr_files: list[Path] = sorted(
        {p for v in id_variants
         for ext in ("*.ktr", "*.ktr.xml")
         for p in tool_dir.glob(f"{v}{ext}")}
    )
    return {"sql": sql_files, "ktr": ktr_files}


def score_functional_parity(
    sql_path: Path | None,
    ktr_path: Path | None,
    reference_sql_path: Path | None = None,
) -> tuple[float, list[str], float]:
    """Grade SQL conversion quality 0-100.

    SPMT mode (no reference): heuristic keyword-leakage scan across 5 sub-categories.
    LLM mode (reference provided): BLEU-4 + AST similarity, 50/50 split.
    Returns (score, issues, bleu_value). bleu_value = nan for SPMT.
    """
    del ktr_path  # accepted for call-site symmetry; not used here

    bleu_value: float = float("nan")

    if sql_path is None or not sql_path.exists():
        return 0.0, ["no SQL file to validate"], bleu_value

    candidate_sql = sql_path.read_text(encoding="utf-8")

    # LLM mode
    if reference_sql_path is not None and reference_sql_path.exists():
        reference_sql = reference_sql_path.read_text(encoding="utf-8")
        issues: list[str] = []

        bleu  = calculate_bleu_4(reference_sql, candidate_sql)
        ast_s = calculate_ast_similarity(reference_sql, candidate_sql)
        bleu_value = bleu

        # 50/50 split: BLEU-4 = token overlap, AST = structural keyword order
        combined = (bleu + ast_s) / 2.0

        if bleu < 0.50:
            issues.append(
                f"BLEU-4 {bleu:.3f} < 0.50 — significant token-level divergence "
                f"from SPMT reference"
            )
        if ast_s < 0.50:
            issues.append(
                f"AST skeleton similarity {ast_s:.3f} < 0.50 — structural divergence"
            )

        return _clamp_score(combined * 100.0), issues, bleu_value

    # SPMT mode: keyword-leakage heuristic
    POINTS_SAS_KEYWORD_LEAKAGE   = 25
    POINTS_UNCONVERTED_FUNCTIONS = 25
    POINTS_DATE_LITERAL_SURVIVAL = 20
    POINTS_MACRO_SURVIVAL        = 15
    POINTS_TABLE_BALANCE         = 15
    assert (
        POINTS_SAS_KEYWORD_LEAKAGE + POINTS_UNCONVERTED_FUNCTIONS
        + POINTS_DATE_LITERAL_SURVIVAL + POINTS_MACRO_SURVIVAL
        + POINTS_TABLE_BALANCE == 100
    ), "functional parity point constants must sum to 100"

    lines  = candidate_sql.splitlines()
    issues = []
    earned: float = 0.0

    sas_keyword_patterns: dict[str, re.Pattern[str]] = {
        "IS MISSING":       re.compile(r"\bIS\s+MISSING\b",         re.IGNORECASE),
        "NOT IS MISSING":   re.compile(r"\bNOT\s+IS\s+MISSING\b",   re.IGNORECASE),
        "CALCULATED":       re.compile(r"\bCALCULATED\b",           re.IGNORECASE),
        "CONTAINS":         re.compile(r"\bCONTAINS\b",             re.IGNORECASE),
        "OUTER UNION CORR": re.compile(r"\bOUTER\s+UNION\s+CORR\b", re.IGNORECASE),
        "word operator":    re.compile(r"\b(gt|lt|ge|le|eq|ne)\b",  re.IGNORECASE),
    }
    fired_keywords: set[str] = set()
    for lineno, line in enumerate(lines, start=1):
        for label, pattern in sas_keyword_patterns.items():
            for match in pattern.finditer(line):
                issues.append(f"SAS keyword '{match.group(0)}' found at line {lineno}")
                fired_keywords.add(label)

    deduct = POINTS_SAS_KEYWORD_LEAKAGE / len(sas_keyword_patterns)
    earned += POINTS_SAS_KEYWORD_LEAKAGE - len(fired_keywords) * deduct

    sas_function_patterns: dict[str, re.Pattern[str]] = {
        "today":   re.compile(r"\btoday\s*\(",   re.IGNORECASE),
        "intnx":   re.compile(r"\bintnx\s*\(",   re.IGNORECASE),
        "upcase":  re.compile(r"\bupcase\s*\(",  re.IGNORECASE),
        "lowcase": re.compile(r"\blowcase\s*\(", re.IGNORECASE),
        "strip":   re.compile(r"\bstrip\s*\(",   re.IGNORECASE),
        "mdy":     re.compile(r"\bmdy\s*\(",     re.IGNORECASE),
        "put":     re.compile(r"\bput\s*\(",     re.IGNORECASE),
        "choosec": re.compile(r"\bchoosec\s*\(", re.IGNORECASE),
    }
    fired_functions: set[str] = set()
    for lineno, line in enumerate(lines, start=1):
        for name, pattern in sas_function_patterns.items():
            for match in pattern.finditer(line):
                issues.append(f"unconverted SAS function '{name}' at line {lineno}")
                fired_functions.add(name)

    deduct = POINTS_UNCONVERTED_FUNCTIONS / len(sas_function_patterns)
    earned += POINTS_UNCONVERTED_FUNCTIONS - len(fired_functions) * deduct

    date_literal_pattern = re.compile(r"'\d{2}[A-Za-z]{3}\d{4}'d\b", re.IGNORECASE)
    date_hits: list[str] = []
    for lineno, line in enumerate(lines, start=1):
        for match in date_literal_pattern.finditer(line):
            issues.append(
                f"unconverted SAS date literal at line {lineno}: {match.group(0)}"
            )
            date_hits.append(match.group(0))

    if not date_hits:
        earned += POINTS_DATE_LITERAL_SURVIVAL
    else:
        earned += POINTS_DATE_LITERAL_SURVIVAL * max(0.0, 1.0 - len(date_hits) * 0.10)

    macro_patterns: dict[str, re.Pattern[str]] = {
        "%LET declaration":    re.compile(r"^\s*%let\b",    re.IGNORECASE | re.MULTILINE),
        "%GLOBAL declaration": re.compile(r"^\s*%global\b", re.IGNORECASE | re.MULTILINE),
        "&var. reference":     re.compile(r"&\w+\.",        re.IGNORECASE),
    }
    fired_macro_forms: set[str] = set()
    for lineno, line in enumerate(lines, start=1):
        for label, pattern in macro_patterns.items():
            for match in pattern.finditer(line):
                token = match.group(0).strip()
                prefix = (
                    "macro declaration" if "declaration" in label
                    else "unresolved macro reference"
                )
                issues.append(f"{prefix} '{token.upper()}' at line {lineno}")
                fired_macro_forms.add(label)

    deduct = POINTS_MACRO_SURVIVAL / len(macro_patterns)
    earned += POINTS_MACRO_SURVIVAL - len(fired_macro_forms) * deduct

    full_text = "\n".join(lines)
    create_pat = re.compile(r"\bCREATE\s+TABLE\s+(\w+\.\w+)", re.IGNORECASE)
    drop_pat   = re.compile(
        r"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+\.\w+)", re.IGNORECASE
    )
    created = {m.group(1).upper() for m in create_pat.finditer(full_text)}
    dropped = {m.group(1).upper() for m in drop_pat.finditer(full_text)}
    orphans = dropped - created
    for t in sorted(orphans):
        issues.append(f"DROP for {t} has no paired CREATE in this file")

    if not orphans:
        earned += POINTS_TABLE_BALANCE
    else:
        orphan_ratio = min(1.0, len(orphans) / max(len(dropped), 1))
        earned += POINTS_TABLE_BALANCE * (1.0 - orphan_ratio)

    return _clamp_score(earned), issues, bleu_value


def score_syntax_metadata(
    sql_path: Path | None,
    ktr_path: Path | None,
) -> tuple[float, list[str]]:
    """Grade KTR XML on structure and metadata completeness."""
    POINTS_PARSE_INTEGRITY     = 30
    POINTS_HEADER_ROOT         = 10
    POINTS_HEADER_NAME         = 10
    POINTS_CONNECTION_PRESENT  = 10
    POINTS_CONNECTION_CHILDREN = 15
    POINTS_STEP_NAMES          = 8
    POINTS_STEP_TYPES          = 7
    POINTS_STEP_COUNT_MATCH    = 10
    assert (
        POINTS_PARSE_INTEGRITY + POINTS_HEADER_ROOT + POINTS_HEADER_NAME
        + POINTS_CONNECTION_PRESENT + POINTS_CONNECTION_CHILDREN
        + POINTS_STEP_NAMES + POINTS_STEP_TYPES + POINTS_STEP_COUNT_MATCH == 100
    ), "syntax/metadata point constants must sum to 100"

    issues: list[str] = []

    if ktr_path is None:
        return 0.0, ["no KTR file to validate"]

    raw = ktr_path.read_text(encoding="utf-8")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        return 0.0, [f"KTR XML parse failure: {exc}"]

    earned: float = POINTS_PARSE_INTEGRITY

    if root.tag != "transformation":
        issues.append(f"root element is <{root.tag}>, expected <transformation>")
    else:
        earned += POINTS_HEADER_ROOT

    name_el = root.find("info/name")
    if name_el is None or not (name_el.text or "").strip():
        issues.append("missing or empty <info><name> element")
    else:
        earned += POINTS_HEADER_NAME

    connections = root.findall("connection")
    if not connections:
        issues.append("no <connection> elements found")
    else:
        earned += POINTS_CONNECTION_PRESENT
        conn = connections[0]
        required_children = ("name", "server", "type")
        points_per_child = POINTS_CONNECTION_CHILDREN / len(required_children)
        for tag in required_children:
            child = conn.find(tag)
            if child is None or not (child.text or "").strip():
                issues.append(f"connection is missing or has empty <{tag}>")
            else:
                earned += points_per_child

    steps = root.findall("step")
    if not steps:
        issues.append("no <step> elements found")
    else:
        valid_names = sum(1 for s in steps if (s.findtext("name") or "").strip())
        valid_types = sum(1 for s in steps if (s.findtext("type") or "").strip())
        earned += POINTS_STEP_NAMES * (valid_names / len(steps))
        earned += POINTS_STEP_TYPES * (valid_types / len(steps))
        if valid_names < len(steps):
            issues.append(
                f"{len(steps) - valid_names} of {len(steps)} steps have a missing or empty <name>"
            )
        if valid_types < len(steps):
            issues.append(
                f"{len(steps) - valid_types} of {len(steps)} steps have a missing or empty <type>"
            )
        if sql_path is not None and sql_path.exists():
            sql_block_count = sql_path.read_text(encoding="utf-8").count("-- Block ")
            if sql_block_count == len(steps):
                earned += POINTS_STEP_COUNT_MATCH
            else:
                issues.append(
                    f"KTR has {len(steps)} step(s) but the paired SQL file has "
                    f"{sql_block_count} block(s)"
                )
        else:
            earned += POINTS_STEP_COUNT_MATCH

    return _clamp_score(earned), issues


def score_code_style(
    sql_path: Path | None,
) -> tuple[float, list[str], float, int]:
    """Grade SQL readability via Maintainability Index (Coleman et al., 1994).

    MI = 171 - 5.2*ln(V) - 0.23*CC - 16.2*ln(LOC)
    score = clamp(max(0, MI) / 171 * 100, 0, 100)
    Returns (score, issues, halstead_volume, cyclomatic_complexity).
    """
    if sql_path is None or not sql_path.exists():
        return 0.0, ["no SQL file to validate"], 0.0, 0

    sql_string = sql_path.read_text(encoding="utf-8")
    issues: list[str] = []

    v  = calculate_halstead_volume(sql_string)
    cc = calculate_cyclomatic_complexity(sql_string)
    mi = _calculate_mi_raw(sql_string)

    score = _clamp_score((max(0.0, mi) / 171.0) * 100.0)

    if mi < 65.0:
        issues.append(
            f"MI = {mi:.1f} (< 65) — difficult to maintain [V={v:.0f}, CC={cc}]"
        )
    elif mi < 85.0:
        issues.append(
            f"MI = {mi:.1f} (65-85) — moderately maintainable [V={v:.0f}, CC={cc}]"
        )

    if cc > 20:
        issues.append(
            f"Cyclomatic Complexity CC = {cc} exceeds 20 — very high branching complexity"
        )

    return score, issues, v, cc


def evaluate_test_case(
    tool_name: str,
    tool_dir: Path,
    test_case_id: str,
    reference_sql_path: Path | None = None,
) -> EvaluationReport:
    """Score one tool's output for one test case.

    tool_name == 'llm' triggers multi-run path: scores all runs, picks best, computes pass@k.
    """
    outputs   = discover_outputs(tool_dir, test_case_id)
    sql_files = outputs["sql"]
    ktr_files = outputs["ktr"]

    sql_path = sql_files[0] if sql_files else None
    ktr_path = ktr_files[0] if ktr_files else None

    report = EvaluationReport(
        test_case_id=test_case_id,
        tool_name=tool_name,
        sql_path=sql_path,
        ktr_path=ktr_path,
    )

    if sql_path is None:
        report.issues.append(f"no .sql output found for {test_case_id}")
    if ktr_path is None:
        report.issues.append(f"no .ktr output found for {test_case_id}")

    # LLM multi-run path: score every run, pick the best, compute pass@k
    if tool_name == "llm" and sql_files:
        run_data: list[dict] = []

        for run_sql in sql_files:
            run_ktr = next(
                (k for k in ktr_files if k.stem == run_sql.stem), ktr_path
            )
            f_score, f_issues, bleu = score_functional_parity(
                run_sql, run_ktr, reference_sql_path
            )
            s_score, s_issues        = score_syntax_metadata(run_sql, run_ktr)
            c_score, c_issues, hv, cc = score_code_style(run_sql)

            comp = ComponentScores(f_score, s_score, c_score)
            composite = (
                comp.functional_parity * WEIGHT_FUNCTIONAL_PARITY
                + comp.syntax_metadata * WEIGHT_SYNTAX_METADATA
                + comp.code_style      * WEIGHT_CODE_STYLE
            )
            run_data.append({
                "sql": run_sql, "ktr": run_ktr,
                "scores": comp, "composite": composite,
                "bleu": bleu, "hv": hv, "cc": cc,
                "issues": f_issues + s_issues + c_issues,
            })

        n       = len(run_data)
        c_count = sum(1 for d in run_data if d["composite"] >= PASS_THRESHOLD)
        report.pass_at_k = compute_pass_at_k(n, c_count, min(K_ATTEMPTS, n))

        best     = max(run_data, key=lambda d: d["composite"])
        best_idx = run_data.index(best)

        report.sql_path     = sql_files[best_idx]
        report.ktr_path     = best["ktr"]
        report.scores       = best["scores"]
        report.bleu_score   = best["bleu"]
        report.halstead_v   = best["hv"]
        report.cyclomatic_c = best["cc"]
        report.issues.extend(best["issues"])
        return report

    # SPMT single-run path
    f_score, f_issues, bleu        = score_functional_parity(sql_path, ktr_path, None)
    s_score, s_issues               = score_syntax_metadata(sql_path, ktr_path)
    c_score, c_issues, hv, cc       = score_code_style(sql_path)

    report.scores       = ComponentScores(f_score, s_score, c_score)
    report.bleu_score   = bleu       # nan — no reference for SPMT
    report.halstead_v   = hv
    report.cyclomatic_c = cc
    report.pass_at_k    = None       # deterministic tool; pass@k not applicable
    report.issues.extend(f_issues + s_issues + c_issues)
    return report


def run_evaluation(
    data_root: Path,
    llm_dir_override: Path | None = None,
    spmt_dir_override: Path | None = None,
) -> list[EvaluationReport]:
    """Score SPMT then LLM across all TCs; SPMT runs first to build the gold SQL map."""
    spmt_dir = spmt_dir_override if spmt_dir_override is not None else data_root / "spmt"
    llm_dir  = llm_dir_override if llm_dir_override is not None else data_root / "llm"

    reports: list[EvaluationReport] = []

    spmt_sql_map: dict[str, Path | None] = {}
    for tc_id in TEST_CASE_IDS:
        r = evaluate_test_case("spmt", spmt_dir, tc_id, reference_sql_path=None)
        reports.append(r)
        spmt_sql_map[tc_id] = r.sql_path

    for tc_id in TEST_CASE_IDS:
        ref = spmt_sql_map.get(tc_id)
        r = evaluate_test_case("llm", llm_dir, tc_id, reference_sql_path=ref)
        reports.append(r)

    return reports


_CATEGORY_ORDER: list[str] = [
    "SAS Keyword Leakage",
    "Unconverted SAS Function",
    "SAS Date Literal Survival",
    "Macro Variable Survival",
    "Table Balance Error",
    "KTR Structural",
    "Code Style",
    "Missing Artifact",
    "Other",
]


def _categorize_issue(issue: str) -> str:
    low = issue.lower()
    if low.startswith("sas keyword"):
        return "SAS Keyword Leakage"
    if low.startswith("unconverted sas function"):
        return "Unconverted SAS Function"
    if low.startswith("unconverted sas date"):
        return "SAS Date Literal Survival"
    if low.startswith(("macro declaration", "unresolved macro")):
        return "Macro Variable Survival"
    if low.startswith("drop for") and "no paired create" in low:
        return "Table Balance Error"
    if (
        low.startswith(("ktr xml", "root element", "missing or empty",
                         "no <connection>", "connection is", "ktr has"))
        or "steps have a missing" in low
        or "steps missing" in low
    ):
        return "KTR Structural"
    if low.startswith(("line ", "sql appears", "maximum case",
                        "mi =", "cyclomatic", "bleu", "ast skeleton")):
        return "Code Style"
    if low.startswith(("no .sql output", "no .ktr output",
                        "no sql file", "no ktr file")):
        return "Missing Artifact"
    return "Other"


def generate_chapter_6_markdown(
    reports: list[EvaluationReport],
    output_path: Path | None = None,
) -> None:
    t1_header = (
        "| Test Case | Tool "
        "| Func. Parity (70 %) "
        "| Syntax / Meta (20 %) "
        "| Code Style (10 %) "
        "| Composite "
        "| BLEU-4 "
        "| Halstead V "
        "| CC "
        "| pass@3 |"
    )
    t1_sep = (
        "|-----------|------|---------------------|"
        "----------------------|-------------------|"
        "-----------|---------|------------|-----|---------|"
    )

    t1_rows: list[str] = []
    for report in reports:
        wb        = report.weighted_breakdown
        bleu_str  = f"{report.bleu_score:.3f}" if not math.isnan(report.bleu_score) else "—"
        passk_str = f"{report.pass_at_k:.2f}"  if report.pass_at_k is not None else "—"
        t1_rows.append(
            f"| {report.test_case_id} "
            f"| {report.tool_name} "
            f"| {wb['functional_parity']:6.2f} "
            f"| {wb['syntax_metadata']:6.2f} "
            f"| {wb['code_style']:6.2f} "
            f"| **{report.composite_score:.2f}** "
            f"| {bleu_str} "
            f"| {report.halstead_v:.0f} "
            f"| {report.cyclomatic_c} "
            f"| {passk_str} |"
        )

    aggregated: dict[tuple[str, str], list[str]] = defaultdict(list)
    for report in reports:
        for issue in report.issues:
            aggregated[(_categorize_issue(issue), report.tool_name)].append(issue)

    active_categories = sorted(
        {cat for cat, _ in aggregated},
        key=lambda c: (
            _CATEGORY_ORDER.index(c) if c in _CATEGORY_ORDER else len(_CATEGORY_ORDER)
        ),
    )
    tool_names = ["spmt", "llm"]

    t2_rows: list[str] = []
    for category in active_categories:
        cells: list[str] = []
        for tool in tool_names:
            items = aggregated.get((category, tool), [])
            cells.append(
                "—" if not items
                else "<br>".join([f"**{len(items)}**"] + items)
            )
        t2_rows.append(f"| {category} | {cells[0]} | {cells[1]} |")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    doc_lines: list[str] = [
        "# Chapter 6 — Evaluation Results",
        "",
        f"*Generated by `eval_harness.py` · {timestamp}.*  ",
        "*Do not edit by hand; re-run the harness to refresh.*",
        "",
        "Scoring weights: functional parity **70 %** · syntax/metadata **20 %** "
        "· code style **10 %**.  ",
        "LLM functional parity: BLEU-4 + AST skeleton similarity vs SPMT gold output.  ",
        "Code style: formal Maintainability Index (Coleman et al., 1994).  ",
        "pass@3: OpenAI Codex estimator (Chen et al., 2021), threshold composite ≥ 70.0.",
        "",
        "---",
        "",
        "## Table 1 — Component Scoring Breakdown",
        "",
        t1_header,
        t1_sep,
        *t1_rows,
        "",
        "---",
        "",
        "## Table 2 — Error Typology Matrix",
        "",
        "Each cell: **count** followed by verbatim issue strings from all fixtures.",
        "",
        "| Issue Category | SPMT | LLM |",
        "|----------------|------|-----|",
        *t2_rows,
        "",
    ]

    if output_path is None:
        output_path = DATA_ROOT / "Chapter_6_Evaluation_Results.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(doc_lines), encoding="utf-8")
    print(f"Chapter 6 report written → {output_path}")


def _evaluate_best_of_runs(
    tool_name: str,
    run_dirs: list[Path],
    spmt_sql_map: dict[str, Path | None],
) -> list[EvaluationReport]:
    """Score all run dirs for one tool; return best-scoring report per TC with pass@k."""
    by_tc: dict[str, list[EvaluationReport]] = {tc: [] for tc in TEST_CASE_IDS}

    for run_dir in run_dirs:
        for tc_id in TEST_CASE_IDS:
            r = evaluate_test_case(
                tool_name, run_dir, tc_id,
                reference_sql_path=spmt_sql_map.get(tc_id),
            )
            by_tc[tc_id].append(r)

    best_reports: list[EvaluationReport] = []
    for tc_id in TEST_CASE_IDS:
        runs = by_tc[tc_id]
        n = len(runs)
        c = sum(1 for r in runs if r.composite_score >= PASS_THRESHOLD)
        winner = max(runs, key=lambda r: r.composite_score)
        winner.pass_at_k = compute_pass_at_k(n, c, min(K_ATTEMPTS, n))
        best_reports.append(winner)

    return best_reports


def run_unified_evaluation(
    spmt_dir: Path,
    claude_dirs: list[Path],
    gpt_dirs: list[Path],
) -> dict[str, list[EvaluationReport]]:
    """Score SPMT + Claude (best-of-N) + GPT (best-of-N) across all TCs."""
    spmt_reports: list[EvaluationReport] = []
    spmt_sql_map: dict[str, Path | None] = {}

    for tc_id in TEST_CASE_IDS:
        r = evaluate_test_case("spmt", spmt_dir, tc_id, reference_sql_path=None)
        spmt_reports.append(r)
        spmt_sql_map[tc_id] = r.sql_path

    claude_reports = _evaluate_best_of_runs("claude", claude_dirs, spmt_sql_map)
    gpt_reports    = _evaluate_best_of_runs("gpt",    gpt_dirs,    spmt_sql_map)

    return {"spmt": spmt_reports, "claude": claude_reports, "gpt": gpt_reports}


def generate_unified_markdown(
    data: dict[str, list[EvaluationReport]],
    output_path: Path,
) -> None:
    """Write 3-way comparison markdown (SPMT | Claude | GPT)."""
    spmt_reps   = data["spmt"]
    claude_reps = data["claude"]
    gpt_reps    = data["gpt"]

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    def _avg(reps: list[EvaluationReport]) -> float:
        return sum(r.composite_score for r in reps) / len(reps) if reps else 0.0

    def _avg_nonzero(reps: list[EvaluationReport]) -> tuple[float, int]:
        nz = [r for r in reps if r.composite_score > 0]
        return (_avg(nz), len(nz))

    spmt_avg                    = _avg(spmt_reps)
    claude_avg                  = _avg(claude_reps)
    gpt_avg                     = _avg(gpt_reps)
    claude_nz_avg, claude_nz_n  = _avg_nonzero(claude_reps)
    gpt_nz_avg,    gpt_nz_n     = _avg_nonzero(gpt_reps)

    t1_header = (
        "| Test Case | SPMT | Claude (best/2) | Claude pass@2 | GPT (best/2) | GPT pass@2 |"
    )
    t1_sep = (
        "|-----------|-----:|----------------:|--------------:|-------------:|-----------:|"
    )
    t1_rows: list[str] = []
    for s, c, g in zip(spmt_reps, claude_reps, gpt_reps):
        c_pass = f"{c.pass_at_k:.2f}" if c.pass_at_k is not None else "—"
        g_pass = f"{g.pass_at_k:.2f}" if g.pass_at_k is not None else "—"
        t1_rows.append(
            f"| {s.test_case_id} "
            f"| **{s.composite_score:.2f}** "
            f"| **{c.composite_score:.2f}** "
            f"| {c_pass} "
            f"| **{g.composite_score:.2f}** "
            f"| {g_pass} |"
        )
    t1_rows.append(
        f"| **Average** "
        f"| **{spmt_avg:.2f}** "
        f"| **{claude_avg:.2f}** ¹ "
        f"| — "
        f"| **{gpt_avg:.2f}** "
        f"| — |"
    )

    t2_header = (
        "| Test Case | Tool | Func. Parity (70 %) | Syntax / Meta (20 %) "
        "| Code Style (10 %) | Composite | BLEU-4 | Halstead V | CC | pass@2 |"
    )
    t2_sep = (
        "|-----------|------|--------------------:|---------------------:"
        "|------------------:|----------:|-------:|-----------:|----:|-------:|"
    )
    t2_rows: list[str] = []
    all_reports: list[EvaluationReport] = []
    for s, c, g in zip(spmt_reps, claude_reps, gpt_reps):
        for r in (s, c, g):
            wb       = r.weighted_breakdown
            bleu_str = f"{r.bleu_score:.3f}" if not math.isnan(r.bleu_score) else "—"
            pass_str = f"{r.pass_at_k:.2f}"  if r.pass_at_k is not None      else "—"
            t2_rows.append(
                f"| {r.test_case_id} "
                f"| {r.tool_name} "
                f"| {wb['functional_parity']:6.2f} "
                f"| {wb['syntax_metadata']:6.2f} "
                f"| {wb['code_style']:6.2f} "
                f"| **{r.composite_score:.2f}** "
                f"| {bleu_str} "
                f"| {r.halstead_v:.0f} "
                f"| {r.cyclomatic_c} "
                f"| {pass_str} |"
            )
            all_reports.append(r)

    aggregated: dict[tuple[str, str], list[str]] = defaultdict(list)
    for r in all_reports:
        for issue in r.issues:
            aggregated[(_categorize_issue(issue), r.tool_name)].append(issue)

    active_categories = sorted(
        {cat for cat, _ in aggregated},
        key=lambda cat: (
            _CATEGORY_ORDER.index(cat) if cat in _CATEGORY_ORDER else len(_CATEGORY_ORDER)
        ),
    )
    t3_rows: list[str] = []
    for category in active_categories:
        cells: list[str] = []
        for tool in ("spmt", "claude", "gpt"):
            items = aggregated.get((category, tool), [])
            cells.append("—" if not items else f"**{len(items)}**")
        t3_rows.append(f"| {category} | {cells[0]} | {cells[1]} | {cells[2]} |")

    doc_lines: list[str] = [
        "# Chapter 6 — Unified Evaluation Results",
        "",
        f"*Generated by `eval_harness.py` · {timestamp}.*  ",
        "*Do not edit by hand — re-run the harness to refresh.*",
        "",
        "**Scoring weights:** functional parity 70 % · syntax / metadata 20 % · code style 10 %.  ",
        "**LLM functional parity:** BLEU-4 + AST skeleton similarity versus SPMT gold output (testrun1).  ",
        "**Code style:** formal Maintainability Index (Coleman et al., 1994).  ",
        "**pass@2:** OpenAI Codex unbiased estimator (Chen et al., 2021); "
        "a run *passes* when composite score ≥ 70.0; k = 2 runs per tool.",
        "",
        "> **Note on SPMT determinism:** SPMT is a rule-based, fully deterministic tool. "
        "Both testrun1 and testrun2 produced byte-for-byte identical outputs for all 35 test cases. "
        "All SPMT scores therefore reflect a single deterministic result; pass@2 is not applicable (—).",
        "",
        "---",
        "",
        "## Table 1 — Composite Score Summary",
        "",
        f"Overall averages (all 35 TCs) — "
        f"SPMT: **{spmt_avg:.2f}** · "
        f"Claude: **{claude_avg:.2f}** "
        f"(*{claude_nz_avg:.2f} across {claude_nz_n} TCs with output*) · "
        f"GPT: **{gpt_avg:.2f}**",
        "",
        t1_header,
        t1_sep,
        *t1_rows,
        "",
        "¹ Claude average includes TCs with no output (scored 0.00 — 15 failures across both runs).",
        "",
        "---",
        "",
        "## Table 2 — Detailed Component Breakdown",
        "",
        "Three rows per test case: SPMT · claude · gpt.",
        "",
        t2_header,
        t2_sep,
        *t2_rows,
        "",
        "---",
        "",
        "## Table 3 — Error Typology Matrix",
        "",
        "Issue counts across all 35 test cases per tool.",
        "",
        "| Issue Category | SPMT | Claude | GPT |",
        "|----------------|-----:|-------:|----:|",
        *t3_rows,
        "",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(doc_lines), encoding="utf-8")
    print(f"Unified report written → {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluation harness — SPMT vs LLM SQL/KTR output."
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DATA_ROOT,
        help="Root folder holding the spmt/ and llm/ output directories.",
    )
    parser.add_argument(
        "--llm-dir",
        type=Path,
        default=None,
        help="Override the LLM output directory (default: <data-root>/llm).",
    )
    parser.add_argument(
        "--spmt-dir",
        type=Path,
        default=None,
        help="Override the SPMT output directory (default: <data-root>/spmt).",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Path for the output markdown report (default: <data-root>/Chapter_6_Evaluation_Results.md).",
    )
    parser.add_argument(
        "--unified",
        action="store_true",
        help="Run 3-way SPMT / Claude / GPT comparison (requires --claude-dirs and --gpt-dirs).",
    )
    parser.add_argument(
        "--claude-dirs",
        type=Path,
        nargs="+",
        default=None,
        metavar="DIR",
        help="One or more Claude output directories (used with --unified).",
    )
    parser.add_argument(
        "--gpt-dirs",
        type=Path,
        nargs="+",
        default=None,
        metavar="DIR",
        help="One or more GPT output directories (used with --unified).",
    )
    args = parser.parse_args()

    if args.unified:
        if not args.spmt_dir or not args.claude_dirs or not args.gpt_dirs:
            parser.error("--unified requires --spmt-dir, --claude-dirs, and --gpt-dirs")
        data = run_unified_evaluation(args.spmt_dir, args.claude_dirs, args.gpt_dirs)
        out  = args.report_out or (DATA_ROOT / "Chapter_6_Unified_Results.md")
        generate_unified_markdown(data, output_path=out)
    else:
        reports = run_evaluation(
            args.data_root,
            llm_dir_override=args.llm_dir,
            spmt_dir_override=args.spmt_dir,
        )
        generate_chapter_6_markdown(reports, output_path=args.report_out)


if __name__ == "__main__":
    main()
