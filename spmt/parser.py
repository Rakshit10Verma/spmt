import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ParsedBlock:
    """The actual PROC SQL block we want to convert."""
    block_number: int
    original_sql: str
    line_start: int
    line_end: int


@dataclass
class MacroDeclaration:
    """Stores a %LET or %GLOBAL setup so we know the variable state."""
    name: str
    value: str
    directive: str  # 'LET' or 'GLOBAL'
    line_number: int


@dataclass
class DropdsCall:
    table_name: str
    line_number: int


@dataclass
class ParseResult:
    """Everything we managed to scrape out of the file."""
    sql_blocks: list[ParsedBlock] = field(default_factory=list)
    macro_declarations: list[MacroDeclaration] = field(default_factory=list)
    dropds_calls: list[DropdsCall] = field(default_factory=list)
    source_file: str = ""


# Strip %macro blocks first — otherwise the regex picks up nested PROC SQL
# definitions inside macros. dotall makes '.' grab newlines.
_RE_MACRO_DEF = re.compile(
    r"%macro\b.*?%mend\b[^;]*;",
    re.IGNORECASE | re.DOTALL,
)

_RE_PROC_SQL = re.compile(
    r"(PROC\s+SQL\s*;.*?QUIT\s*;)",
    re.IGNORECASE | re.DOTALL,
)

# grab everything to the semicolon — values can contain nested macro calls like %SYSFUNC
_RE_LET = re.compile(
    r"%LET\s+(\w+)\s*=\s*(.+?)\s*;",
    re.IGNORECASE,
)

_RE_GLOBAL = re.compile(
    r"%GLOBAL\s+(\w+)\s*;",
    re.IGNORECASE,
)

_RE_DROPDS = re.compile(
    r"%_eg_conditional_dropds\s*\(\s*([\w.]+)\s*\)\s*;",
    re.IGNORECASE,
)


def _line_number_at_offset(text: str, offset: int) -> int:
    return text[:offset].count("\n") + 1


def _strip_macro_definitions(text: str) -> str:
    """Replace each %macro...%mend block with empty lines to preserve line numbering."""
    result = text
    for match in _RE_MACRO_DEF.finditer(text):
        original = match.group(0)
        replacement = "\n" * original.count("\n")
        result = result.replace(original, replacement, 1)
    return result


def parse_file(filepath: str | Path) -> ParseResult:
    """Scrape a .sas file for PROC SQL blocks, %LET declarations, and dropds calls."""
    filepath = Path(filepath)
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            raw_text = filepath.read_text(encoding=enc)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        raw_text = filepath.read_text(encoding="latin-1", errors="replace")

    result = ParseResult(source_file=str(filepath))

    # strip macro bodies first to avoid parsing inner %let statements and
    # nested PROC SQL that shouldn't be processed
    cleaned_text = _strip_macro_definitions(raw_text)

    all_lets: list[tuple[str, str, int]] = []  # (name, value, line)
    for match in _RE_LET.finditer(cleaned_text):
        line = _line_number_at_offset(cleaned_text, match.start())
        all_lets.append((match.group(1), match.group(2).strip(), line))

    all_globals: list[tuple[str, int]] = []  # (name, line)
    for match in _RE_GLOBAL.finditer(cleaned_text):
        line = _line_number_at_offset(cleaned_text, match.start())
        all_globals.append((match.group(1), line))

    for match in _RE_DROPDS.finditer(cleaned_text):
        line = _line_number_at_offset(cleaned_text, match.start())
        result.dropds_calls.append(DropdsCall(
            table_name=match.group(1),
            line_number=line,
        ))

    for block_idx, match in enumerate(_RE_PROC_SQL.finditer(cleaned_text), start=1):
        line_start = _line_number_at_offset(cleaned_text, match.start())
        line_end = _line_number_at_offset(cleaned_text, match.end() - 1)

        result.sql_blocks.append(ParsedBlock(
            block_number=block_idx,
            original_sql=match.group(1),
            line_start=line_start,
            line_end=line_end,
        ))

    def _is_outside_proc_sql(line_num: int) -> bool:
        for block in result.sql_blocks:
            if block.line_start <= line_num <= block.line_end:
                return False
        return True

    for name, value, line in all_lets:
        if _is_outside_proc_sql(line):
            result.macro_declarations.append(MacroDeclaration(
                name=name,
                value=value,
                directive="LET",
                line_number=line,
            ))

    for name, line in all_globals:
        if _is_outside_proc_sql(line):
            result.macro_declarations.append(MacroDeclaration(
                name=name,
                value="",
                directive="GLOBAL",
                line_number=line,
            ))

    result.macro_declarations.sort(key=lambda d: d.line_number)

    return result


def parse_string(text: str, source_name: str = "<string>") -> ParseResult:
    """Helper for testing or handling code already in memory."""
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sas", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(text)
        tmp_path = tmp.name

    try:
        result = parse_file(tmp_path)
        result.source_file = source_name
        return result
    finally:
        Path(tmp_path).unlink(missing_ok=True)
