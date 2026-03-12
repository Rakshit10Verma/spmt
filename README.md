# SPMT

A rule-based tool that converts SAS PROC SQL into Oracle SQL for use in Pentaho Data Integration.

## What this does

SPMT reads `.sas` files, finds every `PROC SQL...QUIT` block, and rewrites each one as Oracle-compatible SQL. It also generates `.ktr` files that Pentaho can open directly and produces migration reports documenting what changed and why.

The converter handles 42 distinct SAS-to-Oracle patterns: date functions, macro variables, string operations, NULL semantics, join rewrites, format removal, and more. Each rule is tested against 8 synthetic test cases built from real migration work.

This tool was developed as part of a master's thesis comparing rule-based migration against LLM-assisted conversion.

## Installation

Requires Python 3.10 or later. No runtime dependencies outside the standard library.

```bash
git clone https://github.com/YOUR_USERNAME/spmt.git
cd spmt
pip install -e ".[dev]"
```

The `[dev]` extra installs pytest, pytest-cov, and pylint for development and testing.

Alternatively, install just the test dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Convert a single SAS file to Oracle SQL:

```bash
python -m spmt input.sas --output ./results/
```

Generate Oracle SQL, a Pentaho `.ktr` file, and a migration report:

```bash
python -m spmt input.sas --output ./results/ --format both --report
```

Generate learning documentation alongside the conversion:

```bash
python -m spmt input.sas --output ./results/ --report --learn --verbose
```

Options:

    --output DIR       Output directory (default: ./output/)
    --format TYPE      Output format: sql, ktr, or both (default: sql)
    --report           Generate a migration report in Markdown
    --learn            Generate learning documentation for each converted block
    --verbose          Print conversion details to the console

## Architecture

The pipeline is straightforward. `parser.py` reads a `.sas` file and pulls out every `PROC SQL...QUIT` block along with any `%LET` or `%GLOBAL` declarations. It returns these as a list of structured objects.

Those parsed blocks go into `converter.py`, which is where the actual work happens. It processes each block through eight steps in a fixed order: strip the `PROC SQL`/`QUIT` wrappers, handle the `%_eg_conditional_dropds` macro, substitute macro variables (via `variable_handler.py`), remap table references (via `table_mapper.py`), apply the 42 pattern-conversion rules from `rules.py`, remove `FORMAT=` and `LABEL=` attributes, drop `ORDER BY` from `CREATE TABLE AS SELECT` statements, and rename SAS name literals to valid Oracle identifiers.

Once conversion is done, the converted SQL can go two places. It gets written out as a plain `.sql` file. If you asked for Pentaho output, `ktr_generator.py` also wraps each SQL block into a Table Input step inside a valid `.ktr` XML file that Pentaho can open directly.

Optionally, `documenter.py` reads the conversion results and produces two Markdown files: a migration report (what changed, which rules fired, any warnings) and a learning document that explains each conversion for someone unfamiliar with the differences between SAS and Oracle SQL.

## Conversion rules

The 42 rules are organized into 8 categories:

**NULL handling** (2 rules) -- `IS MISSING` becomes `IS NULL`. `NOT IS MISSING` becomes `IS NOT NULL`.

**Date functions** (8 rules) -- `today()` maps to `TRUNC(SYSDATE)`. `INTNX` calls map to `ADD_MONTHS`, `LAST_DAY`, or `TRUNC` depending on the alignment argument. SAS date literals like `'01Jan2025'd` become `TO_DATE()` calls. The `'31Dec9999'd` pattern (meaning "record is still active") converts to `IS NULL`. `mdy(m,d,y)` becomes `TO_DATE()` with reordered arguments.

**String functions** (4 rules) -- `UPCASE` to `UPPER`, `LOWCASE` to `LOWER`, `STRIP` to `TRIM`. `COMPRESS` is the tricky one: with no modifier it maps to `REPLACE`, but `COMPRESS(x,,'kd')` (keep digits) and `COMPRESS(x,,'ka')` (keep alpha) need `REGEXP_REPLACE`.

**Macro variables** (3 rules) -- `&var.` becomes `${prop_var}` for Pentaho substitution. `%LET` and `%GLOBAL` declarations are extracted as Pentaho parameters. Date macro variables get wrapped in `TO_DATE()`.

**Type conversion** (4 rules) -- `PUT()` with numeric formats becomes `CASE WHEN`. `PUT()` with character formats (the `$` prefix ones) also becomes `CASE WHEN`. `PUT()` with standard numeric formats like `3.` maps to `TO_CHAR()`. `CHOOSEC(INPUT())` becomes a `CASE TO_NUMBER() WHEN` block.

**SAS keywords** (7 rules) -- `CALCULATED` references get replaced with the full repeated expression. `CONTAINS` becomes `LIKE '%...%'`. `NOT =` becomes `<>`. SAS comparison operators `gt`, `le`, etc. become `>`, `<=`. `OUTER UNION CORR` becomes `UNION ALL`. `FORMAT=` and `LABEL=` attributes are stripped.

**Table mapping** (3 rules) -- `WORK.table` maps to a configurable staging schema with a prefix. `SOURCE.table` and `DWH.table` map to configurable Oracle schemas. `LIBNAME META` statements and `%include` directives are ignored.

**Join patterns** (3 rules) -- `RIGHT JOIN` with `WHERE` on the outer table (which turns it into an effective `INNER JOIN`) is flagged. Name literals like `'Column Name'n` are converted to valid Oracle identifiers. `ORDER BY` inside `CREATE TABLE AS SELECT` is removed since Oracle ignores it and it hurts performance.

There are also 8 rules covering less common patterns: time-slice filters, `PK_STAND` period keys, correlated subqueries for lookups, `NVL` chains for null-safe arithmetic, the SAS `sum()` function (which ignores NULLs, unlike Oracle's `+` operator), inline `VALUES` subqueries, dynamic table suffixes, and the `%_eg_conditional_dropds` macro.

The full matrix showing which rules apply to which test cases is in `MASTER_INDEX.md`.

## Test suite

The project includes 8 synthetic test cases (TC-01 through TC-08), ranging from basic to high complexity. They were written from scratch based on real migration patterns but contain no proprietary code or data.

| ID | Complexity | SQL blocks | What it tests |
|----|-----------|------------|---------------|
| TC-01 | Basic | 5 | NULL handling, COMPRESS, UNION, INTNX begin/end |
| TC-02 | Basic | 3 | today(), CHOOSEC, PUT to TO_CHAR, active record dates |
| TC-03 | Medium | 4 | Double quotes, NOT=, INTNX alignment, MAX subquery |
| TC-04 | Medium | 5 | 31Dec9999, RIGHT JOIN semantics, CALCULATED, name literals |
| TC-05 | Medium | 6 | PUT to CASE WHEN, LABEL/FORMAT removal, dynamic tables, HAVING |
| TC-06 | High | 5 | Four different PUT formats, time slices, mdy(), NVL arithmetic |
| TC-07 | High | 6 | Chained temp tables, SAS sum() null safety, CALCULATED in HAVING |
| TC-08 | High | 4 | CONTAINS, 14-branch CASE, 5-table JOIN, name literal propagation |

That's 38 SQL blocks total, covering all 42 conversion patterns at least once.

Run the tests:

```bash
pytest tests/ -v
```

With coverage:

```bash
pytest tests/ -v --cov=spmt --cov-report=term-missing
```

## Project structure

```
spmt/
  __init__.py
  parser.py            Extract PROC SQL blocks from .sas files
  rules.py             42 conversion rules as structured data
  variable_handler.py  SAS macro vars to Pentaho parameters
  table_mapper.py      SAS library.table to Oracle schema.table
  converter.py         Core engine, orchestrates everything
  ktr_generator.py     Pentaho .ktr XML output
  documenter.py        Migration reports and learning docs
  cli.py               Command-line interface
  __main__.py          Entry point for python -m spmt
config/
  variable_mappings.json
  table_mappings.json
tests/
  fixtures/            TC-01 through TC-08 .sas files
  test_parser.py
  test_rules.py
  test_variable_handler.py
  test_table_mapper.py
  test_converter.py
  test_ktr_generator.py
  test_documenter.py
docs/
output/
```

## License

MIT
