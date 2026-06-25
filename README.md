# SPMT

A rule-based tool that converts SAS PROC SQL into Oracle SQL for use in Pentaho Data Integration.

## What this does

SPMT reads `.sas` files, finds every `PROC SQL...QUIT` block, and rewrites each one as Oracle-compatible SQL. It also generates `.ktr` files that Pentaho can open directly and produces migration reports documenting what changed and why.

The converter handles 42 distinct SAS-to-Oracle patterns: date functions, macro variables, string operations, NULL semantics, join rewrites, format removal, and more. Each rule is tested against a suite of 35 synthetic test cases built from real migration work.

This tool was developed as part of a master's thesis comparing rule-based migration against LLM-assisted conversion (Claude and GPT-4o). The `eval_harness.py` script at the repo root scores SPMT and LLM outputs on functional parity, syntax validity, and code style.

## Installation

Requires Python 3.10 or later.

```bash
git clone https://github.com/Rakshit10Verma/spmt.git
cd spmt
```

Install the package in editable mode with the dev extras:

```bash
pip install -e ".[dev]"
```

That installs the package itself plus `pytest` and `pytest-cov`. The `anthropic` and `openai` packages are included as runtime dependencies for the LLM batch-conversion scripts in `scripts/`; they are not required to run the core SPMT converter.

For a one-command setup plus test run, use:

```bash
./scripts/bootstrap_and_test.sh
```

## Usage

Convert a single SAS file to Oracle SQL and a Pentaho KTR:

```bash
spmt input.sas --output data_outputs/spmt --format both
```

Or via the module entry point:

```bash
python -m spmt input.sas --output data_outputs/spmt --format both
```

Supported options:

```
  input_path             SAS file or directory of .sas files
  --output DIR           Output directory (default: data_outputs/spmt)
  --format TYPE          Output format: sql, ktr, or both (default: both)
  --report               Generate a migration report in Markdown
  --learn                Generate learning notes in Markdown
  --transformation-name  Pentaho transformation name
  --connection-name      Pentaho connection name
  --host                 Oracle host for the KTR placeholder
  --port                 Oracle port for the KTR placeholder
  --db-name              Oracle database name for the KTR placeholder
  --schema               Default schema shown in the KTR placeholder
  --username             Oracle username shown in the KTR placeholder
```

The CLI flow is:

1. Parse the SAS file into blocks and macro declarations.
2. Convert each block through variable handling, table mapping, and the rule engine.
3. Write SQL, KTR, report, and learning-doc outputs based on the flags you choose.

## Architecture

The pipeline is straightforward. `parser.py` reads a `.sas` file and pulls out every `PROC SQL...QUIT` block along with any `%LET` or `%GLOBAL` declarations. It returns these as a list of structured objects.

Those parsed blocks go into `converter.py`, which is where the actual work happens. It processes each block through eight steps in a fixed order: strip the `PROC SQL`/`QUIT` wrappers, handle the `%_eg_conditional_dropds` macro, substitute macro variables (via `variable_handler.py`), remap table references (via `table_mapper.py`), apply the 42 pattern-conversion rules from `rules.py`, remove `FORMAT=` and `LABEL=` attributes, drop `ORDER BY` from `CREATE TABLE AS SELECT` statements, and rename SAS name literals to valid Oracle identifiers.

Once conversion is done, the converted SQL can go two places. It gets written out as a plain `.sql` file. If you asked for Pentaho output, `ktr_generator.py` wraps each SQL block into a Table Input step inside a valid `.ktr` XML file that Pentaho can open directly. `documenter.py` handles the optional migration report and learning-notes output.

## Conversion rules

The 42 rules are organized into 8 categories:

**NULL handling** (2 rules) — `IS MISSING` becomes `IS NULL`. `NOT IS MISSING` becomes `IS NOT NULL`.

**Date functions** (8 rules) — `today()` maps to `TRUNC(SYSDATE)`. `INTNX` calls map to `ADD_MONTHS`, `LAST_DAY`, or `TRUNC` depending on the alignment argument. SAS date literals like `'01Jan2025'd` become `TO_DATE()` calls. The `'31Dec9999'd` pattern (meaning "record is still active") converts to `IS NULL`. `mdy(m,d,y)` becomes `TO_DATE()` with reordered arguments.

**String functions** (4 rules) — `UPCASE` to `UPPER`, `LOWCASE` to `LOWER`, `STRIP` to `TRIM`. `COMPRESS` is the tricky one: with no modifier it maps to `REPLACE`, but `COMPRESS(x,,'kd')` (keep digits) and `COMPRESS(x,,'ka')` (keep alpha) need `REGEXP_REPLACE`.

**Macro variables** (3 rules) — `&var.` becomes `${prop_var}` for Pentaho substitution. `%LET` and `%GLOBAL` declarations are extracted as Pentaho parameters. Date macro variables get wrapped in `TO_DATE()`.

**Type conversion** (4 rules) — `PUT()` with numeric formats becomes `CASE WHEN`. `PUT()` with character formats (the `$` prefix ones) also becomes `CASE WHEN`. `PUT()` with standard numeric formats like `3.` maps to `TO_CHAR()`. `CHOOSEC(INPUT())` becomes a `CASE TO_NUMBER() WHEN` block.

**SAS keywords** (7 rules) — `CALCULATED` references get replaced with the full repeated expression. `CONTAINS` becomes `LIKE '%...%'`. `NOT =` becomes `<>`. SAS comparison operators `gt`, `le`, etc. become `>`, `<=`. `OUTER UNION CORR` becomes `UNION ALL`. `FORMAT=` and `LABEL=` attributes are stripped.

**Table mapping** (3 rules) — `WORK.table` maps to a configurable staging schema with a prefix. `SOURCE.table` and `DWH.table` map to configurable Oracle schemas. `LIBNAME META` statements and `%include` directives are ignored.

**Join patterns** (3 rules) — `RIGHT JOIN` with `WHERE` on the outer table (which turns it into an effective `INNER JOIN`) is flagged. Name literals like `'Column Name'n` are converted to valid Oracle identifiers. `ORDER BY` inside `CREATE TABLE AS SELECT` is removed since Oracle ignores it and it hurts performance.

There are also 8 rules covering less common patterns: time-slice filters, `PK_STAND` period keys, correlated subqueries for lookups, `NVL` chains for null-safe arithmetic, the SAS `sum()` function (which ignores NULLs, unlike Oracle's `+` operator), inline `VALUES` subqueries, dynamic table suffixes, and the `%_eg_conditional_dropds` macro.

## Test suite

The project includes 35 synthetic test cases (TC-01 through TC-35), written from scratch based on real migration patterns. They contain no proprietary code or data.

The first 8 cover the core rule matrix end-to-end:

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

TC-09 through TC-35 extend coverage into 8 feature categories used as the basis for the thesis evaluation: string functions (CATS, TRANWRD, SCAN, VERIFY, PROPCASE), date and datetime arithmetic (INTCK, DATEPART, DHMS, YRDIF, SCD2 temporal joins), PUT/PROC FORMAT and type conversion, macro variables and control flow (SELECT INTO, conditional SQL, DATA step arrays), table and schema mapping (passthrough, LIBNAME remapping, PROC VIEW, dictionary tables), analytical and window functions (RANK, LAG, CONNECT BY, ROLLUP, GROUPING SETS, remerge), and DML (INSERT, UPDATE, DELETE).

Run the tests:

```bash
python -m pytest tests/ -v
```

With coverage:

```bash
python -m pytest tests/ -v --cov=spmt --cov-report=term-missing
```

## Evaluation harness

`eval_harness.py` at the repo root scores SPMT and LLM outputs against the 35 test cases using three axes:

- **Functional parity (70%)** — BLEU-4 and AST skeleton similarity between the LLM output and the SPMT gold SQL.
- **Syntax / metadata validity (20%)** — checks for unconverted SAS functions, keyword leakage, and KTR XML well-formedness.
- **Code style (10%)** — Maintainability Index (Coleman et al., 1994) and Cyclomatic Complexity of the output SQL.

SPMT scores are computed directly against the PROC SQL reference rather than against another tool's output. The composite threshold for a passing run is 70.0, and pass@2 is estimated using the Chen et al. (2021) unbiased estimator.

Pre-generated evaluation results live in `data_outputs/`:

```
data_outputs/
  spmt/                        SPMT-converted SQL and KTR files
  llm_claude/                  Claude outputs (testrun1, testrun2)
  llm_gpt/                     GPT-4o outputs (testrun1, testrun2)
  eval_claude_testrun1.md      Scored results — Claude run 1
  eval_claude_testrun2.md      Scored results — Claude run 2
  eval_gpt_testrun1.md         Scored results — GPT run 1
  eval_gpt_testrun2.md         Scored results — GPT run 2
  Chapter_6_Unified_Results.md Best-of-2 composite scores, all tools
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
  documenter.py        Migration report and learning-notes writer
  cli.py               CLI entry point (spmt command)
  __main__.py          Module entry point (python -m spmt)
config/
  variable_mappings.json
  table_mappings.json
scripts/
  bootstrap_and_test.sh
  llm_batch_convert.py       Batch conversion via Claude API
  llm_batch_convert_gpt.py   Batch conversion via OpenAI API
eval_harness.py              Scoring engine for thesis evaluation
tests/
  fixtures/            TC-01 through TC-35 .sas files
  test_parser.py
  test_rules.py
  test_variable_handler.py
  test_table_mapper.py
  test_converter.py
  test_ktr_generator.py
data_outputs/          Generated SQL, KTR, and evaluation reports
```

## License

MIT
