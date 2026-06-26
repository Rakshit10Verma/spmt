#!/usr/bin/env python3
"""Batch-convert SAS fixture files to .ktr + .sql pairs using the Anthropic API."""

import argparse
import os
import re
import time
from pathlib import Path

import anthropic

PROJECT_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_outputs" / "llm" / "claude"
PROMPT_FILE = PROJECT_ROOT / "scripts" / "llm_conversion_prompt.md"

_SYSTEM_START = "## SYSTEM CONTEXT — paste this once"
_SYSTEM_END = "## PER-FILE TRIGGER"


def _extract_system_prompt() -> str:
    text = PROMPT_FILE.read_text(encoding="utf-8")
    start = text.index(_SYSTEM_START) + len(_SYSTEM_START)
    end = text.index(_SYSTEM_END)
    return text[start:end].strip()


def _extract_fenced_block(text: str, lang: str) -> str | None:
    m = re.search(rf"```{lang}\n(.*?)```", text, re.DOTALL)
    return m.group(1).strip() if m else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for .ktr and .sql files (default: data_outputs/llm/claude)",
    )
    args = parser.parse_args()
    output_dir = args.out_dir

    output_dir.mkdir(parents=True, exist_ok=True)

    system_prompt = _extract_system_prompt()
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    sas_files = sorted(FIXTURES_DIR.glob("TC-*.sas"))
    total = len(sas_files)
    failed: list[str] = []
    needs_sleep = False

    for idx, sas_file in enumerate(sas_files, start=1):
        stem = sas_file.stem
        ktr_out = output_dir / f"{stem}.ktr"
        sql_out = output_dir / f"{stem}.sql"
        prefix = f"[{idx:02d}/{total}] {stem}"

        if ktr_out.exists() and sql_out.exists():
            print(f"{prefix} — skipped (already exists)")
            continue

        if needs_sleep:
            time.sleep(2)

        file_content = sas_file.read_text(encoding="utf-8")
        user_msg = f"Convert the following SAS file named {sas_file.name}:\n\n{file_content}"

        try:
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=8192,
                system=system_prompt,
                messages=[{"role": "user", "content": user_msg}],
            )
            raw: str = response.content[0].text
            needs_sleep = True

            ktr_content = _extract_fenced_block(raw, "xml")
            sql_content = _extract_fenced_block(raw, "sql")

            if ktr_content is None or sql_content is None:
                missing = []
                if ktr_content is None:
                    missing.append("no ```xml block")
                if sql_content is None:
                    missing.append("no ```sql block")
                reason = "; ".join(missing)
                print(f"{prefix} ✗ ({reason})")
                (output_dir / f"{stem}.raw.txt").write_text(raw, encoding="utf-8")
                failed.append(stem)
            else:
                ktr_out.write_text(ktr_content, encoding="utf-8")
                sql_out.write_text(sql_content, encoding="utf-8")
                print(f"{prefix} ✓")

        except Exception as exc:
            print(f"{prefix} ✗ (error: {exc})")
            failed.append(stem)
            needs_sleep = True

    succeeded = total - len(failed)
    print(f"\nCompleted: {succeeded}/{total} succeeded.", end="")
    if failed:
        print(f" Failed: {', '.join(failed)}")
    else:
        print()


if __name__ == "__main__":
    main()
