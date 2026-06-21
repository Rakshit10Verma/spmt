from __future__ import annotations

from pathlib import Path

from spmt.converter import FileConversionResult


def render_migration_report(result: FileConversionResult) -> str:
    lines = [
        f"# Migration Report: {Path(result.source_file).name or 'Unknown Source'}",
        "",
        f"- Blocks converted: {len(result.blocks)}",
        f"- Drop statements: {len(result.drop_statements)}",
        f"- Parameters: {len(result.parameters)}",
        f"- Warnings: {sum(len(block.warnings) for block in result.blocks) + len(result.warnings)}",
        "",
    ]

    if result.parameters:
        lines.extend([
            "## Parameters",
            "",
            *[f"- {param}" for param in result.parameters],
            "",
        ])

    if result.drop_statements:
        lines.extend([
            "## Drop Statements",
            "",
        ])
        for statement in result.drop_statements:
            lines.append("```sql")
            lines.append(statement)
            lines.append("```")
            lines.append("")

    lines.append("## Block Summary")
    lines.append("")
    for block in result.blocks:
        lines.append(f"### Block {block.block_number:02d}")
        if block.target_table:
            lines.append(f"- Target table: {block.target_table}")
        if block.rules_applied:
            lines.append(f"- Rules applied: {', '.join(block.rules_applied)}")
        if block.warnings:
            lines.append("- Warnings:")
            lines.extend([f"  - {warning}" for warning in block.warnings])
        lines.append("")

    if result.warnings:
        lines.extend([
            "## File Warnings",
            "",
            *[f"- {warning}" for warning in result.warnings],
            "",
        ])

    return "\n".join(lines).rstrip() + "\n"


def render_learning_docs(result: FileConversionResult) -> str:
    lines = [
        f"# Learning Notes: {Path(result.source_file).name or 'Unknown Source'}",
        "",
        "This document highlights what changed in each converted PROC SQL block.",
        "",
    ]

    for block in result.blocks:
        lines.append(f"## Block {block.block_number:02d}")
        if block.target_table:
            lines.append(f"- Target table: {block.target_table}")
        lines.append("")
        lines.append("### Original SQL")
        lines.append("```sql")
        lines.append(block.original_sql.strip())
        lines.append("```")
        lines.append("")
        lines.append("### Converted SQL")
        lines.append("```sql")
        lines.append(block.converted_sql.strip())
        lines.append("```")
        lines.append("")
        if block.rules_applied:
            lines.append(f"- Rules applied: {', '.join(block.rules_applied)}")
        if block.warnings:
            lines.append("- Warnings:")
            lines.extend([f"  - {warning}" for warning in block.warnings])
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_migration_report(result: FileConversionResult, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_migration_report(result), encoding="utf-8")
    return path


def write_learning_docs(result: FileConversionResult, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_learning_docs(result), encoding="utf-8")
    return path