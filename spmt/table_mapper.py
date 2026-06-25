"""Rewrites SAS library.table references to Oracle schema.table. Mappings from config/table_mappings.json."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TableMapping:
    """Stores one library.table reference that was remapped."""
    original: str
    mapped: str
    library: str
    table_name: str


@dataclass
class TableMappingResult:
    """What map_tables() gives back: the rewritten SQL plus metadata."""
    converted_sql: str
    mappings: list[TableMapping] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class LibraryConfig:
    """Config for one SAS library — which Oracle schema and prefix to use."""
    oracle_schema: str
    table_prefix: str = ""
    description: str = ""


class TableMapper:
    """Only remaps dot-prefixed tokens where the prefix is a known SAS library; skips column aliases like t1.col."""

    def __init__(
        self,
        library_mappings: dict[str, LibraryConfig],
        default_schema: str = "",
        default_prefix: str = "",
    ) -> None:
        self._mappings = library_mappings
        self._default_schema = default_schema
        self._default_prefix = default_prefix

        # Compile the table ref pattern once at init. Matches LIBNAME.TABLE including
        # ${var} tokens in table names (e.g. CUSTOMERS_${gPeriodeTable} from TC-05).
        # We use a generic \w+ for the library side and check inside the replacer
        # callback whether it's a known library — simpler than listing all names in the regex.
        self._table_ref_re = re.compile(
            r"\b(\w+)\.([\w]+(?:\$\{\w+\})*)",
            re.IGNORECASE,
        )

    @classmethod
    def from_config(cls, config_path: str | Path) -> "TableMapper":
        path = Path(config_path)
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)

        lib_mappings: dict[str, LibraryConfig] = {}
        for lib_name, lib_cfg in raw.get("library_mappings", {}).items():
            lib_mappings[lib_name.upper()] = LibraryConfig(
                oracle_schema=lib_cfg["oracle_schema"],
                table_prefix=lib_cfg.get("table_prefix", ""),
                description=lib_cfg.get("description", ""),
            )

        return cls(
            library_mappings=lib_mappings,
            default_schema=raw.get("default_schema", ""),
            default_prefix=raw.get("default_prefix", ""),
        )

    @property
    def known_libraries(self) -> list[str]:
        return sorted(self._mappings.keys())

    def _resolve(self, library: str, table: str) -> tuple[str, bool]:
        """Map SAS library + table to Oracle schema.table. Returns (mapped, was_known)."""
        lib_upper = library.upper()
        cfg = self._mappings.get(lib_upper)

        if cfg is not None:
            schema = cfg.oracle_schema
            prefix = cfg.table_prefix
            mapped_table = f"{prefix}{table}"
            return f"{schema}.{mapped_table}", True

        if self._default_schema:
            mapped_table = f"{self._default_prefix}{table}"
            return f"{self._default_schema}.{mapped_table}", False

        return f"{library}.{table}", False

    def map_tables(self, sql: str) -> TableMappingResult:
        """Remap all known library.table refs in the SQL. Skips unknown prefixes to avoid alias collision (t1.col, etc.)."""
        result = TableMappingResult(converted_sql=sql)
        seen: set[str] = set()

        known_upper = set(self._mappings.keys())

        def _replacer(match: re.Match) -> str:
            library = match.group(1)
            table = match.group(2)
            original = match.group(0)

            # alias collision safeguard: only remap if prefix is a known SAS library
            # (prevents t1.column, Source_Acct.field, etc. from being remapped)
            if library.upper() not in known_upper:
                return original

            mapped, was_known = self._resolve(library, table)

            # dedup the mappings list — record each library.table pair only once
            key = f"{library.upper()}.{table.upper()}"
            if key not in seen:
                seen.add(key)
                result.mappings.append(TableMapping(
                    original=original,
                    mapped=mapped,
                    library=library,
                    table_name=table,
                ))

            if not was_known:
                result.warnings.append(
                    f"Unknown library '{library}' in '{original}' "
                    f"-- mapped to default schema '{self._default_schema}'"
                )

            return mapped

        result.converted_sql = self._table_ref_re.sub(_replacer, sql)
        return result

    def get_mapping_for(self, library: str) -> LibraryConfig | None:
        return self._mappings.get(library.upper())

    def summary(self) -> str:
        lines = [f"Table mapper: {len(self._mappings)} library mappings configured"]
        for lib, cfg in sorted(self._mappings.items()):
            prefix_note = f" (prefix: {cfg.table_prefix})" if cfg.table_prefix else ""
            lines.append(f"  {lib} -> {cfg.oracle_schema}{prefix_note}")
        if self._default_schema:
            lines.append(f"  Default fallback: {self._default_schema}")
        return "\n".join(lines)
