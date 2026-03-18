"""
spmt/table_mapper.py Rewrites SAS library.table references to Oracle schema.table.

In SAS, tables are organized under "libraries" like WORK, SOURCE, DWH, STAGING.
Oracle does not have this concept, so I need to replace each library prefix with the
correct Oracle schema name. WORK tables also get a TMP_ prefix so they do not clash
with real tables in the staging schema.

The mappings are loaded from config/table_mappings.json so they can be changed without
touching code. Each SAS library name maps to an Oracle schema and an optional prefix.

One important thing: this module runs after variable_handler, so by the time we see
the SQL, any macro variables in table names (like source.CUSTOMERS_&gPeriodeTable.)
have already become Pentaho syntax (source.CUSTOMERS_${gPeriodeTable}). The regex
I use for matching table references accounts for those ${...} tokens.
"""

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
    """Config for one SAS library, which Oracle schema and prefix to use."""
    oracle_schema: str
    table_prefix: str = ""
    description: str = ""


class TableMapper:
    """Scans SQL text for SAS library.table patterns and rewrites them.

    I load the library to schema mappings from a JSON config file.
    If a library name is not in the config, I leave the reference alone.
    That is the key design decision. I only remap dot prefixed tokens
    where the prefix matches a known SAS library name. This avoids the
    alias collision problem (see inline comments in map_tables).
    """

    def __init__(
        self,
        library_mappings: dict[str, LibraryConfig],
        default_schema: str = "",
        default_prefix: str = "",
    ) -> None:
        self._mappings = library_mappings
        self._default_schema = default_schema
        self._default_prefix = default_prefix

        # I build the regex once at init time instead of recompiling every call.
        #
        # The pattern matches SOMETHING.SOMETHING where the left side could be
        # a library name and the right side is the table name. The table part
        # can also contain ${var} tokens for dynamic table names like
        # CUSTOMERS_${gPeriodeTable} from TC-05.
        #
        # I use a generic \w+ for the library side and then check inside the
        # replacement callback whether it is actually a known library. This is
        # simpler than trying to list all library names in the regex itself.
        # SAS is case insensitive so I use re.IGNORECASE.
        self._table_ref_re = re.compile(
            r"\b(\w+)\.([\w]+(?:\$\{\w+\})*)",
            re.IGNORECASE,
        )

    @classmethod
    def from_config(cls, config_path: str | Path) -> "TableMapper":
        """Load library mappings from a JSON config file."""
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
        """Which SAS library names I have mappings for."""
        return sorted(self._mappings.keys())

    def _resolve(self, library: str, table: str) -> tuple[str, bool]:
        """Turn a SAS library + table into Oracle schema.table.

        Returns the mapped string and a boolean for whether the library
        was in my config. If it was not and there is no default schema,
        I just give back the original text unchanged.
        """
        lib_upper = library.upper()
        cfg = self._mappings.get(lib_upper)

        if cfg is not None:
            schema = cfg.oracle_schema
            prefix = cfg.table_prefix
            mapped_table = f"{prefix}{table}"
            return f"{schema}.{mapped_table}", True

        # unknown library, try the default if we have one
        if self._default_schema:
            mapped_table = f"{self._default_prefix}{table}"
            return f"{self._default_schema}.{mapped_table}", False

        # no default either, leave it as is
        return f"{library}.{table}", False

    def map_tables(self, sql: str) -> TableMappingResult:
        """Find all library.table references in the SQL and remap them.

        This is where the alias collision problem gets solved. SAS SQL
        is full of things like t1.CUSTOMER_ID, Source_Acct.CONTRACT_NUMBER,
        Target_Cust.PARTNER_NUMBER . these all look like library.table to
        a naive regex but they are actually table aliases referencing columns.

        My solution: I only remap a dot prefixed token if the part before
        the dot is a known SAS library name from the config. If it is not
        in my list, I skip it. This is simple and it works because the set
        of SAS library names is small and known in advance (WORK, SOURCE,
        DWH, STAGING in our case).

        I considered fancier approaches like building an alias registry from
        the FROM/JOIN clauses, but that would be fragile and unnecessary.
        """
        result = TableMappingResult(converted_sql=sql)
        seen: set[str] = set()

        # the set of library names I know about, uppercased for comparison
        known_upper = set(self._mappings.keys())

        def _replacer(match: re.Match) -> str:
            library = match.group(1)
            table = match.group(2)
            original = match.group(0)

            # the alias collision check - if the prefix is not a known
            # SAS library, I leave it alone. This is what prevents
            # t1.column or Source_Acct.field from getting remapped.
            if library.upper() not in known_upper:
                return original

            mapped, was_known = self._resolve(library, table)

            # only record each unique library.table pair once in the
            # mappings list, even if it appears multiple times in the SQL
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
        """Look up the config for a SAS library name (case insensitive)."""
        return self._mappings.get(library.upper())

    def summary(self) -> str:
        """Print a quick overview of what is configured."""
        lines = [f"Table mapper: {len(self._mappings)} library mappings configured"]
        for lib, cfg in sorted(self._mappings.items()):
            prefix_note = f" (prefix: {cfg.table_prefix})" if cfg.table_prefix else ""
            lines.append(f"  {lib} -> {cfg.oracle_schema}{prefix_note}")
        if self._default_schema:
            lines.append(f"  Default fallback: {self._default_schema}")
        return "\n".join(lines)
