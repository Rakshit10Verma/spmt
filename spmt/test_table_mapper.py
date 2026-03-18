"""
tests/test_table_mapper.py - Tests for the table mapper module.

I test config loading, each of the four SAS libraries, the alias collision
problem, dynamic table names with Pentaho ${var} tokens, deduplication of
mapping results, and a few integration tests using SQL from the TC files.
"""

import json
import pytest
from pathlib import Path

from spmt.table_mapper import TableMapper, TableMapping, TableMappingResult, LibraryConfig


# Fixtures 

@pytest.fixture
def default_mapper():
    """All four SAS libraries configured, with a DBO fallback."""
    return TableMapper(
        library_mappings={
            "WORK": LibraryConfig(oracle_schema="STAGING", table_prefix="TMP_"),
            "SOURCE": LibraryConfig(oracle_schema="SOURCE_DATA"),
            "DWH": LibraryConfig(oracle_schema="DWH_PROD"),
            "STAGING": LibraryConfig(oracle_schema="STAGING"),
        },
        default_schema="DBO",
    )


@pytest.fixture
def no_default_mapper():
    """Only WORK configured, no fallback schema at all."""
    return TableMapper(
        library_mappings={
            "WORK": LibraryConfig(oracle_schema="STAGING", table_prefix="TMP_"),
        },
    )


@pytest.fixture
def config_file(tmp_path):
    """Write a realistic table_mappings.json to a temp dir for loading tests."""
    cfg = {
        "library_mappings": {
            "WORK": {
                "oracle_schema": "STAGING",
                "table_prefix": "TMP_",
                "description": "Temp tables",
            },
            "SOURCE": {
                "oracle_schema": "SOURCE_DATA",
            },
            "DWH": {
                "oracle_schema": "DWH_PROD",
                "table_prefix": "",
            },
            "STAGING": {
                "oracle_schema": "STAGING",
            },
        },
        "default_schema": "DBO",
        "default_prefix": "",
    }
    path = tmp_path / "table_mappings.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return path


# Config loading 

class TestConfigLoading:
    """Make sure from_config() reads the JSON correctly."""

    def test_loads_all_libraries(self, config_file):
        mapper = TableMapper.from_config(config_file)
        assert sorted(mapper.known_libraries) == ["DWH", "SOURCE", "STAGING", "WORK"]

    def test_work_has_prefix(self, config_file):
        mapper = TableMapper.from_config(config_file)
        cfg = mapper.get_mapping_for("WORK")
        assert cfg is not None
        assert cfg.table_prefix == "TMP_"
        assert cfg.oracle_schema == "STAGING"

    def test_source_no_prefix(self, config_file):
        mapper = TableMapper.from_config(config_file)
        cfg = mapper.get_mapping_for("SOURCE")
        assert cfg is not None
        assert cfg.table_prefix == ""

    def test_case_insensitive_lookup(self, config_file):
        mapper = TableMapper.from_config(config_file)
        assert mapper.get_mapping_for("work") is not None
        assert mapper.get_mapping_for("Work") is not None
        assert mapper.get_mapping_for("WORK") is not None

    def test_unknown_library_returns_none(self, config_file):
        mapper = TableMapper.from_config(config_file)
        assert mapper.get_mapping_for("NONEXISTENT") is None

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            TableMapper.from_config(tmp_path / "nope.json")

    def test_description_loaded(self, config_file):
        mapper = TableMapper.from_config(config_file)
        cfg = mapper.get_mapping_for("WORK")
        assert cfg.description == "Temp tables"

    def test_missing_optional_fields_default(self, tmp_path):
        """If table_prefix and description are not in the JSON, they default."""
        cfg = {
            "library_mappings": {
                "MYLIB": {"oracle_schema": "MY_SCHEMA"}
            }
        }
        path = tmp_path / "minimal.json"
        path.write_text(json.dumps(cfg), encoding="utf-8")
        mapper = TableMapper.from_config(path)
        lib_cfg = mapper.get_mapping_for("MYLIB")
        assert lib_cfg.oracle_schema == "MY_SCHEMA"
        assert lib_cfg.table_prefix == ""
        assert lib_cfg.description == ""


# WORK library 

class TestWorkMapping:
    """WORK tables get the TMP_ prefix and go into the STAGING schema."""

    def test_basic_work_table(self, default_mapper):
        r = default_mapper.map_tables("FROM WORK.CUSTOMER_FILTERED t1")
        assert "STAGING.TMP_CUSTOMER_FILTERED" in r.converted_sql

    def test_work_in_create_table(self, default_mapper):
        r = default_mapper.map_tables("CREATE TABLE WORK.MONTHLY_TRANSACTIONS AS")
        assert "STAGING.TMP_MONTHLY_TRANSACTIONS" in r.converted_sql

    def test_work_lowercase(self, default_mapper):
        r = default_mapper.map_tables("FROM work.credit_contracts t1")
        assert "STAGING.TMP_credit_contracts" in r.converted_sql

    def test_work_mixed_case(self, default_mapper):
        r = default_mapper.map_tables("FROM Work.SomeTable t1")
        assert "STAGING.TMP_SomeTable" in r.converted_sql


# SOURCE library 

class TestSourceMapping:
    """SOURCE tables go to SOURCE_DATA schema, no prefix."""

    def test_basic_source_table(self, default_mapper):
        r = default_mapper.map_tables("FROM SOURCE.CUSTOMER_MASTER t1")
        assert "SOURCE_DATA.CUSTOMER_MASTER" in r.converted_sql

    def test_source_lowercase(self, default_mapper):
        r = default_mapper.map_tables("FROM source.CATEGORY_LOOKUP t2")
        assert "SOURCE_DATA.CATEGORY_LOOKUP" in r.converted_sql


# DWH library 

class TestDwhMapping:
    """DWH tables go to DWH_PROD schema."""

    def test_basic_dwh_table(self, default_mapper):
        r = default_mapper.map_tables("LEFT JOIN DWH.VC_ACCOUNTS t2 ON")
        assert "DWH_PROD.VC_ACCOUNTS" in r.converted_sql

    def test_dwh_lowercase(self, default_mapper):
        r = default_mapper.map_tables("FROM dwh.customer_master t2")
        assert "DWH_PROD.customer_master" in r.converted_sql


# STAGING library 

class TestStagingMapping:
    """STAGING maps to STAGING - the schema name stays the same here."""

    def test_staging_table(self, default_mapper):
        r = default_mapper.map_tables("FROM STAGING.REGIONAL_OFFICE t1")
        assert "STAGING.REGIONAL_OFFICE" in r.converted_sql

    def test_staging_lowercase(self, default_mapper):
        r = default_mapper.map_tables("INNER JOIN staging.contract_details t2")
        assert "STAGING.contract_details" in r.converted_sql


# Alias skipping
# This is the alias collision problem. In SAS SQL, t1.COLUMN_NAME and
# Source_Acct.CONTRACT_NUMBER look exactly like LIBRARY.TABLE to a regex.
# I solve this by only remapping prefixes that match a known library name.

class TestAliasSkipping:
    """Table aliases must not be treated as library references."""

    def test_t1_alias_not_mapped(self, default_mapper):
        sql = "WHERE t1.ORG_UNIT_CODE = 'X'"
        r = default_mapper.map_tables(sql)
        assert r.converted_sql == sql
        assert len(r.mappings) == 0

    def test_t2_alias_not_mapped(self, default_mapper):
        sql = "AND t2.snapshot_period = 202505"
        r = default_mapper.map_tables(sql)
        assert r.converted_sql == sql

    def test_named_alias_not_mapped(self, default_mapper):
        """Source_Acct, Target_Acct, Source_Cust are aliases from TC-08."""
        sql = (
            "Source_Acct.CONTRACT_NUMBER = t1.ID "
            "AND Target_Acct.PARTNER_NUMBER = Source_Cust.PARTNER_NUMBER"
        )
        r = default_mapper.map_tables(sql)
        assert r.converted_sql == sql
        assert len(r.mappings) == 0

    def test_alias_alongside_library(self, default_mapper):
        """Real library refs get mapped, aliases in the same SQL stay."""
        sql = (
            "FROM WORK.CONTRACTS t1 "
            "LEFT JOIN DWH.VC_ACCOUNTS Source_Acct "
            "ON t1.ID = Source_Acct.CONTRACT_NUMBER"
        )
        r = default_mapper.map_tables(sql)
        assert "STAGING.TMP_CONTRACTS" in r.converted_sql
        assert "DWH_PROD.VC_ACCOUNTS" in r.converted_sql
        # aliases should still be there untouched
        assert "Source_Acct.CONTRACT_NUMBER" in r.converted_sql
        assert "t1.ID" in r.converted_sql


# Dynamic table names with Pentaho variables
# TC-05 has source.CUSTOMERS_&gPeriodeTable. which after variable_handler
# becomes source.CUSTOMERS_${gPeriodeTable}. The regex needs to handle that.

class TestDynamicTableNames:
    """Table names with ${var} tokens from variable_handler."""

    def test_pentaho_var_in_table_name(self, default_mapper):
        """TC-05 pattern: source.CUSTOMERS_${gPeriodeTable}"""
        sql = "SELECT * FROM source.CUSTOMERS_${gPeriodeTable}"
        r = default_mapper.map_tables(sql)
        assert "SOURCE_DATA.CUSTOMERS_${gPeriodeTable}" in r.converted_sql

    def test_pentaho_var_recorded_in_mappings(self, default_mapper):
        sql = "FROM source.CUSTOMERS_${gPeriodeTable}"
        r = default_mapper.map_tables(sql)
        assert len(r.mappings) == 1
        assert r.mappings[0].table_name == "CUSTOMERS_${gPeriodeTable}"


# Mapping result tracking

class TestMappingResults:
    """Check that the mapper tracks what it did and deduplicates."""

    def test_mappings_recorded(self, default_mapper):
        sql = "FROM WORK.TABLE_A t1 INNER JOIN SOURCE.TABLE_B t2 ON t1.id = t2.id"
        r = default_mapper.map_tables(sql)
        assert len(r.mappings) == 2
        originals = {m.original for m in r.mappings}
        assert "WORK.TABLE_A" in originals
        assert "SOURCE.TABLE_B" in originals

    def test_duplicate_references_mapped_once(self, default_mapper):
        """Same table in CREATE and FROM should only appear once in mappings."""
        sql = (
            "CREATE TABLE WORK.OUTPUT AS "
            "SELECT * FROM WORK.OUTPUT WHERE 1=0"
        )
        r = default_mapper.map_tables(sql)
        # WORK.OUTPUT shows up twice in the SQL but once in the mappings list
        work_output_count = sum(
            1 for m in r.mappings if m.original.upper() == "WORK.OUTPUT"
        )
        assert work_output_count == 1

    def test_both_occurrences_replaced(self, default_mapper):
        """Both text occurrences should be rewritten even if deduped in list."""
        sql = (
            "CREATE TABLE WORK.RESULT AS "
            "SELECT * FROM WORK.RESULT"
        )
        r = default_mapper.map_tables(sql)
        assert r.converted_sql.count("STAGING.TMP_RESULT") == 2

    def test_mapping_fields_populated(self, default_mapper):
        r = default_mapper.map_tables("FROM DWH.FACT_TABLE t1")
        m = r.mappings[0]
        assert m.library.upper() == "DWH"
        assert m.table_name == "FACT_TABLE"
        assert m.mapped == "DWH_PROD.FACT_TABLE"

    def test_no_mappings_for_plain_sql(self, default_mapper):
        sql = "SELECT 1 FROM DUAL"
        r = default_mapper.map_tables(sql)
        assert len(r.mappings) == 0
        assert r.converted_sql == sql


# Unknown libraries

class TestUnknownLibraries:
    """If a library is not in the config I should not touch it."""

    def test_unknown_lib_not_mapped_without_default(self, no_default_mapper):
        """No default schema configured, so CUSTOM.MY_TABLE stays as-is."""
        sql = "FROM CUSTOM.MY_TABLE t1"
        r = no_default_mapper.map_tables(sql)
        # CUSTOM is not a known library and there is no default
        assert r.converted_sql == sql

    def test_known_lib_still_works(self, no_default_mapper):
        sql = "FROM WORK.MY_TABLE t1"
        r = no_default_mapper.map_tables(sql)
        assert "STAGING.TMP_MY_TABLE" in r.converted_sql


# Integration tests using real TC file SQL 
# These use bigger SQL fragments from the actual test cases to make sure
# everything works together: library remapping, alias skipping, chaining.

class TestIntegration:
    """Larger SQL fragments from TC files."""

    def test_tc01_block1(self, default_mapper):
        """TC-01 Block 1: basic WORK + SOURCE references."""
        sql = (
            "CREATE TABLE WORK.CUSTOMER_FILTERED AS\n"
            "SELECT t1.CUSTOMER_ID, t1.CUSTOMER_NAME\n"
            "FROM SOURCE.CUSTOMER_MASTER t1\n"
            "WHERE t1.ORG_UNIT_CODE = 'X'"
        )
        r = default_mapper.map_tables(sql)
        assert "STAGING.TMP_CUSTOMER_FILTERED" in r.converted_sql
        assert "SOURCE_DATA.CUSTOMER_MASTER" in r.converted_sql
        assert "t1.CUSTOMER_ID" in r.converted_sql
        assert "t1.ORG_UNIT_CODE" in r.converted_sql

    def test_tc01_block5_chained(self, default_mapper):
        """TC-01 Block 5: WORK table in both CREATE and FROM (chained dependency)."""
        sql = (
            "CREATE TABLE WORK.CUSTOMER_ORDERS AS\n"
            "SELECT DISTINCT t1.CUSTOMER_ID\n"
            "FROM WORK.CUSTOMER_FILTERED t1\n"
            "INNER JOIN SOURCE.ORDERS t2 ON t1.CUSTOMER_ID = t2.CUSTOMER_ID"
        )
        r = default_mapper.map_tables(sql)
        assert "STAGING.TMP_CUSTOMER_ORDERS" in r.converted_sql
        assert "STAGING.TMP_CUSTOMER_FILTERED" in r.converted_sql
        assert "SOURCE_DATA.ORDERS" in r.converted_sql

    def test_tc08_five_joins(self, default_mapper):
        """TC-08 Block 4: the big one with 5 JOINs and named aliases.

        This is the main test for the alias collision problem. The SQL
        has WORK and DWH library refs mixed with aliases like Source_Acct,
        Target_Acct, Source_Cust, Target_Cust. Only the library refs
        should change.
        """
        sql = (
            "FROM WORK.CONTRACT_SPLITS t1\n"
            "LEFT JOIN DWH.VC_ACCOUNTS Source_Acct "
            "ON (t1.SOURCE_CONTRACT_ID = Source_Acct.CONTRACT_NUMBER)\n"
            "LEFT JOIN DWH.VC_ACCOUNTS Target_Acct "
            "ON (t1.TARGET_CONTRACT_ID = Target_Acct.CONTRACT_NUMBER)\n"
            "INNER JOIN WORK.REGIONAL_OFFICES t2 "
            "ON (Source_Acct.ORIGINATING_OFFICE_NR = t2.OFFICE_NUMBER)\n"
            "INNER JOIN DWH.VC_CUSTOMERS Source_Cust "
            "ON (Source_Acct.PARTNER_NUMBER = Source_Cust.PARTNER_NUMBER)\n"
            "INNER JOIN DWH.VC_CUSTOMERS Target_Cust "
            "ON (Target_Acct.PARTNER_NUMBER = Target_Cust.PARTNER_NUMBER)"
        )
        r = default_mapper.map_tables(sql)
        assert "STAGING.TMP_CONTRACT_SPLITS" in r.converted_sql
        assert "DWH_PROD.VC_ACCOUNTS" in r.converted_sql
        assert "STAGING.TMP_REGIONAL_OFFICES" in r.converted_sql
        assert "DWH_PROD.VC_CUSTOMERS" in r.converted_sql
        # these are aliases, not libraries - they must survive unchanged
        assert "Source_Acct.CONTRACT_NUMBER" in r.converted_sql
        assert "Target_Acct.CONTRACT_NUMBER" in r.converted_sql
        assert "Source_Cust.PARTNER_NUMBER" in r.converted_sql
        assert "Target_Cust.PARTNER_NUMBER" in r.converted_sql

    def test_tc06_staging_and_dwh(self, default_mapper):
        """TC-06: staging and dwh side by side in the same query."""
        sql = (
            "FROM dwh.approval_daily t1\n"
            "LEFT JOIN dwh.customer_daily t2 ON t1.id = t2.id\n"
            "INNER JOIN staging.contract_details t3 ON t1.id = t3.id"
        )
        r = default_mapper.map_tables(sql)
        assert "DWH_PROD.approval_daily" in r.converted_sql
        assert "DWH_PROD.customer_daily" in r.converted_sql
        assert "STAGING.contract_details" in r.converted_sql

    def test_tc07_all_work_tables(self, default_mapper):
        """TC-07: chained WORK tables plus a staging reference."""
        sql = (
            "CREATE TABLE WORK.ENRICHED_CONTRACTS AS\n"
            "SELECT t1.contract_id\n"
            "FROM WORK.MONTHLY_TRANSACTIONS t1\n"
            "INNER JOIN staging.contract_details t2 "
            "ON t1.contract_id = t2.contract_id"
        )
        r = default_mapper.map_tables(sql)
        assert "STAGING.TMP_ENRICHED_CONTRACTS" in r.converted_sql
        assert "STAGING.TMP_MONTHLY_TRANSACTIONS" in r.converted_sql
        assert "STAGING.contract_details" in r.converted_sql


# Summary helper 

class TestSummary:
    """Quick checks on the summary() output."""

    def test_summary_contains_all_libraries(self, default_mapper):
        s = default_mapper.summary()
        assert "WORK" in s
        assert "SOURCE" in s
        assert "DWH" in s
        assert "STAGING" in s

    def test_summary_mentions_prefix(self, default_mapper):
        s = default_mapper.summary()
        assert "TMP_" in s

    def test_summary_mentions_count(self, default_mapper):
        s = default_mapper.summary()
        assert "4 library mappings" in s

    def test_known_libraries_sorted(self, default_mapper):
        libs = default_mapper.known_libraries
        assert libs == sorted(libs)
