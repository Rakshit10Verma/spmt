"""
tests/test_ktr_generator.py

Tests for the Pentaho .ktr XML generator.

Organized into four groups:
  - XML validity:    output parses as well-formed XML
  - Structure:       expected elements exist (info, connection, steps)
  - Step content:    SQL is embedded correctly, step names are sensible
  - Edge cases:      empty input, special characters in SQL, many blocks
"""

import xml.etree.ElementTree as ET

import pytest

from spmt.ktr_generator import (
    KtrResult,
    SqlBlock,
    generate_ktr,
    write_ktr,
    _step_position,
    _extract_table_name,
)


# Fixtures

@pytest.fixture
def single_block():
    """One simple CREATE TABLE block."""
    return [
        SqlBlock(
            block_number=1,
            converted_sql=(
                "CREATE TABLE STAGING.TMP_CUSTOMER_FILTERED AS\n"
                "SELECT t1.CUSTOMER_ID, t1.CUSTOMER_NAME\n"
                "FROM SOURCE_DATA.CUSTOMER_MASTER t1\n"
                "WHERE t1.STATUS_FLAG = 'A'"
            ),
            source_table="STAGING.TMP_CUSTOMER_FILTERED",
            rules_applied=["R-01", "R-02"],
        )
    ]


@pytest.fixture
def multiple_blocks():
    """Five blocks mimicking TC-01 output."""
    return [
        SqlBlock(
            block_number=i,
            converted_sql=f"CREATE TABLE STAGING.TMP_TABLE_{i} AS\nSELECT * FROM DUAL",
            source_table=f"STAGING.TMP_TABLE_{i}",
        )
        for i in range(1, 6)
    ]


@pytest.fixture
def block_with_special_chars():
    """SQL containing characters that need XML escaping."""
    return [
        SqlBlock(
            block_number=1,
            converted_sql=(
                "SELECT * FROM STAGING.TMP_DATA\n"
                "WHERE amount > 1000 AND status <> 'CLOSED'\n"
                "AND name = 'O''Brien & Sons'"
            ),
        )
    ]


# XML validity

class TestXmlValidity:
    """The output must be parseable XML — if this fails, Pentaho can't open it."""

    def test_output_is_valid_xml(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        assert root.tag == "transformation"

    def test_xml_declaration_present(self, single_block):
        result = generate_ktr(single_block)
        assert result.xml_string.startswith("<?xml")

    def test_special_chars_are_escaped(self, block_with_special_chars):
        """Ampersands and angle brackets in SQL must be XML-escaped."""
        result = generate_ktr(block_with_special_chars)
        # Should not raise
        root = ET.fromstring(result.xml_string)
        # The SQL should survive the round-trip intact
        sql_elem = root.find(".//step/sql")
        assert "O''Brien & Sons" in sql_elem.text
        assert "status <> 'CLOSED'" in sql_elem.text


# Transformation structure

class TestTransformationStructure:
    """Pentaho expects specific top-level elements."""

    def test_has_info_element(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        info = root.find("info")
        assert info is not None

    def test_transformation_name_in_info(self, single_block):
        result = generate_ktr(single_block, transformation_name="My_Test")
        root = ET.fromstring(result.xml_string)
        name = root.find("info/name")
        assert name.text == "My_Test"

    def test_has_connection_element(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        conn = root.find("connection")
        assert conn is not None

    def test_connection_type_is_oracle(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        conn_type = root.find("connection/type")
        assert conn_type.text == "ORACLE"

    def test_connection_name_matches_steps(self, single_block):
        """The connection name in steps must match the connection definition."""
        result = generate_ktr(single_block, connection_name="test_db")
        root = ET.fromstring(result.xml_string)
        conn_name = root.find("connection/name").text
        step_conn = root.find(".//step/connection").text
        assert conn_name == step_conn == "test_db"

    def test_has_notepads_element(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        assert root.find("notepads") is not None

    def test_has_step_error_handling(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        assert root.find("step_error_handling") is not None

    def test_custom_connection_config(self, single_block):
        cfg = {"host": "db.example.com", "port": "1522", "db_name": "PROD"}
        result = generate_ktr(single_block, connection_config=cfg)
        root = ET.fromstring(result.xml_string)
        assert root.find("connection/server").text == "db.example.com"
        assert root.find("connection/port").text == "1522"
        assert root.find("connection/database").text == "PROD"


# Step content

class TestStepContent:
    """Each SQL block should produce one correctly configured Table Input step."""

    def test_step_count_matches_blocks(self, multiple_blocks):
        result = generate_ktr(multiple_blocks)
        root = ET.fromstring(result.xml_string)
        steps = root.findall("step")
        assert len(steps) == 5

    def test_result_step_count(self, multiple_blocks):
        result = generate_ktr(multiple_blocks)
        assert result.step_count == 5

    def test_step_type_is_table_input(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        step_type = root.find(".//step/type")
        assert step_type.text == "TableInput"

    def test_sql_embedded_in_step(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        sql_elem = root.find(".//step/sql")
        assert "STAGING.TMP_CUSTOMER_FILTERED" in sql_elem.text
        assert "SOURCE_DATA.CUSTOMER_MASTER" in sql_elem.text

    def test_step_name_includes_table(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        step_name = root.find(".//step/name").text
        assert "STAGING.TMP_CUSTOMER_FILTERED" in step_name

    def test_step_name_includes_block_number(self, single_block):
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        step_name = root.find(".//step/name").text
        assert "01" in step_name

    def test_variables_active_is_yes(self, single_block):
        """Pentaho variables must be active so ${param} substitution works."""
        result = generate_ktr(single_block)
        root = ET.fromstring(result.xml_string)
        var_active = root.find(".//step/variables_active")
        assert var_active.text == "Y"

    def test_each_step_has_gui_position(self, multiple_blocks):
        result = generate_ktr(multiple_blocks)
        root = ET.fromstring(result.xml_string)
        for step in root.findall("step"):
            gui = step.find("GUI")
            assert gui is not None
            xloc = gui.find("xloc")
            yloc = gui.find("yloc")
            assert xloc is not None and xloc.text.isdigit()
            assert yloc is not None and yloc.text.isdigit()

    def test_steps_have_distinct_positions(self, multiple_blocks):
        """No two steps should overlap on the canvas."""
        result = generate_ktr(multiple_blocks)
        root = ET.fromstring(result.xml_string)
        positions = set()
        for step in root.findall("step"):
            gui = step.find("GUI")
            pos = (gui.find("xloc").text, gui.find("yloc").text)
            assert pos not in positions, f"Duplicate position: {pos}"
            positions.add(pos)

    def test_drop_steps_inserted_before_matching_sql_steps(self, single_block):
        drops = [
            "BEGIN\n"
            "   EXECUTE IMMEDIATE 'DROP TABLE STAGING.TMP_CUSTOMER_FILTERED';\n"
            "EXCEPTION\n"
            "   WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
            "END;\n/"
        ]
        result = generate_ktr(single_block, drop_statements=drops)
        root = ET.fromstring(result.xml_string)
        names = [n.text for n in root.findall("step/name")]
        assert names[0].startswith("DROP_01_STAGING.TMP_CUSTOMER_FILTERED")
        assert names[1].startswith("SQL_01_STAGING.TMP_CUSTOMER_FILTERED")

        types = [t.text for t in root.findall("step/type")]
        assert types[0] == "ExecSQL"
        assert types[1] == "TableInput"

    def test_drop_and_sql_are_connected_in_hops(self, single_block):
        drops = [
            "BEGIN\n"
            "   EXECUTE IMMEDIATE 'DROP TABLE STAGING.TMP_CUSTOMER_FILTERED';\n"
            "EXCEPTION\n"
            "   WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
            "END;\n/"
        ]
        result = generate_ktr(single_block, drop_statements=drops)
        root = ET.fromstring(result.xml_string)
        hop_from = root.find("hop/from").text
        hop_to = root.find("hop/to").text
        assert hop_from.startswith("DROP_01_STAGING.TMP_CUSTOMER_FILTERED")
        assert hop_to.startswith("SQL_01_STAGING.TMP_CUSTOMER_FILTERED")


# Step layout

class TestStepLayout:
    """Grid positioning should keep the canvas clean."""

    def test_first_step_at_base_position(self):
        x, y = _step_position(0)
        assert x == 150
        assert y == 150

    def test_second_step_offset_right(self):
        x0, y0 = _step_position(0)
        x1, y1 = _step_position(1)
        assert x1 > x0
        assert y1 == y0

    def test_row_wrap_after_four_steps(self):
        """Step 4 (index 4) should wrap to a new row."""
        _, y3 = _step_position(3)
        x4, y4 = _step_position(4)
        assert y4 > y3
        # Should be back to the left column
        x0, _ = _step_position(0)
        assert x4 == x0


# Table name extraction

class TestExtractTableName:
    """The helper that pulls CREATE TABLE names from SQL."""

    def test_simple_create_table(self):
        sql = "CREATE TABLE STAGING.TMP_ORDERS AS SELECT * FROM DUAL"
        assert _extract_table_name(sql) == "STAGING.TMP_ORDERS"

    def test_create_table_no_schema(self):
        sql = "CREATE TABLE MY_TABLE AS SELECT 1 FROM DUAL"
        assert _extract_table_name(sql) == "MY_TABLE"

    def test_plain_select_returns_none(self):
        sql = "SELECT * FROM STAGING.TMP_ORDERS"
        assert _extract_table_name(sql) is None

    def test_with_pentaho_variable(self):
        sql = "CREATE TABLE ${schema}.TMP_DATA AS SELECT 1 FROM DUAL"
        assert _extract_table_name(sql) == "${schema}.TMP_DATA"

    def test_leading_whitespace(self):
        sql = "   \n  CREATE TABLE STAGING.TMP_X AS SELECT 1 FROM DUAL"
        assert _extract_table_name(sql) == "STAGING.TMP_X"


# Edge cases

class TestEdgeCases:
    """Boundary conditions and unusual inputs."""

    def test_empty_block_list(self):
        result = generate_ktr([])
        root = ET.fromstring(result.xml_string)
        assert root.tag == "transformation"
        assert result.step_count == 0
        assert len(result.warnings) == 1

    def test_block_without_source_table(self):
        """Non-CTAS blocks should still get a step with a fallback name."""
        blocks = [
            SqlBlock(
                block_number=3,
                converted_sql="SELECT COUNT(*) FROM STAGING.TMP_DATA",
            )
        ]
        result = generate_ktr(blocks)
        root = ET.fromstring(result.xml_string)
        step_name = root.find(".//step/name").text
        # Should contain block number even without a table name
        assert "03" in step_name

    def test_many_blocks_layout(self):
        """Ten blocks should produce a two-row layout without errors."""
        blocks = [
            SqlBlock(block_number=i, converted_sql=f"SELECT {i} FROM DUAL")
            for i in range(1, 11)
        ]
        result = generate_ktr(blocks)
        assert result.step_count == 10
        root = ET.fromstring(result.xml_string)
        assert len(root.findall("step")) == 10

    def test_sql_with_pentaho_variables(self):
        """${var} syntax should pass through into the XML intact."""
        blocks = [
            SqlBlock(
                block_number=1,
                converted_sql=(
                    "SELECT * FROM SOURCE_DATA.CONTRACTS\n"
                    "WHERE period_key = ${report_period}"
                ),
            )
        ]
        result = generate_ktr(blocks)
        root = ET.fromstring(result.xml_string)
        sql_text = root.find(".//step/sql").text
        assert "${report_period}" in sql_text


# File writing

class TestWriteKtr:
    """Test the disk-write helper."""

    def test_write_creates_file(self, single_block, tmp_path):
        result = generate_ktr(single_block)
        out = write_ktr(result, tmp_path / "test_output.ktr")
        assert out.exists()
        assert out.suffix == ".ktr"

    def test_write_adds_extension(self, single_block, tmp_path):
        result = generate_ktr(single_block)
        out = write_ktr(result, tmp_path / "no_extension")
        assert out.suffix == ".ktr"

    def test_written_file_is_valid_xml(self, single_block, tmp_path):
        result = generate_ktr(single_block)
        out = write_ktr(result, tmp_path / "valid.ktr")
        root = ET.parse(str(out)).getroot()
        assert root.tag == "transformation"

    def test_write_creates_parent_dirs(self, single_block, tmp_path):
        result = generate_ktr(single_block)
        out = write_ktr(result, tmp_path / "nested" / "dir" / "output.ktr")
        assert out.exists()
