"""
spmt/ktr_generator.py

Generates a Pentaho Data Integration .ktr (transformation) file from
converted Oracle SQL blocks.

Pentaho stores transformations as XML. Each SQL block becomes a Table Input
step that Pentaho can execute against an Oracle database. The generator
also creates database connection placeholders and lays out steps in a grid
so the transformation opens with a clean visual layout in Spoon (the PDI
designer).

I chose xml.etree.ElementTree over string templates because it handles
XML escaping automatically — important since SQL strings often contain
angle brackets, ampersands, and quotes that would break raw XML.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional


# Result dataclass

@dataclass
class KtrResult:
    """Output from the KTR generation process.

    Attributes:
        xml_string:   The complete .ktr XML as a string.
        step_count:   Number of Table Input steps created.
        warnings:     Any issues encountered during generation.
    """
    xml_string: str = ""
    step_count: int = 0
    warnings: list[str] = field(default_factory=list)


# Input dataclass — what the converter feeds us

@dataclass
class SqlBlock:
    """A single converted SQL block ready for embedding in a .ktr step.

    The converter produces these after running the full pipeline
    (variable substitution, table mapping, rule application).

    Attributes:
        block_number:   Sequential position from the source .sas file.
        converted_sql:  Oracle-compatible SQL to embed in the Table Input step.
        source_table:   Target table name from the CREATE TABLE statement,
                        or None if the block is a non-CTAS query.
        rules_applied:  Rule IDs that were used during conversion (for docs).
    """
    block_number: int
    converted_sql: str
    source_table: Optional[str] = None
    rules_applied: list[str] = field(default_factory=list)


# Layout constants

# Starting X/Y position for the first step in the Spoon canvas.
_BASE_X = 150
_BASE_Y = 150

# Horizontal spacing between steps. Pentaho's default grid is roughly
# 100px per step, but 200 gives a cleaner layout when step labels are long.
_STEP_SPACING_X = 250

# Maximum steps per row before wrapping to the next line.
_STEPS_PER_ROW = 4

# Vertical spacing when wrapping to a new row.
_ROW_SPACING_Y = 200


def _step_position(index: int) -> tuple[int, int]:
    """Calculate the x, y canvas position for step number *index* (0-based).

    Steps are laid out left-to-right in rows. After _STEPS_PER_ROW steps
    the layout wraps to the next row so the canvas stays readable.
    """
    col = index % _STEPS_PER_ROW
    row = index // _STEPS_PER_ROW
    x = _BASE_X + col * _STEP_SPACING_X
    y = _BASE_Y + row * _ROW_SPACING_Y
    return x, y


def _build_info_element(name: str, description: str) -> ET.Element:
    """Build the <info> element with transformation metadata.

    This is the header Pentaho reads to display the transformation name,
    description, and engine settings. Most values are Pentaho defaults —
    I only customised the name and description fields.
    """
    info = ET.Element("info")

    ET.SubElement(info, "name").text = name
    ET.SubElement(info, "description").text = description
    ET.SubElement(info, "extended_description")
    ET.SubElement(info, "trans_version")
    ET.SubElement(info, "trans_type").text = "Normal"

    # Pentaho uses these for internal scheduling. Defaults are fine.
    ET.SubElement(info, "trans_status").text = "0"
    ET.SubElement(info, "directory").text = "/"

    # Logging and performance settings — all defaults
    log = ET.SubElement(info, "log")
    for log_type in ("trans-log-table", "perf-log-table",
                     "channel-log-table", "step-log-table",
                     "metrics-log-table"):
        log_table = ET.SubElement(log, log_type)
        ET.SubElement(log_table, "connection")
        ET.SubElement(log_table, "schema")
        ET.SubElement(log_table, "table")
        ET.SubElement(log_table, "size_limit_lines")
        ET.SubElement(log_table, "interval")
        ET.SubElement(log_table, "timeout_days")

    # Misc engine settings
    ET.SubElement(info, "maxdateconnection")
    ET.SubElement(info, "maxdatetable")
    ET.SubElement(info, "maxdatefield")
    ET.SubElement(info, "maxdateoffset").text = "0.0"
    ET.SubElement(info, "maxdatediff").text = "0.0"
    ET.SubElement(info, "size_rowset").text = "10000"
    ET.SubElement(info, "sleep_time_empty").text = "50"
    ET.SubElement(info, "sleep_time_full").text = "50"
    ET.SubElement(info, "unique_connections").text = "N"
    ET.SubElement(info, "feedback_shown").text = "Y"
    ET.SubElement(info, "feedback_size").text = "50000"
    ET.SubElement(info, "using_thread_priorities").text = "Y"
    ET.SubElement(info, "shared_objects_file")
    ET.SubElement(info, "capture_step_performance").text = "N"
    ET.SubElement(info, "step_performance_capturing_delay").text = "1000"
    ET.SubElement(info, "step_performance_capturing_size_limit").text = "100"

    # Created / modified timestamps
    now_str = datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")[:-3]
    ET.SubElement(info, "created_user").text = "SPMT"
    ET.SubElement(info, "created_date").text = now_str
    ET.SubElement(info, "modified_user").text = "SPMT"
    ET.SubElement(info, "modified_date").text = now_str

    return info


def _build_connection_element(
    conn_name: str = "oracle_dwh",
    host: str = "localhost",
    port: str = "1521",
    db_name: str = "ORCL",
    schema: str = "",
    username: str = "dwh_user",
) -> ET.Element:
    """Build a <connection> element for an Oracle database.

    These are placeholder values — the real connection details get
    configured in Pentaho when the transformation is deployed. Having
    the structure here means Pentaho can open the .ktr without errors
    and the user just updates the credentials.
    """
    conn = ET.Element("connection")

    ET.SubElement(conn, "name").text = conn_name
    ET.SubElement(conn, "server").text = host
    ET.SubElement(conn, "type").text = "ORACLE"
    ET.SubElement(conn, "access").text = "Native"
    ET.SubElement(conn, "database").text = db_name
    ET.SubElement(conn, "port").text = port
    ET.SubElement(conn, "username").text = username
    ET.SubElement(conn, "password").text = "Encrypted 00000000000000000000"
    ET.SubElement(conn, "servername")
    ET.SubElement(conn, "data_tablespace")
    ET.SubElement(conn, "index_tablespace")

    # Connection pooling — Pentaho defaults
    attrs = ET.SubElement(conn, "attributes")
    for key, val in [
        ("FORCE_IDENTIFIERS_TO_LOWERCASE", "N"),
        ("FORCE_IDENTIFIERS_TO_UPPERCASE", "N"),
        ("IS_CLUSTERED", "N"),
        ("PORT_NUMBER", port),
        ("PRESERVE_RESERVED_WORD_CASE", "Y"),
        ("QUOTE_ALL_FIELDS", "N"),
        ("SUPPORTS_BOOLEAN_DATA_TYPE", "Y"),
        ("SUPPORTS_TIMESTAMP_DATA_TYPE", "Y"),
        ("USE_POOLING", "N"),
    ]:
        attr = ET.SubElement(attrs, "attribute")
        ET.SubElement(attr, "code").text = key
        ET.SubElement(attr, "attribute").text = val

    if schema:
        ET.SubElement(conn, "schema").text = schema

    return conn


def _build_step_hop(from_step: str, to_step: str) -> ET.Element:
    """Build a <hop> element that connects two steps in sequence.

    In Pentaho, hops define the data flow between steps. The from_step
    sends its output to the to_step. Multiple hops between steps create
    the transformation pipeline.

    Args:
        from_step:  Name of the source step.
        to_step:    Name of the destination step.

    Returns:
        A <hop> element configured for sequential execution.
    """
    hop = ET.Element("hop")
    ET.SubElement(hop, "from").text = from_step
    ET.SubElement(hop, "to").text = to_step
    ET.SubElement(hop, "enabled").text = "Y"
    return hop


def _build_table_input_step(
    step_name: str,
    sql: str,
    connection_name: str,
    x: int,
    y: int,
) -> ET.Element:
    """Build a <step> element for a Pentaho Table Input step.

    Table Input is the standard step type for running SQL against a
    database. The SQL goes into <sql> and Pentaho executes it at runtime.
    """
    step = ET.Element("step")

    ET.SubElement(step, "name").text = step_name
    ET.SubElement(step, "type").text = "TableInput"
    ET.SubElement(step, "description")
    ET.SubElement(step, "distribute").text = "Y"
    ET.SubElement(step, "custom_distribution")
    ET.SubElement(step, "copies").text = "1"

    # Partition handling — not needed for simple table reads
    partitioning = ET.SubElement(step, "partitioning")
    ET.SubElement(partitioning, "method").text = "none"
    ET.SubElement(partitioning, "schema_name")

    # The actual SQL and connection reference
    ET.SubElement(step, "connection").text = connection_name
    ET.SubElement(step, "sql").text = sql
    ET.SubElement(step, "limit").text = "0"
    ET.SubElement(step, "lookup")
    ET.SubElement(step, "execute_each_row").text = "N"
    ET.SubElement(step, "variables_active").text = "Y"
    ET.SubElement(step, "lazy_conversion_active").text = "N"

    # Canvas position for Spoon layout
    gui = ET.SubElement(step, "GUI")
    ET.SubElement(gui, "xloc").text = str(x)
    ET.SubElement(gui, "yloc").text = str(y)
    ET.SubElement(gui, "draw").text = "Y"

    return step


def _extract_table_name(sql: str) -> Optional[str]:
    """Try to pull the target table name from a CREATE TABLE statement.

    Looks for CREATE TABLE schema.table or CREATE TABLE table at the
    start of the SQL. Returns None if it's not a CTAS query (e.g. a
    plain SELECT or a DROP TABLE).
    """
    import re
    match = re.match(
        r"\s*CREATE\s+TABLE\s+([\w$.{}]+(?:\.[\w$.{}]+)?)",
        sql,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    return None


def generate_ktr(
    sql_blocks: list[SqlBlock],
    transformation_name: str = "SPMT_Migration",
    description: str = "Auto-generated by SPMT from SAS PROC SQL source",
    connection_name: str = "oracle_dwh",
    connection_config: Optional[dict] = None,
) -> KtrResult:
    """Generate a Pentaho .ktr transformation from converted SQL blocks.

    Each SqlBlock becomes a Table Input step. Steps are laid out in a grid
    on the Spoon canvas. A placeholder Oracle connection is included so the
    .ktr opens without errors in Pentaho.

    Args:
        sql_blocks:           Converted SQL blocks from the converter.
        transformation_name:  Name shown in Pentaho's transformation tab.
        description:          Description metadata for the transformation.
        connection_name:      Name of the database connection to reference.
        connection_config:    Optional dict with host, port, db_name, schema,
                              username keys to override the default placeholder.

    Returns:
        KtrResult with the XML string, step count, and any warnings.
    """
    result = KtrResult()

    if not sql_blocks:
        result.warnings.append("No SQL blocks provided — generated empty transformation")

    # Root element
    root = ET.Element("transformation")

    # Transformation metadata
    root.append(_build_info_element(transformation_name, description))

    # Notepads section (empty but Pentaho expects it)
    ET.SubElement(root, "notepads")

    # Database connection
    conn_cfg = connection_config or {}
    root.append(_build_connection_element(
        conn_name=connection_name,
        host=conn_cfg.get("host", "localhost"),
        port=conn_cfg.get("port", "1521"),
        db_name=conn_cfg.get("db_name", "ORCL"),
        schema=conn_cfg.get("schema", ""),
        username=conn_cfg.get("username", "dwh_user"),
    ))

    # One Table Input step per SQL block
    step_names = []
    for block in sql_blocks:
        # Build a descriptive step name
        table = block.source_table or _extract_table_name(block.converted_sql)
        if table:
            step_name = f"SQL_{block.block_number:02d}_{table}"
        else:
            step_name = f"SQL_{block.block_number:02d}"

        x, y = _step_position(len(step_names))

        step_elem = _build_table_input_step(
            step_name=step_name,
            sql=block.converted_sql,
            connection_name=connection_name,
            x=x,
            y=y,
        )
        root.append(step_elem)
        step_names.append(step_name)

    result.step_count = len(step_names)

    # Create hops to connect steps sequentially
    # Each step connects to the next in order, forming a pipeline
    for i in range(len(step_names) - 1):
        hop_elem = _build_step_hop(step_names[i], step_names[i + 1])
        root.append(hop_elem)

    # Step error handling (empty — Pentaho expects the element)
    ET.SubElement(root, "step_error_handling")

    # Slave servers (empty — only needed for clustered execution)
    ET.SubElement(root, "slave-step-copy-partition-distribution")
    ET.SubElement(root, "slave_transformation").text = "N"

    # Convert the tree to a string
    ET.indent(root, space="  ")
    result.xml_string = ET.tostring(root, encoding="unicode", xml_declaration=True)

    return result


def write_ktr(
    ktr_result: KtrResult,
    output_path: str | Path,
) -> Path:
    """Write a KtrResult to a .ktr file on disk.

    Args:
        ktr_result:   The result from generate_ktr().
        output_path:  Where to save the file. Adds .ktr extension if missing.

    Returns:
        The resolved Path to the written file.
    """
    path = Path(output_path)
    if path.suffix.lower() != ".ktr":
        path = path.with_suffix(".ktr")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ktr_result.xml_string, encoding="utf-8")
    return path
