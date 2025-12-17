# src/com/dataready_ai/snf/core/utils/snowflakeutils.py

from com.drai.snf.core.utils.logger_utils import get_logger, log_execution

logger = get_logger()

# -------------------------------------------------------------
# INTERNAL HELPER — executes SQL and returns rows
# -------------------------------------------------------------
def _execute_query(sf_conn, sql: str):
    logger.debug(f"[QUERY] {sql}")
    cur = sf_conn.cursor()
    try:
        cur.execute(sql)
        result = cur.fetchall()
        cols = [col[0] for col in cur.description]
        return [dict(zip(cols, row)) for row in result]
    finally:
        cur.close()

# -------------------------------------------------------------
# INTERNAL HELPER — executes SQL without returning rows
# -------------------------------------------------------------
def _execute_non_query(sf_conn, sql: str):
    logger.debug(f"[NON-QUERY] {sql}")
    cur = sf_conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()


# =============================================================
# PUBLIC FUNCTIONS
# =============================================================

@log_execution()
def list_stages(sf_conn):
    """Return all stages in current database & schema."""
    sql = "SHOW STAGES"
    return _execute_query(sf_conn, sql)

@log_execution()
def list_databases(sf_conn):
    """Return all stages in current database & schema."""
    sql = "SHOW DATABASES"
    return _execute_query(sf_conn, sql)

@log_execution()
def stage_exists(sf_conn, stage_name: str) -> bool:
    """Check whether a stage exists."""
    sql = f"SHOW STAGES LIKE '{stage_name.upper()}'"
    rows = _execute_query(sf_conn, sql)
    return len(rows) > 0


@log_execution()
def validate_stage(sf_conn, stage_name: str):
    """Validate stage existence, raise if not found."""
    if not stage_exists(sf_conn, stage_name):
        msg = f"❌ Stage NOT found: {stage_name}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"✔ Stage exists: {stage_name}")


# -------------------------------------------------------------
# TABLE VALIDATION
# -------------------------------------------------------------
@log_execution()
def table_exists(sf_conn, table_name: str) -> bool:
    sql = f"SHOW TABLES LIKE '{table_name.upper()}'"
    rows = _execute_query(sf_conn, sql)
    return len(rows) > 0


@log_execution()
def create_table(sf_conn, table_name: str, schema_dict: dict):
    """
    schema_dict example:
    {
        "id": "NUMBER",
        "name": "VARCHAR",
        "amount": "FLOAT",
        "load_ts": "TIMESTAMP"
    }
    """

    cols_sql = ", ".join([f"{col} {dtype}" for col, dtype in schema_dict.items()])
    sql = f"CREATE TABLE {table_name} ({cols_sql})"
    logger.info(f"Creating table: {table_name}")
    _execute_non_query(sf_conn, sql)


@log_execution()
def validate_or_create_target_table(sf_conn, table_name: str, schema_dict: dict):
    """
    Ensures table exists; if not, create it using schema_dict.
    """

    if table_exists(sf_conn, table_name):
        logger.info(f"✔ Target table exists: {table_name}")
        return

    logger.warning(f"⚠ Target table NOT found: {table_name}")
    logger.info(f"Creating table {table_name} with schema from dataset YAML")

    create_table(sf_conn, table_name, schema_dict)
    logger.info(f"✔ Table created: {table_name}")

@log_execution()
def insert_audit_record(sf_conn, database: str, schema: str, audit_table_name: str, audit_record: dict):
    """
    Insert audit_record (dict) into DATABASE.SCHEMA.AUDIT_TABLE.
    audit_record keys expected:
      dataset_name, file_name, load_status, row_count, rows_loaded, errors_seen, errors_limit, load_time
    Uses parameterized execution with named parameters for safety.
    """
    fq_name = f"{database}.{schema}.{audit_table_name}"
    insert_sql = f"""
        INSERT INTO {fq_name}
        (DATASET_NAME, FILE_NAME, LOAD_STATUS, ROW_COUNT, ROWS_LOADED, ERRORS_SEEN, ERRORS_LIMIT, LOAD_TIME)
        VALUES (%(dataset_name)s, %(file_name)s, %(load_status)s, %(row_count)s, %(rows_loaded)s, %(errors_seen)s, %(errors_limit)s, %(load_time)s)
    """
    logger.debug(f"Inserting audit record into {fq_name}: {audit_record}")
    cur = sf_conn.cursor()
    try:
        cur.execute(insert_sql, audit_record)
    finally:
        cur.close()
    logger.info(f"Inserted audit record into {fq_name}")


@log_execution()
def ensure_audit_table(sf_conn, database: str, schema: str, audit_table_name: str = "INGESTION_AUDIT_LOG"):
    """
    Create audit table in provided database.schema if not exists.
    """
    fq_name = f"{database}.{schema}.{audit_table_name}"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {fq_name} (
            DATASET_NAME    VARCHAR,
            FILE_NAME       VARCHAR,
            LOAD_STATUS     VARCHAR,
            ROW_COUNT       NUMBER,
            ROWS_LOADED     NUMBER,
            ERRORS_SEEN     NUMBER,
            ERRORS_LIMIT    NUMBER,
            LOAD_TIME       TIMESTAMP_LTZ,
            INSERT_TS       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """
    logger.info(f"Ensuring audit table exists: {fq_name}")
    _execute_non_query(sf_conn, sql)
    logger.info(f"Audit table ready: {fq_name}")
    return fq_name

@log_execution()
def drop_and_infer_create_table(sf_conn, full_table, stage_full, file_format):
    cur = sf_conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {full_table}")
    create_sql = f"""
        CREATE OR REPLACE TABLE {full_table}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '{stage_full}',
                    FILE_FORMAT => '{file_format}'
                )
            )
        );
    """
    cur.execute(create_sql)
