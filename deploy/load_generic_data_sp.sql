CREATE OR REPLACE PROCEDURE LOAD_GENERIC_DATA_PROCEDURE(
    CONFIG_STAGE STRING,
    CONFIG_FILE  STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
PACKAGES=('snowflake-snowpark-python','pyyaml')
HANDLER='main'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
import yaml
import json
from datetime import datetime

# ==============================================================================
#                              COMMON UTILITIES
# ==============================================================================

def escape(v):
    """Escape single quotes for SQL strings."""
    return str(v).replace("'", "''") if v is not None else ""

def read_yaml(session, stage_name, file_name):
    """Load YAML config file from a Snowflake stage."""
    stage_name_clean = stage_name.lstrip('@')
    path = "@" + stage_name_clean + "/" + file_name
    try:
        with session.file.get_stream(path) as f:
            content = f.read().decode("utf-8")
            return yaml.safe_load(content)
    except Exception as e:
        raise Exception("Unable to read YAML at " + path + ": " + str(e))

def table_exists(session, table_name):
    """Check whether a table exists."""
    try:
        session.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False

def get_table_columns(session, table_name):
    """Get column names from an existing table."""
    try:
        rows = session.sql("DESCRIBE TABLE " + table_name).collect()
        return [r['name'] for r in rows]
    except Exception:
        return []

def get_row_count(session, table_name):
    """Safe row count helper."""
    try:
        return session.sql("SELECT COUNT(*) AS CNT FROM " + table_name).collect()[0]['CNT']
    except Exception:
        return 0

def get_next_audit_id(session, audit_table_full):
    """Get next audit ID manually (instead of using AUTOINCREMENT)."""
    try:
        result = session.sql(f"SELECT COALESCE(MAX(AUDIT_ID), 0) + 1 AS NEXT_ID FROM {audit_table_full}").collect()
        return result[0]['NEXT_ID']
    except Exception:
        return 1

def move_invalid_rows_to_error(session, table_full, error_table_full):
    """
    Strict row validation engine (A):
    - For NUMBER / DECIMAL / INT / FLOAT / DOUBLE: invalid if NULL
    - For DATE / TIME / TIMESTAMP / BOOLEAN: invalid if NULL
    - For VARIANT / OBJECT / ARRAY: invalid if NULL
    - For STRING-like types: invalid if NULL OR TRIM(value) = ''
    Any row where ANY column is invalid:
      → inserted into error table
      → deleted from target table
    Returns the number of invalid rows moved.
    """
    try:
        schema_rows = session.sql("DESCRIBE TABLE " + table_full).collect()
    except Exception:
        return 0

    if not schema_rows:
        return 0

    col_names = [r['name'] for r in schema_rows]

    invalid_conds = []
    for r in schema_rows:
        col_raw = r['name']
        col = '"' + col_raw + '"'
        typ = (r['type'] or "").upper()

        if any(x in typ for x in ['NUMBER', 'DECIMAL', 'INT', 'FLOAT', 'DOUBLE']):
            invalid_conds.append(col + " IS NULL")
        elif 'DATE' in typ or 'TIME' in typ or 'TIMESTAMP' in typ or 'BOOLEAN' in typ:
            invalid_conds.append(col + " IS NULL")
        elif 'VARIANT' in typ or 'OBJECT' in typ or 'ARRAY':
            invalid_conds.append(col + " IS NULL")
        else:
            # Treat as string-like: NULL or empty/blank = invalid
            invalid_conds.append("(" + col + " IS NULL OR TRIM(" + col + ") = '')")

    if not invalid_conds:
        return 0

    invalid_where = " OR ".join(invalid_conds)

    # Build RAW_DATA as pipe-separated row for debugging
    base_cols_sql = ", ".join(['"' + c + '"' for c in col_names])
    raw_concat = " || '|' || ".join(
        ["COALESCE(TO_VARCHAR(\"" + c + "\"),'NULL')" for c in col_names]
    )

    # Insert invalid rows into error table
    insert_error_sql = (
        "INSERT INTO " + error_table_full +
        " (" + base_cols_sql + ", SOURCE_FILE_NAME, ERROR_MESSAGE, ERROR_CODE, ERROR_LINE, RAW_DATA) "
        "SELECT " + base_cols_sql +
        ", 'UNKNOWN', 'ROW validation failed', 'ROW_VALIDATION', NULL, "
        "'[' || " + raw_concat + " || ']' "
        "FROM " + table_full +
        " WHERE " + invalid_where
    )
    session.sql(insert_error_sql).collect()

    # Count invalid rows BEFORE deletion
    cnt_res = session.sql(
        "SELECT COUNT(*) AS CNT FROM " + table_full + " WHERE " + invalid_where
    ).collect()
    invalid_cnt = cnt_res[0]['CNT'] if cnt_res else 0

    # Delete invalid rows from target
    delete_sql = "DELETE FROM " + table_full + " WHERE " + invalid_where
    session.sql(delete_sql).collect()

    return invalid_cnt


# ==============================================================================
#                              AUDIT TABLE HELPERS
# ==============================================================================

def ensure_audit_table(session, audit_table_full):
    """Create or recreate audit table without FILE_NAME column and with manual ID management."""
    try:
        # Check if table exists
        if not table_exists(session, audit_table_full):
            # Create new audit table without FILE_NAME and without AUTOINCREMENT
            sql = (
                "CREATE TABLE " + audit_table_full + " ("
                "AUDIT_ID NUMBER, "
                "DATASET_NAME STRING, "
                "FILE_FORMAT STRING, "
                "LOAD_STATUS STRING, "
                "ROW_COUNT NUMBER, "
                "ROWS_LOADED NUMBER, "
                "ERRORS_SEEN NUMBER, "
                "ERRORS_LIMIT NUMBER, "
                "LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), "
                "ERROR_DETAILS STRING, "
                "JOB_ID STRING, "
                "AUDIT_TYPE STRING DEFAULT 'FILE_LEVEL', "
                "PROCESS_DURATION_SECONDS NUMBER, "
                "START_TIME TIMESTAMP_NTZ, "
                "END_TIME TIMESTAMP_NTZ)"
            )
            session.sql(sql).collect()
        else:
            # Check if it has FILE_NAME column
            cols = get_table_columns(session, audit_table_full)
            if 'FILE_NAME' in cols:
                # Create backup, drop, and recreate
                print("Table has FILE_NAME column, recreating...")
                backup_sql = f"CREATE OR REPLACE TABLE {audit_table_full}_BACKUP AS SELECT * FROM {audit_table_full}"
                session.sql(backup_sql).collect()
                session.sql(f"DROP TABLE {audit_table_full}").collect()
                
                # Create new table
                sql = (
                    "CREATE TABLE " + audit_table_full + " ("
                    "AUDIT_ID NUMBER, "
                    "DATASET_NAME STRING, "
                    "FILE_FORMAT STRING, "
                    "LOAD_STATUS STRING, "
                    "ROW_COUNT NUMBER, "
                    "ROWS_LOADED NUMBER, "
                    "ERRORS_SEEN NUMBER, "
                    "ERRORS_LIMIT NUMBER, "
                    "LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), "
                    "ERROR_DETAILS STRING, "
                    "JOB_ID STRING, "
                    "AUDIT_TYPE STRING DEFAULT 'FILE_LEVEL', "
                    "PROCESS_DURATION_SECONDS NUMBER, "
                    "START_TIME TIMESTAMP_NTZ, "
                    "END_TIME TIMESTAMP_NTZ)"
                )
                session.sql(sql).collect()
                
                # Try to restore data (without FILE_NAME column)
                try:
                    restore_sql = f"""
                    INSERT INTO {audit_table_full} 
                    (AUDIT_ID, DATASET_NAME, FILE_FORMAT, LOAD_STATUS, ROW_COUNT, 
                     ROWS_LOADED, ERRORS_SEEN, ERRORS_LIMIT, LOAD_TIME, ERROR_DETAILS, 
                     JOB_ID, AUDIT_TYPE, PROCESS_DURATION_SECONDS, START_TIME, END_TIME)
                    SELECT 
                    AUDIT_ID, DATASET_NAME, FILE_FORMAT, LOAD_STATUS, ROW_COUNT, 
                    ROWS_LOADED, ERRORS_SEEN, ERRORS_LIMIT, LOAD_TIME, ERROR_DETAILS, 
                    JOB_ID, AUDIT_TYPE, PROCESS_DURATION_SECONDS, START_TIME, END_TIME
                    FROM {audit_table_full}_BACKUP
                    """
                    session.sql(restore_sql).collect()
                except Exception as e:
                    print(f"Could not restore backup data: {e}")
                
                # Drop backup
                session.sql(f"DROP TABLE IF EXISTS {audit_table_full}_BACKUP").collect()
        
        return True
    except Exception as e:
        print("Error ensuring audit table: " + str(e))
        # Try to create it anyway
        try:
            sql = (
                "CREATE TABLE IF NOT EXISTS " + audit_table_full + " ("
                "AUDIT_ID NUMBER, "
                "DATASET_NAME STRING, "
                "FILE_FORMAT STRING, "
                "LOAD_STATUS STRING, "
                "ROW_COUNT NUMBER, "
                "ROWS_LOADED NUMBER, "
                "ERRORS_SEEN NUMBER, "
                "ERRORS_LIMIT NUMBER, "
                "LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), "
                "ERROR_DETAILS STRING, "
                "JOB_ID STRING, "
                "AUDIT_TYPE STRING DEFAULT 'FILE_LEVEL', "
                "PROCESS_DURATION_SECONDS NUMBER, "
                "START_TIME TIMESTAMP_NTZ, "
                "END_TIME TIMESTAMP_NTZ)"
            )
            session.sql(sql).collect()
            return True
        except Exception as e2:
            raise Exception("Failed to create audit table: " + str(e2))

def insert_audit(
    session,
    audit_table,
    dataset,
    file_format,
    status,
    row_count,
    rows_loaded,
    errors_seen,
    errors_limit,
    error_details="",
    job_id="",
    audit_type="FILE_LEVEL",
    process_duration=None,
    start_time=None,
    end_time=None
):
    """Insert audit record with manual ID generation."""
    ensure_audit_table(session, audit_table)
    
    # Get next audit ID
    audit_id = get_next_audit_id(session, audit_table)
    
    pdur = "NULL" if process_duration is None else str(process_duration)
    s_time = "NULL" if start_time is None else ("'" + str(start_time) + "'")
    e_time = "NULL" if end_time is None else ("'" + str(end_time) + "'")
    
    sql = (
        "INSERT INTO " + audit_table +
        " (AUDIT_ID, DATASET_NAME, FILE_FORMAT, LOAD_STATUS, ROW_COUNT, ROWS_LOADED, "
        "ERRORS_SEEN, ERRORS_LIMIT, LOAD_TIME, ERROR_DETAILS, JOB_ID, AUDIT_TYPE, "
        "PROCESS_DURATION_SECONDS, START_TIME, END_TIME) VALUES ("
        + str(audit_id) + ","
        "'" + escape(dataset) + "',"
        "'" + escape(file_format) + "',"
        "'" + escape(status) + "',"
        + str(row_count) + ","
        + str(rows_loaded) + ","
        + str(errors_seen) + ","
        + str(errors_limit) + ","
        "CURRENT_TIMESTAMP(),"
        "'" + escape(error_details) + "',"
        "'" + escape(job_id) + "',"
        "'" + escape(audit_type) + "',"
        + pdur + ","
        + s_time + ","
        + e_time +
        ")"
    )
    session.sql(sql).collect()


# ==============================================================================
#                       ERROR TABLE CREATION / INSERTION
# ==============================================================================

def create_error_table_for_dataset(session, error_table_full, base_table=None):
    """Create error table matching target table."""
    if table_exists(session, error_table_full):
        return True
    cols = []
    if base_table and table_exists(session, base_table):
        base_cols = get_table_columns(session, base_table)
        for c in base_cols:
            cols.append('"' + c + '" STRING')
    cols.append("ERROR_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
    cols.append("SOURCE_FILE_NAME STRING")
    cols.append("ERROR_MESSAGE STRING")
    cols.append("ERROR_CODE STRING")
    cols.append("ERROR_LINE NUMBER")
    cols.append("ERROR_COLUMN STRING")
    cols.append("RAW_DATA STRING")
    cols.append("RAW_VARIANT VARIANT")
    sql = "CREATE TABLE IF NOT EXISTS " + error_table_full + " (" + ", ".join(cols) + ")"
    session.sql(sql).collect()
    return True

def insert_error_row_simple(
    session,
    error_table_full,
    source_file,
    error_message,
    error_code="",
    error_line=None,
    error_column=None,
    raw_data=None,
    raw_variant=None,
    base_row=None
):
    """Insert an error row into error table."""
    raw_variant_sql = "NULL"
    if raw_variant is not None:
        try:
            raw_variant_sql = "PARSE_JSON('" + escape(json.dumps(raw_variant)) + "')"
        except Exception:
            raw_variant_sql = "'" + escape(str(raw_variant)) + "'"

    base_cols = []
    base_vals = []
    if base_row:
        for k, v in base_row.items():
            base_cols.append('"' + k + '"')
            base_vals.append("'" + escape(v) + "'")

    insert_cols = base_cols + [
        "SOURCE_FILE_NAME", "ERROR_MESSAGE", "ERROR_CODE",
        "ERROR_LINE", "ERROR_COLUMN", "RAW_DATA", "RAW_VARIANT"
    ]
    insert_vals = base_vals + [
        "'" + escape(source_file) + "'",
        "'" + escape(error_message) + "'",
        "'" + escape(error_code) + "'",
        ("NULL" if error_line is None else str(error_line)),
        ("NULL" if error_column is None else ("'" + escape(error_column) + "'")),
        ("NULL" if raw_data is None else ("'" + escape(raw_data) + "'")),
        raw_variant_sql
    ]

    sql = (
        "INSERT INTO " + error_table_full +
        " (" + ", ".join(insert_cols) + ") VALUES (" +
        ", ".join(insert_vals) + ")"
    )
    session.sql(sql).collect()


# ==============================================================================
#                          CSV TWO-STAGE LOADING
# ==============================================================================

def detect_csv_columns(session, stage_ref, file_format, max_cols=200, sample_rows=5):
    """Find maximum number of columns from CSV rows."""
    try:
        cols = [ "$" + str(i) + " as C" + str(i) for i in range(1, max_cols+1) ]
        sample_sql = (
            "SELECT " + ", ".join(cols) +
            " FROM " + stage_ref +
            " (FILE_FORMAT => '" + file_format + "') LIMIT " + str(sample_rows)
        )
        rows = session.sql(sample_sql).collect()
        if not rows:
            return []
        max_found = 0
        for r in rows:
            d = r.as_dict()
            for i in range(1, max_cols+1):
                if d.get("C" + str(i)) is not None:
                    max_found = max(max_found, i)
        return [ "COL" + str(i) for i in range(1, max_found+1) ]
    except Exception as e:
        print("CSV detect error: " + str(e))
        return []

def csv_two_stage_load(
    session,
    dataset_name,
    table_full,
    stage_ref,
    file_format,
    schema_def,
    error_table_full,
    audit_table,
    job_id,
    copy_cfg
):
    files = session.sql("LIST " + stage_ref).collect()
    if not files:
        raise Exception("No files found in stage: " + stage_ref)

    total_loaded = 0
    total_errors = 0
    total_rows = 0

    for idx, f in enumerate(files):
        fname = f['name']
        fname_short = fname.split('/')[-1]
        start = datetime.now()

        # Detect headerless CSV column count
        csv_cols = detect_csv_columns(session, stage_ref, file_format)
        if not csv_cols:
            csv_cols = ['COL1']
        ncols = len(csv_cols)

        # Create temp staging table
        temp_stg = "TEMP_" + table_full.replace('.','_') + "_STG_" + str(idx)
        stg_cols = ", ".join(
            ['"' + "COL" + str(i+1) + '" STRING' for i in range(ncols)] +
            ['ROW_NUMBER NUMBER']
        )
        session.sql(
            "CREATE OR REPLACE TEMPORARY TABLE " + temp_stg + " (" + stg_cols + ")"
        ).collect()

        # Copy into staging
        select_parts = ", ".join(
            [ "$" + str(i+1) + " as \"COL" + str(i+1) + "\"" for i in range(ncols) ] +
            ["METADATA$FILE_ROW_NUMBER as ROW_NUMBER"]
        )
        copy_sql = (
            "COPY INTO " + temp_stg +
            " FROM ( SELECT " + select_parts +
            " FROM " + stage_ref +
            " (FILE_FORMAT => '" + file_format + "') ) " +
            " PATTERN='.*" + fname_short + "' FORCE=TRUE"
        )
        session.sql(copy_sql).collect()

        row_cnt = session.sql("SELECT COUNT(*) AS CNT FROM " + temp_stg).collect()[0]['CNT']
        total_rows += row_cnt

        if row_cnt == 0:
            insert_audit(
                session, audit_table, dataset_name, file_format,
                "NO_DATA", 0, 0, 0, 0, "No rows found in staging", job_id,
                "FILE_LEVEL", 0, start, datetime.now()
            )
            session.sql("DROP TABLE IF EXISTS " + temp_stg).collect()
            continue

        # Build validation & cast rules
        target_cols = list(schema_def.keys())
        insert_select_parts = []
        validation_conditions = []

        for i, tgt_col in enumerate(target_cols):
            tgt_type = schema_def[tgt_col].upper()
            stg_col = '"COL' + str(i+1) + '"' if i < ncols else "NULL"

            if tgt_type in ['NUMBER','INT','INTEGER']:
                insert_select_parts.append(
                    "TRY_CAST(" + stg_col + " AS NUMBER) as \"" + tgt_col + "\""
                )
                validation_conditions.append("TRY_CAST(" + stg_col + " AS NUMBER) IS NOT NULL")

            elif tgt_type in ['FLOAT','DOUBLE','DECIMAL','REAL']:
                insert_select_parts.append(
                    "TRY_CAST(" + stg_col + " AS FLOAT) as \"" + tgt_col + "\""
                )
                validation_conditions.append("TRY_CAST(" + stg_col + " AS FLOAT) IS NOT NULL")

            elif tgt_type in ['DATE']:
                insert_select_parts.append(
                    "TRY_CAST(" + stg_col + " AS DATE) as \"" + tgt_col + "\""
                )
                validation_conditions.append("TRY_CAST(" + stg_col + " AS DATE) IS NOT NULL")

            elif tgt_type in ['TIMESTAMP','DATETIME','TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ']:
                insert_select_parts.append(
                    "TRY_CAST(" + stg_col + " AS TIMESTAMP_NTZ) as \"" + tgt_col + "\""
                )
                validation_conditions.append("TRY_CAST(" + stg_col + " AS TIMESTAMP_NTZ) IS NOT NULL")

            else:
                # STRICT: string invalid if NULL or blank
                insert_select_parts.append(stg_col + " as \"" + tgt_col + "\"")
                validation_conditions.append("(" + stg_col + " IS NOT NULL AND TRIM(" + stg_col + ") <> '')")

        # Insert valid rows
        insert_sql = (
            "INSERT INTO " + table_full +
            " SELECT " + ", ".join(insert_select_parts) +
            " FROM " + temp_stg +
            " WHERE " + " AND ".join(validation_conditions)
        )
        session.sql(insert_sql).collect()

        valid_cnt = session.sql(
            "SELECT COUNT(*) AS CNT FROM " + temp_stg +
            " WHERE " + " AND ".join(validation_conditions)
        ).collect()[0]['CNT']
        invalid_cnt = row_cnt - valid_cnt

        # Insert error rows
        if invalid_cnt > 0:
            raw_parts = [ "COALESCE(\"COL" + str(i+1) + "\",'NULL')" for i in range(ncols) ]
            raw_concat = " || '|' || ".join(raw_parts)
            base_cols_sql = ", ".join(['"' + c + '"' for c in target_cols])

            insert_error_sql = (
                "INSERT INTO " + error_table_full + " (" +
                base_cols_sql + ", SOURCE_FILE_NAME, ERROR_MESSAGE, ERROR_CODE, ERROR_LINE, RAW_DATA) " +
                "SELECT " +
                ", ".join(
                    ["COALESCE(\"COL" + str(i+1) + "\",'NULL')" for i in range(len(target_cols))]
                ) + ", 'UNKNOWN', 'CSV validation failed', 'CSV_VALIDATION', ROW_NUMBER, " +
                "'[' || " + raw_concat + " || ']' " +
                "FROM " + temp_stg + " WHERE NOT (" + " AND ".join(validation_conditions) + ")"
            )
            session.sql(insert_error_sql).collect()

        # Apply strict validation to ensure no invalid rows remain
        invalid_after_validation = move_invalid_rows_to_error(session, table_full, error_table_full)
        if invalid_after_validation > 0:
            invalid_cnt += invalid_after_validation
            valid_cnt -= invalid_after_validation

        # Finalize audit
        end = datetime.now()
        rows_loaded = valid_cnt
        rows_error = invalid_cnt

        total_loaded += rows_loaded
        total_errors += rows_error

        status = (
            "LOADED" if rows_loaded > 0 and rows_error == 0 else
            ("PARTIAL" if rows_loaded > 0 and rows_error > 0 else "FAILED")
        )
        insert_audit(
            session, audit_table, dataset_name, file_format,
            status, row_cnt, rows_loaded, rows_error, 0,
            "CSV loaded " + str(rows_loaded) + ", errors " + str(rows_error),
            job_id, "FILE_LEVEL", (end - start).total_seconds(), start, end
        )

        session.sql("DROP TABLE IF EXISTS " + temp_stg).collect()

    return total_loaded, total_errors, total_rows


# ==============================================================================
#                          PARQUET STRICT COPY
# ==============================================================================

def infer_parquet_schema(session, stage_ref, file_format):
    try:
        sql = (
            "SELECT COLUMN_NAME, TYPE FROM TABLE("
            "INFER_SCHEMA(LOCATION => '" + stage_ref +
            "', FILE_FORMAT => '" + file_format + "')) ORDER BY ORDER_ID"
        )
        rows = session.sql(sql).collect()
        if not rows:
            return {}
        schema = {}
        # preserve inferred order (Option 1)
        for r in rows:
            col = r['COLUMN_NAME']
            typ = r['TYPE']
            if typ in ('VARIANT','OBJECT','ARRAY'):
                final = "VARIANT"
            elif 'TIMESTAMP' in typ or 'DATE' in typ:
                final = typ
            elif 'NUMBER' in typ or 'DECIMAL' in typ or 'INT' in typ:
                final = 'NUMBER'
            elif 'FLOAT' in typ or 'DOUBLE' in typ:
                final = 'FLOAT'
            else:
                final = 'STRING'
            schema[col] = final
        return schema
    except Exception as e:
        print("Parquet infer failed: " + str(e))
        return {}

def parquet_simple_copy(
    session,
    dataset_name,
    table_full,
    stage_ref,
    file_format,
    error_table_full,
    audit_table,
    job_id,
    copy_cfg,
    schema_definition=None
):
    pattern = copy_cfg.get("pattern", ".*.parquet")
    pattern_clause = ("PATTERN='" + pattern + "'") if copy_cfg.get("pattern") else ""
    on_error = copy_cfg.get("on_error", "CONTINUE")
    force = str(copy_cfg.get("force", False)).upper()
    purge = str(copy_cfg.get("purge", False)).upper()

    # BEFORE COPY: remember previous row counts
    prev_tgt_rows = get_row_count(session, table_full)
    prev_err_rows = get_row_count(session, error_table_full)

    # If table exists with only SRC column, infer schema and recreate
    if table_exists(session, table_full):
        cols = get_table_columns(session, table_full)
        if len(cols) == 1 and cols[0].upper() == "SRC":
            inferred = infer_parquet_schema(session, stage_ref, file_format)
            if inferred:
                session.sql("DROP TABLE IF EXISTS " + table_full).collect()
                col_defs = [ ('"' + c + '" ' + t) for c, t in inferred.items() ]
                session.sql(
                    "CREATE TABLE " + table_full + " (" + ", ".join(col_defs) + ")"
                ).collect()

    copy_sql = (
        "COPY INTO " + table_full +
        " FROM " + stage_ref +
        " FILE_FORMAT = (FORMAT_NAME='" + file_format + "') " +
        (pattern_clause + " " if pattern_clause else "") +
        " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" +
        " ON_ERROR='" + on_error + "'" +
        " FORCE=" + force +
        " PURGE=" + purge
    )

    results = session.sql(copy_sql).collect()
    files_processed = 0

    # Log raw COPY results file-by-file into audit (as before)
    for r in results:
        d = r.as_dict()
        rows_loaded = d.get('rows_loaded') or 0
        rows_parsed = d.get('rows_parsed') or 0
        errors_seen = d.get('errors_seen') or 0
        first_error = d.get('first_error') or None
        first_error_line = d.get('first_error_line') or None
        first_error_column = d.get('first_error_column_name') or None

        files_processed += 1

        if errors_seen > 0:
            emsg = "Parquet COPY error: " + str(first_error)
            insert_error_row_simple(
                session,
                error_table_full,
                'UNKNOWN',
                emsg,
                "PARQUET_COPY_ERROR",
                first_error_line,
                first_error_column,
                raw_data=None,
                raw_variant={
                    'rows_parsed': rows_parsed,
                    'rows_loaded': rows_loaded,
                    'errors_seen': errors_seen
                }
            )

        status = (
            "LOADED" if errors_seen == 0 and rows_loaded > 0 else
            ("PARTIAL" if rows_loaded > 0 else "FAILED")
        )

        insert_audit(
            session, audit_table, dataset_name, file_format,
            status, rows_parsed, rows_loaded, errors_seen,
            0, first_error or "", job_id, "FILE_LEVEL",
            None, None, None
        )

    # STRICT VALIDATION AFTER COPY:
    parquet_invalid_cnt = move_invalid_rows_to_error(session, table_full, error_table_full)

    # Recompute actual loaded and error counts for this run
    new_tgt_rows = get_row_count(session, table_full)
    new_err_rows = get_row_count(session, error_table_full)

    # Total loaded = rows that survived validation
    total_loaded = new_tgt_rows - prev_tgt_rows
    # Total errors = error table delta (COPY + validation errors)
    total_errors = (new_err_rows - prev_err_rows)

    return total_loaded, total_errors, files_processed


# ==============================================================================
#                    JSON (STRUCTURED LOAD WITH PATH SUPPORT)
# ==============================================================================

def infer_json_schema(session, stage_ref, file_format):
    try:
        sql = (
            "SELECT COLUMN_NAME, TYPE FROM TABLE("
            "INFER_SCHEMA(LOCATION => '" + stage_ref +
            "', FILE_FORMAT => '" + file_format + "')) ORDER BY ORDER_ID"
        )
        rows = session.sql(sql).collect()
        if not rows:
            return {"RAW_JSON": "VARIANT"}
        schema = {}
        # preserve inferred order (Option 1)
        for r in rows:
            col = r['COLUMN_NAME']
            typ = r['TYPE']
            if typ in ('VARIANT','OBJECT','ARRAY'):
                final = "VARIANT"
            elif 'TIMESTAMP' in typ or 'DATE' in typ:
                final = typ
            elif 'NUMBER' in typ or 'DECIMAL' in typ or 'INT' in typ:
                final = 'NUMBER'
            elif 'FLOAT' in typ or 'DOUBLE' in typ:
                final = 'FLOAT'
            else:
                final = 'STRING'
            schema[col] = final
        return schema
    except Exception as e:
        print("JSON infer failed: " + str(e))
        return {"RAW_JSON": "VARIANT"}

def build_json_column_mappings(schema_columns, json_paths=None):
    """
    schema_columns: dict of COL_NAME -> TYPE (UPPER)
    json_paths (optional): dict of COL_NAME -> json path string, e.g. $.customer.id
                           provided from YAML: datasets[].json_paths
    """
    mappings = []
    json_paths_upper = {k.upper(): v for k, v in (json_paths or {}).items()}

    for col_name, col_type in schema_columns.items():
        col_upper = col_name.upper()
        override_path = json_paths_upper.get(col_upper)

        if override_path:
            # Explicit path from YAML
            base_expr = (
                "GET_PATH(PARSE_JSON($1), '" + override_path.replace("'", "''") + "')"
            )
        else:
            # Try a couple of simple variants: exact and lower-case
            base_expr = (
                "COALESCE("
                "GET_PATH(PARSE_JSON($1), '" + col_name.replace("'", "''") + "'),"
                "GET_PATH(PARSE_JSON($1), '" + col_name.lower().replace("'", "''") + "')"
                ")"
            )

        vexpr = "(" + base_expr + ")"

        if col_type == "VARIANT":
            mappings.append(vexpr + " as \"" + col_name + "\"")
        elif 'TIMESTAMP' in col_type:
            mappings.append(
                "TRY_TO_TIMESTAMP_NTZ(" + vexpr + "::STRING) as \"" + col_name + "\""
            )
        elif 'DATE' in col_type:
            mappings.append(
                "TRY_TO_DATE(" + vexpr + "::STRING) as \"" + col_name + "\""
            )
        elif col_type in [
            'NUMBER','DECIMAL','INT','FLOAT','DOUBLE','INTEGER','BIGINT'
        ]:
            mappings.append(
                "TRY_TO_NUMBER(" + vexpr + "::STRING) as \"" + col_name + "\""
            )
        else:
            mappings.append(
                vexpr + "::STRING as \"" + col_name + "\""
            )

    return mappings

def json_structured_load(
    session,
    dataset_name,
    table_full,
    stage_ref,
    file_format,
    schema_def,
    error_table_full,
    audit_table,
    job_id,
    copy_cfg,
    load_strategy,
    json_paths=None
):
    # Determine schema columns (YAML > inferred)
    if schema_def:
        schema_cols = { k: str(v).upper() for k, v in schema_def.items() }
    else:
        schema_cols = infer_json_schema(session, stage_ref, file_format)

    # If legacy table exists with only SRC and no schema_def, convert to structured
    if table_exists(session, table_full):
        cols = get_table_columns(session, table_full)
        if len(cols) == 1 and cols[0].upper() == "SRC" and not schema_def:
            session.sql("DROP TABLE IF EXISTS " + table_full).collect()
            col_defs = [ ('"' + c + '" ' + t) for c, t in schema_cols.items() ]
            session.sql(
                "CREATE TABLE " + table_full + " (" + ", ".join(col_defs) + ")"
            ).collect()

    ls = load_strategy.upper()

    if ls == "DROP_AND_CREATE":
        session.sql("DROP TABLE IF EXISTS " + table_full).collect()
        col_defs = [ ('"' + c + '" ' + t) for c, t in schema_cols.items() ]
        session.sql(
            "CREATE TABLE " + table_full + " (" + ", ".join(col_defs) + ")"
        ).collect()
    elif ls == "TRUNCATE_AND_LOAD":
        if not table_exists(session, table_full):
            raise Exception("TRUNCATE_AND_LOAD requires table to exist: " + table_full)
        session.sql("TRUNCATE TABLE " + table_full).collect()
    elif ls == "APPEND":
        if not table_exists(session, table_full):
            raise Exception("APPEND requires table to exist: " + table_full)
    else:
        raise Exception("Unsupported load strategy for JSON: " + load_strategy)

    # BEFORE COPY: remember previous row counts
    prev_tgt_rows = get_row_count(session, table_full)
    prev_err_rows = get_row_count(session, error_table_full)

    column_mappings = build_json_column_mappings(schema_cols, json_paths)
    select_clause = "SELECT " + ", ".join(column_mappings) + " FROM " + stage_ref

    pattern = copy_cfg.get("pattern", ".*.json")
    pattern_clause = ("PATTERN='" + pattern + "'") if copy_cfg.get("pattern") else ""
    on_error = copy_cfg.get("on_error", "CONTINUE")
    force = str(copy_cfg.get("force", False)).upper()
    purge = str(copy_cfg.get("purge", False)).upper()

    # Important: STRIP_OUTER_ARRAY=TRUE to handle JSON arrays like [ {..}, {..} ]
    copy_sql = (
        "COPY INTO " + table_full +
        " FROM ( " + select_clause + " ) " +
        " FILE_FORMAT = (FORMAT_NAME='" + file_format + "', STRIP_OUTER_ARRAY=TRUE) " +
        (pattern_clause + " " if pattern_clause else "") +
        " ON_ERROR='" + on_error + "'" +
        " FORCE=" + force +
        " PURGE=" + purge
    )

    results = session.sql(copy_sql).collect()

    files_processed = 0

    for r in results:
        d = r.as_dict()
        rows_loaded = d.get('rows_loaded') or 0
        rows_parsed = d.get('rows_parsed') or 0
        errors_seen = d.get('errors_seen') or 0
        first_error = d.get('first_error') or None
        first_error_line = d.get('first_error_line') or None
        first_error_column = d.get('first_error_column_name') or None

        files_processed += 1

        if errors_seen > 0:
            emsg = "JSON COPY error: " + str(first_error)
            insert_error_row_simple(
                session,
                error_table_full,
                'UNKNOWN',
                emsg,
                "JSON_COPY_ERROR",
                first_error_line,
                first_error_column,
                raw_data=(None if first_error is None else first_error),
                raw_variant={
                    'rows_parsed': rows_parsed,
                    'rows_loaded': rows_loaded,
                    'errors_seen': errors_seen
                }
            )

        status = (
            "LOADED" if errors_seen == 0 and rows_loaded > 0 else
            ("PARTIAL" if rows_loaded > 0 else "FAILED")
        )

        insert_audit(
            session,
            audit_table,
            dataset_name,
            file_format,
            status, rows_parsed, rows_loaded, errors_seen,
            0, first_error or "", job_id, "FILE_LEVEL",
            None, None, None
        )

    # STRICT VALIDATION AFTER COPY:
    json_invalid_cnt = move_invalid_rows_to_error(session, table_full, error_table_full)

    # Recompute actual loaded and error counts for this run
    new_tgt_rows = get_row_count(session, table_full)
    new_err_rows = get_row_count(session, error_table_full)

    total_loaded = max(new_tgt_rows - prev_tgt_rows, 0)
    total_errors = max(new_err_rows - prev_err_rows, 0)

    return total_loaded, total_errors, files_processed


# ==============================================================================
#                               ARCHIVE SUPPORT
# ==============================================================================

def archive_stage_files(session, src_stage_ref, archive_stage_ref, pattern=None, remove=True):
    """
    Move files from source stage to archive stage using COPY FILES + optional REMOVE.
    Returns number of files archived.
    """
    copy_sql = f"COPY FILES INTO {archive_stage_ref} FROM {src_stage_ref}"
    if pattern:
        copy_sql += " PATTERN='" + escape(pattern) + "'"
    result = session.sql(copy_sql).collect()

    archived_files = len(result)

    if remove:
        rm_sql = f"REMOVE {src_stage_ref}"
        if pattern:
            rm_sql += " PATTERN='" + escape(pattern) + "'"
        session.sql(rm_sql).collect()

    return archived_files


# ==============================================================================
#                              MAIN ENTRYPOINT
# ==============================================================================

def main(session: Session, CONFIG_STAGE: str, CONFIG_FILE: str):
    try:
        cfg = read_yaml(session, CONFIG_STAGE, CONFIG_FILE)
        datasets = cfg.get("datasets", [])
        if not datasets:
            return "No datasets found in YAML configuration"

        audit_cfg = cfg.get("audit", {})
        audit_schema = audit_cfg.get("schema", "DRAI_ING_SCHEMA")
        audit_table_name = audit_cfg.get("table", "INGESTION_AUDIT_LOG")
        results = []
        failed = False

        try:
            job_id = str(session.sql("SELECT CURRENT_SESSION()").collect()[0][0])
        except Exception:
            job_id = "UNKNOWN"

        audit_db = audit_cfg.get('database')
        if audit_db:
            audit_table_full = audit_db + "." + audit_schema + "." + audit_table_name
        else:
            first_db = datasets[0]['target']['database']
            audit_table_full = first_db + "." + audit_schema + "." + audit_table_name

        # Ensure audit schema exists
        try:
            parts = audit_table_full.split('.')
            if len(parts) >= 2:
                session.sql(
                    "CREATE SCHEMA IF NOT EXISTS " + parts[0] + "." + parts[1]
                ).collect()
        except Exception:
            pass

        ensure_audit_table(session, audit_table_full)

        # Process each dataset
        for ds in datasets:
            ds_start = datetime.now()
            try:
                dataset_name = ds.get("name")

                tgt = ds.get("target")
                db = tgt['database']
                sch = tgt['schema']
                tbl = tgt['table']
                table_full = db + "." + sch + "." + tbl

                stage_cfg = tgt['stage']
                stage_name = stage_cfg['name']
                stage_path = stage_cfg.get('path', '')
                file_format = stage_cfg['file_format']

                if stage_path and stage_path != "/":
                    stage_ref = "@" + stage_name + "/" + stage_path
                else:
                    stage_ref = "@" + stage_name

                copy_cfg = ds.get('ingestion', {}).get('copy_options', {})
                load_strategy = copy_cfg.get(
                    'load_strategy',
                    ds.get('load_strategy', 'DROP_AND_CREATE')
                )

                schema_definition = tgt.get('schema_definition', None)
                if schema_definition:
                    schema_definition = {
                        k.upper(): v.upper() for k, v in schema_definition.items()
                    }

                # Optional JSON paths from YAML (dataset-level)
                json_paths = ds.get('json_paths', None)

                # === ARCHIVE CONFIG FROM YAML (APPLIES TO ALL FORMATS) ===
                archive_cfg = ds.get("archive", {}) or {}
                archive_enabled = bool(archive_cfg.get("enabled", False))
                archive_stage_name = archive_cfg.get("stage")
                archive_path = (archive_cfg.get("path", "") or "").strip()
                archive_pattern = archive_cfg.get("pattern")
                archive_remove = bool(archive_cfg.get("remove_from_source", True))

                archive_stage_ref = None
                if archive_enabled and archive_stage_name:
                    archive_stage_ref = "@" + archive_stage_name
                    if archive_path and archive_path != "/":
                        archive_stage_ref = archive_stage_ref + "/" + archive_path

                ff = file_format.lower()

                # Ensure target schema exists
                try:
                    session.sql("CREATE SCHEMA IF NOT EXISTS " + db + "." + sch).collect()
                except Exception:
                    pass

                error_table_full = db + "." + sch + "." + tbl + "_ERRORS"

                # Handle load strategy at table-level
                if load_strategy.upper() == "DROP_AND_CREATE":
                    session.sql("DROP TABLE IF EXISTS " + table_full).collect()
                    if schema_definition:
                        col_defs = []
                        for c, t in schema_definition.items():
                            tu = str(t).upper()
                            if tu in ['NUMBER','INT','INTEGER']:
                                col_type = 'NUMBER'
                            elif tu in ['FLOAT','DOUBLE','DECIMAL','REAL']:
                                col_type = 'FLOAT'
                            elif tu in ['DATE']:
                                col_type = 'DATE'
                            elif 'TIMESTAMP' in tu or 'DATETIME' in tu:
                                col_type = 'TIMESTAMP_NTZ'
                            else:
                                col_type = 'STRING'
                            col_defs.append('"' + c + '" ' + col_type)
                        session.sql(
                            "CREATE TABLE " + table_full + " (" + ", ".join(col_defs) + ")"
                        ).collect()
                    else:
                        # For JSON & CSV without schema, table will be created in the specific loader.
                        if ('json' in ff) or ('ndjson' in ff) or ('csv' in ff):
                            pass
                        else:
                            session.sql(
                                "CREATE TABLE " + table_full + " (SRC VARIANT)"
                            ).collect()
                elif load_strategy.upper() == "TRUNCATE_AND_LOAD":
                    if not table_exists(session, table_full):
                        raise Exception(
                            "TRUNCATE_AND_LOAD requires table to exist: " + table_full
                        )
                    session.sql("TRUNCATE TABLE " + table_full).collect()
                elif load_strategy.upper() == "APPEND":
                    if not table_exists(session, table_full):
                        raise Exception(
                            "APPEND requires table to exist: " + table_full
                        )
                else:
                    raise Exception("Unsupported load strategy: " + load_strategy)

                # Error table creation
                create_error_table_for_dataset(
                    session,
                    error_table_full,
                    base_table=(table_full if table_exists(session, table_full) else None)
                )

                # ============================================================
                # INGEST DATA BY FILE FORMAT
                # ============================================================

                if 'csv' in ff:
                    if not schema_definition:
                        if table_exists(session, table_full):
                            existing_cols = get_table_columns(session, table_full)
                            if len(existing_cols) == 1 and existing_cols[0].upper() == 'SRC':
                                raise Exception(
                                    "CSV requires schema_definition in YAML or a target "
                                    "table with proper columns. Dataset: " +
                                    str(dataset_name)
                                )
                            schema_definition = {
                                c.upper(): 'STRING' for c in existing_cols
                            }
                        else:
                            raise Exception(
                                "CSV requires schema_definition in YAML or an existing "
                                "target table. Dataset: " + str(dataset_name)
                            )

                    loaded, errors, total = csv_two_stage_load(
                        session,
                        dataset_name,
                        table_full,
                        stage_ref,
                        file_format,
                        schema_definition,
                        error_table_full,
                        audit_table_full,
                        job_id,
                        copy_cfg
                    )
                    results.append(
                        str(dataset_name) + ": CSV loaded " + str(loaded) +
                        " rows, " + str(errors) + " errors, total rows " + str(total)
                    )

                elif 'parquet' in ff:
                    loaded, errors, files_proc = parquet_simple_copy(
                        session,
                        dataset_name,
                        table_full,
                        stage_ref,
                        file_format,
                        error_table_full,
                        audit_table_full,
                        job_id,
                        copy_cfg,
                        schema_definition
                    )
                    results.append(
                        str(dataset_name) + ": Parquet loaded " + str(loaded) +
                        " rows from " + str(files_proc) +
                        " files with " + str(errors) + " errors"
                    )

                elif 'json' in ff or 'ndjson' in ff:
                    loaded, errors, files_proc = json_structured_load(
                        session,
                        dataset_name,
                        table_full,
                        stage_ref,
                        file_format,
                        schema_definition,
                        error_table_full,
                        audit_table_full,
                        job_id,
                        copy_cfg,
                        load_strategy,
                        json_paths
                    )
                    results.append(
                        str(dataset_name) + ": JSON loaded " + str(loaded) +
                        " rows from " + str(files_proc) +
                        " files with " + str(errors) + " errors"
                    )
                else:
                    raise Exception(
                        "Unsupported/unknown file format '" + file_format +
                        "' for dataset " + str(dataset_name)
                    )

                # ============================================================
                # ARCHIVE FILES (COUNT + DELAYED AUDIT)
                # ============================================================

                archived_file_count = 0
                archive_error = None

                if archive_enabled and archive_stage_ref:
                    try:
                        archived_file_count = archive_stage_files(
                            session,
                            stage_ref,
                            archive_stage_ref,
                            pattern=archive_pattern,
                            remove=archive_remove
                        )
                    except Exception as arch_ex:
                        archive_error = str(arch_ex)

                # ============================================================
                # DATASET-LEVEL SUMMARY AUDIT
                # ============================================================

                ds_end = datetime.now()
                ds_duration = (ds_end - ds_start).total_seconds()

                # Get FINAL counts after all processing
                tgt_rows = get_row_count(session, table_full)
                err_rows = get_row_count(session, error_table_full)

                # Get total rows processed (loaded + errors)
                total_rows_processed = tgt_rows + err_rows

                ds_status = (
                    "LOADED" if tgt_rows > 0 and err_rows == 0 else
                    ("PARTIAL" if tgt_rows > 0 and err_rows > 0 else
                     ("FAILED" if err_rows > 0 else "NO_DATA"))
                )

                insert_audit(
                    session,
                    audit_table_full,
                    dataset_name,
                    file_format,
                    ds_status,
                    total_rows_processed,  # Total rows processed
                    tgt_rows,              # Successfully loaded rows
                    err_rows,              # Total errors
                    0,
                    "Dataset " + str(dataset_name) + " summary. Target: " + str(tgt_rows) + " rows, Errors: " + str(err_rows) + " rows",
                    job_id,
                    "DATASET_SUMMARY",
                    ds_duration,
                    ds_start,
                    ds_end
                )

                # ============================================================
                # ARCHIVE AUDIT AFTER DATASET_SUMMARY
                # ============================================================

                if archive_enabled and archive_stage_ref:
                    if archive_error is not None:
                        # Archive failed
                        insert_audit(
                            session,
                            audit_table_full,
                            dataset_name,
                            file_format,
                            "ARCHIVE_FAILED",
                            0,
                            0,
                            0,
                            0,
                            archive_error,
                            job_id,
                            "ARCHIVE",
                            None,
                            None,
                            None
                        )
                    else:
                        # Option A: always log, even if count = 0
                        if archived_file_count > 0:
                            details = (
                                f"{archived_file_count} file(s) moved from "
                                f"{stage_ref} to {archive_stage_ref}"
                            )
                            insert_audit(
                                session,
                                audit_table_full,
                                dataset_name,
                                file_format,
                                "ARCHIVED",
                                archived_file_count,  # row_count = number of files archived
                                0,
                                0,
                                0,
                                details,
                                job_id,
                                "ARCHIVE",
                                None,
                                None,
                                None
                            )
                        else:
                            insert_audit(
                                session,
                                audit_table_full,
                                dataset_name,
                                file_format,
                                "ARCHIVE_SKIPPED",
                                0,
                                0,
                                0,
                                0,
                                "No files archived (no matching files found)",
                                job_id,
                                "ARCHIVE",
                                None,
                                None,
                                None
                            )

            except Exception as dex:
                failed = True
                results.append(
                    "FAILED: " + str(ds.get('name', 'unknown')) + " -> " + str(dex)
                )
                try:
                    insert_audit(
                        session,
                        audit_table_full,
                        str(ds.get('name', 'unknown')),
                        (
                            tgt.get('stage', {}).get('file_format', 'UNKNOWN')
                            if 'tgt' in locals() else 'UNKNOWN'
                        ),
                        "FAILED",
                        0,
                        0,
                        0,
                        0,
                        str(dex),
                        job_id,
                        "DATASET_SUMMARY",
                        (datetime.now() - ds_start).total_seconds(),
                        ds_start,
                        datetime.now()
                    )
                except Exception as ae:
                    print("Failed to insert failure audit: " + str(ae))

        if failed:
            return "INGESTION COMPLETED WITH FAILURES:\n" + "\n".join(results)
        else:
            return "INGESTION COMPLETED SUCCESSFULLY:\n" + "\n".join(results)

    except Exception as e:
        return "PROCEDURE FAILED: " + str(e)
$$;



-- CALL LOAD_GENERIC_DATA_PROCEDURE('DRAI_INTERNAL_CONFIG_STAGE', 'sales_data_stg_schema.yaml');

CALL LOAD_GENERIC_DATA_PROCEDURE('DRAI_INTERNAL_CONFIG_STAGE', 'product_data_stg_schema.yaml');

-- CALL LOAD_GENERIC_DATA_PROCEDURE('DRAI_INTERNAL_CONFIG_STAGE', 'customer_details_stg_schema_json.yaml');    

-- CALL LOAD_GENERIC_DATA_PROCEDURE('DRAI_INTERNAL_CONFIG_STAGE', 'product_details_stg_schema_parquet.yaml');

-- select * from product_details;

-- select * from product_details_errors;

-- select * from sales_data;









