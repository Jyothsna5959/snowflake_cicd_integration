# copy_into_plugin.py  (simple version for initial testing)

def run_copy_into(sf_conn, cfg: dict):
    """
    Lightweight COPY INTO processor.
    Supports:
      - load_strategy: truncate_and_load
      - on_error: abort
      - purge, force options
      - folder-level or file-level stage paths
    """

    target_cfg = cfg['target']
    db = target_cfg["database"]
    schema = target_cfg["schema"]
    table = target_cfg["table"]
    stage_cfg = target_cfg["stage"]
    stage_nm = stage_cfg['name']
    stage_path = stage_cfg['path']
    file_format = stage_cfg["file_format"]
    if target_cfg['ingestion']['method'].lower() == 'copy_into':
        ing_cfg = target_cfg['ingestion']['copy_options'] 
        load_strategy = ing_cfg["load_strategy"]         # truncate_and_load
        purge = target_cfg.get("purge", False)
        force = target_cfg.get("force", False)

        full_table = f"{db}.{schema}.{table}"
        stage_full = f"@{stage_nm}/{stage_path}".rstrip("/")

        if load_strategy == "truncate_and_load":
            print(f"🔄 Truncating table: {full_table}")
            sf_conn.cursor().execute(f"TRUNCATE TABLE {full_table}") 
            purge_clause = "PURGE = TRUE" if purge else "PURGE = FALSE"
            force_clause = "FORCE = TRUE" if force else "FORCE = FALSE"
            copy_sql = f"""
                COPY INTO {full_table}
                FROM {stage_full}
                FILE_FORMAT = (FORMAT_NAME = {file_format})
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'ABORT_STATEMENT'
                {purge_clause}
                {force_clause};
                    """

            print("\n📥 Running COPY INTO command:")
            print(copy_sql)

            try:
                cursor = sf_conn.cursor()
                result = cursor.execute(copy_sql).fetchall()

                print("✅ COPY INTO completed successfully")

                return {
                    "status": "SUCCESS",
                    "results": result
                }

            except Exception as e:
                print("❌ COPY INTO FAILED")
                print(str(e))

                return {
                    "status": "FAILED",
                    "error": str(e)
                }

    