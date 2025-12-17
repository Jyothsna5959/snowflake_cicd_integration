from com.drai.snf.core.utils import snowflakeutils as sf_utils

class DropAndCreateStrategy:

    def __init__(self, conn,conf,logger):
        self.logger = logger
        self.sf_conn = conn
        self.cfg = conf
        self.execute()

    def execute(self):
        self.logger.info("Drop  and create table load process has started ..!!")
        target_cfg = self.cfg['target']
        db = target_cfg["database"]
        schema = target_cfg["schema"]
        table = target_cfg["table"]
        stage_cfg = target_cfg["stage"]
        stage_nm = stage_cfg['name']
        stage_path = stage_cfg['path']
        file_format = stage_cfg["file_format"]
        if target_cfg['ingestion']['method'].lower() == 'copy_into':
            ing_cfg = target_cfg['ingestion']['copy_options'] 
            load_mode = ing_cfg["load_mode"]        
            purge = ing_cfg.get("purge", False)
            force = ing_cfg.get("force", False)

            full_table = f"{db}.{schema}.{table}"
            stage_full = f"@{stage_nm}/{stage_path}".rstrip("/")
            print("full table is:",full_table)
            print("stage full is:",stage_full)
            sf_utils.drop_and_infer_create_table(self.sf_conn,full_table,stage_full,file_format)
            purge_clause = "PURGE = TRUE" if purge else "PURGE = FALSE"
            force_clause = "FORCE = TRUE" if force else "FORCE = FALSE"
            copy_sql = f"""
                    COPY INTO {full_table}
                        FROM {stage_full}
                        FILE_FORMAT = (FORMAT_NAME = {file_format})
                        ON_ERROR = 'ABORT_STATEMENT'
                        {purge_clause}
                        {force_clause};
                    """

            self.logger.info("\n📥 Running COPY INTO command:")
            self.logger.info(copy_sql)

            try:
                cursor = self.sf_conn.cursor()
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

