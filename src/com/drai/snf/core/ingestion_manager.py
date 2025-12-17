# src/com/dataready_ai/snf/core/ingestion_manager.py
import datetime
from com.drai.snf.core.jobutils.connector_factory import get_connector
from com.drai.snf.core.jobutils.copy_into_plugin import run_copy_into
from com.drai.snf.core.utils.logger_utils import get_logger
from com.drai.snf.core.utils import snowflakeutils as sf_utils
from com.drai.snf.core.utils import ingestion_audit_utils as inj_utils
from com.drai.snf.core.jobutils.load_strategies.truncate_and_load import TruncateAndLoadStrategy
from com.drai.snf.core.jobutils.load_strategies.drop_and_create import DropAndCreateStrategy



class IngestionManager:
    """
    Self-contained ingestion manager: handles source/target connectors internally.
    """

    def __init__(self, config: dict, logger,env):

        self.config = config
        self.logger = logger or get_logger("ingestion_manager", "logs/ingestion_manager.log")
        self.load_strategy_registry = {
            "truncate_and_load": TruncateAndLoadStrategy,
            "drop_and_create": DropAndCreateStrategy,
            #"select_matched": SelectMatchedStrategy,
            # "default_missing": DefaultMissingStrategy,
        }
        self._ingest_dataset(logger)


    def _ingest_dataset(self,logger):
        # Loop through all datasets in the YAML
        
        for dataset in self.config.get('datasets', []):
            dataset_name = dataset['name']
            source_type = dataset['source']['type']
            stage_nm = dataset['target']['stage']['name']
            database_nm = dataset['target']['database']
            schema_nm  = dataset['target']['schema']
            table_nm = dataset['target']['table']
            load_mode = dataset['target']['ingestion']['copy_options']['load_strategy']
            audit_table  = self.config.get("audit", {}).get("table", "INGESTION_AUDIT_LOG")
            
            # Create source and target connector objects
            source_connector = get_connector(dataset['source']['type'], dataset['source'], logger=self.logger)
            target_connector = get_connector(dataset['target']['type'], dataset['target'], logger=self.logger)

            # Get connections
            source_conn = source_connector.get_connection()
            target_conn = target_connector.get_connection()
            logger.info("Connectors establishment is done....")

            if sf_utils.validate_stage(target_conn,stage_nm):
                logger.info(f"Provided stage is  exists in snowflake..{stage_nm}")
            if sf_utils.table_exists(target_conn,table_nm):
                logger.info(f"Provided table does exist in {database_nm}.{schema_nm}.{table_nm}")
            strategy_class = self.load_strategy_registry[load_mode]
            logger.info(f"load Strategy class is: {strategy_class} ")
            strategy = strategy_class(target_conn,
                dataset,
                logger
            )
            copy_results = strategy .execute()
            print("copy results:",copy_results)
            audit_info = inj_utils.parse_copy_into_result(copy_results)
            print("audit entry is:",audit_info)
            sf_utils.ensure_audit_table(target_conn, database_nm, schema_nm, audit_table)
            if audit_info:
                logger.info(f"Logging the audit entry for : {dataset_name}")
                audit_payload = {
                    "dataset_name": dataset_name,
                    "file_name": audit_info.get("file_name"),
                    "load_status": audit_info.get("load_status"),
                    "row_count": audit_info.get("row_count"),
                    "rows_loaded": audit_info.get("rows_loaded"),
                    "errors_seen": audit_info.get("errors_seen"),
                    "errors_limit": audit_info.get("errors_limit"),
                    "load_time": audit_info.get("load_date") or datetime.datetime.utcnow(),
                }

                sf_utils.insert_audit_record(
                    target_conn,
                    database_nm,
                    schema_nm,
                    audit_table,
                    audit_payload
                )

                logger.info(f"Audit record inserted for dataset: {dataset_name}")

            else:
                logger.warning("No audit data parsed — inserting fallback audit row.")

                sf_utils.insert_audit_record(
                    target_conn,
                    database_nm,
                    schema_nm,
                    audit_table,
                    {
                        "dataset_name": dataset_name,
                        "file_name": None,
                        "load_status": copy_results.get("status", "FAILED"),
                        "row_count": 0,
                        "rows_loaded": 0,
                        "errors_seen": None,
                        "errors_limit": None,
                        "load_time": datetime.datetime.utcnow(),
                    }
                )

            

        print("All dataset logging tested successfully.")

        

        