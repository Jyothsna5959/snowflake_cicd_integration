import os
import sys

# Add src folder to sys.path
#$env:PYTHONPATH="C:\Users\krish\TestPerform\snowflake-ingestion\src"
# to check the PYTHON PATH : echo $env:PYTHONPATH
#$env:SNOWFLAKE_PASSWORD = "YourStrongPassword"
from com.drai.snf.core.utils.config import ConfigLoader
from com.drai.snf.core.utils.logger_utils import get_logger
from com.drai.snf.core.ingestion_manager import IngestionManager

def main(env: str ,config_path: str):
    try:
        # Load the job YAML + environment connection YAMLs
        config = ConfigLoader(config_path).load_config()

        # Global log path
        log_dir = config['logging']['log_path']
        os.makedirs(log_dir, exist_ok=True)
        print("Log dir is:", log_dir)

        dataset_nm = config.get('metadata')['owner']
        # Create object-level logger per dataset
        log_file_name = f"{dataset_nm}.log"
        log_file_path = os.path.join(log_dir, log_file_name)
        logger = get_logger(dataset_nm, log_file_path)
        print("_______________________")
        IngestionManager(config,logger,env)

        

    except Exception as e:
        print(f"Failed to process config: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Path to your multi-dataset YAML
    env = "dev"
    job_yaml_path = "src/com/drai/snf/resources/configs/jobs/sales_csv_to_snowflake.yaml"
    main(env,job_yaml_path)
