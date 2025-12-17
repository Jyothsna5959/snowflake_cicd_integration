# src/com/dataready_ai/snf/core/utils/config.py
import os
import yaml
import re
from com.drai.snf.core.utils.logger_utils import get_logger, log_execution

class ConfigLoader:
    """
    Loads job YAML and dynamic connection YAMLs.
    Uses provided logger or default singleton logger.
    """

    def __init__(self, job_config_path: str, environment: str = None, logger=None):
        self.job_config_path = job_config_path
        self.environment = environment or os.environ.get("ENV", "dev")
        self.config = {}
        self.logger = logger or get_logger()  # Use default logger if none passed
        print(" path is :::::",self.job_config_path)

    #@log_execution()
    def _replace_env_vars(self, value):
        """Replaces ${VAR_NAME} in strings with environment variable values."""
        pattern = re.compile(r"\$\{([^}^{]+)\}")
        if isinstance(value, str):
            matches = pattern.findall(value)
            for var in matches:
                env_value = os.getenv(var)
                if env_value:
                    self.logger.debug(f"Resolved env var '{var}' -> '{env_value[:4]}****'")
                    value = value.replace(f"${{{var}}}", env_value)
                else:
                    self.logger.warning(f"Environment variable '{var}' not set.")
        return value
    
    def _traverse_and_replace(self, data):
        """Recursively traverse dict/list and replace env vars."""
        if isinstance(data, dict):
            return {k: self._traverse_and_replace(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._traverse_and_replace(v) for v in data]
        else:
            return self._replace_env_vars(data)
        
    #@log_execution()  # Logs entry/exit and args/return
    def load_yaml(self, file_path: str) -> dict:
        if not os.path.exists(file_path):
            self.logger.error(f"YAML file not found: {file_path}")
            raise FileNotFoundError(f"YAML file not found: {file_path}")
        self.logger.info(f"Loading YAML file: {file_path}")
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        data = self._traverse_and_replace(data)    
        return data

    #@log_execution()
    def derive_conn_path(self, conn_type: str, prefix: str) -> str:
        path = os.path.join(
            os.path.dirname(self.job_config_path),
            "../connections",
            conn_type,
            self.environment,
            f"{prefix}_{self.environment}.yaml"
        )
        self.logger.debug(f"Derived path for {conn_type} connection '{prefix}': {path}")
        return path

    #@log_execution()
    def load_config(self) -> dict:
        self.logger.info(f"Loading job YAML: {self.job_config_path}")
        job_config = self.load_yaml(self.job_config_path)
        self.config = job_config

        conn_prefixes = job_config.get("connections", {})
        if not conn_prefixes.get("source") or not conn_prefixes.get("target"):
            self.logger.error("Job YAML must define 'connections: source' and 'connections: target'")
            raise ValueError("Job YAML must define 'connections: source' and 'connections: target'")

        for dataset in self.config.get('datasets', []):
            dataset_name = dataset.get("name", "unknown_dataset")
            self.logger.info(f"Processing dataset: {dataset_name}")

            # Source connection
            src_type = dataset['source']['type'].lower()
            src_prefix = conn_prefixes['source']
            if src_type.lower() not in ('s3','csv'):
                src_conn_path = self.derive_conn_path(src_type, src_prefix)
                if os.path.exists(src_conn_path):
                    self.logger.info(f"Loading source connection YAML for dataset '{dataset_name}'")
                    dataset['source']['connection'] = self.load_yaml(src_conn_path)
                else:
                    self.logger.error(f"Source connection file missing for dataset '{dataset_name}': {src_conn_path}")
                    raise FileNotFoundError(f"Source connection file missing: {src_conn_path}")
                
            # Target connection
            tgt_type = dataset['target']['type'].lower()
            tgt_prefix = conn_prefixes['target']
            tgt_conn_path = self.derive_conn_path(tgt_type, tgt_prefix)
            if not os.path.exists(tgt_conn_path):
                self.logger.error(f"Target connection file missing for dataset '{dataset_name}': {tgt_conn_path}")
                raise FileNotFoundError(f"Target connection file missing: {tgt_conn_path}")
            self.logger.info(f"Loading target connection YAML for dataset '{dataset_name}'")
            dataset['target']['connection'] = self.load_yaml(tgt_conn_path)

        self.logger.info("All datasets and connections loaded successfully")
        return self.config
