# src/com/dataready_ai/snf/core/connectors/csv_connector.py
from com.drai.snf.core.utils.logger_utils import get_logger, log_execution

class CSVConnector:
    """
    Connector class for CSV files.
    Returns file path(s) as 'connection'.
    """

    def __init__(self, source_config: dict, logger=None):
        self.source_config = source_config
        self.logger = logger or get_logger("csv_connector", "logs/csv_connector.log")

    @log_execution()
    def get_connection(self):
        """
        For CSV, the 'connection' is just the file path.
        """
        file_path = self.source_config['properties'].get("file_path")
        if not file_path:
            raise ValueError("CSV file path must be provided in source properties")
        self.logger.info(f"CSVConnector returning file path: {file_path}")
        return file_path
