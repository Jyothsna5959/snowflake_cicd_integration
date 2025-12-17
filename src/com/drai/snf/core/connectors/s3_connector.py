# src/com/dataready_ai/snf/core/connectors/csv_connector.py
from com.drai.snf.core.utils.logger_utils import get_logger, log_execution

class S3Connector:
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
        For S3, the 'connection' is just the dummy for now.
        """
        print("source config is:",self.source_config)
