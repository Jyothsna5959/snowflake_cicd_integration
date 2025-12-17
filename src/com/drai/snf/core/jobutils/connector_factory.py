# src/com/dataready_ai/snf/core/core/jobutils/connector_factory.py
from com.drai.snf.core.connectors.csv_connector import CSVConnector
# from com.drai.snf.core.connectors.parquet_connector import ParquetConnector
from com.drai.snf.core.connectors.snowflake_connector import SnowflakeConnector
from com.drai.snf.core.connectors.s3_connector import S3Connector

# Unified connector map for both source and target
CONNECTOR_MAP = {
    "csv": CSVConnector,
    "s3": S3Connector,
    "snowflake": SnowflakeConnector,
    # "parquet":ParquetConnector
    # Add other sources/targets here
}

def get_connector(connector_type: str, config: dict, logger=None):
    """
    Returns connector instance for given type.
    """
    connector_type = connector_type.lower()
    connector_class = CONNECTOR_MAP.get(connector_type)
    if not connector_class:
        raise ValueError(f"Unsupported connector type: {connector_type}")

    # Initialize and return connector object
    return connector_class(config, logger=logger)
