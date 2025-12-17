import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from com.drai.snf.core.utils.logger_utils import get_logger, log_execution


class SnowflakeConnector:
    """
    Establishes connection to Snowflake using either password or keypair authentication.
    """

    def __init__(self, conn_config: dict, logger=None):
        self.conn_config = conn_config
        self.logger = logger or get_logger()
        self.connection = None

    @log_execution()
    def _load_private_key(self, key_path: str, passphrase: str = None):
        """
        Loads a private RSA key from a PEM (.p8) file.
        """
        self.logger.info(f"Loading private key from: {key_path}")
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Private key file not found at {key_path}")

        with open(key_path, "rb") as key_file:
            key_data = key_file.read()
            if passphrase:
                private_key = serialization.load_pem_private_key(
                    key_data,
                    password=passphrase.encode(),
                    backend=default_backend()
                )
            else:
                private_key = serialization.load_pem_private_key(
                    key_data,
                    password=None,
                    backend=default_backend()
                )

        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return private_key_bytes

    @log_execution()
    def get_connection(self):
        """
        Creates a Snowflake connection using password or keypair auth.
        """
        conn_info = self.conn_config['connection']['connection']
        auth_info = conn_info.get("authentication", {})
        options = conn_info.get("options", {})

        method = auth_info.get("method", "password").lower()
        self.logger.info(f"Connecting to Snowflake using '{method}' authentication")

        conn_args = {
            "account": conn_info["account"],
            "user": conn_info["user"],
            "warehouse": conn_info.get("warehouse"),
            "database": conn_info.get("database"),
            "schema": conn_info.get("schema"),
            "role": conn_info.get("role"),
            "autocommit": options.get("autocommit", True),
            "session_parameters": options.get("session_parameters", {}),
        }

        if method == "password":
            conn_args["password"] = auth_info.get("password")
            self.logger.debug("Using password-based authentication")

        elif method == "keypair":
            key_path = auth_info.get("private_key_path")
            passphrase = auth_info.get("private_key_passphrase")
            private_key_bytes = self._load_private_key(key_path, passphrase)
            conn_args["private_key"] = private_key_bytes
            self.logger.debug("Using keypair-based authentication")

        else:
            raise ValueError(f"Unsupported authentication method: {method}")

        try:
            self.connection = snowflake.connector.connect(**conn_args)
            self.logger.info("✅ Snowflake connection established successfully.")
            return self.connection
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise e

    @log_execution()
    def test_connection(self):
        """
        Runs a simple query to validate the connection.
        """
        if not self.connection:
            self.logger.warning("No active connection found. Attempting to reconnect...")
            self.connect()

        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                result = cur.fetchone()
                self.logger.info(f"Connected to Snowflake Version: {result[0]}")
                return result
        except Exception as e:
            self.logger.error(f"Error testing Snowflake connection: {e}")
            raise e
