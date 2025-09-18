import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from loguru import logger


class Config:
    """Configuration manager for CEA Assistant"""

    def __init__(self):
        # Load .env file if it exists
        env_path = Path(".env")
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"Loaded configuration from {env_path}")
        else:
            logger.info("No .env file found, using environment variables and defaults")

        self._load_config()

    def _load_config(self):
        """Load configuration from environment variables with defaults"""
        self.cea_root = os.getenv("CEA_ROOT", "./test_cea_scripts")
        self.script_discovery_timeout = float(os.getenv("SCRIPT_DISCOVERY_TIMEOUT", "10.0"))
        self.database_path = os.getenv("DATABASE_PATH", "cea_assistant.db")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        # Ensure CEA_ROOT is a Path object
        self.cea_root = Path(self.cea_root)

        logger.info(f"Configuration loaded:")
        logger.info(f"  CEA_ROOT: {self.cea_root}")
        logger.info(f"  SCRIPT_DISCOVERY_TIMEOUT: {self.script_discovery_timeout}")
        logger.info(f"  DATABASE_PATH: {self.database_path}")
        logger.info(f"  LOG_LEVEL: {self.log_level}")

    def get_cea_root(self) -> Path:
        """Get CEA root directory as Path"""
        return self.cea_root

    def get_script_discovery_timeout(self) -> float:
        """Get script discovery timeout in seconds"""
        return self.script_discovery_timeout

    def get_database_path(self) -> str:
        """Get database file path"""
        return self.database_path

    def get_log_level(self) -> str:
        """Get logging level"""
        return self.log_level


# Global config instance
config = Config()