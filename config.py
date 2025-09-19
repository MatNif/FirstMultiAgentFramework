"""
Configuration management using pydantic-settings
"""

import os
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator

try:
    from pydantic_settings import BaseSettings
except ImportError:
    # Fallback for when pydantic-settings is not available
    from pydantic import BaseModel
    import dotenv
    dotenv.load_dotenv()

    class BaseSettings(BaseModel):
        def __init__(self, **kwargs):
            # Load from environment variables
            for field_name, field_info in self.model_fields.items():
                if field_name not in kwargs:
                    env_name = field_info.alias or field_name.upper()
                    env_value = os.getenv(env_name)
                    if env_value is not None:
                        kwargs[field_name] = env_value
            super().__init__(**kwargs)
from loguru import logger


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Database configuration
    db_path: str = Field(
        default="cea_assistant.db",
        env="DB_PATH",
        description="Path to SQLite database file"
    )

    # CEA configuration
    cea_root: str = Field(
        default="./test_cea_scripts",
        env="CEA_ROOT",
        description="Path to CEA scripts directory"
    )

    # Script discovery configuration
    script_discovery_timeout: float = Field(
        default=10.0,
        env="SCRIPT_DISCOVERY_TIMEOUT",
        description="Timeout in seconds for script discovery operations"
    )

    # Logging configuration
    log_level: str = Field(
        default="INFO",
        env="LOG_LEVEL",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )

    log_file: Optional[str] = Field(
        default=None,
        env="LOG_FILE",
        description="Optional log file path. If not set, logs to console only"
    )

    # Development settings
    debug: bool = Field(
        default=False,
        env="DEBUG",
        description="Enable debug mode"
    )

    # Test configuration
    test_mode: bool = Field(
        default=False,
        env="TEST_MODE",
        description="Running in test mode"
    )

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False
    }

    @field_validator("cea_root")
    def validate_cea_root(cls, v, info):
        """Validate that CEA_ROOT exists or can be created"""
        path = Path(v)
        if not path.exists():
            try:
                path.mkdir(parents=True, exist_ok=True)
                logger.warning(f"Created CEA_ROOT directory: {path}")
            except Exception as e:
                raise ValueError(f"Cannot create CEA_ROOT directory {path}: {e}")

        if not path.is_dir():
            raise ValueError(f"CEA_ROOT must be a directory, got: {path}")

        return str(path.resolve())

    @field_validator("log_level")
    def validate_log_level(cls, v, info):
        """Validate log level"""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of: {valid_levels}")
        return v_upper

    @field_validator("script_discovery_timeout")
    def validate_timeout(cls, v, info):
        """Validate timeout is positive"""
        if v <= 0:
            raise ValueError("Script discovery timeout must be positive")
        return v

    @field_validator("db_path")
    def validate_db_path(cls, v, info):
        """Validate database path"""
        path = Path(v)

        # Create parent directories if they don't exist
        if path.parent != Path('.'):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise ValueError(f"Cannot create database directory {path.parent}: {e}")

        return str(path.resolve())

    def get_cea_root_path(self) -> Path:
        """Get CEA_ROOT as a Path object"""
        return Path(self.cea_root)

    def get_db_path(self) -> Path:
        """Get database path as a Path object"""
        return Path(self.db_path)

    def setup_logging(self, conversation_id: Optional[str] = None) -> None:
        """Setup structured logging with loguru"""
        # Remove default logger
        logger.remove()

        # Create log format with conversation_id
        if conversation_id:
            log_format = (
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                f"<cyan>conv:{conversation_id}</cyan> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            )
        else:
            log_format = (
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            )

        # Add console handler
        logger.add(
            sink=lambda msg: print(msg, end=""),
            format=log_format,
            level=self.log_level,
            colorize=True
        )

        # Add file handler if configured
        if self.log_file:
            log_file_path = Path(self.log_file)
            log_file_path.parent.mkdir(parents=True, exist_ok=True)

            logger.add(
                sink=str(log_file_path),
                format=log_format,
                level=self.log_level,
                rotation="10 MB",
                retention="10 days",
                compression="gz"
            )

    def model_dump_safe(self) -> dict:
        """Dump model data safely (for logging/debugging)"""
        data = self.dict()
        # Remove sensitive data if any
        return data


# Global settings instance
try:
    settings = Settings()
except Exception as e:
    print(f"❌ Configuration Error: {e}")
    print("\nPlease check your environment variables:")
    print("- CEA_ROOT: Path to CEA scripts directory")
    print("- DB_PATH: Path to database file (optional)")
    print("- LOG_LEVEL: Logging level - DEBUG, INFO, WARNING, ERROR, CRITICAL (optional)")
    print("- SCRIPT_DISCOVERY_TIMEOUT: Timeout for script operations in seconds (optional)")
    print("\nExample .env file:")
    print("CEA_ROOT=./cea_scripts")
    print("DB_PATH=./data/cea_assistant.db")
    print("LOG_LEVEL=INFO")
    print("SCRIPT_DISCOVERY_TIMEOUT=10.0")
    raise SystemExit(1)


def get_settings() -> Settings:
    """Get global settings instance"""
    return settings


def setup_logging(conversation_id: Optional[str] = None) -> None:
    """Setup logging using global settings"""
    settings.setup_logging(conversation_id)


# Setup logging on import
if not settings.test_mode:
    setup_logging()