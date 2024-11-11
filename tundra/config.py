from configparser import ConfigParser
from pathlib import Path
from typing import Dict
from .exceptions import ConfigurationError

class ConfigManager:
    """Manages configuration for Snowflake connections"""
    
    def __init__(self, config_path: Path):
        """
        Initialize ConfigManager with path to config file
        
        Args:
            config_path (Path): Path to the config.ini file
        """
        self.config = ConfigParser()
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        self.config.read(config_path)

    def get_snowflake_config(self) -> Dict[str, str]:
        """
        Get Snowflake configuration from config file
        
        Returns:
            Dict[str, str]: Dictionary containing Snowflake connection parameters
        """
        try:
            return {
                "user": self.config.get("snowflake", "user"),
                "password": self.config.get("snowflake", "password"),
                "account": self.config.get("snowflake", "account"),
                "warehouse": self.config.get("snowflake", "warehouse"),
                "database": self.config.get("snowflake", "database"),
                "schema": self.config.get("snowflake", "schema"),
                "role": self.config.get("snowflake", "role")
            }
        except Exception as e:
            raise ConfigurationError(f"Error reading Snowflake configuration: {str(e)}")