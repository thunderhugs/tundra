class TundraError(Exception):
    """Base exception for Tundra library"""
    pass

class ConfigurationError(TundraError):
    """Raised when there's an error in configuration"""
    pass

class ConnectionError(TundraError):
    """Raised when there's an error connecting to Snowflake"""
    pass