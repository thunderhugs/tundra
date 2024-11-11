from pathlib import Path
from typing import Dict, Optional, Union
import pandas as pd
import snowflake.connector
from .exceptions import ConnectionError

class SnowflakeConnector:
    """Manages connections and queries to Snowflake"""
    
    def __init__(self, config: Dict[str, str]):
        """
        Initialize SnowflakeConnector with configuration
        
        Args:
            config (Dict[str, str]): Snowflake connection parameters
        """
        self.config = config
        self.conn = None

    def connect(self) -> None:
        """Establish connection to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(**self.config)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {str(e)}")

    def disconnect(self) -> None:
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query: Union[str, Path]) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame
        
        Args:
            query (Union[str, Path]): SQL query string or path to SQL file
            
        Returns:
            pd.DataFrame: Query results as DataFrame
        """
        if not self.conn:
            self.connect()

        try:
            # Handle query input
            if isinstance(query, Path) or (isinstance(query, str) and Path(query).is_file()):
                # Convert to Path object if it's a string path
                query_path = Path(query) if isinstance(query, str) else query
                with open(query_path, 'r') as file:
                    sql_query = file.read()
            else:
                # Treat as a direct SQL string
                sql_query = query

            cursor = self.conn.cursor()
            cursor.execute(sql_query)
            results = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            cursor.close()
            
            return pd.DataFrame(results, columns=column_names)
            
        except Exception as e:
            raise ConnectionError(f"Error executing query: {str(e)}")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()