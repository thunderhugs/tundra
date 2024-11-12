from pathlib import Path
from typing import Dict, Optional, Union, List, Any
import pandas as pd
from io import StringIO, BytesIO
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.sharepoint.files.creation_information import FileCreationInformation
from .exceptions import ConfigurationError, ConnectionError

class SharePointConnector:
    """Manages connections and operations with SharePoint"""
    
    def __init__(self, config: Dict[str, str]):
        """
        Initialize SharePointConnector with configuration
        
        Args:
            config (Dict[str, str]): SharePoint connection parameters containing
                                   site_url, username, and password
        """
        self.config = config
        self.ctx = None
        
    def connect(self) -> None:
        """Establish connection to SharePoint"""
        try:
            site_url = self.config.get("site_url")
            username = self.config.get("username")
            password = self.config.get("password")
            
            if not all([site_url, username, password]):
                raise ConfigurationError("Missing required SharePoint configuration")
                
            ctx_auth = AuthenticationContext(url=site_url)
            if ctx_auth.acquire_token_for_user(username, password):
                self.ctx = ClientContext(site_url, ctx_auth)
                web = self.ctx.web
                self.ctx.load(web)
                self.ctx.execute_query()
                print(f"Connected to SharePoint site: {web.properties['Title']}")
            else:
                raise ConnectionError(f"SharePoint authentication failed: {ctx_auth.get_last_error()}")
                
        except Exception as e:
            raise ConnectionError(f"Failed to connect to SharePoint: {str(e)}")

    def disconnect(self) -> None:
        """Close SharePoint connection"""
        if self.ctx:
            self.ctx = None

    def read_csv_to_dataframe(self, file_path: str, **pandas_kwargs) -> pd.DataFrame:
        """
        Read CSV file from SharePoint into a pandas DataFrame
        
        Args:
            file_path (str): Full SharePoint path to the CSV file
            **pandas_kwargs: Additional arguments to pass to pd.read_csv
            
        Returns:
            pd.DataFrame: DataFrame containing the CSV data
        """
        if not self.ctx:
            self.connect()
            
        try:
            # Read file content
            response = File.open_binary(self.ctx, file_path)
            
            # Convert to DataFrame
            return pd.read_csv(BytesIO(response.content), **pandas_kwargs)
            
        except Exception as e:
            raise ConnectionError(f"Failed to read CSV from SharePoint: {str(e)}")

    def read_excel_to_dataframe(self, file_path: str, **pandas_kwargs) -> pd.DataFrame:
        """
        Read Excel file from SharePoint into a pandas DataFrame
        
        Args:
            file_path (str): Full SharePoint path to the Excel file
            **pandas_kwargs: Additional arguments to pass to pd.read_excel
            
        Returns:
            pd.DataFrame: DataFrame containing the Excel data
        """
        if not self.ctx:
            self.connect()
            
        try:
            # Read file content
            response = File.open_binary(self.ctx, file_path)
            
            # Convert to DataFrame
            return pd.read_excel(BytesIO(response.content), **pandas_kwargs)
            
        except Exception as e:
            raise ConnectionError(f"Failed to read Excel from SharePoint: {str(e)}")

    def get_list_items(self, list_name: str, fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get items from a SharePoint list
        
        Args:
            list_name (str): Name of the SharePoint list
            fields (Optional[List[str]]): List of field names to retrieve
            
        Returns:
            pd.DataFrame: DataFrame containing the list items
        """
        if not self.ctx:
            self.connect()
            
        try:
            list_obj = self.ctx.web.lists.get_by_title(list_name)
            items = list_obj.get_items().execute_query()
            data = [item.properties for item in items]
            df = pd.DataFrame(data)
            
            if fields and not df.empty:
                return df[fields]
            return df
            
        except Exception as e:
            print(f"An error occurred while retrieving list items: {e}")
            return pd.DataFrame()

    def update_list_item(self, list_name: str, item_id: int, update_dict: Dict[str, Any]) -> None:
        """
        Update a SharePoint list item
        
        Args:
            list_name (str): Name of the SharePoint list
            item_id (int): ID of the item to update
            update_dict (Dict[str, Any]): Dictionary of field names and values to update
        """
        if not self.ctx:
            self.connect()
            
        try:
            target_list = self.ctx.web.lists.get_by_title(list_name)
            item = target_list.items.get_by_id(item_id)
            
            item.update(update_dict)
            self.ctx.execute_query()
            
        except Exception as e:
            raise ConnectionError(f"Failed to update SharePoint list item: {str(e)}")

    def add_list_item(self, list_name: str, item_dict: Dict[str, Any]) -> int:
        """
        Add a new item to a SharePoint list
        
        Args:
            list_name (str): Name of the SharePoint list
            item_dict (Dict[str, Any]): Dictionary of field names and values for the new item
            
        Returns:
            int: ID of the newly created item
        """
        if not self.ctx:
            self.connect()
            
        try:
            target_list = self.ctx.web.lists.get_by_title(list_name)
            item = target_list.add_item(item_dict)
            self.ctx.execute_query()
            return item.properties['Id']
            
        except Exception as e:
            raise ConnectionError(f"Failed to add SharePoint list item: {str(e)}")

    def delete_list_item(self, list_name: str, item_id: int) -> None:
        """
        Delete an item from a SharePoint list
        
        Args:
            list_name (str): Name of the SharePoint list
            item_id (int): ID of the item to delete
        """
        if not self.ctx:
            self.connect()
            
        try:
            target_list = self.ctx.web.lists.get_by_title(list_name)
            item = target_list.items.get_by_id(item_id)
            
            item.delete_object()
            self.ctx.execute_query()
            
        except Exception as e:
            raise ConnectionError(f"Failed to delete SharePoint list item: {str(e)}")

    def get_list_fields(self, list_name: str) -> List[str]:
        """
        Get all field names from a SharePoint list
        
        Args:
            list_name (str): Name of the SharePoint list
            
        Returns:
            List[str]: List of field names
        """
        if not self.ctx:
            self.connect()
            
        try:
            target_list = self.ctx.web.lists.get_by_title(list_name)
            fields = target_list.fields
            self.ctx.load(fields)
            self.ctx.execute_query()
            
            # Filter out internal fields
            return [field.properties['Title'] for field in fields 
                   if not field.properties['InternalName'].startswith('_')]
            
        except Exception as e:
            raise ConnectionError(f"Failed to get SharePoint list fields: {str(e)}")

    def save_dataframe(self, 
                      df: pd.DataFrame, 
                      folder_name: str, 
                      file_name: str,
                      file_type: str = 'csv',
                      overwrite: bool = True,
                      **pandas_kwargs) -> None:
        """
        Save DataFrame to SharePoint folder
        
        Args:
            df (pd.DataFrame): DataFrame to save
            folder_name (str): Name of the SharePoint folder
            file_name (str): Name of the file to create
            file_type (str): Type of file to save ('csv' or 'excel')
            overwrite (bool): Whether to overwrite existing file
            **pandas_kwargs: Additional arguments to pass to DataFrame.to_csv or DataFrame.to_excel
        """
        if not self.ctx:
            self.connect()
            
        try:
            # Get the target folder
            target_folder = self.ctx.web.get_folder_by_server_relative_url(folder_name)
            self.ctx.load(target_folder)
            self.ctx.execute_query()
            
            # Create buffer and save DataFrame to it
            buffer = BytesIO()
            if file_type.lower() == 'csv':
                df.to_csv(buffer, index=False, **pandas_kwargs)
            elif file_type.lower() == 'excel':
                df.to_excel(buffer, index=False, **pandas_kwargs)
            else:
                raise ValueError("file_type must be either 'csv' or 'excel'")
            
            # Get the buffer content
            buffer.seek(0)
            content = buffer.getvalue()

            # Create file info
            file_info = FileCreationInformation()
            file_info.content = content
            file_info.url = file_name
            file_info.overwrite = overwrite

            # Upload file
            target_folder.files.add(file_info)
            self.ctx.execute_query()
            
        except Exception as e:
            raise ConnectionError(f"Failed to save DataFrame to SharePoint: {str(e)}")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
