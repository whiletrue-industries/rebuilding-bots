"""
Google Sheets synchronization module for PDF extraction pipeline.

This module provides functionality to upload CSV data to Google Sheets.
"""

import logging
import csv
import os
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import json
from botnim.config import get_logger

# Google Sheets API imports
from google.oauth2 import service_account
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = get_logger(__name__)

class GoogleSheetsSync:
    def __init__(self, credentials_path: Optional[str] = None, use_adc: bool = False):
        """
        Initialize Google Sheets sync with service account credentials or Application Default Credentials.
        
        Args:
            credentials_path: Path to service account JSON credentials file (optional if use_adc=True)
            use_adc: If True, use Application Default Credentials instead of service account key
        """
        self.credentials_path = credentials_path
        self.use_adc = use_adc
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Sheets API using service account or Application Default Credentials."""
        try:
            if self.use_adc:
                # Use Application Default Credentials
                credentials, project = default()
                logger.info(f"Authenticated with Application Default Credentials (project: {project})")
            else:
                # Use service account credentials
                if not self.credentials_path:
                    raise ValueError("credentials_path is required when use_adc=False")
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                logger.info("Authenticated with service account credentials")
            
            self.service = build('sheets', 'v4', credentials=credentials)
            logger.info("Successfully authenticated with Google Sheets API")
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise
    
    def create_or_update_sheet(self, spreadsheet_id: str, sheet_name: str, 
                              data: List[List], replace_existing: bool = False) -> bool:
        """
        Create or update a sheet in the specified spreadsheet.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            data: List of lists representing rows (first row should be headers)
            replace_existing: If True, replace entire sheet. If False, append new rows.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if replace_existing:
                return self._replace_sheet(spreadsheet_id, sheet_name, data)
            else:
                return self._append_to_sheet(spreadsheet_id, sheet_name, data)
        except Exception as e:
            logger.error(f"Failed to create/update sheet: {e}")
            return False
    
    def _replace_sheet(self, spreadsheet_id: str, sheet_name: str, data: List[List]) -> bool:
        """Replace entire sheet content and rename if needed."""
        try:
            # First, check if there's an existing sheet with a different name that we should rename
            existing_sheet_name = self._get_first_sheet_name(spreadsheet_id)
            if existing_sheet_name and existing_sheet_name != sheet_name:
                logger.info(f"Found existing sheet '{existing_sheet_name}', clearing content and renaming to '{sheet_name}'")
                # Clear the existing sheet content
                success = self._clear_sheet(spreadsheet_id, existing_sheet_name)
                if not success:
                    logger.warning(f"Failed to clear existing sheet content, attempting to delete and recreate")
                    return self._delete_and_recreate_sheet(spreadsheet_id, sheet_name, data)
                
                # Rename the existing sheet to the target name
                success = self._rename_sheet(spreadsheet_id, existing_sheet_name, sheet_name)
                if not success:
                    logger.warning(f"Failed to rename sheet, attempting to delete and recreate")
                    return self._delete_and_recreate_sheet(spreadsheet_id, sheet_name, data)
                
                # Now the sheet should exist with the target name
                logger.info(f"Successfully renamed sheet from '{existing_sheet_name}' to '{sheet_name}'")
            elif not self._sheet_exists(spreadsheet_id, sheet_name):
                # Create new sheet if it doesn't exist and there's no existing sheet to rename
                logger.info(f"Creating new sheet '{sheet_name}'")
                self._create_sheet(spreadsheet_id, sheet_name)
            
            # At this point, the sheet should exist with the target name
            # Clear and update the content
            try:
                # Clear the existing sheet content
                logger.info(f"Clearing existing sheet '{sheet_name}' content")
                success = self._clear_sheet(spreadsheet_id, sheet_name)
                if not success:
                    logger.warning(f"Failed to clear existing sheet content, attempting to delete and recreate")
                    return self._delete_and_recreate_sheet(spreadsheet_id, sheet_name, data)
            except Exception as e:
                logger.warning(f"Failed to clear existing sheet: {e}, attempting to delete and recreate")
                return self._delete_and_recreate_sheet(spreadsheet_id, sheet_name, data)
            
            # Write new data (use just the sheet name as the range)
            range_name = sheet_name  # No !A1 or !A:Z
            body = {'values': data}
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"Successfully replaced sheet '{sheet_name}' with {len(data)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Failed to replace sheet: {e}")
            return False

    def _clear_sheet(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """
        Clear the content of a sheet without deleting it, preserving the GID.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to clear
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Clear the sheet content by setting it to empty
            range_name = sheet_name  # No !A1 or !A:Z
            body = {'values': []}
            self.service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            logger.info(f"Successfully cleared sheet '{sheet_name}' content")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear sheet '{sheet_name}': {e}")
            return False

    def _delete_and_recreate_sheet(self, spreadsheet_id: str, sheet_name: str, data: List[List]) -> bool:
        """
        Delete and recreate a sheet (fallback method when clear fails).
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to delete and recreate
            data: Data to write to the new sheet
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the sheet ID to delete it
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            sheets = spreadsheet.get('sheets', [])
            sheet_id = None
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == sheet_name:
                    sheet_id = sheet.get('properties', {}).get('sheetId')
                    break
            
            if sheet_id is not None:
                # Delete the existing sheet
                request = {
                    'deleteSheet': {
                        'sheetId': sheet_id
                    }
                }
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': [request]}
                ).execute()
                logger.info(f"Deleted existing sheet '{sheet_name}'")
            
            # Create new sheet
            self._create_sheet(spreadsheet_id, sheet_name)
            
            # Write new data
            range_name = sheet_name
            body = {'values': data}
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"Successfully recreated sheet '{sheet_name}' with {len(data)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete and recreate sheet: {e}")
            return False
    
    def _append_to_sheet(self, spreadsheet_id: str, sheet_name: str, data: List[List]) -> bool:
        """Append new rows to existing sheet."""
        try:
            # Try to create the sheet first (it will fail silently if it exists)
            try:
                self._create_sheet(spreadsheet_id, sheet_name)
            except Exception:
                # Sheet might already exist, continue
                pass
            
            # Append data (use just the sheet name as the range)
            range_name = sheet_name  # No !A:A or !A1:Z1
            body = {'values': data}
            self.service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            logger.info(f"Successfully appended {len(data)} rows to sheet '{sheet_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to append to sheet: {e}")
            return False
    
    def _create_sheet(self, spreadsheet_id: str, sheet_name: str):
        """Create a new sheet in the spreadsheet."""
        try:
            request = {
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }
            
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': [request]}
            ).execute()
            
            logger.info(f"Created new sheet '{sheet_name}'")
        except Exception as e:
            logger.error(f"Failed to create sheet: {e}")
            raise
    
    def append_data_rows(self, data_rows: List[List], spreadsheet_id: str, sheet_name: str,
                        replace_existing: bool = False, headers: List[str] = None) -> bool:
        """
        Append data rows to Google Sheets, adding headers only on first upload.
        
        Args:
            data_rows: List of data rows (without headers)
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
            headers: List of header names to add on first upload
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not data_rows:
                logger.warning("No data rows to upload")
                return False
            

            
            # Check if sheet exists
            sheet_exists = self._sheet_exists(spreadsheet_id, sheet_name)
            
            if replace_existing:
                # Replace entire sheet - include headers
                if headers:
                    full_data = [headers] + data_rows
                else:
                    full_data = data_rows
                logger.info(f"Replacing sheet '{sheet_name}' with {len(full_data)} rows (including headers)")
                return self._replace_sheet(spreadsheet_id, sheet_name, full_data)
            elif not sheet_exists:
                # First upload - create sheet with headers
                if headers:
                    full_data = [headers] + data_rows
                else:
                    full_data = data_rows
                logger.info(f"Creating new sheet '{sheet_name}' with {len(full_data)} rows (including headers)")
                return self._replace_sheet(spreadsheet_id, sheet_name, full_data)
            else:
                # Append to existing sheet - no headers
                logger.info(f"Appending {len(data_rows)} data rows to existing sheet '{sheet_name}'")
                return self._append_to_sheet(spreadsheet_id, sheet_name, data_rows)
            
        except Exception as e:
            logger.error(f"Failed to upload data rows: {e}")
            return False
    
    def _sheet_exists(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """Check if a sheet exists by title (not by range)."""
        try:
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            sheets = spreadsheet.get('sheets', [])
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == sheet_name:
                    return True
            return False
        except Exception as e:
            logger.error(f"Failed to check if sheet exists: {e}")
            return False

    def upload_csv_to_sheet(self, csv_file_path: str, spreadsheet_id: str, sheet_name: str, replace_existing: bool = False) -> Tuple[bool, Optional[str]]:
        """
        Upload CSV data to a Google Sheet.
        
        Args:
            csv_file_path: Path to the CSV file
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet
            replace_existing: Whether to replace existing content
            
        Returns:
            Tuple of (success: bool, gid: Optional[str])
        """
        try:
            if not os.path.exists(csv_file_path):
                logger.error(f"CSV file not found: {csv_file_path}")
                return False, None
            
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                data = list(reader)
            
            if not data:
                logger.error(f"CSV file is empty: {csv_file_path}")
                return False, None
            
            logger.info(f"Read {len(data)} rows from CSV file")
            return self.create_or_update_sheet_with_gid(spreadsheet_id, sheet_name, data, replace_existing)
            
        except Exception as e:
            logger.error(f"Failed to upload CSV: {e}")
            return False, None

    def upload_csv_to_sheet_by_gid(self, csv_file_path: str, spreadsheet_id: str, gid: str, replace_existing: bool = False) -> Tuple[bool, Optional[str]]:
        """
        Upload CSV data to a Google Sheet using GID.
        
        Args:
            csv_file_path: Path to the CSV file
            spreadsheet_id: Google Sheets spreadsheet ID
            gid: Google Sheets GID (sheet ID)
            replace_existing: Whether to replace existing content
            
        Returns:
            Tuple of (success: bool, gid: Optional[str])
        """
        try:
            if not os.path.exists(csv_file_path):
                logger.error(f"CSV file not found: {csv_file_path}")
                return False, None
            
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                data = list(reader)
            
            if not data:
                logger.error(f"CSV file is empty: {csv_file_path}")
                return False, None
            
            # Determine the target sheet name from the CSV filename
            csv_filename = os.path.basename(csv_file_path)
            target_sheet_name = os.path.splitext(csv_filename)[0]  # Remove .csv extension
            
            # Resolve GID to sheet name
            gid_sheet_name = self._get_sheet_name_from_gid(spreadsheet_id, gid)
            if not gid_sheet_name:
                logger.error(f"No sheet found with GID {gid}")
                return False, None
            
            logger.info(f"Resolved GID {gid} to sheet name: {gid_sheet_name}")
            logger.info(f"Target sheet name from CSV: {target_sheet_name}")
            
            # Always use the existing sheet (identified by GID) and clear/rename it if needed
            # This preserves the GID while updating content and name
            if replace_existing:
                logger.info(f"Replacing content and renaming sheet '{gid_sheet_name}' to '{target_sheet_name}' (GID preserved: {gid})")
                return self.create_or_update_sheet_with_gid(spreadsheet_id, target_sheet_name, data, replace_existing)
            else:
                # Use the GID-resolved sheet name for append operations
                logger.info(f"Appending to existing sheet '{gid_sheet_name}' (GID: {gid})")
                return self.create_or_update_sheet_with_gid(spreadsheet_id, gid_sheet_name, data, replace_existing)
            
        except Exception as e:
            logger.error(f"Failed to upload CSV by GID: {e}")
            return False, None

    def _get_sheet_name_from_gid(self, spreadsheet_id: str, gid: str) -> Optional[str]:
        """
        Get sheet name from GID using Google Sheets API.
        
        Args:
            spreadsheet_id: The ID of the spreadsheet
            gid: The GID of the sheet
            
        Returns:
            The sheet name if found, None otherwise
        """
        try:
            # Get spreadsheet metadata to find sheet name by GID
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            if spreadsheet and 'sheets' in spreadsheet:
                for sheet in spreadsheet['sheets']:
                    if 'properties' in sheet and str(sheet['properties'].get('sheetId')) == gid:
                        return sheet['properties'].get('title')
            
            logger.warning(f"Sheet name not found for GID {gid} in spreadsheet {spreadsheet_id}")
            return None
        except Exception as e:
            logger.error(f"Failed to get sheet name for GID {gid} in spreadsheet {spreadsheet_id}: {e}")
            return None

    def _get_sheet_gid(self, spreadsheet_id: str, sheet_name: str) -> Optional[str]:
        """
        Get the GID (sheet ID) for a specific sheet name.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet
            
        Returns:
            GID (sheet ID) as string, or None if not found
        """
        try:
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            sheets = spreadsheet.get('sheets', [])
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == sheet_name:
                    return str(sheet.get('properties', {}).get('sheetId'))
            return None
        except Exception as e:
            logger.error(f"Failed to get GID for sheet '{sheet_name}': {e}")
            return None

    def create_or_update_sheet_with_gid(self, spreadsheet_id: str, sheet_name: str, data: List[List], replace_existing: bool = False) -> Tuple[bool, Optional[str]]:
        """
        Create or update a sheet and return the GID.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet
            data: Data to write to the sheet
            replace_existing: Whether to replace existing content
            
        Returns:
            Tuple of (success: bool, gid: Optional[str])
        """
        try:
            if replace_existing:
                success = self._replace_sheet(spreadsheet_id, sheet_name, data)
            else:
                success = self.append_data_rows(data[1:] if len(data) > 1 else [], spreadsheet_id, sheet_name, False, data[0] if data else [])
            
            if success:
                # Get the GID after successful creation/update
                gid = self._get_sheet_gid(spreadsheet_id, sheet_name)
                if gid:
                    logger.info(f"Successfully created/updated sheet '{sheet_name}' with GID: {gid}")
                else:
                    logger.warning(f"Successfully created/updated sheet '{sheet_name}' but could not retrieve GID")
                return True, gid
            else:
                return False, None
                
        except Exception as e:
            logger.error(f"Failed to create/update sheet '{sheet_name}': {e}")
            return False, None

    def download_sheet_data(self, spreadsheet_id: str, sheet_name: str) -> Optional[List[List]]:
        """
        Download data from a Google Sheet.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to download
            
        Returns:
            List of rows (each row is a list of values) if successful, None otherwise
        """
        try:
            logger.info(f"Downloading data from sheet '{sheet_name}' in spreadsheet {spreadsheet_id}")
            
            # Get the sheet data
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                logger.warning(f"No data found in sheet '{sheet_name}'")
                return None
            
            logger.info(f"Downloaded {len(values)} rows from sheet '{sheet_name}'")
            return values
            
        except HttpError as e:
            logger.error(f"HTTP error downloading sheet '{sheet_name}': {e}")
            return None
        except Exception as e:
            logger.error(f"Error downloading sheet '{sheet_name}': {e}")
            return None

    def download_sheet_data_by_gid(self, spreadsheet_id: str, gid: str) -> Optional[List[List]]:
        """
        Download data from a Google Sheet using GID (sheet ID).
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            gid: Google Sheets GID (sheet ID)
            
        Returns:
            List of rows (each row is a list of values) if successful, None otherwise
        """
        try:
            # Get sheet name from GID
            sheet_name = self._get_sheet_name_from_gid(spreadsheet_id, gid)
            if not sheet_name:
                logger.error(f"Could not find sheet name for GID {gid}")
                return None
            
            logger.info(f"Downloading data from sheet '{sheet_name}' (GID: {gid}) in spreadsheet {spreadsheet_id}")
            
            # Get the sheet data
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                logger.warning(f"No data found in sheet '{sheet_name}' (GID: {gid})")
                return None
            
            logger.info(f"Downloaded {len(values)} rows from sheet '{sheet_name}' (GID: {gid})")
            return values
            
        except HttpError as e:
            logger.error(f"HTTP error downloading sheet by GID {gid}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error downloading sheet by GID {gid}: {e}")
            return None

    def create_spreadsheet(self, title: str, description: str = "") -> str:
        """
        Create a new Google Spreadsheet.
        
        Args:
            title: Title of the new spreadsheet
            description: Description for the new spreadsheet (optional)
            
        Returns:
            The ID of the newly created spreadsheet
        """
        try:
            spreadsheet = {
                'properties': {
                    'title': title,
                    'description': description
                }
            }
            spreadsheet = self.service.spreadsheets().create(body=spreadsheet).execute()
            logger.info(f"Created new spreadsheet with ID: {spreadsheet.get('spreadsheetId')}")
            return spreadsheet.get('spreadsheetId')
        except HttpError as e:
            logger.error(f"HTTP error creating spreadsheet: {e}")
            return ""
        except Exception as e:
            logger.error(f"Error creating spreadsheet: {e}")
            return "" 

    def _get_first_sheet_name(self, spreadsheet_id: str) -> Optional[str]:
        """
        Get the name of the first sheet in the spreadsheet.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            
        Returns:
            Name of the first sheet, or None if not found
        """
        try:
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            sheets = spreadsheet.get('sheets', [])
            if sheets:
                return sheets[0].get('properties', {}).get('title')
            return None
        except Exception as e:
            logger.error(f"Failed to get first sheet name: {e}")
            return None 

    def _rename_sheet(self, spreadsheet_id: str, old_name: str, new_name: str) -> bool:
        """
        Rename a sheet while preserving its GID.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            old_name: Current name of the sheet
            new_name: New name for the sheet
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the sheet ID for the old name
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            sheets = spreadsheet.get('sheets', [])
            sheet_id = None
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == old_name:
                    sheet_id = sheet.get('properties', {}).get('sheetId')
                    break
            
            if sheet_id is None:
                logger.error(f"Could not find sheet '{old_name}' to rename")
                return False
            
            # Rename the sheet
            request = {
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet_id,
                        'title': new_name
                    },
                    'fields': 'title'
                }
            }
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': [request]}
            ).execute()
            
            logger.info(f"Successfully renamed sheet from '{old_name}' to '{new_name}' (GID preserved: {sheet_id})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rename sheet from '{old_name}' to '{new_name}': {e}")
            return False 