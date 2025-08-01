"""
Google Sheets synchronization module for PDF extraction pipeline.

This module provides functionality to upload CSV data to Google Sheets.
"""

import logging
import csv
import os
from typing import List, Dict, Optional
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
        """Replace entire sheet content."""
        try:
            # Check if sheet exists and delete it if it does
            if self._sheet_exists(spreadsheet_id, sheet_name):
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
                except Exception as e:
                    logger.warning(f"Failed to delete existing sheet: {e}")
            
            # Create new sheet
            self._create_sheet(spreadsheet_id, sheet_name)
            
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

    def upload_csv_to_sheet(self, csv_path: str, spreadsheet_id: str, sheet_name: str,
                           replace_existing: bool = False) -> bool:
        """
        Upload CSV file to Google Sheets.
        
        Args:
            csv_path: Path to CSV file
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Read CSV file
            data = []
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    data.append(row)
            
            if not data:
                logger.warning("CSV file is empty")
                return False
            
            logger.info(f"Read {len(data)} rows from CSV file")
            return self.create_or_update_sheet(spreadsheet_id, sheet_name, data, replace_existing)
            
        except Exception as e:
            logger.error(f"Failed to upload CSV: {e}")
            return False 