"""
Google Sheets service for uploading CSV data.
"""

import csv
import os
import io
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime
from botnim.config import get_logger
from .google_sheets_sync import GoogleSheetsSync

import pandas as pd


logger = get_logger(__name__)

class GoogleSheetsService:
    """
    Google Sheets service for uploading CSV data, can be used to upload any CSV data to Google Sheets.
    """
    
    def __init__(self, credentials_path: Optional[str] = None, use_adc: bool = False):
        """
        Initialize Google Sheets service.
        
        Args:
            credentials_path: Path to service account JSON credentials file
            use_adc: If True, use Application Default Credentials
        """
        self.sync = GoogleSheetsSync(credentials_path=credentials_path, use_adc=use_adc)
    
    def upload_csv_to_sheet(self, csv_path: str, spreadsheet_id: str, sheet_name: str,
                           replace_existing: bool = False) -> Tuple[bool, Optional[str]]:
        """
        Upload a CSV file to Google Sheets.
        
        Args:
            csv_path: Path to CSV file
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
        
        Returns:
            Tuple of (success: bool, gid: Optional[str])
        """
        try:
            logger.info(f"Uploading CSV to Google Sheets: {csv_path} -> {sheet_name}")
            success, gid = self.sync.upload_csv_to_sheet(
                csv_path, spreadsheet_id, sheet_name, replace_existing
            )
            
            if success:
                logger.info(f"✅ Successfully uploaded CSV to Google Sheets: {sheet_name}")
                if gid:
                    logger.info(f"📝 Sheet GID: {gid}")
            else:
                logger.error(f"❌ Failed to upload CSV to Google Sheets: {sheet_name}")
            
            return success, gid
            
        except Exception as e:
            logger.error(f"Error uploading CSV to Google Sheets: {e}")
            return False, None
    
    def upload_directory_csvs(self, directory_path: str, spreadsheet_id: str,
                             replace_existing: bool = False, 
                             prefer_output_csv: bool = True,
                             gid: str = None) -> Dict[str, bool]:
        """
        Upload CSV files in a directory to Google Sheets.
        
        Args:
            directory_path: Directory containing CSV files
            spreadsheet_id: Google Sheets spreadsheet ID
            replace_existing: If True, replace entire sheets. If False, append new rows.
            prefer_output_csv: If True, prefer source-specific CSV files over output.csv
            gid: Google Sheets GID (sheet ID) to use for upload if provided
        
        Returns:
            Dictionary mapping sheet names to success status
        """
        results = {}
        directory = Path(directory_path)
        
        # Look for source-specific CSV files first (preferred)
        source_csv_files = []
        for pattern in ["*החלטות*.csv", "*מכתבי*.csv", "*חוות*.csv"]:
            source_csv_files.extend(directory.glob(pattern))
        
        if source_csv_files:
            logger.info(f"Found {len(source_csv_files)} source-specific CSV files to upload")
            
            for csv_file in source_csv_files:
                # Use filename (without extension) as sheet name
                sheet_name = csv_file.stem
                
                # Create safe sheet name
                safe_sheet_name = self._create_safe_sheet_name(sheet_name)
                
                # Use GID-based upload if GID is provided, otherwise use sheet name
                if gid:
                    success, new_gid = self.sync.upload_csv_to_sheet_by_gid(
                        str(csv_file), spreadsheet_id, gid, replace_existing
                    )
                    results[safe_sheet_name] = success
                    if new_gid and new_gid != gid:
                        logger.info(f"📝 New GID detected for {csv_file.name}: {new_gid} (was: {gid})")
                else:
                    success, gid = self.upload_csv_to_sheet(
                        str(csv_file), spreadsheet_id, safe_sheet_name, replace_existing
                    )
                    results[safe_sheet_name] = success
                
                logger.info(f"Uploaded {csv_file.name} to sheet '{safe_sheet_name}'")
            
            return results
        
        # Fallback to output.csv if no source-specific files found
        if prefer_output_csv:
            output_csv = directory / "output.csv"
            if output_csv.exists():
                logger.info("No source-specific CSV files found, using output.csv and splitting by source")
                return self._upload_output_csv_by_source(str(output_csv), spreadsheet_id, replace_existing, gid)
        
        # Otherwise, upload any CSV files
        csv_files = list(directory.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in directory: {directory_path}")
            return results
        
        logger.info(f"Found {len(csv_files)} CSV files to upload")
        
        for csv_file in csv_files:
            # Use filename (without extension) as sheet name
            sheet_name = csv_file.stem
            
            # Create safe sheet name
            safe_sheet_name = self._create_safe_sheet_name(sheet_name)
            
            # Use GID-based upload if GID is provided, otherwise use sheet name
            if gid:
                success, new_gid = self.sync.upload_csv_to_sheet_by_gid(
                    str(csv_file), spreadsheet_id, gid, replace_existing
                )
                results[safe_sheet_name] = success
                if new_gid and new_gid != gid:
                    logger.info(f"📝 New GID detected for {csv_file.name}: {new_gid} (was: {gid})")
            else:
                success, gid = self.upload_csv_to_sheet(
                    str(csv_file), spreadsheet_id, safe_sheet_name, replace_existing
                )
                results[safe_sheet_name] = success
        
        return results
    
    def _upload_output_csv_by_source(self, output_csv_path: str, spreadsheet_id: str,
                                    replace_existing: bool = False, gid: str = None) -> Dict[str, bool]:
        """
        Split output.csv by source and upload each source to a separate sheet.
        
        Args:
            output_csv_path: Path to output.csv file
            spreadsheet_id: Google Sheets spreadsheet ID
            replace_existing: If True, replace entire sheets. If False, append new rows.
            gid: Google Sheets GID (sheet ID) to use for upload if provided
        
        Returns:
            Dictionary mapping sheet names to success status
        """
        
        try:
            # Read the output.csv file
            df = pd.read_csv(output_csv_path)
            
            if df.empty:
                logger.warning("Output CSV file is empty")
                return {}
            
            # Group by source_name (assuming this column exists)
            if 'source_name' not in df.columns:
                logger.error("No 'source_name' column found in output.csv")
                return {}
            
            results = {}
            
            # Group by source and upload each group to a separate sheet
            for source_name, group_df in df.groupby('source_name'):
                # Create safe sheet name
                safe_sheet_name = self._create_safe_sheet_name(source_name)
                
                # Convert group to CSV string
                csv_data = group_df.to_csv(index=False)
                
                # Upload to Google Sheets
                success, gid = self.upload_csv_to_sheet(
                    io.StringIO(csv_data).getvalue(), spreadsheet_id, safe_sheet_name, replace_existing
                )
                results[safe_sheet_name] = success
                
                logger.info(f"Uploaded {len(group_df)} records for source '{source_name}' to sheet '{safe_sheet_name}'")
            
            return results
            
        except Exception as e:
            logger.error(f"Error splitting and uploading output.csv by source: {e}")
            return {}
    
    def _upload_csv_data_to_sheet(self, csv_data: str, spreadsheet_id: str, sheet_name: str,
                                 replace_existing: bool = False) -> bool:
        """
        Upload CSV data (as string) to Google Sheets.
        
        Args:
            csv_data: CSV data as string
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Parse CSV data
            
            # Read CSV data
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            rows = list(csv_reader)
            
            if not rows:
                logger.warning(f"No data to upload for sheet '{sheet_name}'")
                return False
            
            # Get fieldnames
            fieldnames = list(rows[0].keys())
            
            # Upload to Google Sheets
            return self._upload_rows_to_sheet(rows, fieldnames, spreadsheet_id, sheet_name, replace_existing)
            
        except Exception as e:
            logger.error(f"Error uploading CSV data to sheet '{sheet_name}': {e}")
            return False
    
    def _upload_rows_to_sheet(self, rows: List[Dict[str, Any]], fieldnames: List[str], 
                             spreadsheet_id: str, sheet_name: str, replace_existing: bool = False) -> bool:
        """
        Upload rows of data to Google Sheets.
        
        Args:
            rows: List of dictionaries representing rows
            fieldnames: List of column names
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare data for Google Sheets
            values = [fieldnames]  # Header row
            for row in rows:
                values.append([row.get(field, '') for field in fieldnames])
            
            # Upload to Google Sheets using the existing sync functionality
            if replace_existing:
                # Replace entire sheet with new data
                success = self.sync.create_or_update_sheet(spreadsheet_id, sheet_name, values, replace_existing=True)
            else:
                # Append new data to existing sheet
                success = self.sync.append_data_rows(values[1:], spreadsheet_id, sheet_name, replace_existing=False, headers=values[0])
            
            return success
            
        except Exception as e:
            logger.error(f"Error uploading rows to sheet '{sheet_name}': {e}")
            return False
    
    def upload_output_csv(self, output_csv_path: str, spreadsheet_id: str,
                         sheet_name: str = "PDF_Extraction_Results",
                         replace_existing: bool = False) -> bool:
        """
        Upload the output.csv file from PDF extraction to Google Sheets.
        
        Args:
            output_csv_path: Path to output.csv file
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
        
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(output_csv_path):
            logger.error(f"Output CSV file not found: {output_csv_path}")
            return False
        
        success, gid = self.upload_csv_to_sheet(
            output_csv_path, spreadsheet_id, sheet_name, replace_existing
        )
        return success

    def upload_output_csv_by_gid(self, csv_file_path: str, spreadsheet_id: str, gid: str, replace_existing: bool = False) -> Tuple[bool, Optional[str]]:
        """
        Upload CSV file to Google Sheets using GID.
        
        Args:
            csv_file_path: Path to the CSV file
            spreadsheet_id: Google Sheets spreadsheet ID
            gid: Google Sheets GID (sheet ID)
            replace_existing: Whether to replace existing content
            
        Returns:
            Tuple of (success: bool, gid: Optional[str])
        """
        try:
            success, new_gid = self.sync.upload_csv_to_sheet_by_gid(
                csv_file_path, spreadsheet_id, gid, replace_existing
            )
            
            if success:
                logger.info(f"✅ Successfully uploaded CSV to Google Sheets using GID: {gid}")
                if new_gid and new_gid != gid:
                    logger.info(f"📝 New GID detected: {new_gid} (was: {gid})")
                return True, new_gid
            else:
                logger.error(f"❌ Failed to upload CSV to Google Sheets using GID: {gid}")
                return False, None
                
        except Exception as e:
            logger.error(f"❌ Failed to upload CSV to Google Sheets: {e}")
            return False, None
    
    def download_sheet_as_csv(self, spreadsheet_id: str, sheet_name: str) -> Optional[str]:
        """
        Download a Google Sheet as CSV content.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to download
            
        Returns:
            CSV content as string if successful, None otherwise
        """
        try:
            logger.info(f"Downloading sheet '{sheet_name}' from spreadsheet {spreadsheet_id}")
            
            # Use the underlying sync service to download data
            data = self.sync.download_sheet_data(spreadsheet_id, sheet_name)
            
            if not data:
                logger.warning(f"No data found in sheet '{sheet_name}'")
                return None
            
            # Convert to CSV format
            if not data:
                return None
            
            # Get headers from first row
            headers = data[0] if data else []
            rows = data[1:] if len(data) > 1 else []
            
            # Convert to CSV string
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write headers
            writer.writerow(headers)
            
            # Write data rows
            for row in rows:
                writer.writerow(row)
            
            csv_content = output.getvalue()
            output.close()
            
            logger.info(f"Downloaded {len(rows)} rows from sheet '{sheet_name}'")
            return csv_content
            
        except Exception as e:
            logger.error(f"Error downloading sheet '{sheet_name}' from Google Sheets: {e}")
            return None

    def download_sheet_as_csv_by_gid(self, spreadsheet_id: str, gid: str) -> Optional[str]:
        """
        Download a Google Sheet as CSV content using GID.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            gid: Google Sheets GID (sheet ID)
            
        Returns:
            CSV content as string if successful, None otherwise
        """
        try:
            logger.info(f"Downloading sheet with GID {gid} from spreadsheet {spreadsheet_id}")
            
            # Use the underlying sync service to download data by GID
            data = self.sync.download_sheet_data_by_gid(spreadsheet_id, gid)
            
            if not data:
                logger.warning(f"No data found in sheet with GID {gid}")
                return None
            
            # Convert to CSV format
            if not data:
                return None
            
            # Get headers from first row
            headers = data[0] if data else []
            rows = data[1:] if len(data) > 1 else []
            
            # Convert to CSV string
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write headers
            writer.writerow(headers)
            
            # Write data rows
            for row in rows:
                writer.writerow(row)
            
            csv_content = output.getvalue()
            output.close()
            
            logger.info(f"Downloaded {len(rows)} rows from sheet with GID {gid}")
            return csv_content
            
        except Exception as e:
            logger.error(f"Error downloading sheet with GID {gid} from Google Sheets: {e}")
            return None

    def create_spreadsheet(self, title: str, description: str = "") -> str:
        """
        Create a new Google Spreadsheet.
        
        Args:
            title: Title of the new spreadsheet
            description: Description for the new spreadsheet
            
        Returns:
            ID of the newly created spreadsheet
        """
        try:
            logger.info(f"Creating new spreadsheet: {title}")
            spreadsheet_id = self.sync.create_spreadsheet(title, description)
            logger.info(f"✅ Successfully created spreadsheet: {title} (ID: {spreadsheet_id})")
            return spreadsheet_id
        except Exception as e:
            logger.error(f"Error creating spreadsheet '{title}': {e}")
            return ""
    
    def _create_safe_sheet_name(self, name: str) -> str:
        """
        Create a safe sheet name for Google Sheets.
        
        Args:
            name: Original name
            
        Returns:
            Safe sheet name that complies with Google Sheets restrictions
        """
        # Remove or replace problematic characters
        safe_name = name.replace('"', '').replace('/', '_').replace('\\', '_')
        safe_name = safe_name.replace(':', '_').replace('?', '_').replace('*', '_')
        safe_name = safe_name.replace('[', '_').replace(']', '_')
        
        # Limit to 31 characters (Google Sheets limit)
        if len(safe_name) > 31:
            safe_name = safe_name[:28] + "..."
        
        return safe_name 

    def update_config_with_gid(self, config_file_path: str, source_id: str, new_gid: str) -> bool:
        """
        Update the config file with a new GID for a specific source.
        
        Args:
            config_file_path: Path to the config file
            source_id: ID of the source to update
            new_gid: New GID to set
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import yaml
            
            # Read the current config
            with open(config_file_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            
            # Find the source and update its GID
            updated = False
            for source in config.get('sources', []):
                if source.get('id') == source_id:
                    # Check if it's a PDF pipeline source
                    if source.get('type') == 'pdf_pipeline' and 'pdf_config' in source:
                        if 'output_config' in source['pdf_config']:
                            source['pdf_config']['output_config']['gid'] = new_gid
                            updated = True
                            logger.info(f"📝 Updated GID for PDF pipeline source '{source_id}' to {new_gid}")
                    # Check if it's a spreadsheet source
                    elif source.get('type') == 'spreadsheet' and 'spreadsheet_config' in source:
                        # Update the URL with the new GID
                        url = source['spreadsheet_config'].get('url', '')
                        if 'gid=' in url:
                            # Replace the GID in the URL
                            import re
                            new_url = re.sub(r'gid=\d+', f'gid={new_gid}', url)
                            source['spreadsheet_config']['url'] = new_url
                            updated = True
                            logger.info(f"📝 Updated GID in URL for spreadsheet source '{source_id}' to {new_gid}")
                    break
            
            if updated:
                # Write the updated config back
                with open(config_file_path, 'w', encoding='utf-8') as file:
                    yaml.dump(config, file, default_flow_style=False, allow_unicode=True)
                logger.info(f"✅ Successfully updated config file with new GID: {new_gid}")
                return True
            else:
                logger.warning(f"⚠️ Could not find source '{source_id}' in config file")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to update config file with new GID: {e}")
            return False 