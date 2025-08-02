"""
Google Sheets service for uploading CSV data.
"""

import csv
import os
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime
from botnim.config import get_logger
from .google_sheets_sync import GoogleSheetsSync

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
                           replace_existing: bool = False) -> bool:
        """
        Upload a CSV file to Google Sheets.
        
        Args:
            csv_path: Path to CSV file
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Uploading CSV to Google Sheets: {csv_path} -> {sheet_name}")
            success = self.sync.upload_csv_to_sheet(
                csv_path, spreadsheet_id, sheet_name, replace_existing
            )
            
            if success:
                logger.info(f"✅ Successfully uploaded CSV to Google Sheets: {sheet_name}")
            else:
                logger.error(f"❌ Failed to upload CSV to Google Sheets: {sheet_name}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error uploading CSV to Google Sheets: {e}")
            return False
    
    def upload_directory_csvs(self, directory_path: str, spreadsheet_id: str,
                             replace_existing: bool = False, 
                             prefer_output_csv: bool = True) -> Dict[str, bool]:
        """
        Upload CSV files in a directory to Google Sheets.
        
        Args:
            directory_path: Directory containing CSV files
            spreadsheet_id: Google Sheets spreadsheet ID
            replace_existing: If True, replace entire sheets. If False, append new rows.
            prefer_output_csv: If True, prefer source-specific CSV files over output.csv
        
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
                
                success = self.upload_csv_to_sheet(
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
                return self._upload_output_csv_by_source(str(output_csv), spreadsheet_id, replace_existing)
        
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
            
            success = self.upload_csv_to_sheet(
                str(csv_file), spreadsheet_id, safe_sheet_name, replace_existing
            )
            results[safe_sheet_name] = success
        
        return results
    
    def _upload_output_csv_by_source(self, output_csv_path: str, spreadsheet_id: str,
                                    replace_existing: bool = False) -> Dict[str, bool]:
        """
        Split output.csv by source and upload each source to a separate sheet.
        
        Args:
            output_csv_path: Path to output.csv file
            spreadsheet_id: Google Sheets spreadsheet ID
            replace_existing: If True, replace entire sheets. If False, append new rows.
        
        Returns:
            Dictionary mapping sheet names to success status
        """
        import pandas as pd
        
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
                success = self._upload_csv_data_to_sheet(
                    csv_data, spreadsheet_id, safe_sheet_name, replace_existing
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
            import io
            import csv
            
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
        
        return self.upload_csv_to_sheet(
            output_csv_path, spreadsheet_id, sheet_name, replace_existing
        )
    
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