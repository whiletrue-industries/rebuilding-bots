"""
Mock Open Budget Data Source for testing.

This module provides a mock implementation of OpenBudgetDataSource
that uses local files instead of making HTTP requests.
"""

import os
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Set
from io import StringIO

from botnim.config import get_logger

logger = get_logger(__name__)


class MockOpenBudgetDataSource:
    """
    Mock data source for Open Budget datapackages.
    
    Uses local files instead of making HTTP requests for testing.
    """
    
    def __init__(self, index_csv_url: str, datapackage_url: str, test_data_dir: str = None):
        """
        Initialize mock Open Budget data source.
        
        Args:
            index_csv_url: URL to the index.csv file (used for identification)
            datapackage_url: URL to the datapackage.json file (used for identification)
            test_data_dir: Directory containing test data files
        """
        self.index_csv_url = index_csv_url
        self.datapackage_url = datapackage_url
        self.test_data_dir = test_data_dir or self._get_default_test_data_dir()
        self._cached_datapackage = None
        self._cached_index = None
        
    def _get_default_test_data_dir(self) -> str:
        """Get the default test data directory."""
        current_dir = Path(__file__).parent
        return str(current_dir / "data")
        
    def get_current_revision(self) -> str:
        """
        Get the current revision from datapackage.json.
        
        Returns:
            Current revision string
            
        Raises:
            ValueError: If revision field is missing from datapackage
            FileNotFoundError: If datapackage file doesn't exist
        """
        datapackage = self._fetch_datapackage()
        
        if 'revision' not in datapackage:
            raise ValueError(
                f"Datapackage at {self.datapackage_url} is missing required 'revision' field. "
                f"This field is required for change detection."
            )
        
        return datapackage['revision']
    
    def get_current_hash(self) -> str:
        """
        Get the current hash from datapackage.json.
        
        Returns:
            Current hash string
            
        Raises:
            ValueError: If hash field is missing from datapackage
            FileNotFoundError: If datapackage file doesn't exist
        """
        datapackage = self._fetch_datapackage()
        
        if 'hash' not in datapackage:
            raise ValueError(
                f"Datapackage at {self.datapackage_url} is missing required 'hash' field. "
                f"This field is required for change detection."
            )
        
        return datapackage['hash']
    
    def get_files_to_process(self, existing_urls: Set[str], existing_revision: str) -> List[Dict]:
        """
        Get list of files that need processing based on change detection.
        
        Args:
            existing_urls: Set of URLs that have already been processed
            existing_revision: Revision string from last processing
            
        Returns:
            List of file metadata dictionaries with keys:
            - url: Original PDF URL
            - filename: Local filename
            - title: Document title (optional)
            - date: Document date (optional)
        """
        current_revision = self.get_current_revision()
        index_data = self._fetch_index()
        
        files_to_process = []
        
        # If revision changed, process all files
        if current_revision != existing_revision:
            logger.info(f"Revision changed from {existing_revision} to {current_revision}, processing all files")
            for _, row in index_data.iterrows():
                files_to_process.append({
                    'url': row['url'],
                    'filename': row['filename'],
                    'title': row.get('title', ''),
                    'date': row.get('date', '')
                })
        else:
            # Only process new files
            for _, row in index_data.iterrows():
                if row['url'] not in existing_urls:
                    files_to_process.append({
                        'url': row['url'],
                        'filename': row['filename'],
                        'title': row.get('title', ''),
                        'date': row.get('date', '')
                    })
            
            logger.info(f"Revision unchanged ({current_revision}), processing {len(files_to_process)} new files")
            logger.debug(f"Existing URLs: {existing_urls}")
            logger.debug(f"Index URLs: {[row['url'] for _, row in index_data.iterrows()]}")
        
        logger.info(f"Found {len(files_to_process)} files to process out of {len(index_data)} total files")
        return files_to_process
    
    def download_pdf(self, filename: str, download_dir: str) -> str:
        """
        Download a PDF file from the Open Budget datapackage.
        
        Args:
            filename: Filename from index.csv
            download_dir: Directory to save the PDF
            
        Returns:
            Path to the downloaded PDF file
            
        Raises:
            FileNotFoundError: If PDF file doesn't exist in test data
        """
        # Create download directory if it doesn't exist
        os.makedirs(download_dir, exist_ok=True)
        
        # Look for the PDF in test data directory
        test_pdf_path = Path(self.test_data_dir) / filename
        
        if not test_pdf_path.exists():
            # If the specific file doesn't exist, create a mock PDF
            mock_pdf_path = Path(download_dir) / filename
            self._create_mock_pdf(mock_pdf_path)
            logger.info(f"Created mock PDF: {mock_pdf_path}")
            return str(mock_pdf_path)
        
        # Copy the test PDF to download directory
        local_path = Path(download_dir) / filename
        import shutil
        shutil.copy2(test_pdf_path, local_path)
        
        logger.info(f"Copied test PDF from {test_pdf_path} to {local_path}")
        return str(local_path)
    
    def _create_mock_pdf(self, pdf_path: Path):
        """Create a mock PDF file for testing."""
        # Create a simple text file that looks like a PDF header
        with open(pdf_path, 'w', encoding='utf-8') as f:
            f.write("%PDF-1.4\n")
            f.write("1 0 obj\n")
            f.write("<<\n")
            f.write("/Type /Catalog\n")
            f.write("/Pages 2 0 R\n")
            f.write(">>\n")
            f.write("endobj\n")
            f.write("2 0 obj\n")
            f.write("<<\n")
            f.write("/Type /Pages\n")
            f.write("/Kids [3 0 R]\n")
            f.write("/Count 1\n")
            f.write(">>\n")
            f.write("endobj\n")
            f.write("3 0 obj\n")
            f.write("<<\n")
            f.write("/Type /Page\n")
            f.write("/Parent 2 0 R\n")
            f.write("/MediaBox [0 0 612 792]\n")
            f.write("/Contents 4 0 R\n")
            f.write(">>\n")
            f.write("endobj\n")
            f.write("4 0 obj\n")
            f.write("<<\n")
            f.write("/Length 44\n")
            f.write(">>\n")
            f.write("stream\n")
            f.write("BT\n")
            f.write("/F1 12 Tf\n")
            f.write("72 720 Td\n")
            f.write("(Mock PDF for testing) Tj\n")
            f.write("ET\n")
            f.write("endstream\n")
            f.write("endobj\n")
            f.write("xref\n")
            f.write("0 5\n")
            f.write("0000000000 65535 f \n")
            f.write("0000000009 00000 n \n")
            f.write("0000000058 00000 n \n")
            f.write("0000000115 00000 n \n")
            f.write("0000000204 00000 n \n")
            f.write("trailer\n")
            f.write("<<\n")
            f.write("/Size 5\n")
            f.write("/Root 1 0 R\n")
            f.write(">>\n")
            f.write("startxref\n")
            f.write("297\n")
            f.write("%%EOF\n")
    
    def _fetch_datapackage(self) -> Dict:
        """Fetch and cache datapackage.json from local file."""
        if self._cached_datapackage is None:
            datapackage_path = Path(self.test_data_dir) / "mock_datapackage.json"
            
            if not datapackage_path.exists():
                raise FileNotFoundError(f"Mock datapackage not found: {datapackage_path}")
            
            logger.info(f"Loading mock datapackage from {datapackage_path}")
            with open(datapackage_path, 'r', encoding='utf-8') as f:
                self._cached_datapackage = json.load(f)
        
        return self._cached_datapackage
    
    def _fetch_index(self) -> pd.DataFrame:
        """Fetch and cache index.csv from local file."""
        if self._cached_index is None:
            index_path = Path(self.test_data_dir) / "mock_index.csv"
            
            if not index_path.exists():
                raise FileNotFoundError(f"Mock index not found: {index_path}")
            
            logger.info(f"Loading mock index from {index_path}")
            self._cached_index = pd.read_csv(index_path, encoding='utf-8')
            
            # Validate required columns
            required_columns = ['url', 'filename']
            missing_columns = [col for col in required_columns if col not in self._cached_index.columns]
            if missing_columns:
                raise ValueError(f"Index CSV missing required columns: {missing_columns}")
        
        return self._cached_index 