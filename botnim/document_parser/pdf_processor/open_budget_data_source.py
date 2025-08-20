"""
Open Budget Data Source for PDF Pipeline.

This module provides integration with Open Budget's datapackage infrastructure
for fetching PDF metadata and change detection.
"""

import requests
import pandas as pd
from typing import List, Dict, Set
import os
from urllib.parse import urljoin
from io import StringIO

from botnim.config import get_logger

logger = get_logger(__name__)


class OpenBudgetDataSource:
    """
    Data source for Open Budget datapackages.
    
    Handles fetching and parsing datapackage.json and index.csv files
    for change detection and file metadata.
    """
    
    def __init__(self, index_csv_url: str, datapackage_url: str):
        """
        Initialize Open Budget pdf data source.
        
        Args:
            index_csv_url: URL to the index.csv file
            datapackage_url: URL to the datapackage.json file
        """
        self.index_csv_url = index_csv_url
        self.datapackage_url = datapackage_url
        self._cached_datapackage = None
        self._cached_index = None
        
    def get_current_revision(self) -> str:
        """
        Get the current revision from datapackage.json.
        
        Returns:
            Current revision string
            
        Raises:
            ValueError: If revision field is missing from datapackage
            requests.RequestException: If unable to fetch datapackage
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
            requests.RequestException: If unable to fetch datapackage
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
            requests.RequestException: If download fails
        """
        # Construct the full URL for the PDF file
        base_url = self.index_csv_url.rsplit('/', 1)[0] + '/'
        pdf_url = urljoin(base_url, filename)
        
        # Create download directory if it doesn't exist
        os.makedirs(download_dir, exist_ok=True)
        
        # Download the file
        local_path = os.path.join(download_dir, filename)
        
        logger.info(f"Downloading {pdf_url} to {local_path}")
        response = requests.get(pdf_url, timeout=60)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Successfully downloaded {filename}")
        return local_path
    
    def _fetch_datapackage(self) -> Dict:
        """Fetch and cache datapackage.json."""
        if self._cached_datapackage is None:
            logger.info(f"Fetching datapackage from {self.datapackage_url}")
            response = requests.get(self.datapackage_url, timeout=30)
            response.raise_for_status()
            self._cached_datapackage = response.json()
        
        return self._cached_datapackage
    
    def _fetch_index(self) -> pd.DataFrame:
        """Fetch and cache index.csv."""
        if self._cached_index is None:
            logger.info(f"Fetching index from {self.index_csv_url}")
            response = requests.get(self.index_csv_url, timeout=30)
            response.raise_for_status()
            
            # Read CSV from response content
            self._cached_index = pd.read_csv(
                StringIO(response.text),
                encoding='utf-8'
            )
            
            # Validate required columns
            required_columns = ['url', 'filename']
            missing_columns = [col for col in required_columns if col not in self._cached_index.columns]
            if missing_columns:
                raise ValueError(f"Index CSV missing required columns: {missing_columns}")
        
        return self._cached_index 