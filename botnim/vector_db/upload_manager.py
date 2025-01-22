import asyncio
from typing import List, Dict
import logging
from enum import Enum

class UploadStatus(Enum):
    SUCCESS = "success"
    RATE_LIMITED = "rate_limited"
    FAILED = "failed"

async def _upload_single_file(
    file: Dict,
    retries: int = 3,
    initial_backoff: float = 1.0,
    max_backoff: float = 30.0
) -> Dict:
    """Upload a single file with robust error handling and backoff
    
    Args:
        file: File dictionary to upload
        retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
        max_backoff: Maximum backoff time in seconds
    """
    for attempt in range(retries):
        try:
            result = await upload_single_file(file)
            return result
            
        except Exception as e:
            backoff = min(initial_backoff * (2 ** attempt), max_backoff)
            
            if "rate_limit" in str(e).lower():
                logging.warning(f"Rate limited on attempt {attempt + 1}, waiting {backoff}s")
                await asyncio.sleep(backoff)
                continue
                
            if attempt < retries - 1:
                logging.warning(f"Upload failed on attempt {attempt + 1}, retrying in {backoff}s: {str(e)}")
                await asyncio.sleep(backoff)
                continue
                
            logging.error(f"Upload failed after {retries} attempts: {str(e)}")
            raise

async def _upload_batch_with_polling(
    files: List[Dict],
    batch_size: int = 10,
    upload_delay: float = 0.1,
    max_concurrent: int = 3
) -> List[Dict]:
    """Upload files in batches with rate limiting and concurrency control
    
    Args:
        files: List of file dictionaries to upload
        batch_size: Number of files to process in each batch
        upload_delay: Delay between individual file uploads
        max_concurrent: Maximum number of concurrent uploads
    """
    results = []
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def upload_with_semaphore(file: Dict) -> Dict:
        async with semaphore:
            if results:  # Add delay between uploads except for first file
                await asyncio.sleep(upload_delay)
            return await _upload_single_file(file)
    
    for i in range(0, len(files), batch_size):
        batch = files[i:i + batch_size]
        logging.debug(f"Processing batch of {len(batch)} files")
        
        # Create tasks with semaphore control
        upload_tasks = [
            asyncio.create_task(upload_with_semaphore(file))
            for file in batch
        ]
        
        try:
            batch_results = await asyncio.gather(*upload_tasks, return_exceptions=True)
            
            # Process results and handle errors
            for result in batch_results:
                if isinstance(result, Exception):
                    logging.error(f"Upload failed: {str(result)}")
                else:
                    results.append(result)
                    
        except Exception as e:
            logging.error(f"Batch upload failed: {str(e)}")
        
        logging.debug(f"Completed batch of {len(batch)} files")
    
    return results

async def upload_files(files: List[Dict]) -> List[Dict]:
    """Main entry point for file uploads"""
    if not files:
        return []
    
    logging.debug(f"Starting batch upload of {len(files)} files")
    uploaded_files = await _upload_batch_with_polling(files)
    logging.debug(f"Completed batch upload of {len(files)} files")
    return uploaded_files

async def upload_single_file(file: Dict):
    """Helper function to upload a single file"""
    # Your existing single file upload logic here
    # This should be the async version of your current upload code
    pass 