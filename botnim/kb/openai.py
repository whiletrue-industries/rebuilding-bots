import os
from pathlib import Path
from typing import List, Union, BinaryIO, Tuple, Optional
from openai import OpenAI
from dotenv import load_dotenv
from .base import VectorStore, ProgressCallback
from ..config import get_logger
import io
import time
import openai
import asyncio
import aiohttp
from abc import ABC, abstractmethod

logger = get_logger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get API key from environment
api_key = os.environ.get('OPENAI_API_KEY')
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set. Please check your .env file.")

client = OpenAI(api_key=api_key)

class OpenAIVectorStore(VectorStore):
    def __init__(self, production: bool = False):
        super().__init__(production)
        self.client = client
        self.batch_size = 50  # Increased from 20 to 50 for better throughput
        self._processed_files = set()

    async def create(self, name: str) -> str:
        """Create a new vector store and return its ID"""
        try:
            vector_store = self.client.beta.vector_stores.create(
                name=name  # name should already include environment suffix
            )
            return vector_store.id
        except Exception as e:
            logger.error(f"Failed to create vector store {name}: {str(e)}")
            raise

    async def _upload_single_file(self, file_tuple: Tuple[str, bytes], session: aiohttp.ClientSession, retries=3):
        """Upload a single file asynchronously with retries"""
        filename, content = file_tuple
        file_obj = io.BytesIO(content)
        file_obj.name = filename
        
        for attempt in range(retries):
            try:
                headers = {
                    "Authorization": f"Bearer {self.client.api_key}",
                }
                
                data = aiohttp.FormData()
                data.add_field('purpose', 'assistants')
                data.add_field('file', file_obj, filename=filename)
                
                async with session.post(
                    'https://api.openai.com/v1/files',
                    headers=headers,
                    data=data,
                    timeout=60  # 60 second timeout per file
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.debug(f"Uploaded file {filename} with ID {result['id']}")
                        return result
                    elif response.status == 500 and attempt < retries - 1:
                        error = await response.text()
                        logger.warning(f"Attempt {attempt + 1}/{retries} failed for {filename}: {error}")
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                    else:
                        error = await response.text()
                        raise Exception(f"Upload failed with status {response.status}: {error}")
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Attempt {attempt + 1}/{retries} failed for {filename}: {str(e)}")
                    await asyncio.sleep(1 * (attempt + 1))
                    continue
                logger.error(f"Failed to upload file {filename} after {retries} attempts: {str(e)}")
                raise

    async def _upload_batch_with_polling(self, kb_id: str, batch: List[Tuple[str, bytes]]):
        """Upload a batch of files concurrently with polling for completion"""
        try:
            logger.info(f"Processing batch of {len(batch)} files")
            
            # Use connection pooling with higher limits
            conn = aiohttp.TCPConnector(limit=50)
            timeout = aiohttp.ClientTimeout(total=300)
            
            # Upload files concurrently
            async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
                tasks = [self._upload_single_file(file_tuple, session) for file_tuple in batch]
                chunk_size = 10
                successful_files = []
                
                for i in range(0, len(tasks), chunk_size):
                    chunk = tasks[i:i + chunk_size]
                    chunk_results = await asyncio.gather(*chunk, return_exceptions=True)
                    successful_files.extend([f for f in chunk_results if isinstance(f, dict)])
                    
                if len(successful_files) != len(batch):
                    logger.warning(f"Only {len(successful_files)} of {len(batch)} files uploaded successfully")
            
            if not successful_files:
                raise Exception("No files were uploaded successfully")
            
            # Create file batch for vector store
            file_ids = [f['id'] for f in successful_files]
            batch_result = self.client.beta.vector_stores.file_batches.create(
                vector_store_id=kb_id,
                file_ids=file_ids
            )
            
            # Poll for completion with exponential backoff
            poll_interval = 1.0
            max_interval = 10.0
            max_attempts = 30  # Maximum number of polling attempts
            attempts = 0
            
            while attempts < max_attempts:
                attempts += 1
                batch_info = self.client.beta.vector_stores.file_batches.retrieve(
                    vector_store_id=kb_id,
                    batch_id=batch_result.id
                )
                
                if batch_info.status == 'succeeded':
                    logger.info(f"Batch processing completed after {attempts} attempts")
                    break
                elif batch_info.status == 'failed':
                    raise Exception(f"Batch processing failed: {batch_info.error}")
                elif attempts == max_attempts:
                    raise Exception(f"Batch processing timed out after {max_attempts} attempts")
                
                # Log status and wait with exponential backoff
                logger.debug(f"Batch processing in progress (attempt {attempts}/{max_attempts}), status: {batch_info.status}")
                await asyncio.sleep(poll_interval)
                poll_interval = min(poll_interval * 1.5, max_interval)
            
            logger.info(f"Successfully uploaded batch to vector store {kb_id}")
            return successful_files

        except Exception as e:
            logger.error(f"Failed to process batch: {str(e)}")
            raise

    async def upload_documents(self, kb_id: str, documents: List[Union[BinaryIO, Tuple[str, BinaryIO, str]]], progress_callback: Optional[ProgressCallback] = None) -> None:
        """Upload documents to the vector store with progress tracking"""
        try:
            # Get list of existing files
            existing_files = set()
            files_list = self.client.beta.vector_stores.files.list(vector_store_id=kb_id)
            for file in files_list.data:
                file_name = getattr(file, 'filename', None) or getattr(file, 'name', None)
                if file_name:
                    existing_files.add(file_name)
            logger.info(f"Found {len(existing_files)} existing files in vector store")

            # Store document data for batching
            document_data = []
            for doc in documents:
                if isinstance(doc, tuple):
                    filename, file_data, content_type = doc
                    if filename in existing_files:
                        logger.info(f"Skipping existing file: {filename}")
                        continue
                    
                    # Ensure filename has .md extension
                    if not filename.lower().endswith('.md'):
                        filename = f"{filename}.md"
                    
                    # Store the content and metadata
                    if isinstance(file_data, (str, Path)):
                        with open(file_data, 'rb') as f:
                            content = f.read()
                    else:
                        content = file_data.read()
                        file_data.seek(0)  # Reset position for potential reuse
                    
                    document_data.append((filename, content))
            
            # Process in batches
            total_files = len(document_data)
            batch_size = 20  # Process 20 files at a time
            for i in range(0, total_files, batch_size):
                batch = document_data[i:i + batch_size]
                logger.info(f'Processing batch {i//batch_size + 1} of {(total_files + batch_size - 1)//batch_size} '
                           f'({len(batch)} files, {i+1}-{min(i+len(batch), total_files)} of {total_files})')
                
                try:
                    # Run async upload directly
                    await self._upload_batch_with_polling(kb_id, batch)
                    if progress_callback:
                        await progress_callback.update(len(batch))
                except Exception as e:
                    logger.error(f"Failed to process batch starting at index {i}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Failed to upload documents to vector store {kb_id}: {str(e)}")
            raise

    def delete(self, vector_store_id: str) -> None:
        """Delete the vector store and all its associated files"""
        try:
            # First delete all files
            self.delete_files(vector_store_id)
            # Then delete the vector store
            self.client.beta.vector_stores.delete(vector_store_id)
            logger.info(f"Deleted vector store: {vector_store_id}")
        except Exception as e:
            logger.error(f"Failed to delete vector store {vector_store_id}: {str(e)}")
            raise

    async def delete_files(self, kb_id: str) -> None:
        """Delete all files from a vector store"""
        try:
            # Get list of files in vector store
            files_list = self.client.beta.vector_stores.files.list(vector_store_id=kb_id)
            if not files_list.data:
                logger.info(f"No files to delete in vector store {kb_id}")
                return

            # Delete each file from vector store and files API
            for file in files_list.data:
                try:
                    # First remove from vector store
                    self.client.beta.vector_stores.files.delete(
                        vector_store_id=kb_id,
                        file_id=file.id
                    )
                    
                    # Then delete the file itself
                    try:
                        self.client.files.delete(file.id)
                    except openai.NotFoundError:
                        # File was already deleted, that's fine
                        logger.debug(f"File {file.id} was already deleted")
                        pass
                    except Exception as e:
                        logger.error(f"Failed to delete file {file.id}: {str(e)}")
                        
                except Exception as e:
                    logger.error(f"Failed to delete file {file.id} from vector store: {str(e)}")
                    continue

            logger.info(f"Deleted all files from vector store: {kb_id}")
            
            # Verify deletion
            files_list = self.client.beta.vector_stores.files.list(vector_store_id=kb_id)
            logger.info(f"Found {len(files_list.data)} existing files in vector store")

        except Exception as e:
            logger.error(f"Failed to delete files from vector store {kb_id}: {str(e)}")
            raise

    def list(self) -> List[dict]:
        """List all vector stores"""
        try:
            vector_stores = self.client.beta.vector_stores.list()
            return [
                {
                    'id': vs.id,
                    'name': vs.name,
                    'created_at': vs.created_at
                }
                for vs in vector_stores.data
            ]
        except Exception as e:
            logger.error(f"Failed to list vector stores: {str(e)}")
            raise

    async def setup_contexts(
        self,
        name: str,
        context_documents: List[Tuple[str, List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]]],
        progress_callback: Optional[ProgressCallback] = None
    ) -> dict:
        """OpenAI implementation with progress tracking"""
        # Check if vector store exists
        existing_stores = self.list()
        vector_store_id = None
        
        for store in existing_stores:
            if store['name'] == name:  # name already includes environment suffix
                vector_store_id = store['id']
                logger.info(f"Found existing vector store: {vector_store_id}")
                await self.delete_files(vector_store_id)
                break
        
        if vector_store_id is None:
            vector_store_id = await self.create(name)
            logger.info(f"Created new vector store: {vector_store_id}")
        
        # Combine all documents into one list
        all_documents = []
        for _, documents in context_documents:
            all_documents.extend(documents)
        
        # Upload documents with progress tracking
        await self.upload_documents(
            vector_store_id,
            all_documents,
            progress_callback=progress_callback
        )
        
        return {
            "tools": [{"type": "file_search"}],
            "tool_resources": {"file_search": {"vector_store_ids": [vector_store_id]}}
        }

    async def process_files_batch(self, files: List[Tuple[str, BinaryIO, str]], batch_size: int = 20):
        """Process a batch of files"""
        results = []
        
        # Filter out already processed files
        new_files = [(name, file_obj, content_type) 
                    for name, file_obj, content_type in files 
                    if name not in self._processed_files]
        
        if not new_files:
            return results

        # Process in smaller batches
        for i in range(0, len(new_files), batch_size):
            batch = new_files[i:i + batch_size]
            try:
                # Upload files
                uploaded = []
                for name, file_obj, content_type in batch:
                    response = self.client.files.create(
                        file=file_obj,
                        purpose="assistants"
                    )
                    uploaded.append(response)
                    self._processed_files.add(name)
                    
                results.extend(uploaded)
                logger.info(f"Successfully processed batch of {len(batch)} files")
                
            except Exception as e:
                logger.error(f"Failed to process batch: {str(e)}")
                # Clean up any uploaded files from this batch
                for file in uploaded:
                    try:
                        self.client.files.delete(file.id)
                    except Exception as delete_error:
                        logger.error(f"Failed to delete file {file.id}: {str(delete_error)}")
                raise
                
        return results
