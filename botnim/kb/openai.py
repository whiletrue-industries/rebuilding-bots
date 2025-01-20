import os
from pathlib import Path
from typing import List, Union, BinaryIO, Tuple
from openai import OpenAI
from dotenv import load_dotenv
from .base import VectorStore
from ..config import get_logger
import io

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

    def create(self, name: str) -> str:
        """Create a new vector store and return its ID"""
        try:
            vector_store = self.client.beta.vector_stores.create(
                name=name  # name should already include environment suffix
            )
            return vector_store.id
        except Exception as e:
            logger.error(f"Failed to create vector store {name}: {str(e)}")
            raise

    def upload_documents(self, kb_id: str, documents: List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]) -> None:
        """Upload documents to the vector store"""
        try:
            # Get list of existing files
            existing_files = set()
            files_list = self.client.beta.vector_stores.files.list(vector_store_id=kb_id)
            for file in files_list.data:
                # Try both name and filename since API responses can vary
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
                    
                    # Store the content and metadata instead of the file object
                    if isinstance(file_data, (str, Path)):
                        with open(file_data, 'rb') as f:
                            content = f.read()
                    else:
                        content = file_data.read()
                        file_data.seek(0)  # Reset position for potential reuse
                    
                    document_data.append((filename, content))
            
            # Process in batches
            BATCH_SIZE = 20
            for i in range(0, len(document_data), BATCH_SIZE):
                batch = document_data[i:i + BATCH_SIZE]
                self._upload_batch_with_polling(kb_id, batch)
                
        except Exception as e:
            logger.error(f"Failed to upload documents to vector store {kb_id}: {str(e)}")
            raise

    def _upload_batch_with_polling(self, kb_id: str, batch: List[Tuple[str, bytes]]):
        """Upload a batch of files with polling for completion"""
        try:
            logger.info(f"Processing batch of {len(batch)} files")
            
            # Create file batch with fresh file objects
            batch_files = []
            for filename, content in batch:
                file_obj = io.BytesIO(content)
                file_obj.name = filename
                batch_files.append((filename, file_obj))
            
            # Upload and poll for completion
            batch_result = self.client.beta.vector_stores.file_batches.upload_and_poll(
                vector_store_id=kb_id,
                files=batch_files
            )
            
            # Check status instead of failed_files
            if batch_result.status != 'completed':
                logger.error(f"Batch upload failed with status: {batch_result.status}")
                raise Exception(f"Failed to upload batch: {batch_result.status}")
                
            logger.info(f"Successfully uploaded batch to vector store {kb_id}")
            
        except Exception as e:
            logger.error(f"Failed to process batch: {str(e)}")
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

    def delete_files(self, vector_store_id: str) -> None:
        """Delete all files associated with a vector store"""
        try:
            files = self.client.beta.vector_stores.files.list(
                vector_store_id=vector_store_id
            )
            for file in files.data:
                self.client.beta.vector_stores.files.delete(
                    vector_store_id=vector_store_id,
                    file_id=file.id
                )
                # Also delete the file itself
                self.client.files.delete(file.id)
            logger.info(f"Deleted all files from vector store: {vector_store_id}")
        except Exception as e:
            logger.error(f"Failed to delete files from vector store {vector_store_id}: {str(e)}")
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

    def setup_contexts(self, name: str, context_documents: List[Tuple[str, List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]]]) -> dict:
        """OpenAI implementation uses a single vector store for all contexts"""
        # Check if vector store exists
        name_with_env = name
        existing_stores = self.list()
        vector_store_id = None
        
        for store in existing_stores:
            if store['name'] == name_with_env:
                vector_store_id = store['id']
                logger.info(f"Found existing vector store: {vector_store_id}")
                self.delete_files(vector_store_id)
                break
        
        if vector_store_id is None:
            vector_store_id = self.create(name_with_env)
            logger.info(f"Created new vector store: {vector_store_id}")
        
        # Combine all documents into one list
        all_documents = []
        for _, documents in context_documents:
            all_documents.extend(documents)
            
        self.upload_documents(vector_store_id, all_documents)
        
        return {
            "tools": [{"type": "file_search"}],
            "tool_resources": {"file_search": {"vector_store_ids": [vector_store_id]}}
        }
