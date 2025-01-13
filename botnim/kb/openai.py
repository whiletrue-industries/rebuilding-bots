import os
from typing import List, Union, BinaryIO, Tuple
from openai import OpenAI
from dotenv import load_dotenv
from .base import VectorStore
from ..config import get_logger

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
                name=self.get_environment_name(name)
            )
            logger.info(f"Created vector store: {vector_store.id}")
            return vector_store.id
        except Exception as e:
            logger.error(f"Failed to create vector store {name}: {str(e)}")
            raise

    def upload_documents(self, kb_id: str, documents: List[Union[BinaryIO, Tuple[str, str, str]]]) -> None:
        """Upload documents to the vector store"""
        try:
            # Get list of existing files in the vector store
            existing_files = set()
            files_list = self.client.beta.vector_stores.files.list(vector_store_id=kb_id)
            for file in files_list.data:
                existing_files.add(file.filename)
            logger.info(f"Found {len(existing_files)} existing files in vector store")

            for doc in documents:
                if isinstance(doc, tuple):
                    filename, file_path, content_type = doc
                    if filename in existing_files:
                        logger.info(f"Skipping existing file: {filename}")
                        continue
                    # Open file only when needed
                    with open(file_path, 'rb') as file_stream:
                        file = self.client.files.create(
                            file=file_stream,
                            purpose='assistants'
                        )
                else:
                    # Handle direct file objects (for backward compatibility)
                    filename = getattr(doc, 'name', 'unnamed_file')
                    if filename in existing_files:
                        logger.info(f"Skipping existing file: {filename}")
                        continue
                    file = self.client.files.create(
                        file=doc,
                        purpose='assistants'
                    )
                
                logger.info(f"Created file: {filename} (ID: {file.id})")
                
                # Add to vector store
                self.client.beta.vector_stores.files.create(
                    vector_store_id=kb_id,
                    file_id=file.id
                )
                logger.info(f"Added {filename} to vector store {kb_id}")
                
        except Exception as e:
            logger.error(f"Failed to upload documents to vector store {kb_id}: {str(e)}")
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
