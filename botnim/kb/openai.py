import os
import logging
from typing import List, Union, BinaryIO, Tuple
from openai import OpenAI
from dotenv import load_dotenv
from .base import KnowledgeBase

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get API key from environment
api_key = os.environ.get('OPENAI_API_KEY')
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set. Please check your .env file.")

client = OpenAI(api_key=api_key)

class OpenAIVectorStore(KnowledgeBase):
    def __init__(self, production: bool = False):
        super().__init__(production)
        self.client = client
        self.asst_params = {}  # Store assistant parameters

    def set_assistant_params(self, params: dict):
        """Set parameters for assistant creation/update"""
        self.asst_params = params

    def create(self, name: str) -> Tuple[str, str]:
        """Create or update assistant with a new vector store"""
        try:
            # First check if assistant exists
            logger.info(f"Checking if assistant exists for {name}")
            exists, old_vector_store_id, assistant_id = self.exists(name)
            logger.info(f"Exists check result: exists={exists}, old_vs={old_vector_store_id}, asst={assistant_id}")
            
            # Create new vector store
            logger.info(f"Creating new vector store for {name}")
            vector_store = self.client.beta.vector_stores.create(
                name=self.get_environment_name(name)
            )
            logger.info(f"Created vector store: {vector_store.id}")

            # First update/create without tool_resources to ensure base configuration
            base_params = {
                **self.asst_params,
                'tools': [{"type": "file_search"}]
            }

            if not exists:
                # Create new assistant if it doesn't exist
                logger.info(f"Creating new assistant for {name}")
                assistant = self.client.beta.assistants.create(
                    name=self.get_environment_name(name),
                    model="gpt-4o",
                    description=f"Assistant using knowledge base for {name}",
                    **base_params
                )
                assistant_id = assistant.id
                logger.info(f"Created new assistant: {assistant_id}")
            else:
                # Update existing assistant base configuration
                logger.info(f"Updating assistant {assistant_id} configuration")
                assistant = self.client.beta.assistants.update(
                    assistant_id=assistant_id,
                    **base_params
                )

            # Then make a separate call to update tool_resources
            logger.info(f"Connecting vector store {vector_store.id} to assistant {assistant_id}")
            assistant = self.client.beta.assistants.update(
                assistant_id=assistant_id,
                tool_resources={
                    "file_search": {
                        "vector_store_ids": [vector_store.id]
                    }
                }
            )
            logger.info(f"Updated assistant {assistant_id} with vector store {vector_store.id}")
            
            return vector_store.id, assistant_id
        except Exception as e:
            logger.error(f"Failed to create/update knowledge base {name}: {str(e)}")
            raise

    def exists(self, name: str) -> Tuple[bool, str, str]:
        """Check if an assistant exists and return (exists, vector_store_id, assistant_id)"""
        try:
            env_name = self.get_environment_name(name)
            logger.info(f"Looking for assistant with name: {env_name}")
            assistants = self.client.beta.assistants.list()
            for assistant in assistants:
                if assistant.name == env_name:
                    logger.info(f"Found assistant: {assistant.id}")
                    # Get vector store ID if it exists
                    vector_store_id = ""
                    if hasattr(assistant, 'tool_resources') and assistant.tool_resources:
                        logger.info(f"Assistant has tool_resources: {assistant.tool_resources}")
                        file_search = getattr(assistant.tool_resources, 'file_search', None)
                        if file_search and file_search.vector_store_ids:
                            vector_store_id = file_search.vector_store_ids[0]
                            logger.info(f"Found vector store: {vector_store_id}")
                    return True, vector_store_id, assistant.id
            logger.info("Assistant not found")
            return False, "", ""
        except Exception as e:
            logger.error(f"Failed to check existence for {name}: {str(e)}")
            raise

    def delete(self, kb_id: str) -> None:
        """Delete an assistant and its associated files"""
        try:
            # First delete all files associated with this assistant
            files = self.client.beta.assistants.files.list(
                assistant_id=kb_id
            )
            for file in files.data:
                self.client.beta.assistants.files.delete(
                    assistant_id=kb_id,
                    file_id=file.id
                )
                # Also delete the file itself since we're cleaning up completely
                self.client.files.delete(file.id)
            
            # Then delete the assistant itself
            self.client.beta.assistants.delete(kb_id)
            logger.info(f"Deleted assistant/KB: {kb_id}")
        except Exception as e:
            logger.error(f"Failed to delete knowledge base {kb_id}: {str(e)}")
            raise

    def upload_documents(self, kb_id: str, documents: List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]) -> None:
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
                    filename, file_stream, content_type = doc
                else:
                    filename = getattr(doc, 'name', 'unnamed_file')
                    file_stream = doc
                    content_type = 'text/plain'

                # Skip if file already exists
                if filename in existing_files:
                    logger.info(f"Skipping existing file: {filename}")
                    continue

                # Create file with proper name by creating a named file-like object
                from io import BytesIO
                if isinstance(file_stream, BytesIO):
                    content = file_stream.getvalue()
                else:
                    content = file_stream.read()
                    file_stream.close()

                named_file = BytesIO(content)
                named_file.name = filename
                
                # Create file
                file = self.client.files.create(
                    file=named_file,
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

    def create_vector_store(self, name: str) -> str:
        """Create a new vector store and return its ID"""
        try:
            env_name = self.get_environment_name(name)
            logger.info(f"Creating new vector store with name: {env_name}")
            vector_store = self.client.beta.vector_stores.create(
                name=env_name
            )
            logger.info(f"Created vector store: {vector_store.id}")
            return vector_store.id
        except Exception as e:
            logger.error(f"Failed to create vector store {name}: {str(e)}")
            raise
