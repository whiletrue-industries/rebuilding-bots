from abc import ABC, abstractmethod
from typing import List, Union, BinaryIO, Tuple, Optional, AsyncIterator
from ..config import get_logger
import logging

logger = get_logger(__name__)

class ProgressCallback:
    def __init__(self, total: int, logger: Optional[logging.Logger] = None):
        self.total = total
        self.current = 0
        self.logger = logger or logging.getLogger(__name__)
        
    async def update(self, count: int = 1):
        """Update progress and log status"""
        self.current += count
        percentage = (self.current / self.total) * 100
        self.logger.info(f"Progress: {self.current}/{self.total} ({percentage:.1f}%)")

class VectorStore(ABC):
    """Abstract base class for vector store implementations"""
    
    def __init__(self, production: bool = False):
        self.production = production

    @abstractmethod
    async def create(self, name: str) -> str:
        """Create a new vector store and return its ID"""
        pass

    @abstractmethod
    def list(self) -> List[dict]:
        """List all vector stores
        
        Returns:
            List of dictionaries containing vector store information:
            - id: str
            - name: str
            - created_at: datetime
        """
        pass

    @abstractmethod
    async def upload_documents(self, kb_id: str, documents: List[Union[BinaryIO, Tuple[str, BinaryIO, str]]], progress_callback: Optional[ProgressCallback] = None) -> None:
        """Upload documents to the vector store"""
        pass

    @abstractmethod
    def delete(self, vector_store_id: str) -> None:
        """Delete the vector store and all its associated files
        
        TODO: Currently not in use, but kept for future administrative tasks:
        - Cleanup of deprecated or unused vector stores
        - Complete removal of a bot's knowledge base
        - Managing vector store lifecycle in production
        
        Args:
            vector_store_id: ID of the vector store to delete
        """
        pass

    @abstractmethod
    async def delete_files(self, vector_store_id: str) -> None:
        """Delete all files associated with a vector store without deleting the store itself"""
        pass

    @abstractmethod
    async def setup_contexts(
        self,
        name: str,
        context_documents: List[Tuple[str, List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]]],
        progress_callback: Optional[ProgressCallback] = None
    ) -> dict:
        """Set up contexts with their documents
        
        Args:
            name: Base name for the vector store(s)
            context_documents: List of (context_name, documents) tuples
            
        Returns:
            dict: Tools and tool_resources for the assistant
        """
        pass
