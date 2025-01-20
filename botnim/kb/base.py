from abc import ABC, abstractmethod
from typing import List, Union, BinaryIO, Tuple
from ..config import get_logger

logger = get_logger(__name__)

class VectorStore(ABC):
    """Abstract base class for vector store implementations"""
    
    def __init__(self, production: bool = False):
        self.production = production

    @abstractmethod
    def create(self, name: str) -> str:
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
    def upload_documents(self, kb_id: str, documents: List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]) -> None:
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
    def delete_files(self, vector_store_id: str) -> None:
        """Delete all files associated with a vector store without deleting the store itself"""
        pass

    @abstractmethod
    def setup_contexts(self, name: str, documents: List[Tuple[str, Union[BinaryIO, Tuple[str, BinaryIO, str]]]]) -> str:
        """Set up contexts with their documents
        
        Args:
            name: Base name for the vector store(s)
            documents: List of (context_name, document) tuples
            
        Returns:
            str: ID of the primary vector store
            
        Each implementation can decide how to organize the contexts:
        - Single vector store for all contexts
        - Separate vector store per context
        - Custom grouping strategy
        """
        pass
