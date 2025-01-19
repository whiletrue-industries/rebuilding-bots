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
        """Delete the vector store and all its associated files"""
        pass

    @abstractmethod
    def delete_files(self, vector_store_id: str) -> None:
        """Delete all files associated with a vector store without deleting the store itself"""
        pass
