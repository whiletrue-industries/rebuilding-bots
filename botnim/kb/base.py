from abc import ABC, abstractmethod
from typing import List, Union, BinaryIO, Tuple
from ..config import get_logger

logger = get_logger(__name__)

class VectorStore(ABC):
    """Abstract base class for vector store implementations"""
    
    def __init__(self, production: bool = False):
        self.production = production

    def get_environment_name(self, name: str) -> str:
        """Add environment suffix if not in production"""
        logger.info(f"get_environment_name called with: {name}, production: {self.production}")
        if not self.production:
            result = f"{name} - פיתוח"
            logger.info(f"Returning modified name: {result}")
            return result
        logger.info(f"Returning original name: {name}")
        return name
    
    @abstractmethod
    def create(self, name: str) -> str:
        """Create a new vector store and return its ID"""
        pass

    @abstractmethod
    def upload_documents(self, kb_id: str, documents: List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]) -> None:
        """Upload documents to the vector store"""
        pass

    @abstractmethod
    def delete_files(self, vector_store_id: str) -> None:
        """Delete all files associated with a vector store"""
        pass
