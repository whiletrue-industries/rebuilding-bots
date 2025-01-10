from abc import ABC, abstractmethod
from typing import List, Union, BinaryIO, Tuple
import logging

logger = logging.getLogger(__name__)

class KnowledgeBase(ABC):
    """Abstract base class for knowledge base implementations"""
    
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
        """Create a new knowledge base and return its ID"""
        pass

    @abstractmethod
    def exists(self, name: str) -> Tuple[bool, str]:
        """Check if a knowledge base exists and return (exists, id)"""
        pass

    @abstractmethod
    def delete(self, kb_id: str) -> None:
        """Delete a knowledge base by ID"""
        pass

    @abstractmethod
    def upload_documents(self, kb_id: str, documents: List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]) -> None:
        """Upload documents to the knowledge base"""
        pass
