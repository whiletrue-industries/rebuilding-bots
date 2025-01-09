from abc import ABC, abstractmethod
from typing import List, Union, BinaryIO, Tuple

class KnowledgeBase(ABC):
    """Abstract base class for knowledge base implementations"""
    
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
