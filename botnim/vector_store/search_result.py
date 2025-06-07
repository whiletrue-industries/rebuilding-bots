from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass
class SearchResult:
    """Data class for search results"""
    score: float
    id: str
    content: str
    full_content: str
    metadata: dict = None
    _explanation: dict = None  # Elasticsearch explanation
    text_score: float = None  # Text similarity score
    vector_score: float = None  # Vector similarity score
    
    @property
    def explanation(self) -> Optional[Dict[str, Any]]:
        """Get formatted explanation including both text and vector scores"""
        if not self._explanation:
            return None
            
        # Extract individual scores from combined explanation
        details = self._explanation.get('details', [])
        text_details = next((d for d in details if d['description'] == 'Text similarity score (BM25)'), {})
        vector_details = next((d for d in details if d['description'] == 'Vector similarity score'), {})
        
        self.text_score = text_details.get('value', 0)
        self.vector_score = vector_details.get('value', 0)
        
        return self._explanation 