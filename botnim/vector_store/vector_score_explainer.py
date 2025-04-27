"""Helper functions for explaining vector similarity scores"""

import numpy as np
from typing import List, Dict, Any, Optional
from ..config import get_logger

logger = get_logger(__name__)

def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Calculate cosine similarity between two vectors"""
    vec1_np = np.array(vec1)
    vec2_np = np.array(vec2)
    similarity = float(np.dot(vec1_np, vec2_np) / (np.linalg.norm(vec1_np) * np.linalg.norm(vec2_np)))
    logger.debug(f"Calculated cosine similarity: {similarity:.4f}")
    return similarity

def explain_vector_scores(query_vector: List[float], doc_vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate and explain vector similarity scores for document vectors.
    Uses max similarity to maximize recall - a document is considered relevant if ANY of its vectors
    match well with the query, rather than requiring all vectors to match well on average.
    
    Args:
        query_vector: Query embedding vector
        doc_vectors: List of document vectors with source field
        
    Returns:
        Dict with vector similarity explanations
    """
    logger.info(f"Processing {len(doc_vectors)} document vectors")
    logger.debug(f"Vector sources: {[vec.get('source', 'unknown') for vec in doc_vectors]}")
    
    vector_scores = []
    
    for vec in doc_vectors:
        source = vec.get("source", "unknown")
        logger.debug(f"Processing vector from source: {source}")
        score = cosine_similarity(query_vector, vec["vector"])
        vector_scores.append({
            "source": source,
            "similarity": score,
            "description": f"Cosine similarity with {source} vector: {score:.4f}"
        })
        logger.debug(f"Vector score for {source}: {score:.4f}")
    
    # Calculate combined score using max to maximize recall
    # This ensures a document is considered relevant if ANY of its vectors match well
    combined_score = np.max([s["similarity"] for s in vector_scores])
    logger.info(f"Combined vector score (max similarity): {combined_score:.4f}")
    
    return {
        "value": combined_score,
        "description": "Combined vector similarity score (max)",
        "details": vector_scores
    }

def combine_text_and_vector_scores(text_score: Dict[str, Any], vector_score: Dict[str, Any], 
                                 text_weight: float = 0.4, vector_weight: float = 0.6) -> Dict[str, Any]:
    """
    Combine text and vector similarity scores with explanations
    
    Args:
        text_score: Text similarity score explanation from Elasticsearch
        vector_score: Vector similarity score explanation 
        text_weight: Weight for text score (default 0.4)
        vector_weight: Weight for vector score (default 0.6)
        
    Returns:
        Combined score explanation
    """
    text_value = text_score.get("value", 0)
    vector_value = vector_score.get("value", 0)
    
    logger.info(f"Text score: {text_value:.4f}, Vector score: {vector_value:.4f}")
    logger.info(f"Weights - Text: {text_weight}, Vector: {vector_weight}")
    
    combined_value = (text_value * text_weight) + (vector_value * vector_weight)
    logger.info(f"Combined score: {combined_value:.4f}")
    
    return {
        "value": combined_value,
        "description": "Combined text and vector similarity score",
        "details": [
            {
                "value": text_value,
                "weight": text_weight,
                "description": "Text similarity score (BM25)",
                "explanation": text_score
            },
            {
                "value": vector_value, 
                "weight": vector_weight,
                "description": "Vector similarity score",
                "explanation": vector_score
            }
        ]
    } 