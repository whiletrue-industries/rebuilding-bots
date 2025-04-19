import pandas as pd
import os
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Union
from dataclasses import dataclass
from botnim.query import run_query
from botnim.config import get_logger
from botnim.vector_store.vector_store_es import VectorStoreES

logger = get_logger(__name__)

# Constants
MAX_RESULTS = 20
DEFAULT_ENVIRONMENT = 'staging'

@dataclass
class QueryResult:
    """Data class to hold query result information."""
    doc_name: str
    score: float
    rank: int

@dataclass
class QueryEvaluation:
    """Data class to hold evaluation results for a single query."""
    total_score: float
    correct_score: float
    query_score: float  # correct_score / total_score
    num_results: int
    num_correct: int
    results: List[QueryResult]

def normalize_path(path: str) -> str:
    """Extract and normalize just the document name from a path."""
    return os.path.basename(str(path).strip())

def parse_store_id(store_id: str) -> Tuple[str, str, str]:
    """Parse store_id into bot name, context, and environment."""
    store_id_parts = store_id.split('__')
    if len(store_id_parts) < 2:
        raise ValueError(f"Invalid store_id format: {store_id}. Expected format: bot_name__context")
    
    bot_name = store_id_parts[0]
    context_name = store_id_parts[1]
    environment = DEFAULT_ENVIRONMENT
    
    logger.info(f"Using bot: {bot_name}, context: {context_name}, environment: {environment}")
    return bot_name, context_name, environment

def process_query_results(results: List[Union[dict, object]]) -> Tuple[List[str], Dict[str, float]]:
    """Process query results into document names and scores.
    
    Args:
        results: List of query results from Elasticsearch
        
    Returns:
        Tuple containing:
        - List of document names in order of retrieval
        - Dictionary mapping document names to their original scores
    """
    retrieved_docs = []
    doc_scores = {}
    
    for result in results:
        if isinstance(result, dict):
            doc_path = result.get('id', '')
            score = result.get('score', 0)
        else:
            doc_path = getattr(result, 'id', '')
            score = getattr(result, 'score', 0)
        
        if doc_path:
            doc_name = normalize_path(doc_path)
            retrieved_docs.append(doc_name)
            doc_scores[doc_name] = score
            logger.info(f"Retrieved document: {doc_name}, Score: {score}")
    
    return retrieved_docs, doc_scores

def calculate_metrics(
    retrieved_docs: List[str],
    expected_docs: List[str],
    doc_scores: Dict[str, float]
) -> QueryEvaluation:
    """Calculate query evaluation metrics based on Elasticsearch scores.
    
    Args:
        retrieved_docs: List of retrieved document names
        expected_docs: List of expected document names
        doc_scores: Dictionary mapping document names to their scores
        
    Returns:
        QueryEvaluation object containing aggregated metrics
    """
    # Calculate total score and correct score
    total_score = sum(doc_scores.values())
    correct_score = sum(score for doc, score in doc_scores.items() if doc in expected_docs)
    
    # Calculate query score (ratio of correct score to total score)
    query_score = (correct_score / total_score) if total_score > 0 else 0
    
    # Create QueryResult objects for all retrieved documents
    results = [
        QueryResult(
            doc_name=doc,
            score=score,
            rank=idx + 1  # Keep rank for reference but not used in scoring, for now
        )
        for idx, (doc, score) in enumerate(doc_scores.items())
    ]
    
    return QueryEvaluation(
        total_score=total_score,
        correct_score=correct_score,
        query_score=query_score,
        num_results=len(retrieved_docs),
        num_correct=sum(1 for doc in retrieved_docs if doc in expected_docs),
        results=results
    )

def create_row_dict(
    base_row: Dict,
    evaluation: QueryEvaluation,
    was_retrieved: bool,
    retrieved_rank: Optional[int] = None,
    retrieved_score: Optional[float] = None
) -> Dict:
    """Create a row dictionary with all required fields."""
    row = base_row.copy()
    row.update({
        'was_retrieved': was_retrieved,
        'retrieved_rank': retrieved_rank,  # Keep for reference but not used in scoring, for now
        'retrieved_score': retrieved_score,
        'total_score': evaluation.total_score,
        'correct_score': evaluation.correct_score,
        'query_score': evaluation.query_score,
        'num_results': evaluation.num_results,
        'num_correct': evaluation.num_correct
    })
    return row

def process_question(
    question_id: str,
    group: pd.DataFrame,
    store_id: str,
    max_results: int = MAX_RESULTS
) -> List[Dict]:
    """Process a single question and its expected documents."""
    question_text = group['question_text'].iloc[0]
    logger.info(f"Processing question {question_id}: {question_text}")
    
    total_expected = group['is_expected'].sum()
    logger.info(f"Total expected documents for question {question_id}: {total_expected}")
    
    try:
        results = run_query(
            store_id=store_id,
            query_text=question_text,
            num_results=max_results
        )
        
        if not results:
            logger.warning(f"No results returned for question {question_id}")
            return [
                create_row_dict(
                    group.loc[idx].to_dict(),
                    QueryEvaluation(
                        total_score=0,
                        correct_score=0,
                        query_score=0,
                        num_results=0,
                        num_correct=0,
                        results=[]
                    ),
                    was_retrieved=False
                )
                for idx in group.index
            ]
        
        retrieved_docs, doc_scores = process_query_results(results)
        logger.info(f"Retrieved {len(retrieved_docs)} documents for question {question_id}")
        logger.info(f"Retrieved documents: {retrieved_docs}")
        logger.info(f"Expected documents: {group['doc_filename'].tolist()}")
        
        evaluation = calculate_metrics(
            retrieved_docs=retrieved_docs,
            expected_docs=group['doc_filename'].tolist(),
            doc_scores=doc_scores
        )
        
        logger.info(f"Question {question_id} scores - Total: {evaluation.total_score:.2f}, Correct: {evaluation.correct_score:.2f}, Query Score: {evaluation.query_score:.2f}")
        
        # Process expected documents
        rows = []
        for idx in group.index:
            expected_doc = group.loc[idx, 'doc_filename']
            logger.info(f"Checking for expected document: {expected_doc}")
            
            if expected_doc in doc_scores:  # Check in doc_scores instead of retrieved_docs
                logger.info(f"MATCH FOUND - {expected_doc}")
                rows.append(create_row_dict(
                    group.loc[idx].to_dict(),
                    evaluation,
                    was_retrieved=True,
                    retrieved_rank=retrieved_docs.index(expected_doc) + 1,  # Keep for reference
                    retrieved_score=doc_scores[expected_doc]
                ))
            else:
                rows.append(create_row_dict(
                    group.loc[idx].to_dict(),
                    evaluation,
                    was_retrieved=False
                ))
        
        return rows
        
    except Exception as e:
        logger.error(f"Error processing question {question_id}: {str(e)}")
        return [
            create_row_dict(
                group.loc[idx].to_dict(),
                QueryEvaluation(
                    total_score=0,
                    correct_score=0,
                    query_score=0,
                    num_results=0,
                    num_correct=0,
                    results=[]
                ),
                was_retrieved=False
            )
            for idx in group.index
        ]

def evaluate_queries(
    csv_path: str,
    store_id: str,
    max_results: int = MAX_RESULTS
) -> pd.DataFrame:
    """
    Evaluate queries from CSV file and mark which expected documents were retrieved
    
    Args:
        csv_path (str): Path to the CSV file containing questions and expected documents
        store_id (str): The vector store ID to query against
        max_results (int): Maximum number of results to retrieve per query
        
    Returns:
        pd.DataFrame: Updated DataFrame with retrieval information
    """
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Parse store_id
    parse_store_id(store_id)
    
    # Process each unique question
    all_rows = []
    for question_id, group in df.groupby('question_id'):
        all_rows.extend(process_question(question_id, group, store_id, max_results))
    
    # Convert all rows to DataFrame
    result_df = pd.DataFrame(all_rows)
    
    # Ensure all columns are in the correct order
    columns = [
        'question_id', 'question_text', 'question_type', 'doc_path', 'doc_filename',
        'comments/questions', 'is_expected', 'total_expected', 'was_retrieved',
        'retrieved_rank', 'retrieved_score', 'total_score', 'correct_score', 'query_score',
        'num_results', 'num_correct'
    ]
    result_df = result_df[columns]
    
    # Sort by question_id and retrieved_score (descending)
    result_df = result_df.sort_values(['question_id', 'retrieved_score'], ascending=[True, False])
    
    return result_df

def print_summary_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics about the evaluation results."""
    # Print summary statistics - only count expected documents
    expected_docs = df[df['is_expected']]
    total_expected = len(expected_docs)
    total_retrieved = expected_docs['was_retrieved'].sum()
    logger.info(f"Total expected documents: {total_expected}")
    logger.info(f"Total retrieved expected documents: {total_retrieved}")
    logger.info(f"Retrieval rate: {(total_retrieved/total_expected)*100:.2f}%")
    
    # Print per-question statistics - only count expected documents
    for question_id, group in df.groupby('question_id'):
        expected = group[group['is_expected']]
        expected_count = len(expected)
        retrieved_count = expected['was_retrieved'].sum()
        logger.info(f"Question {question_id}: {retrieved_count}/{expected_count} documents retrieved ({retrieved_count/expected_count*100:.1f}%)") 