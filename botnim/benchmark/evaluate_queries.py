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
class QuestionMetrics:
    """Data class to hold metrics for a single question."""
    total_expected: int
    retrieved_count: int
    last_expected_rank: int
    recall_ratio: float
    precision_ratio: float
    f1_score: float

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
    """Process query results into document names and scores."""
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
    total_expected: int,
    retrieved_count: int,
    total_retrieved: int,
    last_expected_rank: int
) -> QuestionMetrics:
    """Calculate all metrics for a question."""
    recall_ratio = (retrieved_count / total_expected * 100) if total_expected > 0 else 0
    precision_ratio = (retrieved_count / total_retrieved * 100) if total_retrieved > 0 else 0
    f1_score = 2 * (precision_ratio * recall_ratio) / (precision_ratio + recall_ratio) if (precision_ratio + recall_ratio) > 0 else 0
    
    return QuestionMetrics(
        total_expected=total_expected,
        retrieved_count=retrieved_count,
        last_expected_rank=last_expected_rank,
        recall_ratio=recall_ratio,
        precision_ratio=precision_ratio,
        f1_score=f1_score
    )

def create_row_dict(
    base_row: Dict,
    metrics: QuestionMetrics,
    was_retrieved: bool,
    retrieved_rank: Optional[int] = None,
    retrieved_score: Optional[float] = None
) -> Dict:
    """Create a row dictionary with all required fields."""
    row = base_row.copy()
    row.update({
        'was_retrieved': was_retrieved,
        'retrieved_rank': retrieved_rank,
        'retrieved_score': retrieved_score,
        'total_expected_retrieved': metrics.retrieved_count,
        'last_expected_rank': metrics.last_expected_rank,
        'total_expected_not_retrieved': metrics.total_expected - metrics.retrieved_count,
        'ratio_expected_retrieved': metrics.recall_ratio,
        'ratio_correct_retrieved': metrics.precision_ratio,
        'f1_score': metrics.f1_score
    })
    return row

def process_question(
    question_id: str,
    group: pd.DataFrame,
    store_id: str
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
            num_results=MAX_RESULTS
        )
        
        if not results:
            logger.warning(f"No results returned for question {question_id}")
            return [
                create_row_dict(
                    group.loc[idx].to_dict(),
                    QuestionMetrics(
                        total_expected=total_expected,
                        retrieved_count=0,
                        last_expected_rank=0,
                        recall_ratio=0,
                        precision_ratio=0,
                        f1_score=0
                    ),
                    was_retrieved=False
                )
                for idx in group.index
            ]
        
        retrieved_docs, doc_scores = process_query_results(results)
        logger.info(f"Retrieved {len(retrieved_docs)} documents for question {question_id}")
        
        retrieved_count = sum(1 for doc in group['doc_filename'] if doc in retrieved_docs)
        logger.info(f"Found {retrieved_count} out of {total_expected} expected documents for question {question_id}")
        
        last_expected_rank = max(
            (retrieved_docs.index(doc) + 1 for doc in group['doc_filename'] if doc in retrieved_docs),
            default=0
        )
        logger.info(f"Last expected document found at rank {last_expected_rank}")
        
        metrics = calculate_metrics(total_expected, retrieved_count, len(retrieved_docs), last_expected_rank)
        
        # Process expected documents
        rows = []
        for idx in group.index:
            expected_doc = group.loc[idx, 'doc_filename']
            logger.info(f"Checking for expected document: {expected_doc}")
            
            if expected_doc in retrieved_docs:
                logger.info(f"MATCH FOUND - {expected_doc}")
                row = create_row_dict(
                    group.loc[idx].to_dict(),
                    metrics,
                    was_retrieved=True,
                    retrieved_rank=retrieved_docs.index(expected_doc) + 1,
                    retrieved_score=doc_scores.get(expected_doc, 0)
                )
                logger.info(f"Found document {expected_doc} at rank {row['retrieved_rank']} with score {row['retrieved_score']}")
            else:
                row = create_row_dict(
                    group.loc[idx].to_dict(),
                    metrics,
                    was_retrieved=False
                )
                logger.info(f"Document {expected_doc} not found in results")
            
            rows.append(row)
        
        # Add unexpected retrieved documents
        expected_docs = set(group['doc_filename'].tolist())
        unexpected_docs = [(doc, score) for doc, score in doc_scores.items() if doc not in expected_docs]
        unexpected_docs.sort(key=lambda x: x[1], reverse=True)
        
        for doc_name, score in unexpected_docs:
            unexpected_row = {
                'question_id': question_id,
                'question_text': question_text,
                'question_type': group['question_type'].iloc[0],
                'doc_path': '',
                'doc_filename': doc_name,
                'comments/questions': '',
                'is_expected': False,
                'total_expected': total_expected,
                **create_row_dict(
                    {},
                    metrics,
                    was_retrieved=True,
                    retrieved_rank=retrieved_docs.index(doc_name) + 1,
                    retrieved_score=score
                )
            }
            rows.append(unexpected_row)
            logger.info(f"Added unexpected document: {doc_name} at rank {unexpected_row['retrieved_rank']} with score {score}")
        
        return rows
        
    except Exception as e:
        logger.error(f"Error processing question {question_id}: {str(e)}")
        return [
            create_row_dict(
                group.loc[idx].to_dict(),
                QuestionMetrics(
                    total_expected=total_expected,
                    retrieved_count=0,
                    last_expected_rank=0,
                    recall_ratio=0,
                    precision_ratio=0,
                    f1_score=0
                ),
                was_retrieved=False
            )
            for idx in group.index
        ]

def evaluate_queries(csv_path: str, store_id: str) -> pd.DataFrame:
    """
    Evaluate queries from CSV file and mark which expected documents were retrieved
    
    Args:
        csv_path (str): Path to the CSV file containing questions and expected documents
        store_id (str): The vector store ID to query against
        
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
        all_rows.extend(process_question(question_id, group, store_id))
    
    # Convert all rows to DataFrame
    result_df = pd.DataFrame(all_rows)
    
    # Ensure all columns are in the correct order
    columns = [
        'question_id', 'question_text', 'question_type', 'doc_path', 'doc_filename',
        'comments/questions', 'is_expected', 'total_expected', 'was_retrieved',
        'retrieved_rank', 'retrieved_score', 'total_expected_retrieved',
        'last_expected_rank', 'total_expected_not_retrieved', 'ratio_expected_retrieved',
        'ratio_correct_retrieved', 'f1_score'
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

def main():
    """Main function to run the evaluation"""
    # Get the path to the CSV file relative to the script location
    current_dir = Path(__file__).parent
    csv_path = current_dir / 'query_evaluations.csv'
    store_id = "takanon__legal_text__dev"  # Updated to use staging environment format
    
    # Run evaluation
    df = evaluate_queries(csv_path, store_id)
    
    # Save results back to CSV with UTF-8 encoding
    output_path = current_dir / 'query_evaluations_results.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')  # Using utf-8-sig to include BOM for Excel compatibility
    logger.info(f"Evaluation complete. Results saved to {output_path}")
    
    # Print summary statistics
    print_summary_statistics(df)

if __name__ == "__main__":
    main() 