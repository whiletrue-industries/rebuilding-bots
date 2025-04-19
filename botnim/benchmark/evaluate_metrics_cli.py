import click
from pathlib import Path
from typing import Optional, List
import pandas as pd
from botnim.config import get_logger, AVAILABLE_BOTS, VALID_ENVIRONMENTS, is_production
from botnim.vector_store.vector_store_es import VectorStoreES
from .evaluate_queries import evaluate_queries, print_summary_statistics

logger = get_logger(__name__)

REQUIRED_COLUMNS = ['question_id', 'question_text', 'doc_filename']

def validate_csv(df: pd.DataFrame) -> None:
    """
    Validate the input CSV file has required columns.
    
    Args:
        df (pd.DataFrame): Input DataFrame to validate
        
    Raises:
        click.Abort: If validation fails
    """
    # Check required columns
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise click.Abort(
            f"Missing required columns: {', '.join(missing_columns)}\n"
            f"Your CSV file must contain these columns:\n"
            f"- question_id: Unique identifier for each question (e.g., '1', '2')\n"
            f"- question_text: The actual question text (e.g., 'אילו סעיפים בתקנון עוסקים בהצעות חוק פרטיות?')\n"
            f"- doc_filename: The filename of the expected document (e.g., 'תקנון הכנסת_87.md')\n\n"
            f"Example CSV format:\n"
            f"question_id,question_text,doc_filename\n"
            f"1,אילו סעיפים בתקנון עוסקים בהצעות חוק פרטיות?,תקנון הכנסת_87.md\n"
            f"1,אילו סעיפים בתקנון עוסקים בהצעות חוק פרטיות?,תקנון הכנסת_88.md\n"
            f"2,מה הם התנאים להקמת ועדת חקירה פרלמנטרית?,חוק-יסוד: הכנסת_22.md"
        )
    
    # Check for empty values in required columns
    for col in REQUIRED_COLUMNS:
        if df[col].isna().any():
            raise click.Abort(
                f"Column '{col}' contains empty values.\n"
                f"All required columns must have values. Please check your CSV file for missing data."
            )
    
    # Log CSV contents for debugging
    logger.info(f"CSV file contains {len(df)} rows")
    logger.info(f"Unique question IDs: {df['question_id'].nunique()}")
    logger.info(f"Sample question: {df['question_text'].iloc[0]}")
    logger.info(f"Sample document: {df['doc_filename'].iloc[0]}")

@click.command(name='evaluate')
@click.argument('bot', type=click.Choice(AVAILABLE_BOTS))
@click.argument('context', type=click.STRING)
@click.argument('environment', type=click.Choice(VALID_ENVIRONMENTS))
@click.argument('csv_path', type=click.Path(exists=True))
@click.option('--max-results', type=int, default=20, help='Maximum number of results to retrieve per query (default: 20)')
def evaluate(bot: str, context: str, environment: str, csv_path: str, max_results: int):
    """
    Evaluate queries from a CSV file against a vector store using score-based metrics.
    
    The evaluation uses Elasticsearch scores to measure query performance:
    - total_score: Sum of all document scores
    - correct_score: Sum of scores for expected documents
    - query_score: Ratio of correct_score to total_score
    
    To find available contexts for a bot, use:
        python -m botnim query list-indexes <environment> --bot <bot_name>
    
    Example:
        python -m botnim query list-indexes staging --bot takanon
    
    Required CSV columns:
    - question_id: Unique identifier for each question
    - question_text: The actual question text
    - doc_filename: The filename of the expected document
    
    Example usage:
        python -m botnim evaluate takanon legal_text staging path/to/query_evaluations.csv
    """
    try:
        # Read and validate CSV
        logger.info(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        validate_csv(df)
        
        # Construct store_id using VectorStoreES.encode_index_name
        store_id = VectorStoreES.encode_index_name(bot, context, is_production(environment))
        logger.info(f"Using store_id: {store_id}")
        
        # Run evaluation
        df = evaluate_queries(
            csv_path=csv_path,
            store_id=store_id,
            max_results=max_results
        )
        
        # Save results back to CSV with UTF-8 encoding
        output_path = Path(csv_path).parent / 'query_evaluations_results.csv'
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Evaluation complete. Results saved to {output_path}")
        
        # Print summary statistics
        print_summary_statistics(df)
        
    except Exception as e:
        logger.error(f"Error during evaluation: {str(e)}", exc_info=True)
        raise click.Abort() 