"""
Performance metrics and structured logging for PDF extraction pipeline.

This module provides utilities for tracking performance metrics, timing operations,
and structured logging throughout the PDF extraction process.
"""

import time
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path

@dataclass
class ExtractionMetrics:
    """Metrics for a single PDF extraction operation."""
    pdf_path: str
    source_name: str
    text_extraction_time: float
    field_extraction_time: float
    total_processing_time: float
    text_length: int
    entities_extracted: int
    success: bool
    error_message: Optional[str] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()

@dataclass
class PipelineMetrics:
    """Overall pipeline performance metrics."""
    total_pdfs_processed: int
    successful_extractions: int
    failed_extractions: int
    total_entities_extracted: int
    total_processing_time: float
    average_processing_time: float
    start_time: str
    end_time: str
    sources_processed: list
    errors: list

class MetricsCollector:
    """Collects and manages performance metrics throughout the pipeline."""
    
    def __init__(self, log_file: Optional[str] = None):
        """
        Initialize the metrics collector.
        
        Args:
            log_file: Optional path to save metrics JSON file
        """
        self.extraction_metrics: list[ExtractionMetrics] = []
        self.start_time = time.time()
        self.log_file = log_file
        self.logger = logging.getLogger(__name__)
        
        # Set up structured logging
        self._setup_structured_logging()
    
    def _setup_structured_logging(self):
        """Set up structured logging with JSON format."""
        # Create a custom formatter for structured logging
        class StructuredFormatter(logging.Formatter):
            def format(self, record):
                log_entry = {
                    "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                
                # Add extra fields if present
                if hasattr(record, 'metrics'):
                    log_entry['metrics'] = record.metrics
                if hasattr(record, 'operation'):
                    log_entry['operation'] = record.operation
                if hasattr(record, 'duration'):
                    log_entry['duration'] = record.duration
                
                return json.dumps(log_entry, ensure_ascii=False)
        
        # Set up handler for structured logging
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def log_operation(self, operation: str, duration: float, **kwargs):
        """Log an operation with timing information."""
        extra = {
            'operation': operation,
            'duration': duration,
            'metrics': kwargs
        }
        self.logger.info(f"Operation completed: {operation}", extra=extra)
    
    def start_timer(self) -> float:
        """Start a timer and return the start time."""
        return time.time()
    
    def record_extraction(self, metrics: ExtractionMetrics):
        """Record metrics for a single PDF extraction."""
        self.extraction_metrics.append(metrics)
        
        # Log the extraction with structured data
        extra = {
            'operation': 'pdf_extraction',
            'duration': metrics.total_processing_time,
            'metrics': asdict(metrics)
        }
        
        if metrics.success:
            self.logger.info(
                f"PDF extraction completed: {metrics.pdf_path} - {metrics.entities_extracted} entities",
                extra=extra
            )
        else:
            self.logger.error(
                f"PDF extraction failed: {metrics.pdf_path} - {metrics.error_message}",
                extra=extra
            )
    
    def record_failure(self, pdf_path: str, source_name: str, error_message: str):
        """Record a failed PDF extraction."""
        metrics = ExtractionMetrics(
            pdf_path=pdf_path,
            source_name=source_name,
            text_extraction_time=0.0,
            field_extraction_time=0.0,
            total_processing_time=0.0,
            text_length=0,
            entities_extracted=0,
            success=False,
            error_message=error_message
        )
        self.record_extraction(metrics)
    
    def get_pipeline_summary(self) -> PipelineMetrics:
        """Generate summary metrics for the entire pipeline."""
        end_time = time.time()
        total_time = end_time - self.start_time
        
        successful = [m for m in self.extraction_metrics if m.success]
        failed = [m for m in self.extraction_metrics if not m.success]
        
        total_entities = sum(m.entities_extracted for m in successful)
        avg_time = sum(m.total_processing_time for m in self.extraction_metrics) / len(self.extraction_metrics) if self.extraction_metrics else 0
        
        sources = list(set(m.source_name for m in self.extraction_metrics))
        errors = [m.error_message for m in failed if m.error_message]
        
        return PipelineMetrics(
            total_pdfs_processed=len(self.extraction_metrics),
            successful_extractions=len(successful),
            failed_extractions=len(failed),
            total_entities_extracted=total_entities,
            total_processing_time=total_time,
            average_processing_time=avg_time,
            start_time=datetime.fromtimestamp(self.start_time).isoformat(),
            end_time=datetime.fromtimestamp(end_time).isoformat(),
            sources_processed=sources,
            errors=errors
        )
    
    def save_metrics(self, output_path: Optional[str] = None):
        """Save metrics to a JSON file."""
        if output_path is None and self.log_file is None:
            return
        
        file_path = output_path or self.log_file
        if file_path is None:
            return
        
        summary = self.get_pipeline_summary()
        
        metrics_data = {
            "pipeline_summary": asdict(summary),
            "extraction_details": [asdict(m) for m in self.extraction_metrics],
            "generated_at": datetime.now().isoformat()
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(metrics_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Metrics saved to: {file_path}")
    
    def print_summary(self):
        """Print a human-readable summary of pipeline performance."""
        summary = self.get_pipeline_summary()
        
        print("\n" + "="*60)
        print("PDF EXTRACTION PIPELINE SUMMARY")
        print("="*60)
        print(f"Total PDFs processed: {summary.total_pdfs_processed}")
        print(f"Successful extractions: {summary.successful_extractions}")
        print(f"Failed extractions: {summary.failed_extractions}")
        print(f"Total entities extracted: {summary.total_entities_extracted}")
        print(f"Total processing time: {summary.total_processing_time:.2f} seconds")
        print(f"Average processing time per PDF: {summary.average_processing_time:.2f} seconds")
        print(f"Success rate: {(summary.successful_extractions/summary.total_pdfs_processed*100):.1f}%" if summary.total_pdfs_processed > 0 else "N/A")
        print(f"Sources processed: {', '.join(summary.sources_processed)}")
        
        if summary.errors:
            print(f"\nErrors encountered:")
            for error in summary.errors:
                print(f"  - {error}")
        
        print("="*60)
