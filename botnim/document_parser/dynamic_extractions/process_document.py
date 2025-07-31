#!/usr/bin/env python3
"""
Main pipeline runner for HTML document processing.
"""

import sys
from pathlib import Path
from botnim.config import get_logger
from .pipeline_config import PipelineConfig, PipelineMetadata, PipelineStage, Environment, validate_json_structure
from .extract_structure import extract_structure_from_html, get_openai_client, build_nested_structure
from .extract_content import extract_content_from_html
from .generate_markdown_files import generate_markdown_from_json

import argparse
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
import time


# Logger setup
logger = get_logger(__name__)

LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

def validate_markdown_output(output_dir: Path) -> list:
    errors = []
    if not output_dir.exists() or not output_dir.is_dir():
        errors.append(f"Output directory does not exist: {output_dir}")
        return errors
    md_files = list(output_dir.glob("*.md"))
    if not md_files:
        errors.append(f"No markdown files generated in: {output_dir}")
    for f in md_files:
        if f.stat().st_size == 0:
            errors.append(f"Markdown file is empty: {f}")
    return errors

class PipelineRunner:
    """Main pipeline orchestrator."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.metadata = PipelineMetadata()
        self.start_time = None

    def _run_structure_extraction(self) -> bool:
        logger.info("Stage 1: Extracting document structure (direct function call)")
        stage_start = time.time()

        # Check if output already exists
        if self.config.structure_file.exists() and not self.config.overwrite_existing:
            logger.warning(f"Structure file already exists: {self.config.structure_file}")
            if not self._confirm_overwrite():
                logger.info("Skipping structure extraction")
                self.metadata.stages_completed.append(PipelineStage.EXTRACT_STRUCTURE)
                return True

        try:
            # Read input HTML
            with open(self.config.input_html_file, 'r', encoding='utf-8') as f:
                html_text = f.read()
            # Get OpenAI client
            client = get_openai_client(self.config.environment.value)
            # Extract structure
            structure_items = extract_structure_from_html(
                html_text,
                client,
                self.config.model,
                self.config.max_tokens,
                self.config.content_type
            )
            # Build nested tree structure
            nested_structure = build_nested_structure(structure_items)
            # Convert to JSON-serializable format
            structure_data = nested_structure # No longer using flatten_for_json_serialization
            # Prepare output data with metadata
            output_data = {
                "metadata": {
                    "input_file": str(self.config.input_html_file),
                    "document_name": Path(self.config.input_html_file).stem,
                    "environment": self.config.environment.value,
                    "model": self.config.model,
                    "max_tokens": self.config.max_tokens,
                    "total_items": len(structure_items),
                    "structure_type": "nested_hierarchy",
                    "mark_type": self.config.content_type
                },
                "structure": structure_data
            }
            # Write output JSON file
            with open(self.config.structure_file, 'w', encoding='utf-8') as f:
                if self.config.pretty_json:
                    json.dump(output_data, f, ensure_ascii=False, indent=2)
                else:
                    json.dump(output_data, f, ensure_ascii=False)
            logger.info(f"Structure extraction completed and saved to: {self.config.structure_file}")
            self.metadata.stages_completed.append(PipelineStage.EXTRACT_STRUCTURE)
            return True
        except Exception as e:
            logger.error(f"Structure extraction failed with exception: {e}")
            self.metadata.errors.append(f"Structure extraction exception: {str(e)}")
            return False

    def _run_content_extraction(self) -> bool:
        logger.info("Stage 2: Extracting content (direct function call)")
        stage_start = time.time()

        # Check if output already exists
        if self.config.content_file.exists() and not self.config.overwrite_existing:
            logger.warning(f"Content file already exists: {self.config.content_file}")
            if not self._confirm_overwrite():
                logger.info("Skipping content extraction")
                self.metadata.stages_completed.append(PipelineStage.EXTRACT_CONTENT)
                return True

        try:
            extract_content_from_html(
                html_path=self.config.input_html_file,
                structure_path=self.config.structure_file,
                content_type=self.config.content_type,
                output_path=self.config.content_file,
                mediawiki_mode=self.config.mediawiki_mode
            )
            logger.info(f"Content extraction completed and saved to: {self.config.content_file}")
            self.metadata.stages_completed.append(PipelineStage.EXTRACT_CONTENT)
            return True
        except Exception as e:
            logger.error(f"Stage 2: Extracting content failed: {e}")
            return False
    
    def _run_markdown_generation(self) -> bool:
        logger.info("Stage 3: Generating markdown files (direct function call)")
        stage_start = time.time()

        # Check if output already exists
        if self.config.chunks_dir.exists() and any(self.config.chunks_dir.glob("*.md")) and not self.config.overwrite_existing:
            logger.warning(f"Markdown files already exist in: {self.config.chunks_dir}")
            if not self._confirm_overwrite():
                logger.info("Skipping markdown generation")
                self.metadata.stages_completed.append(PipelineStage.GENERATE_MARKDOWN)
                return True

        try:
            generate_markdown_from_json(
                json_path=self.config.content_file,
                output_dir=self.config.chunks_dir,
                write_files=not self.config.dry_run,
                dry_run=self.config.dry_run
            )
            logger.info(f"Markdown generation completed in {self.config.chunks_dir}")
            self.metadata.stages_completed.append(PipelineStage.GENERATE_MARKDOWN)
            return True
        except Exception as e:
            logger.error(f"Stage 3: Generating markdown files failed: {e}")
            return False
    
    def _markdown_metadata_extra(self):
        if not self.config.dry_run:
            markdown_files = list(self.config.chunks_dir.glob("*.md"))
            total_size = sum(f.stat().st_size for f in markdown_files)
            return {
                "output_directory": str(self.config.chunks_dir),
                "files_generated": len(markdown_files),
                "total_size_bytes": total_size,
                "dry_run": False,
            }
        else:
            return {
                "output_directory": str(self.config.chunks_dir),
                "files_generated": 0,
                "total_size_bytes": 0,
                "dry_run": True,
            }
    
    def _count_content_items(self, json_file: Path) -> int:
        """Count items with content in the JSON structure."""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            def count_items(items):
                count = 0
                for item in items:
                    if item.get('content'):
                        count += 1
                    if 'children' in item:
                        count += count_items(item['children'])
                return count
            
            return count_items(data.get('structure', []))
            
        except Exception as e:
            logger.warning(f"Failed to count content items: {e}")
            return 0
    
    def _confirm_overwrite(self) -> bool:
        """Ask user to confirm overwrite of existing files."""
        if self.config.dry_run:
            return True
        
        response = input("Do you want to overwrite existing files? (y/N): ")
        return response.lower() in ['y', 'yes']
    
    def _save_pipeline_metadata(self):
        """Save pipeline metadata to file in logs directory, with input file name as prefix."""
        try:
            input_html_name = self.config.input_html_file.stem
            metadata_file = LOGS_DIR / f"{input_html_name}_pipeline_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata.to_dict(), f, indent=2, ensure_ascii=False)
            logger.info(f"Pipeline metadata saved to: {metadata_file}")
        except Exception as e:
            logger.error(f"Failed to save pipeline metadata: {e}")
    
    def _print_summary(self, generate_markdown: bool):
        """Print pipeline execution summary."""
        duration = datetime.now() - self.start_time
        total_stages = 3 if generate_markdown else 2
        print("\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Total Duration: {duration}")
        print(f"Stages Completed: {len(self.metadata.stages_completed)}/{total_stages}")
        
        if self.metadata.structure_extraction:
            print(f"Structure Extraction: {self.metadata.structure_extraction['duration_seconds']}s")
        
        if self.metadata.content_extraction:
            print(f"Content Extraction: {self.metadata.content_extraction['duration_seconds']}s")
            print(f"Content Items Found: {self.metadata.content_extraction['content_items_found']}")
        
        if self.metadata.markdown_generation:
            print(f"Markdown Generation: {self.metadata.markdown_generation['duration_seconds']}s")
            if not self.config.dry_run:
                print(f"Files Generated: {self.metadata.markdown_generation['files_generated']}")
        
        if self.metadata.errors:
            print(f"\nErrors: {len(self.metadata.errors)}")
            for error in self.metadata.errors:
                print(f"  - {error}")
        
        if self.metadata.warnings:
            print(f"\nWarnings: {len(self.metadata.warnings)}")
            for warning in self.metadata.warnings:
                print(f"  - {warning}")
        
        print("="*60)

    def run(self, generate_markdown=False) -> bool:
        """Run the complete pipeline."""
        logger.info("Starting HTML processing pipeline")
        logger.info(f"Configuration: {self.config.to_dict()}")

        # Validate configuration
        errors = self.config.validate()
        if errors:
            logger.error("Configuration validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            return False

        # Initialize metadata
        self.start_time = datetime.now()
        self.metadata.start_time = self.start_time.isoformat()

        try:
            # Stage 1: Extract structure
            if not self._run_structure_extraction():
                return False

            # Stage 2: Extract content
            if not self._run_content_extraction():
                return False

            # Stage 3: Generate markdown files (optional)
            if generate_markdown:
                if not self._run_markdown_generation():
                    return False

            # Pipeline completed successfully
            self.metadata.end_time = datetime.now().isoformat()
            self._save_pipeline_metadata()

            logger.info("Pipeline completed successfully")
            self._print_summary(generate_markdown)
            return True

        except Exception as e:
            logger.error(f"Pipeline failed with exception: {e}")
            self.metadata.errors.append(f"Pipeline exception: {str(e)}")
            self.metadata.end_time = datetime.now().isoformat()
            self._save_pipeline_metadata()
            return False 