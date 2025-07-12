#!/usr/bin/env python3
"""
Main pipeline runner for HTML document processing.
"""

import argparse
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import json
import time

from botnim.config import get_logger
from pipeline_config import PipelineConfig, PipelineMetadata, PipelineStage, Environment, validate_json_structure

# Logger setup
logger = get_logger(__name__)

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

    def _run_stage(self, *,
                   stage_name: str,
                   output_path: Path,
                   output_exists_check: callable,
                   confirm_overwrite: callable,
                   command: list,
                   validation_file: Path,
                   validation_keys: list,
                   metadata_key: str,
                   metadata_extra: callable = None,
                   dry_run: bool = False,
                   skip_stage_enum=None) -> bool:
        logger.info(f"{stage_name}")
        stage_start = time.time()

        # Check if output already exists
        if output_exists_check() and not self.config.overwrite_existing:
            logger.warning(f"Output already exists: {output_path}")
            if not confirm_overwrite():
                logger.info(f"Skipping {stage_name.lower()}")
                if skip_stage_enum:
                    self.metadata.stages_completed.append(skip_stage_enum)
                return True

        # Run command
        try:
            logger.info(f"Running command: {' '.join(command)}")
            result = subprocess.run(command, capture_output=True, text=True, cwd=Path(__file__).parent)

            if result.returncode != 0:
                logger.error(f"{stage_name} failed with return code {result.returncode}")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                self.metadata.errors.append(f"{stage_name} failed: {result.stderr}")
                return False

            # Validation
            validation_errors = []
            if stage_name.startswith("Stage 3") and validation_file:
                validation_errors = validate_markdown_output(validation_file)
            elif validation_file and validation_keys and Path(validation_file).is_file():
                validation_errors = validate_json_structure(
                    validation_file, 
                    validation_keys
                )
            if validation_errors:
                logger.error(f"{stage_name} file validation failed:")
                for error in validation_errors:
                    logger.error(f"  - {error}")
                self.metadata.errors.extend(validation_errors)
                return False

            # Record stage completion
            stage_duration = time.time() - stage_start
            meta = {
                "duration_seconds": round(stage_duration, 2),
            }
            if metadata_extra:
                meta.update(metadata_extra())
            setattr(self.metadata, metadata_key, meta)
            if skip_stage_enum:
                self.metadata.stages_completed.append(skip_stage_enum)
            logger.info(f"{stage_name} completed in {stage_duration:.2f}s")
            return True

        except Exception as e:
            logger.error(f"{stage_name} failed with exception: {e}")
            self.metadata.errors.append(f"{stage_name} exception: {str(e)}")
            return False

    def _run_structure_extraction(self) -> bool:
        return self._run_stage(
            stage_name="Stage 1: Extracting document structure",
            output_path=self.config.structure_file,
            output_exists_check=lambda: self.config.structure_file.exists(),
            confirm_overwrite=self._confirm_overwrite,
            command=[
                sys.executable, "extract_structure.py",
                str(self.config.input_html_file),
                str(self.config.structure_file),
                "--environment", self.config.environment.value,
                "--model", self.config.model,
                "--max-tokens", str(self.config.max_tokens),
                "--mark-type", self.config.content_type,
            ] + (["--pretty"] if self.config.pretty_json else []),
            validation_file=self.config.structure_file,
            validation_keys=["metadata", "structure"],
            metadata_key="structure_extraction",
            metadata_extra=lambda: {
                "output_file": str(self.config.structure_file),
                "file_size_bytes": self.config.structure_file.stat().st_size,
            },
            skip_stage_enum=PipelineStage.EXTRACT_STRUCTURE
        )
    
    def _run_content_extraction(self) -> bool:
        return self._run_stage(
            stage_name="Stage 2: Extracting content",
            output_path=self.config.content_file,
            output_exists_check=lambda: self.config.content_file.exists(),
            confirm_overwrite=self._confirm_overwrite,
            command=[
                sys.executable, "extract_content.py",
                str(self.config.input_html_file),
                str(self.config.structure_file),
                self.config.content_type,
                "--output", str(self.config.content_file),
            ],
            validation_file=self.config.content_file,
            validation_keys=["metadata", "structure"],
            metadata_key="content_extraction",
            metadata_extra=lambda: {
                "output_file": str(self.config.content_file),
                "file_size_bytes": self.config.content_file.stat().st_size,
                "content_items_found": self._count_content_items(self.config.content_file),
            },
            skip_stage_enum=PipelineStage.EXTRACT_CONTENT
        )
    
    def _run_markdown_generation(self) -> bool:
        return self._run_stage(
            stage_name="Stage 3: Generating markdown files",
            output_path=self.config.chunks_dir,
            output_exists_check=lambda: self.config.chunks_dir.exists() and any(self.config.chunks_dir.glob("*.md")),
            confirm_overwrite=self._confirm_overwrite,
            command=[
                sys.executable, "generate_markdown_files.py",
                str(self.config.content_file),
                "--output-dir", str(self.config.chunks_dir),
            ] + (["--dry-run"] if self.config.dry_run else []),
            validation_file=self.config.chunks_dir,  # Not used for validation, but required by signature
            validation_keys=[],  # No validation for markdown files
            metadata_key="markdown_generation",
            metadata_extra=lambda: self._markdown_metadata_extra(),
            dry_run=self.config.dry_run,
            skip_stage_enum=PipelineStage.GENERATE_MARKDOWN
        )

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
        """Save pipeline metadata to file."""
        try:
            metadata_file = self.config.output_base_dir / "pipeline_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata.to_dict(), f, indent=2, ensure_ascii=False)
            logger.info(f"Pipeline metadata saved to: {metadata_file}")
        except Exception as e:
            logger.error(f"Failed to save pipeline metadata: {e}")
    
    def _print_summary(self):
        """Print pipeline execution summary."""
        duration = datetime.now() - self.start_time
        
        print("\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Total Duration: {duration}")
        print(f"Stages Completed: {len(self.metadata.stages_completed)}/3")
        
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

    def run(self) -> bool:
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

            # Stage 3: Generate markdown files
            if not self._run_markdown_generation():
                return False

            # Pipeline completed successfully
            self.metadata.end_time = datetime.now().isoformat()
            self._save_pipeline_metadata()

            logger.info("Pipeline completed successfully")
            self._print_summary()
            return True

        except Exception as e:
            logger.error(f"Pipeline failed with exception: {e}")
            self.metadata.errors.append(f"Pipeline exception: {str(e)}")
            self.metadata.end_time = datetime.now().isoformat()
            self._save_pipeline_metadata()
            return False


def main():
    """CLI interface for pipeline runner."""
    parser = argparse.ArgumentParser(
        description="Process HTML legal documents and extract content to markdown files"
    )
    parser.add_argument(
        "input_html_file",
        type=str,
        help="Path to input HTML file"
    )
    parser.add_argument(
        "output_base_dir",
        type=str,
        help="Base directory for all output files"
    )
    parser.add_argument(
        "--content-type",
        default="סעיף",
        help="Type of content to extract (default: 'סעיף')"
    )
    parser.add_argument(
        "--environment",
        choices=["staging", "production"],
        default="staging",
        help="Environment to use (default: staging)"
    )
    parser.add_argument(
        "--model",
        default="gpt-4.1",
        help="OpenAI model to use"
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=32000,
        help="Maximum tokens for OpenAI response (default: 32000)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline in dry-run mode (no files generated in final stage)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files without prompting"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Load configuration from JSON file"
    )
    parser.add_argument(
        "--save-config",
        type=str,
        help="Save configuration to JSON file and exit"
    )
    
    args = parser.parse_args()
    
    # Load configuration from file if specified
    if args.config:
        config_file = Path(args.config)
        if not config_file.exists():
            logger.error(f"Configuration file not found: {config_file}")
            return 1
        
        try:
            config = PipelineConfig.load(config_file)
            logger.info(f"Configuration loaded from: {config_file}")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return 1
    else:
        # Create configuration from command line arguments
        config = PipelineConfig(
            input_html_file=Path(args.input_html_file),
            output_base_dir=Path(args.output_base_dir),
            content_type=args.content_type,
            environment=Environment(args.environment),
            model=args.model,
            max_tokens=args.max_tokens,
            dry_run=args.dry_run,
            overwrite_existing=args.overwrite,
        )
    
    # Save configuration if requested
    if args.save_config:
        config_file = Path(args.save_config)
        try:
            config.save(config_file)
            logger.info(f"Configuration saved to: {config_file}")
            return 0
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            return 1
    
    # Run pipeline
    runner = PipelineRunner(config)
    success = runner.run()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main()) 