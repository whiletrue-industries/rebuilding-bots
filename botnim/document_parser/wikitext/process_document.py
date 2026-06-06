#!/usr/bin/env python3
"""
Main pipeline runner for HTML document processing.
"""

from pathlib import Path
from ...config import get_logger, get_openai_client
from ...storage import get_artifact_store
from ...storage.base import wikitext_cache_key
from .pipeline_config import WikitextProcessorConfig, PipelineMetadata, PipelineStage
from .extract_structure import extract_structure_from_html, build_nested_structure
from .extract_content import extract_content_from_html
from .generate_markdown_files import generate_markdown_from_json

from datetime import datetime
import json


# Logger setup
logger = get_logger(__name__)


# Wikitext structure-extractor version. Bump when:
#   - the system prompt in extract_structure.extract_structure_from_html changes
#   - the response schema (StructureResponse / StructureItem) changes
#   - the model in pipeline_config.WikitextProcessorConfig.model changes
#   - any post-processing in build_nested_structure changes
# Bumping invalidates every cached content_file at one stroke; the next fap
# re-extracts. Unlike the per-chunk extraction_cache, wikitext sources are a
# handful per bot (~10 for unified), so the bump cost is bounded.
WIKITEXT_EXTRACTOR_VERSION = "v1-gpt-4.1-mini"


def _wikitext_cache_key(bot: str, html_sha256: str) -> str:
    """Durable, versioned-by-key location of the cached content_file.

    Delegates to the canonical key builder in ``storage.base`` (the single
    source of truth for the key shape) — this thin wrapper only binds the
    canonical ``WIKITEXT_EXTRACTOR_VERSION`` so callers don't have to.
    Produces ``cache/wikitext/<bot>/<html_sha256>__<version>.json``.
    Bumping WIKITEXT_EXTRACTOR_VERSION changes the key, so an old object is
    never read — no in-place invalidation needed.
    """
    return wikitext_cache_key(bot, html_sha256, WIKITEXT_EXTRACTOR_VERSION)


def _metadata_from_bytes(raw: bytes) -> dict | None:
    """Return the metadata dict from cached content_file bytes, or None.

    Malformed JSON / missing metadata key → None so the caller falls
    through to a fresh extraction — never raise from the cache-check path.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    md = data.get("metadata") if isinstance(data, dict) else None
    return md if isinstance(md, dict) else None

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

class WikitextProcessor:
    """Main pipeline orchestrator."""
    
    def __init__(self, config: WikitextProcessorConfig):
        self.config = config
        self.metadata = PipelineMetadata()
        self.start_time = None

    def _run_structure_extraction(self) -> bool:
        logger.info("Stage 1: Extracting document structure (direct function call)")

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
            # Prepare output data with metadata. ``html_sha256`` and
            # ``wikitext_extractor_version`` are the cache key for the
            # next-run skip in ``run()``. They flow through Stage 2 verbatim
            # (extract_content_for_sections mutates structure data in place,
            # leaves metadata alone) and end up in the final content_file.
            output_data = {
                "metadata": {
                    "input_file": str(self.config.input_url),
                    "document_name": Path(self.config.content_file).stem.replace('_structure_content', ''),
                    "environment": self.config.environment.value,
                    "model": self.config.model,
                    "max_tokens": self.config.max_tokens,
                    "total_items": len(structure_items),
                    "structure_type": "nested_hierarchy",
                    "mark_type": self.config.content_type,
                    "html_sha256": self.config.input_html_sha256,
                    "wikitext_extractor_version": WIKITEXT_EXTRACTOR_VERSION,
                },
                "structure": structure_data
            }
            # Write output JSON file
            with open(self.config.structure_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Structure extraction completed and saved to: {self.config.structure_file}")
            self.metadata.stages_completed.append(PipelineStage.EXTRACT_STRUCTURE)
            return True
        except Exception as e:
            logger.error(f"Structure extraction failed with exception: {e}")
            self.metadata.errors.append(f"Structure extraction exception: {str(e)}")
            return False

    def _run_content_extraction(self) -> bool:
        logger.info("Stage 2: Extracting content (direct function call)")

        try:
            extract_content_from_html(
                html_path=self.config.input_html_file,
                structure_path=self.config.structure_file,
                content_type=self.config.content_type,
                output_path=self.config.content_file,
                mediawiki_mode=True,
                input_url=self.config.input_url
            )
            logger.info(f"Content extraction completed and saved to: {self.config.content_file}")
            self.metadata.stages_completed.append(PipelineStage.EXTRACT_CONTENT)
            return True
        except Exception as e:
            logger.error(f"Stage 2: Extracting content failed: {e}")
            return False
    
    def _run_markdown_generation(self) -> bool:
        logger.info("Stage 3: Generating markdown files (direct function call)")

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
        markdown_files = list(self.config.chunks_dir.glob("*.md"))
        total_size = sum(f.stat().st_size for f in markdown_files)
        return {
            "output_directory": str(self.config.chunks_dir),
            "files_generated": len(markdown_files),
            "total_size_bytes": total_size,
            "dry_run": False,
        }

    def _save_pipeline_metadata(self):
        """Save pipeline metadata to file in logs directory, with input file name as prefix."""
        try:
            with open(self.config.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata.to_dict(), f, indent=2, ensure_ascii=False)
            logger.info(f"Pipeline metadata saved to: {self.config.metadata_file}")
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

        # Cache fast-path (durable, versioned-by-key). Look up the cached
        # content_file by store key cache/wikitext/<bot>/<html_sha256>__<version>.json.
        # A HIT requires ONLY the store object — no local/EFS file. This
        # survives container replacement (the previous on-disk cache did not).
        # Bumping WIKITEXT_EXTRACTOR_VERSION changes the key, forcing a miss.
        store = get_artifact_store()
        cache_key = _wikitext_cache_key(self.config.bot, self.config.input_html_sha256)
        cached_bytes = None
        if store.exists(cache_key):
            try:
                cached_bytes = store.get_bytes(cache_key)
            except FileNotFoundError:
                cached_bytes = None
        cached_md = _metadata_from_bytes(cached_bytes) if cached_bytes is not None else None
        if (cached_md is not None
                and cached_md.get('html_sha256') == self.config.input_html_sha256
                and cached_md.get('wikitext_extractor_version') == WIKITEXT_EXTRACTOR_VERSION):
            logger.info(
                "WIKITEXT_CACHE_HIT: url=%s html_sha256=%s version=%s key=%s "
                "(skipping LLM structure extraction)",
                self.config.input_url,
                self.config.input_html_sha256[:12],
                WIKITEXT_EXTRACTOR_VERSION,
                cache_key,
            )
            # Materialize the cached content_file locally so optional Stage 3
            # markdown generation can read it. Ephemeral — derived from the store.
            self.config.content_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config.content_file, 'wb') as f:
                f.write(cached_bytes)
            if generate_markdown:
                if not self._run_markdown_generation():
                    return False
            self.metadata.end_time = datetime.now().isoformat()
            return True

        logger.info(
            "WIKITEXT_CACHE_MISS: url=%s html_sha256=%s version=%s key=%s",
            self.config.input_url,
            self.config.input_html_sha256[:12],
            WIKITEXT_EXTRACTOR_VERSION,
            cache_key,
        )

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

            # Persist the freshly extracted content_file to durable storage so
            # the next run (a fresh container with no local state) is a HIT.
            # Single atomic write to the versioned-by-key location.
            with open(self.config.content_file, 'rb') as f:
                content_bytes = f.read()
            store.put_atomic(cache_key, content_bytes)
            logger.info(
                "WIKITEXT_CACHE_STORE: key=%s bytes=%d",
                cache_key,
                len(content_bytes),
            )

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