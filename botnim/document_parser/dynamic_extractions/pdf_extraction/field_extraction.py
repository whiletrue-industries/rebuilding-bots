import logging
import json
from typing import Dict
from pydantic import ValidationError
from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_extraction_config import SourceConfig, PDFExtractionConfig
import argparse
import sys
from datetime import datetime

logger = logging.getLogger(__name__)

def extract_fields_from_text(text: str, config: SourceConfig, client, model: str = "gpt-4.1") -> Dict:
    # Build a detailed prompt with field definitions
    field_definitions = []
    for field in config.fields:
        field_def = f"- {field.name}: {field.description}"
        if field.example:
            field_def += f" (example: {field.example})"
        if field.hint:
            field_def += f" (hint: {field.hint})"
        field_definitions.append(field_def)
    
    field_list = "\n".join(field_definitions)
    
    prompt = f"""You are extracting structured data from a Hebrew document. 

Required fields to extract:
{field_list}

{config.extraction_instructions or "Extract the specified fields from the document text. Return a JSON object with the exact field names as specified above."}

IMPORTANT: Return ONLY a JSON object with the exact field names specified above. Do not add any additional fields or change the field names."""
    logger.info("Building prompt for field extraction.")
    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"""Document text:\n{text}\n\nReturn the result as a JSON object."""}
    ]
    try:
        logger.info(f"Sending extraction prompt to OpenAI (model={model})")
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.0
        )
        content = response.choices[0].message.content
        logger.info("Received response from OpenAI. Attempting to parse JSON.")
        try:
            if content.strip().startswith("```json"):
                content = content.strip().split("```json", 1)[1].rsplit("```", 1)[0]
            elif content.strip().startswith("```"):
                content = content.strip().split("```", 1)[1].rsplit("```", 1)[0]
            data = json.loads(content)
        except Exception as e:
            logger.error(f"Failed to parse JSON from LLM response: {e}\nResponse was: {content}")
            raise ValueError(f"Failed to parse JSON from LLM response: {e}")
        missing = [f.name for f in config.fields if f.name not in data]
        if missing:
            logger.warning(f"Missing fields in extraction: {missing}")
        return data
    except Exception as e:
        logger.error(f"Field extraction failed: {e}")
        return {"error": str(e)}

def build_metadata(input_file: str, source_url: str, extraction_date: str, extra_metadata: dict = None) -> dict:
    metadata = {
        "input_file": input_file,
        "source_url": source_url,
        "extraction_date": extraction_date,
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    return metadata

def main():
    parser = argparse.ArgumentParser(description="Extract structured fields from text using OpenAI GPT-4.1 and a YAML config. Output always includes a metadata block and a fields block.")
    parser.add_argument("--text", required=True, help="Path to the extracted text file")
    parser.add_argument("--config", required=True, help="Path to the YAML config file")
    parser.add_argument("--source", required=True, help="Source name as defined in the config")
    parser.add_argument("--model", default="gpt-4.1", help="OpenAI model to use (default: gpt-4.1)")
    parser.add_argument("--output", help="Path to save the extracted fields as JSON (optional)")
    parser.add_argument("--environment", default="staging", choices=["staging", "production"], help="API environment (default: staging)")
    parser.add_argument("--source-url", required=False, help="Direct URL to the PDF file (for metadata and fields)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        from botnim.document_parser.dynamic_extractions.extract_structure import get_openai_client
        client = get_openai_client(args.environment)
        logger.info(f"Loading config from {args.config}")
        config_obj = PDFExtractionConfig.from_yaml(args.config)
        source = next((s for s in config_obj.sources if s.name == args.source), None)
        if not source:
            logger.error(f"Source '{args.source}' not found in config.")
            sys.exit(1)
        logger.info(f"Reading text from {args.text}")
        with open(args.text, "r", encoding="utf-8") as f:
            text = f.read()
        # Determine source_url for metadata and fields
        source_url = args.source_url or ""
        extraction_date = datetime.now().isoformat()
        # Run LLM extraction
        fields = extract_fields_from_text(text, source, client, args.model)
        # Always set full_text to the raw extracted text
        fields["full_text"] = text
        # Always set source_url in fields if not present
        if "source_url" in [f.name for f in source.fields]:
            fields["source_url"] = source_url
        # Build metadata block
        metadata = build_metadata(
            input_file=args.text,
            source_url=source_url,
            extraction_date=extraction_date,
        )
        output = {
            "metadata": metadata,
            "fields": fields
        }
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=2)
            logger.info(f"Extracted fields saved to: {args.output}")
        else:
            print("\n--- Extraction Output ---\n")
            print(json.dumps(output, ensure_ascii=False, indent=2))
            print("\n--- End of Output ---\n")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 