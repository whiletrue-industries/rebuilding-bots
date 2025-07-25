import logging
from pathlib import Path
import argparse
import sys

logger = logging.getLogger(__name__)

def extract_text_with_pdfplumber(pdf_path: Path) -> str:
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber is not installed. Please install it with 'pip install pdfplumber'.")
        raise
    text = ""
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages):
            page_text = page.extract_text() or ""
            logger.info(f"Extracted {len(page_text)} characters from page {i+1}")
            text += page_text + "\n"
    return text

def extract_text_with_pdfminer(pdf_path: Path) -> str:
    try:
        from pdfminer.high_level import extract_text
    except ImportError:
        logger.error("pdfminer.six is not installed. Please install it with 'pip install pdfminer.six'.")
        raise
    text = extract_text(str(pdf_path))
    logger.info(f"Extracted {len(text)} characters using pdfminer.six")
    return text

def extract_text_from_pdf(pdf_path: str, client=None, model: str = "gpt-4o", environment: str = "staging") -> str:
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        logger.error(f"PDF file not found: {pdf_path}")
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")
    # Try pdfplumber first
    try:
        logger.info("Trying to extract text with pdfplumber...")
        text = extract_text_with_pdfplumber(pdf_path)
        if text.strip():
            logger.info("Successfully extracted text with pdfplumber.")
            return text
        else:
            logger.warning("pdfplumber returned empty text. Falling back to pdfminer.six...")
    except Exception as e:
        logger.warning(f"pdfplumber failed: {e}. Falling back to pdfminer.six...")
    # Fallback to pdfminer.six
    try:
        text = extract_text_with_pdfminer(pdf_path)
        if text.strip():
            logger.info("Successfully extracted text with pdfminer.six.")
            return text
        else:
            logger.error("pdfminer.six also returned empty text.")
            raise ValueError("Both pdfplumber and pdfminer.six returned empty text.")
    except Exception as e:
        logger.error(f"pdfminer.six failed: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Extract structured text from a Hebrew PDF using pdfplumber/pdfminer.six.")
    parser.add_argument("input_pdf", help="Path to the input PDF file")
    parser.add_argument("--output", "-o", help="Path to save the extracted text (optional)")
    parser.add_argument("--model", default="gpt-4o", help="(Unused) OpenAI model to use (default: gpt-4o)")
    parser.add_argument("--environment", default="staging", choices=["staging", "production"], help="(Unused) API environment (default: staging)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        text = extract_text_from_pdf(args.input_pdf)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(text)
            logger.info(f"Extracted text saved to: {args.output}")
        else:
            print("\n--- Extracted Text Preview (first 1000 chars) ---\n")
            print(text[:1000])
            print("\n--- End of Preview ---\n")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 