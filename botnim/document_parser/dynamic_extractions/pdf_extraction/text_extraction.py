import logging
from pathlib import Path
import argparse
import sys
from .exceptions import PDFTextExtractionError

logger = logging.getLogger(__name__)

def extract_text_with_pdfplumber(pdf_path: Path) -> str:
    try:
        import pdfplumber
    except ImportError:
        raise PDFTextExtractionError("pdfplumber is not installed. Please install it with 'pip install pdfplumber'.")
    
    try:
        text = ""
        with pdfplumber.open(str(pdf_path)) as pdf:
            if not pdf.pages:
                raise PDFTextExtractionError(f"PDF file {pdf_path} appears to be empty or corrupted")
            
            for i, page in enumerate(pdf.pages):
                page_text = page.extract_text() or ""
                logger.info(f"Extracted {len(page_text)} characters from page {i+1}")
                text += page_text + "\n"
        
        if not text.strip():
            raise PDFTextExtractionError(f"No text content found in PDF {pdf_path}. The PDF might contain only images or be password-protected.")
        
        # Fix Hebrew text direction issues
        text = fix_hebrew_text_direction(text)
        return text
        
    except Exception as e:
        if isinstance(e, PDFTextExtractionError):
            raise
        raise PDFTextExtractionError(f"Failed to extract text from PDF {pdf_path}: {str(e)}")

def fix_hebrew_text_direction(text: str) -> str:
    """
    Fix Hebrew text direction issues that commonly occur in PDF extraction.
    This handles cases where Hebrew text appears reversed or with incorrect character ordering.
    """
    if not text:
        return text
    
    # Check if text contains Hebrew characters
    hebrew_chars = sum(1 for c in text if '\u0590' <= c <= '\u05FF')
    if hebrew_chars < len(text) * 0.1:  # Less than 10% Hebrew, probably not Hebrew text
        return text
    
    # Split into lines and fix each line
    lines = text.split('\n')
    fixed_lines = []
    
    for line in lines:
        if not line.strip():
            fixed_lines.append(line)
            continue
        
        # Check if line contains Hebrew
        hebrew_in_line = sum(1 for c in line if '\u0590' <= c <= '\u05FF')
        if hebrew_in_line < len(line) * 0.3:  # Less than 30% Hebrew in line
            fixed_lines.append(line)
            continue
        
        # For Hebrew-heavy lines, try to fix character ordering
        # This is a simple approach - in practice, you might need more sophisticated RTL handling
        words = line.split()
        fixed_words = []
        
        for word in words:
            # Check if word is primarily Hebrew
            hebrew_in_word = sum(1 for c in word if '\u0590' <= c <= '\u05FF')
            if hebrew_in_word > len(word) * 0.5:  # More than 50% Hebrew
                # Reverse the word to fix character ordering
                fixed_words.append(word[::-1])
            else:
                fixed_words.append(word)
        
        fixed_lines.append(' '.join(fixed_words))
    
    return '\n'.join(fixed_lines)

def reverse_hebrew_line_order(text: str) -> str:
    """
    For each line, reverse the order of words (but not the characters in the words).
    Only applies to lines that are mostly Hebrew.
    """
    lines = text.split('\n')
    fixed_lines = []
    for line in lines:
        # Check if line contains enough Hebrew
        hebrew_in_line = sum(1 for c in line if '\u0590' <= c <= '\u05FF')
        if hebrew_in_line < len(line) * 0.3:
            fixed_lines.append(line)
            continue
        words = line.split()
        fixed_lines.append(' '.join(reversed(words)))
    return '\n'.join(fixed_lines)

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