"""
Text extraction module.
This module is responsible for extracting text from PDF files using pdfplumber and pdfminer.six.
"""

import io
from pathlib import Path

import pdfplumber
from pdfminer.high_level import extract_text
import pytesseract
from PIL import Image
import fitz  # PyMuPDF

from ...config import get_logger
from .exceptions import PDFTextExtractionError


logger = get_logger(__name__)

def test_for_some_hebrew(text: str):
    hebrew_chars = sum(1 for c in text if '\u0590' <= c <= '\u05FF')
    if hebrew_chars < len(text) * 0.5:  # Less than 50% Hebrew, probably not Hebrew text
        raise PDFTextExtractionError("Extracted text does not appear to be primarily Hebrew.")
    print('FOUND HEBREW TEXT', hebrew_chars, 'out of', len(text))

def extract_text_with_pdfplumber(pdf_path: Path) -> str:
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
        test_for_some_hebrew(text)

        # Fix Hebrew text direction issues
        text = fix_hebrew_text_direction(text)
        return text

    except Exception as e:
        if isinstance(e, PDFTextExtractionError):
            raise
        raise PDFTextExtractionError(f"Failed to extract text from PDF {pdf_path}: {str(e)}")

def extract_text_with_ocr(pdf_path: Path) -> str:
    """
    Extract text from image-based PDFs using OCR (Optical Character Recognition).
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Extracted text from OCR
        
    Raises:
        PDFTextExtractionError: When OCR extraction fails
    """
    assert 'heb' in pytesseract.get_languages()
    try:
        text = ""
        doc = fitz.open(str(pdf_path))

        for page_num in range(len(doc)):
            page = doc.load_page(page_num)

            # Get page as image
            mat = fitz.Matrix(2, 2)  # Scale up for better OCR
            pix = page.get_pixmap(matrix=mat)
            img_data = pix.tobytes("png")

            # Convert to PIL Image
            img = Image.open(io.BytesIO(img_data))

            # Extract text using OCR with Hebrew language support
            page_text = pytesseract.image_to_string(
                img, 
                lang='heb',  # Hebrew + English
                config='--psm 6'  # Assume uniform block of text
            )

            logger.info(f"Extracted {len(page_text)} characters from page {page_num+1} using OCR")
            text += page_text + "\n"

        doc.close()

        if not text.strip():
            raise PDFTextExtractionError(f"OCR extraction returned no text from PDF {pdf_path}")

        # Fix Hebrew text direction issues (OCR-specific handling)
        text = fix_hebrew_text_direction(text, is_ocr=True)
        return text

    except Exception as e:
        if isinstance(e, PDFTextExtractionError):
            raise
        raise PDFTextExtractionError(f"Failed to extract text with OCR from PDF {pdf_path}: {str(e)}")

def fix_hebrew_text_direction(text: str, is_ocr: bool = False) -> str:
    """
    Fix Hebrew text direction issues that commonly occur in PDF extraction.
    This handles cases where Hebrew text appears reversed or with incorrect character ordering.
    
    Args:
        text: The text to fix
        is_ocr: Whether this text was extracted using OCR (affects the fixing strategy)
    """
    if not text:
        return text

    # Check if text contains Hebrew characters
    hebrew_chars = sum(1 for c in text if '\u0590' <= c <= '\u05FF')
    if hebrew_chars < len(text) * 0.1:  # Less than 10% Hebrew, probably not Hebrew text
        return text

    if is_ocr:
        # For OCR text, only fix character-level ordering within words
        # OCR usually gets word order and line order correct, but characters within words are reversed
        return fix_ocr_hebrew_text(text)
    else:
        # For regular PDF extraction, apply both line-level and character-level fixes
        # First, fix line-level word ordering (reverse words in Hebrew lines)
        text = reverse_hebrew_line_order(text)

        # Then, fix character-level ordering within words
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


# Hebrew final-form letters (\u05DA, \u05DD, \u05DF, \u05E3, \u05E5) \u2014 Hebrew orthography is
# strict: these letters can ONLY appear as the last character of a word.
# Anywhere else is a typesetting bug. We exploit that as the heuristic.
#
# Empirical validation (2026-05-13, n=100 known-clean Hebrew chunks from
# Wikisource legal_text vs the same chunks word-by-word-reversed):
#
#   signal `final-form at word START`:
#     clean    samples: 0% rate (min/p10/median/p90/max = 0 / 0 / 0 / 0 / 0)
#     reversed samples: 11\u201327% rate (p10/median/p90 = 0.112 / 0.181 / 0.265)
#
# Perfect separation across 100/100 samples. ANY non-zero rate is signal.
# To defend against rare single-character OCR errors / typos in proper
# nouns, require \u22652 distinct final-form-at-start hits and \u226510 Hebrew
# words total before triggering.
#
# Pre-2026-05-13 behavior: reversal was unconditional. Modern Tesseract
# (`heb` lang pack) returns logical-order Hebrew already, so unconditional
# reversal CORRUPTED clean output and yielded the mojibake observed in
# 102 / 548 committee_decisions chunks (and a tail across the other
# PDF-pipeline contexts). The downstream LLM field-extractor then
# hallucinated plausible-looking PublicationDates around the garbled
# text. Probed end-to-end 2026-05-13: raw Tesseract output is clean,
# the broken "fix" was the entire bug.
_HEBREW_FINAL_FORMS = frozenset("\u05DA\u05DD\u05DF\u05E3\u05E5")
_HEBREW_PUNCT_STRIP = ":,./()\"'\u00B7-\u05F4\u05F3"
_HEBREW_MIN_WORDS = 10
_HEBREW_FINAL_START_HITS_FOR_REVERSED = 2


def _hebrew_is_visual_order(text: str, sample_size: int = 200) -> bool:
    """Return True iff `text` looks like character-reversed (visual-order)
    Hebrew. Cheap O(n) sample; safe on any input \u2014 non-Hebrew returns
    False after touching at most a handful of words.

    The signal: Hebrew final-form letters at word-start. They cannot
    legally occur there in correct logical-order Hebrew; in reversed
    Hebrew they're 11\u201327% of words. See module-level comment for the
    100-sample empirical separation.
    """
    hebrew_words = []
    for word in text.split():
        stripped = word.strip(_HEBREW_PUNCT_STRIP)
        if not stripped:
            continue
        if any('\u0590' <= c <= '\u05FF' for c in stripped):
            hebrew_words.append(stripped)
            if len(hebrew_words) >= sample_size:
                break
    if len(hebrew_words) < _HEBREW_MIN_WORDS:
        # Sample too small to confidently distinguish \u2014 no-op is the safe
        # default (modern OCR is logical-order anyway).
        return False
    final_at_start = sum(1 for w in hebrew_words if w[0] in _HEBREW_FINAL_FORMS)
    return final_at_start >= _HEBREW_FINAL_START_HITS_FOR_REVERSED


def fix_ocr_hebrew_text(text: str) -> str:
    """
    Fix Hebrew text direction issues specifically for OCR-extracted text.

    Gated by `_hebrew_is_visual_order` \u2014 only reverses characters if the
    input is detected as character-reversed (visual order). Modern
    Tesseract with the `heb` pack returns LOGICAL-order Hebrew already,
    so this function is a no-op for that case. Kept active for legacy /
    older OCR sources that produce visual order.
    """
    if not _hebrew_is_visual_order(text):
        return text

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

        # For OCR text, fix both character ordering within words AND word order within lines
        words = line.split()
        fixed_words = []

        for word in words:
            # Check if word contains Hebrew characters
            hebrew_in_word = sum(1 for c in word if '\u0590' <= c <= '\u05FF')
            if hebrew_in_word > 0:  # Contains any Hebrew characters
                # For OCR text, reverse the word to fix character ordering
                # This handles mixed Hebrew-English words better
                fixed_words.append(word[::-1])
            else:
                fixed_words.append(word)

        # For now, let's focus on character-level fixes only
        # Word order reversal is too complex and can cause over-fixing
        # The character-level fix is working well for most cases

        fixed_lines.append(' '.join(fixed_words))

    return '\n'.join(fixed_lines)


def fix_ocr_full_content(text: str) -> str:
    """
    Fix Hebrew text direction issues specifically for OCR-extracted full content field.
    This function handles the character-level reversal within Hebrew words that occurs
    in the full content field for OCR documents.

    Gated by `_hebrew_is_visual_order` (see `fix_ocr_hebrew_text`) \u2014 only
    reverses when the input actually looks character-reversed. No-op for
    modern Tesseract output.
    """
    if not text.strip():
        return text

    # Check if text contains Hebrew
    hebrew_in_text = sum(1 for c in text if '\u0590' <= c <= '\u05FF')
    if hebrew_in_text < len(text) * 0.3:  # Less than 30% Hebrew in text
        return text

    if not _hebrew_is_visual_order(text):
        return text

    # For OCR full content, each Hebrew word is reversed at the character level
    # We need to reverse each Hebrew word individually
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

        # Split into words and fix each Hebrew word
        words = line.split()
        fixed_words = []

        for word in words:
            # Check if word contains Hebrew characters
            hebrew_in_word = sum(1 for c in word if '\u0590' <= c <= '\u05FF')
            if hebrew_in_word > 0:  # Contains any Hebrew characters
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
    text = extract_text(str(pdf_path))
    logger.info(f"Extracted {len(text)} characters using pdfminer.six")
    test_for_some_hebrew(text)
    return text

def extract_text_from_pdf(pdf_path: str) -> tuple[str, bool]:
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        logger.error(f"PDF file not found: {pdf_path}")
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    # Check file size
    file_size = pdf_path.stat().st_size
    if file_size < 1024:  # Less than 1KB
        raise ValueError(f"PDF file is too small: {pdf_path}")

    # Try pdfplumber first
    try:
        logger.info("Trying to extract text with pdfplumber...")
        text = extract_text_with_pdfplumber(pdf_path)
        if text.strip():
            logger.info("Successfully extracted text with pdfplumber.")
            return text, False  # Not OCR
        else:
            logger.warning("pdfplumber returned empty text. Falling back to pdfminer.six...")
    except Exception as e:
        logger.warning(f"pdfplumber failed: {e}. Falling back to pdfminer.six...")

    # Fallback to pdfminer.six
    try:
        text = extract_text_with_pdfminer(pdf_path)
        if text.strip():
            logger.info("Successfully extracted text with pdfminer.six.")
            return text, False  # Not OCR
        else:
            logger.warning("pdfminer.six also returned empty text. Trying OCR...")
    except Exception as e:
        logger.warning(f"pdfminer.six failed: {e}. Trying OCR...")

    # Final fallback to OCR for image-based PDFs
    try:
        logger.info("Attempting OCR extraction for image-based PDF...")
        text = extract_text_with_ocr(pdf_path)
        if text.strip():
            logger.info("Successfully extracted text with OCR.")
            return text, True  # OCR was used
        else:
            logger.error("OCR also returned empty text.")
    except Exception as e:
        logger.error(f"OCR extraction failed: {e}")

    # If all methods failed
    raise PDFTextExtractionError(f"Failed to extract text from {pdf_path} with pdfplumber, pdfminer.six, and OCR. The PDF might be password-protected, corrupted, or contain no extractable content.")