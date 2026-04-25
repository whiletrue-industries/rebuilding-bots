"""
Custom exceptions for the PDF extraction pipeline.
"""

class PDFExtractionError(Exception):
    """Base exception for PDF extraction errors."""
    pass


class PDFTextExtractionError(PDFExtractionError):
    """Raised when text extraction from PDF fails."""
    pass


class FieldExtractionError(PDFExtractionError):
    """Raised when LLM field extraction fails."""
    pass


class ConfigurationError(PDFExtractionError):
    """Raised when configuration is invalid or missing."""
    pass


class GoogleSheetsError(PDFExtractionError):
    """Raised when Google Sheets operations fail."""
    pass


class CSVOutputError(PDFExtractionError):
    """Raised when CSV output operations fail."""
    pass


class ValidationError(PDFExtractionError):
    """Raised when data validation fails."""
    pass


class EmptyUpstreamIndex(PDFExtractionError):
    """Raised when an upstream PDF source returns an empty index.csv.

    This is the signal that upstream (typically
    https://next.obudget.org/datapackages/knesset/<feed>/) has gone empty.
    Callers must not overwrite existing extraction CSVs with nothing; the
    caller expectation is to propagate the exception so the refresh job
    fails loudly and the last-known-good CSV on EFS stays untouched.
    """
    pass
