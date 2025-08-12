"""
Centralized Error Tracking and Reporting for the Sync Module.

This module provides a robust system for tracking, categorizing, and reporting
errors that occur during the sync process. It is designed to produce actionable
error messages that can be used for alerting and debugging.

Key Features:
- Custom Exception Classes: Specific exceptions for different types of failures
  (e.g., network, parsing, configuration).
- ErrorTracker: A central class to aggregate and manage all errors that
  occur during a sync run.
- Actionable Reports: Generates detailed error reports that include recovery
  suggestions.
- Severity Levels: Categorizes errors by severity (WARNING, ERROR, CRITICAL)
  to prioritize responses.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any

class ErrorSeverity(Enum):
    """
    Defines the severity of an error.
    """
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

@dataclass
class SyncError:
    """
    A structured object representing a single error that occurred during the sync process.
    """
    message: str
    source_id: Optional[str] = None
    severity: ErrorSeverity = ErrorSeverity.ERROR
    details: Dict[str, Any] = field(default_factory=dict)
    recovery_suggestion: Optional[str] = None

    def to_dict(self):
        return {
            "message": self.message,
            "source_id": self.source_id,
            "severity": self.severity.value,
            "details": self.details,
            "recovery_suggestion": self.recovery_suggestion
        }

# Custom Exception Classes
class SyncException(Exception):
    """Base class for all custom sync exceptions."""
    def __init__(self, message: str, source_id: Optional[str] = None, recovery_suggestion: Optional[str] = None):
        self.message = message
        self.source_id = source_id
        self.recovery_suggestion = recovery_suggestion
        super().__init__(self.message)

class ConfigurationError(SyncException):
    """Indicates an error in the sync_config.yaml file."""
    pass

class SourceFetchError(SyncException):
    """Indicates a failure to fetch content from a source."""
    pass

class DocumentParseError(SyncException):
    """Indicates a failure to parse a document's content."""
    pass

class EmbeddingError(SyncException):
    """Indicates a failure during the embedding process."""
    pass

class CacheError(SyncException):
    """Indicates a failure related to the caching system."""
    pass

class ExternalServiceError(SyncException):
    """Indicates a failure with an external service (e.g., OpenAI, Elasticsearch)."""
    pass


class ErrorTracker:
    """
    A centralized tracker for aggregating errors during a sync run.
    """
    def __init__(self):
        self.errors: List[SyncError] = []

    def report(self, message: str, source_id: Optional[str] = None, severity: ErrorSeverity = ErrorSeverity.ERROR, details: Optional[Dict[str, Any]] = None, recovery_suggestion: Optional[str] = None):
        """
        Report a new error.
        """
        error = SyncError(
            message=message,
            source_id=source_id,
            severity=severity,
            details=details or {},
            recovery_suggestion=recovery_suggestion
        )
        self.errors.append(error)

    def report_exception(self, exc: SyncException, severity: ErrorSeverity = ErrorSeverity.ERROR):
        """
        Report an error from a SyncException.
        """
        self.report(
            message=exc.message,
            source_id=exc.source_id,
            severity=severity,
            recovery_suggestion=exc.recovery_suggestion
        )

    def get_errors(self, min_severity: ErrorSeverity = ErrorSeverity.WARNING) -> List[SyncError]:
        """
        Get all errors at or above a certain severity level.
        """
        severity_map = {
            ErrorSeverity.WARNING: 1,
            ErrorSeverity.ERROR: 2,
            ErrorSeverity.CRITICAL: 3
        }
        min_level = severity_map.get(min_severity, 1)
        return [e for e in self.errors if severity_map.get(e.severity, 1) >= min_level]

    def has_critical_errors(self) -> bool:
        """
        Check if any critical errors have been reported.
        """
        return any(e.severity == ErrorSeverity.CRITICAL for e in self.errors)

    def generate_report(self) -> Dict[str, Any]:
        """
        Generate a summary report of all errors.
        """
        report = {
            "total_errors": len(self.errors),
            "critical_count": len(self.get_errors(ErrorSeverity.CRITICAL)),
            "error_count": len(self.get_errors(ErrorSeverity.ERROR)) - len(self.get_errors(ErrorSeverity.CRITICAL)),
            "warning_count": len(self.get_errors(ErrorSeverity.WARNING)) - len(self.get_errors(ErrorSeverity.ERROR)),
            "errors": [e.to_dict() for e in self.errors]
        }
        return report
