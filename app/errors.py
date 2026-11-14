"""Domain-specific exceptions mapped to HTTP responses in the API layer."""
from __future__ import annotations


class LeakCheckError(Exception):
    """Base class. ``status_code`` selects the HTTP response."""

    status_code = 400


class CsvValidationError(LeakCheckError):
    status_code = 422


class NotFoundError(LeakCheckError):
    status_code = 404


class NotReleasedError(LeakCheckError):
    """Raised when a download is requested but no accepted version exists."""

    status_code = 409


class BusyError(LeakCheckError):
    status_code = 409
