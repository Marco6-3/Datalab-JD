class DataLabError(Exception):
    """Base exception for DataLab."""


class DataReadError(DataLabError):
    """Raised when no readable input data is found."""


class DataValidationError(DataLabError):
    """Raised when data violates configured schema."""
