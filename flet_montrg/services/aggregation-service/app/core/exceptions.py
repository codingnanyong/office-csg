"""
Custom exceptions for the application
"""
from fastapi import HTTPException, status


class AggregationServiceException(Exception):
    """Base exception for aggregation service"""
    pass


class DataNotFoundError(AggregationServiceException):
    """Raised when requested data is not found"""
    pass


class InvalidDataError(AggregationServiceException):
    """Raised when provided data is invalid"""
    pass


class ExternalServiceError(AggregationServiceException):
    """Raised when external service call fails"""
    pass


# HTTP Exception helpers
def raise_not_found(message: str = "Resource not found"):
    """Raise 404 Not Found exception"""
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=message
    )


def raise_bad_request(message: str = "Bad request"):
    """Raise 400 Bad Request exception"""
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=message
    )


def raise_internal_error(message: str = "Internal server error"):
    """Raise 500 Internal Server Error exception"""
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=message
    )
