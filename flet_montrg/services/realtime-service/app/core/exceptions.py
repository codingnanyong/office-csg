"""
Custom exceptions
"""
from fastapi import HTTPException, status


class ThresholdNotFoundException(HTTPException):
    """임계치를 찾을 수 없을 때 발생하는 예외"""
    def __init__(self, threshold_id: int):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Threshold with id {threshold_id} not found"
        )


class InvalidThresholdDataException(HTTPException):
    """잘못된 임계치 데이터일 때 발생하는 예외"""
    def __init__(self, detail: str):
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail
        )


class DatabaseException(HTTPException):
    """데이터베이스 관련 예외"""
    def __init__(self, detail: str):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {detail}"
        )
