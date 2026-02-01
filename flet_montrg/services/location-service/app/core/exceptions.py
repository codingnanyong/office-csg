"""
Custom exceptions
"""
from fastapi import HTTPException, status


class LocationNotFoundException(HTTPException):
    """위치 정보를 찾을 수 없을 때 발생하는 예외"""
    def __init__(self, sensor_id: str):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Location for sensor {sensor_id} not found"
        )


class SensorNotFoundException(HTTPException):
    """센서를 찾을 수 없을 때 발생하는 예외"""
    def __init__(self, sensor_id: str):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sensor {sensor_id} not found"
        )


class DatabaseException(HTTPException):
    """데이터베이스 관련 예외"""
    def __init__(self, detail: str):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {detail}"
        )
