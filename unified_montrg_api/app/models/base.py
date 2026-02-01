"""Base SQLAlchemy mixins for common columns across different models."""

from sqlalchemy import Column, Date, String, Numeric


class DowntimeBaseMixin:
    """Common Downtime Columns Mixin"""
    business_date = Column(Date, primary_key=True, nullable=False)
    factory = Column(String, nullable=True)
    building = Column(String, nullable=True)
    down_time_target = Column(Numeric, nullable=True)
    down_time_value = Column(Numeric, nullable=True)

