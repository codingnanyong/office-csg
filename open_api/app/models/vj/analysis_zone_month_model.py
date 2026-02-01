from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from .zone_base_model import ZoneBaseSchema

Base = declarative_base()

class ZoneMonthSchema(ZoneBaseSchema):
    __tablename__ = "analysis_zone_month"
    __table_args__ = (
        PrimaryKeyConstraint('date', 'zone', 'part_cd', name='analysis_zone_month_pkey'),
        {'schema': 'services'}
    )

