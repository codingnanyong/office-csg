from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from .mach_base_model import MachBaseSchema

Base = declarative_base()

class MachMonthSchema(MachBaseSchema):
    __tablename__ = "analysis_zone_machine_month"
    __table_args__ = (
        PrimaryKeyConstraint('date', 'zone', 'mach_id','part_cd', name='analysis_zone_machine_month_pkey'),
        {'schema': 'services'}
    )

