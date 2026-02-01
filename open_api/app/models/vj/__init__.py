from sqlalchemy.ext.declarative import declarative_base
from .ip_rst_model import IpPrfSchema
from .cmms_wo_model import WorkOrderSchema
from .analysis_zone_year_model import ZoneYearSchema
from .analysis_zone_month_model import ZoneMonthSchema
from .analysis_mach_year_model import MachYearSchema
from .analysis_mach_month_model import MachMonthSchema

Base = declarative_base()

__all__ = ['Base', 
           'IpPrfSchema',
           'WorkOrderSchema',
           'ZoneYearSchema',
           'ZoneMonthSchema',
           'MachYearSchema',
           'MachMonthSchema'
        ]