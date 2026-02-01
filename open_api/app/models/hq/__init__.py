from sqlalchemy.ext.declarative import declarative_base
from .dailystatus_model import DailystatusSchema
from .annual_model import AnnualSchema
from .ws_detail_model import wsDetailSchema
from .ws_his_model import wsHisSchema
from .ws_status_model import wsStatusSchema
from .opcd_model import OpCdSchema
from .common_model import CommonSchema
from .employee_model import EmployeeSchema
from .tags_model import TagsSchema
from .tags_status_model import TagStatusSchema
from .tag_his_model import TagHisSchema
from .sensor_model import SensorSchema
from .temperature_model import TemperatureSchema
from .tooling_model import ToolingSchema

Base = declarative_base()

__all__ = ['Base', 
           'DailystatusSchema',
           'AnnualSchema',
           'wsDetailSchema',
           'wsStatusSchema',
           'wsHisSchema',
           'OpCdSchema',
           'CommonSchema',
           'EmployeeSchema',
           'TagsSchema',
           'TagStatusSchema',
           'TagHisSchema',
           'SensorSchema',
           'TemperatureSchema',
           'ToolingSchema'
         ]
