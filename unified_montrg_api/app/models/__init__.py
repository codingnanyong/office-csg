"""SQLAlchemy models package."""

from app.models.chiller_status import ChillerStatus  # noqa: F401
from app.models.economin_lifespan import EconominLifespan  # noqa: F401
from app.models.ip_rollgap import IpRollgap  # noqa: F401
from app.models.ipi_quality_temperature import IpiQualityTemperature  # noqa: F401
from app.models.machine_grade import MachineGrade  # noqa: F401
from app.models.mart_productivity_by_op_cd import MartProductivityByOpCd  # noqa: F401
from app.models.mart_productivity_by_op_group import MartProductivityByOpGroup  # noqa: F401
from app.models.mart_productivity_by_ip_zone import MartProductivityByIpZone  # noqa: F401
from app.models.mart_productivity_by_ip_machine import MartProductivityByIpMachine  # noqa: F401
from app.models.mart_productivity_by_ph_zone import MartProductivityByPhZone  # noqa: F401
from app.models.mart_productivity_by_ph_machine import MartProductivityByPhMachine  # noqa: F401
from app.models.mart_realtime_productivity_ip_machine import MartRealtimeProductivityIpMachine  # noqa: F401
from app.models.mart_realtime_productivity_ip_zone import MartRealtimeProductivityIpZone  # noqa: F401
from app.models.mart_realtime_productivity_op_cd import MartRealtimeProductivityOpCd  # noqa: F401
from app.models.mart_realtime_productivity_op_group import MartRealtimeProductivityOpGroup  # noqa: F401
from app.models.mart_realtime_productivity_ph_line import MartRealtimeProductivityPhLine  # noqa: F401
from app.models.mart_realtime_productivity_ph_machine import MartRealtimeProductivityPhMachine  # noqa: F401
from app.models.mart_downtime_by_line import MartDowntimeByLine  # noqa: F401
from app.models.mart_downtime_by_machine import MartDowntimeByMachine  # noqa: F401
from app.models.bas_location import BasLocation  # noqa: F401
from app.models.mc_master import McMaster  # noqa: F401
from app.models.rtf_data import RtfData  # noqa: F401
from app.models.sensor import Sensor  # noqa: F401
from app.models.spare_part_inventory import SparePartInventory  # noqa: F401

