"""Service layer for IP Rollgap operations."""

from typing import List
from sqlalchemy import select, func, Numeric, literal
from sqlalchemy.orm import aliased

from app.core.database import get_session
from app.models.sensor import Sensor
from app.models.ip_rollgap import IpRollgap
from app.schemas.ip_rollgap import IpRollgapRecord


def _map_sensor_id_to_roll_name(sensor_id: str) -> str:
    """Map sensor ID to Roll name"""
    mapping = {
        "IPRIOT-A201": "Roll A",
        "IPRIOT-A202": "Roll B",
        "IPRIOT-A203": "Roll C",
        "IPRIOT-A204": "Roll D",
    }
    return mapping.get(sensor_id, sensor_id)


async def fetch_ip_rollgap() -> List[IpRollgapRecord]:
    """Fetch the latest data for rollgap sensors
    
    sensor table (CKP_IP_DATABASE_URL is used)
    
    Returns:
        IP Rollgap record list
    """
    # use LATERAL join to find the latest rollgap data for each sensor
    latest_rollgap_subq = (
        select(
            IpRollgap.capture_dt,
            func.round(IpRollgap.gap_left.cast(Numeric), 2).label("gap_left"),
            func.round(IpRollgap.gap_right.cast(Numeric), 2).label("gap_right"),
        )
        .where(IpRollgap.sensor_id == Sensor.sensor_id)
        .order_by(IpRollgap.capture_dt.desc())
        .limit(1)
        .lateral()
    )
    
    latest_rg = aliased(latest_rollgap_subq, name="rg")
    
    # main query: join the latest Rollgap with Sensor using LATERAL join
    query = (
        select(
            Sensor.sensor_id,
            latest_rg.c.capture_dt,
            latest_rg.c.gap_left,
            latest_rg.c.gap_right,
        )
        .select_from(Sensor)
        .outerjoin(latest_rg, literal(True))
        .where(Sensor.name == "rollgap")
    )
    
    # use CKP_IP_DATABASE_URL (alias: ckp_ip)
    async with get_session(db_alias="ckp_ip") as session:
        result = await session.execute(query)
        rows = result.all()
        
        records = []
        for row in rows:
            sensor_id = row[0]
            roll_name = _map_sensor_id_to_roll_name(sensor_id)
            records.append(
                IpRollgapRecord(
                    roll_name=roll_name,
                    capture_dt=row[1],
                    gap_left=float(row[2]) if row[2] is not None else None,
                    gap_right=float(row[3]) if row[3] is not None else None,
                )
            )
        
        return records

