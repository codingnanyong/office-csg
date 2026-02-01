import pytz
from typing import List, Optional
from datetime import datetime
from sqlalchemy.future import select
from sqlalchemy import func
from app.models.hq.sensor_model import SensorSchema
from app.models.hq.temperature_model import TemperatureSchema
from app.schemas.hq.iot_schema import IoT, Sensor
from app.schemas.hq.sensor_schema import Environment

iot_columns = [
    SensorSchema.mach_id.label('location'),
    SensorSchema.sensor_id.label('sensorid'),
    TemperatureSchema.capture_dt.label('measurement_time'),
    TemperatureSchema.t1.label('temperature'),
    TemperatureSchema.t2.label('humidity')
]

def set_iot(rows) -> List[IoT]:
    sensor_dict = {}

    for row in rows:
        mapping = dict(row)  
        location = mapping['location']
        sensor_id = mapping['sensorid']
        
        environment = Environment(
            measurement_time=mapping['measurement_time'],
            temperature=mapping['temperature'],
            humidity=mapping['humidity']
        )

        if location not in sensor_dict:
            sensor_dict[location] = {}

        if sensor_id not in sensor_dict[location]:
            sensor_dict[location][sensor_id] = Sensor(sensorid=sensor_id, environments=[]) 

        sensor_dict[location][sensor_id].environments.append(environment)

    iot_list = [
        IoT(location=location, sensors=list(sensors.values()))
        for location, sensors in sensor_dict.items()
    ]
    return iot_list

def filter_iot(iots: List[IoT], loc: str = None) -> IoT:
    location_map = {
        '1': '3D Printer Room',
        '2': '나염실'
    }

    if loc in location_map:
        filtered = [iot for iot in iots if iot.location == location_map[loc]]
        return filtered[0] if filtered else None 
    
    return iots[0] if iots else None  

def get_iot_data(
    db, 
    loc: str = None, 
    latest: bool = False, 
    today: bool = False, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> IoT:
    stmt = select(*iot_columns).outerjoin(
        TemperatureSchema, SensorSchema.sensor_id == TemperatureSchema.sensor_id
    )

    KST = pytz.timezone("Asia/Seoul")

    if latest:
        latest_subquery = select(
            TemperatureSchema.sensor_id, 
            func.max(TemperatureSchema.capture_dt).label("latest_time")
        ).group_by(TemperatureSchema.sensor_id).subquery()

        stmt = stmt.join(
            latest_subquery,
            (TemperatureSchema.sensor_id == latest_subquery.c.sensor_id) & 
            (TemperatureSchema.capture_dt == latest_subquery.c.latest_time)
        )

    elif today:

        today_kst = datetime.now(KST).date()

        today_start_kst = datetime.combine(today_kst, datetime.min.time())  
        today_end_kst = datetime.combine(today_kst, datetime.max.time()) 
        
        stmt = stmt.where(
            TemperatureSchema.capture_dt.between(today_start_kst, today_end_kst)
        )

    elif start_date and end_date:
        start_dt_kst = datetime.strptime(start_date, "%Y%m%d")
        end_dt_kst = datetime.strptime(end_date, "%Y%m%d")

        start_dt_kst = datetime.combine(start_dt_kst, datetime.min.time())  
        end_dt_kst = datetime.combine(end_dt_kst, datetime.max.time()) 

        stmt = stmt.where(
            TemperatureSchema.capture_dt.between(start_dt_kst, end_dt_kst)
        )

    rows = db.execute(stmt).mappings().all()
    iots = set_iot(rows)  

    return filter_iot(iots, loc) 

async def get_iot_data_async(
    db, 
    loc: str = None, 
    latest: bool = False, 
    today: bool = False, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> IoT:
    stmt = select(*iot_columns).outerjoin(
        TemperatureSchema, SensorSchema.sensor_id == TemperatureSchema.sensor_id
    )

    KST = pytz.timezone("Asia/Seoul")

    if latest:
        latest_subquery = select(
            TemperatureSchema.sensor_id, 
            func.max(TemperatureSchema.capture_dt).label("latest_time")
        ).group_by(TemperatureSchema.sensor_id).subquery()

        stmt = stmt.join(
            latest_subquery,
            (TemperatureSchema.sensor_id == latest_subquery.c.sensor_id) & 
            (TemperatureSchema.capture_dt == latest_subquery.c.latest_time)
        )

    elif today:
        today_kst = datetime.now(KST).date()

        today_start_kst = datetime.combine(today_kst, datetime.min.time())  
        today_end_kst = datetime.combine(today_kst, datetime.max.time()) 
        
        stmt = stmt.where(
            TemperatureSchema.capture_dt.between(today_start_kst, today_end_kst)
        )

    elif start_date and end_date:
        start_dt_kst = datetime.strptime(start_date, "%Y%m%d")
        end_dt_kst = datetime.strptime(end_date, "%Y%m%d")

        start_dt_kst = datetime.combine(start_dt_kst, datetime.min.time())  
        end_dt_kst = datetime.combine(end_dt_kst, datetime.max.time()) 

        stmt = stmt.where(
            TemperatureSchema.capture_dt.between(start_dt_kst, end_dt_kst)
        )

    result = await db.execute(stmt)
    rows = result.mappings().all()
    iots = set_iot(rows)  

    return filter_iot(iots, loc)
