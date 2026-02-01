from datetime import datetime

from pydantic import BaseModel, ConfigDict


class MeasurementBase(BaseModel):
    time: datetime
    equip_id: int
    sensor_id: int
    value: float | None = None


class MeasurementCreate(MeasurementBase):
    pass


class MeasurementRead(MeasurementBase):
    model_config = ConfigDict(from_attributes=True)
