from pydantic import BaseModel
from typing import List
from .sensor_schema import Sensor

class IoT(BaseModel):
    location : str
    sensors : List[Sensor]

    class Config:
        orm_mode = True