from pydantic import BaseModel, Field
from typing import List
from .environment_schema import Environment

class Sensor(BaseModel):
    sensorid: str
    environments: List[Environment] = Field(default_factory=list) 

    class Config:
        orm_mode = True  
