from pydantic import BaseModel
from datetime import datetime

class Environment(BaseModel):
    measurement_time: datetime  
    temperature: float  
    humidity: float

    class Config:
        orm_mode = True