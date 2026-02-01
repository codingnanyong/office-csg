from pydantic import BaseModel
from .spare_part_schema import SparePart

class Zone(BaseModel):
    date: str
    zone : str
    parts : list[SparePart]
    
    class Config:
        orm_mode = True