from pydantic import BaseModel
from .spare_part_schema import SparePart

class Mach(BaseModel):
    date: str
    zone : str
    mach_id : str
    parts : list[SparePart]
    
    class Config:
        orm_mode = True