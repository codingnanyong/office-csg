from pydantic import BaseModel
from .spare_part_use_schema import SparePartUse

class Usage(BaseModel):
    unit_id : str
    partcd : str
    useage : list[SparePartUse]
    
    class Config:
        orm_mode = True