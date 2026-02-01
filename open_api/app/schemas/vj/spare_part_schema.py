from typing import Optional
from datetime import datetime
from pydantic import BaseModel

class SparePart(BaseModel):
    part_cd : str
    part_nm : str
    cycle_dt : Optional[int]
    current_wo_dt : Optional[datetime] 
    previous_wo_dt : Optional[datetime] 
    total_qty : Optional[float]
    previous_total_qty : Optional[float]
    min: Optional[float]
    max : Optional[float]
    stock_qty: Optional[float]
    rn : int

    class Config:
        orm_mode = True