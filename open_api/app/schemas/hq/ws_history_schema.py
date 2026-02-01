from pydantic import BaseModel
from typing import Optional

class wsHistory(BaseModel):
    opcd: str
    op_name: Optional[str] = None
    op_local_name: Optional[str] = None
    plan_date: Optional[str] = None
    prod_date: Optional[str] = None
    prod_time: Optional[str] = None
    prod_qty: Optional[float] = None
    status : str
    
    class Config:
        orm_mode = True
