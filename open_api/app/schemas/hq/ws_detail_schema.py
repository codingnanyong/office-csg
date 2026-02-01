from pydantic import BaseModel
from typing import Optional

class wsDetail(BaseModel):
    pm: Optional[str] = None
    season :Optional[str] = None
    model: Optional[str] = None
    gender: Optional[str] = None
    colorway: Optional[str] = None
    stylecd: Optional[str] = None
    modelid: Optional[str] = None
    bom: Optional[str] = None
    devstyle: Optional[str] = None
    category: Optional[str] = None
    prodfactory: Optional[str] = None
    size: Optional[str] = None
    sampleqty: float
    
    class Config:
        orm_mode = True
