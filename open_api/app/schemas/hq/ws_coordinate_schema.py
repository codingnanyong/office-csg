from pydantic import BaseModel
from typing import Optional

class wsCoordinate(BaseModel):
    tagid: str
    opcd: Optional[str] = None
    status: Optional[str] = None
    tagtype : str
    floor: Optional[str] = None
    x : float
    y : float
    
    class Config:
        orm_mode = True
