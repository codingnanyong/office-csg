from typing import Optional
from pydantic import BaseModel

class IpPrf(BaseModel):
    date : Optional[str]
    qty : Optional[float]
    
    class Config:
        orm_mode = True