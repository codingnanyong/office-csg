from pydantic import BaseModel
from .ip_prf_schema import IpPrf

class IPModel(BaseModel):
    unit_id : str
    performance : list[IpPrf]

    class Config:
        orm_mode = True