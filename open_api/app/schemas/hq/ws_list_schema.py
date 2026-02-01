from pydantic import BaseModel
from typing import Optional

class wsList(BaseModel):
    wsno: str
    opcd: str
    opname: str
    oplocalname: str
    pm: Optional[str] = None
    model: Optional[str] = None
    stylecd: Optional[str] = None
    season: Optional[str] = None
    bom: Optional[str] = None
    devcolor: Optional[str] = None
    plan_date: str
    prod_date: Optional[str] = None
    status: str

    class Config:
        orm_mode = True
