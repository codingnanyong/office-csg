from pydantic import BaseModel
from typing import Optional

class Tooling(BaseModel):
    barcode:str
    parents_tooling_cd: Optional[str] = None
    part_cd : Optional[str] = None
    part_name : str
    process_cd : str
    process_name : Optional[str] = None
    name : str
    size: Optional[str] = None
    status : str

    class Config:
        orm_mode = True
