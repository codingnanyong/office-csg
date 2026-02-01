from pydantic import BaseModel
from typing import List
from .tooling_scheam import Tooling

class ToolingWarehouse(BaseModel):
    loc_cd : str
    loc_name: str
    toolings : List[Tooling]
    
    class Config:
        orm_mode = True