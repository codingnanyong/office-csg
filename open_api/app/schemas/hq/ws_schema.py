from pydantic import BaseModel
from typing import List
from .ws_detail_schema import wsDetail
from .ws_history_schema import wsHistory
from .ws_coordinate_schema import wsCoordinate

class ws(BaseModel):
    wsno: str
    detail: wsDetail
    history: List[wsHistory]
    coordinate: List[wsCoordinate]
    
    class Config:
        from_attributes = True