from pydantic import BaseModel
from .ws_coordinate_schema import wsCoordinate

class CurrentTag(BaseModel):
    wsno: str
    coordinate: list[wsCoordinate]
    class Config:
        orm_mode = True
