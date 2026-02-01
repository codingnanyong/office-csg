from typing import List
from pydantic import BaseModel
from .tag_schema import Tag

class TagHistory(BaseModel):
    wsno: str
    tags : List[Tag]
    class Config:
        orm_mode = True
