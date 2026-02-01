
from pydantic import BaseModel
from typing import List
from .tag_path_schema import TagPath

class Tag(BaseModel):
    tagid: str
    tagtype: str
    path: List[TagPath]
    
    class Config:
        orm_mode = True
