from typing import Optional
from pydantic import BaseModel

class SparePartUse(BaseModel):
    year : int
    month : int
    useage : float