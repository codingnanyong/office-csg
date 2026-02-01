from pydantic import BaseModel

class Wip(BaseModel):
    opcd: str
    opname: str
    oplocalname: str
    cnt: int
    qty: float

    class Config:
        orm_mode = True
