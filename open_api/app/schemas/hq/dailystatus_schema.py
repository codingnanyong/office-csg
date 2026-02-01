from pydantic import BaseModel

class Dailystatus(BaseModel):
    factory: str
    opcd: str
    opname: str
    oplocalname: str
    plancnt: int
    planqty: float
    prodcnt: int
    prodqty: float
    rate: float

    class Config:
        orm_mode = True
