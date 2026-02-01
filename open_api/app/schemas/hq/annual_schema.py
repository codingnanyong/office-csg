from pydantic import BaseModel

class Annual(BaseModel):
    year:int
    factory:str
    plancnt: int
    planqty: int
    prodcnt : int 
    prodqty: int

    class Config:
        orm_mode = True

