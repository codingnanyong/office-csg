from pydantic import BaseModel, ConfigDict


class LineMstBase(BaseModel):
    line_code: str
    line_name: str | None = None


class LineMstCreate(LineMstBase):
    pass


class LineMstUpdate(BaseModel):
    line_code: str | None = None
    line_name: str | None = None


class LineMstRead(LineMstBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
