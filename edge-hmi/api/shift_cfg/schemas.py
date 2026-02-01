from datetime import time

from pydantic import BaseModel, ConfigDict


class ShiftCfgBase(BaseModel):
    shift_name: str
    start_time: time
    end_time: time


class ShiftCfgCreate(ShiftCfgBase):
    pass


class ShiftCfgUpdate(BaseModel):
    shift_name: str | None = None
    start_time: time | None = None
    end_time: time | None = None


class ShiftCfgRead(ShiftCfgBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
