from datetime import datetime

from pydantic import BaseModel, ConfigDict


class AlarmHisBase(BaseModel):
    time: datetime
    equip_id: int
    alarm_def_id: int
    trigger_val: float | None = None
    alarm_type: str | None = None


class AlarmHisCreate(AlarmHisBase):
    pass


class AlarmHisRead(AlarmHisBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
