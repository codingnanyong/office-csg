from datetime import datetime

from pydantic import BaseModel, ConfigDict


class MaintHisBase(BaseModel):
    equip_id: int
    maint_def_id: int
    alarm_his_id: int | None = None
    worker_id: int | None = None
    start_time: datetime
    end_time: datetime | None = None
    maint_desc: str | None = None


class MaintHisCreate(MaintHisBase):
    pass


class MaintHisRead(MaintHisBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
