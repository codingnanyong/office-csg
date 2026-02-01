from datetime import datetime

from pydantic import BaseModel, ConfigDict


class StatusHisBase(BaseModel):
    equip_id: int
    status_code: str | None = None
    start_time: datetime
    end_time: datetime | None = None


class StatusHisCreate(StatusHisBase):
    pass


class StatusHisRead(StatusHisBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
