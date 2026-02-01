from pydantic import BaseModel, ConfigDict


class AlarmCfgBase(BaseModel):
    alarm_code: str
    severity: str | None = None
    description: str | None = None


class AlarmCfgCreate(AlarmCfgBase):
    pass


class AlarmCfgUpdate(BaseModel):
    alarm_code: str | None = None
    severity: str | None = None
    description: str | None = None


class AlarmCfgRead(AlarmCfgBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
