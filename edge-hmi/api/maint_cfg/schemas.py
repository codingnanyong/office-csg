from pydantic import BaseModel, ConfigDict


class MaintCfgBase(BaseModel):
    maint_type: str
    description: str | None = None


class MaintCfgCreate(MaintCfgBase):
    pass


class MaintCfgUpdate(BaseModel):
    maint_type: str | None = None
    description: str | None = None


class MaintCfgRead(MaintCfgBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
