from pydantic import BaseModel, ConfigDict


class KpiCfgBase(BaseModel):
    equip_id: int
    std_cycle_time: float | None = None
    target_oee: float | None = None


class KpiCfgCreate(KpiCfgBase):
    pass


class KpiCfgUpdate(BaseModel):
    equip_id: int | None = None
    std_cycle_time: float | None = None
    target_oee: float | None = None


class KpiCfgRead(KpiCfgBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
