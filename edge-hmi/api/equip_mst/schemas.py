from pydantic import BaseModel, ConfigDict


class EquipMstBase(BaseModel):
    line_id: int
    equip_code: str
    name: str
    type: str | None = None


class EquipMstCreate(EquipMstBase):
    pass


class EquipMstUpdate(BaseModel):
    line_id: int | None = None
    equip_code: str | None = None
    name: str | None = None
    type: str | None = None


class EquipMstRead(EquipMstBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
