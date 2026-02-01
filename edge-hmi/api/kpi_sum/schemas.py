from datetime import date

from pydantic import BaseModel, ConfigDict


class KpiSumRead(BaseModel):
    id: int
    calc_date: date
    shift_def_id: int | None
    line_id: int | None
    equip_id: int | None
    availability: float | None
    performance: float | None
    quality: float | None
    oee: float | None
    mttr: float | None
    mtbf: float | None
    model_config = ConfigDict(from_attributes=True)
