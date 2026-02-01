from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import KpiSum as KpiSumModel

from kpi_sum.schemas import KpiSumRead

router = APIRouter(prefix="/kpi_sum", tags=["kpi_sum"])


@router.get("", response_model=list[KpiSumRead])
def list_(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = Query(100, le=500),
    calc_date: date | None = None,
    equip_id: int | None = None,
):
    q = db.query(KpiSumModel)
    if calc_date is not None:
        q = q.filter(KpiSumModel.calc_date == calc_date)
    if equip_id is not None:
        q = q.filter(KpiSumModel.equip_id == equip_id)
    return q.offset(skip).limit(limit).all()


@router.get("/{id}", response_model=KpiSumRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(KpiSumModel, id)
    if not row:
        raise HTTPException(404, "kpi_sum not found")
    return row
