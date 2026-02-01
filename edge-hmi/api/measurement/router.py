from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import Measurement as MeasurementModel

from measurement.schemas import MeasurementCreate, MeasurementRead

router = APIRouter(prefix="/measurement", tags=["measurement"])


@router.get("", response_model=list[MeasurementRead])
def list_(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = Query(100, le=1000),
    equip_id: int | None = None,
    sensor_id: int | None = None,
    time_from: datetime | None = None,
    time_to: datetime | None = None,
):
    q = db.query(MeasurementModel)
    if equip_id is not None:
        q = q.filter(MeasurementModel.equip_id == equip_id)
    if sensor_id is not None:
        q = q.filter(MeasurementModel.sensor_id == sensor_id)
    if time_from is not None:
        q = q.filter(MeasurementModel.time >= time_from)
    if time_to is not None:
        q = q.filter(MeasurementModel.time <= time_to)
    return q.order_by(MeasurementModel.time.desc()).offset(skip).limit(limit).all()


@router.post("", response_model=MeasurementRead, status_code=201)
def create(p: MeasurementCreate, db: Session = Depends(get_db)):
    row = MeasurementModel(
        time=p.time,
        equip_id=p.equip_id,
        sensor_id=p.sensor_id,
        value=p.value,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
