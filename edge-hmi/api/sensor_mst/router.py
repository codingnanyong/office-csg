from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import SensorMst as SensorMstModel

from sensor_mst.schemas import SensorMstCreate, SensorMstRead, SensorMstUpdate

router = APIRouter(prefix="/sensor_mst", tags=["sensor_mst"])


@router.get("", response_model=list[SensorMstRead])
def list_(db: Session = Depends(get_db), skip: int = 0, limit: int = Query(100, le=500)):
    return db.query(SensorMstModel).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=SensorMstRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(SensorMstModel, id)
    if not row:
        raise HTTPException(404, "sensor_mst not found")
    return row


@router.post("", response_model=SensorMstRead, status_code=201)
def create(p: SensorMstCreate, db: Session = Depends(get_db)):
    row = SensorMstModel(
        equip_id=p.equip_id,
        sensor_code=p.sensor_code,
        unit=p.unit,
        lsl_val=p.lsl_val,
        usl_val=p.usl_val,
        lcl_val=p.lcl_val,
        ucl_val=p.ucl_val,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/{id}", response_model=SensorMstRead)
def update(id: int, p: SensorMstUpdate, db: Session = Depends(get_db)):
    row = db.get(SensorMstModel, id)
    if not row:
        raise HTTPException(404, "sensor_mst not found")
    for k, v in p.model_dump(exclude_unset=True).items():
        setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/{id}", status_code=204)
def delete(id: int, db: Session = Depends(get_db)):
    row = db.get(SensorMstModel, id)
    if not row:
        raise HTTPException(404, "sensor_mst not found")
    db.delete(row)
    db.commit()
    return None
