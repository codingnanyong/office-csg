from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import ShiftCfg as ShiftCfgModel

from shift_cfg.schemas import ShiftCfgCreate, ShiftCfgRead, ShiftCfgUpdate

router = APIRouter(prefix="/shift_cfg", tags=["shift_cfg"])


@router.get("", response_model=list[ShiftCfgRead])
def list_(db: Session = Depends(get_db), skip: int = 0, limit: int = Query(100, le=500)):
    return db.query(ShiftCfgModel).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=ShiftCfgRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(ShiftCfgModel, id)
    if not row:
        raise HTTPException(404, "shift_cfg not found")
    return row


@router.post("", response_model=ShiftCfgRead, status_code=201)
def create(p: ShiftCfgCreate, db: Session = Depends(get_db)):
    row = ShiftCfgModel(
        shift_name=p.shift_name,
        start_time=p.start_time,
        end_time=p.end_time,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/{id}", response_model=ShiftCfgRead)
def update(id: int, p: ShiftCfgUpdate, db: Session = Depends(get_db)):
    row = db.get(ShiftCfgModel, id)
    if not row:
        raise HTTPException(404, "shift_cfg not found")
    for k, v in p.model_dump(exclude_unset=True).items():
        setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/{id}", status_code=204)
def delete(id: int, db: Session = Depends(get_db)):
    row = db.get(ShiftCfgModel, id)
    if not row:
        raise HTTPException(404, "shift_cfg not found")
    db.delete(row)
    db.commit()
    return None
