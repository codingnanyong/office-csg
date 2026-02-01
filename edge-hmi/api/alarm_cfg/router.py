from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import AlarmCfg as AlarmCfgModel

from alarm_cfg.schemas import AlarmCfgCreate, AlarmCfgRead, AlarmCfgUpdate

router = APIRouter(prefix="/alarm_cfg", tags=["alarm_cfg"])


@router.get("", response_model=list[AlarmCfgRead])
def list_(db: Session = Depends(get_db), skip: int = 0, limit: int = Query(100, le=500)):
    return db.query(AlarmCfgModel).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=AlarmCfgRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(AlarmCfgModel, id)
    if not row:
        raise HTTPException(404, "alarm_cfg not found")
    return row


@router.post("", response_model=AlarmCfgRead, status_code=201)
def create(p: AlarmCfgCreate, db: Session = Depends(get_db)):
    row = AlarmCfgModel(
        alarm_code=p.alarm_code,
        severity=p.severity,
        description=p.description,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/{id}", response_model=AlarmCfgRead)
def update(id: int, p: AlarmCfgUpdate, db: Session = Depends(get_db)):
    row = db.get(AlarmCfgModel, id)
    if not row:
        raise HTTPException(404, "alarm_cfg not found")
    for k, v in p.model_dump(exclude_unset=True).items():
        setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/{id}", status_code=204)
def delete(id: int, db: Session = Depends(get_db)):
    row = db.get(AlarmCfgModel, id)
    if not row:
        raise HTTPException(404, "alarm_cfg not found")
    db.delete(row)
    db.commit()
    return None
