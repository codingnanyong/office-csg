from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import KpiCfg as KpiCfgModel

from kpi_cfg.schemas import KpiCfgCreate, KpiCfgRead, KpiCfgUpdate

router = APIRouter(prefix="/kpi_cfg", tags=["kpi_cfg"])


@router.get("", response_model=list[KpiCfgRead])
def list_(db: Session = Depends(get_db), skip: int = 0, limit: int = Query(100, le=500)):
    return db.query(KpiCfgModel).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=KpiCfgRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(KpiCfgModel, id)
    if not row:
        raise HTTPException(404, "kpi_cfg not found")
    return row


@router.post("", response_model=KpiCfgRead, status_code=201)
def create(p: KpiCfgCreate, db: Session = Depends(get_db)):
    row = KpiCfgModel(
        equip_id=p.equip_id,
        std_cycle_time=p.std_cycle_time,
        target_oee=p.target_oee,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/{id}", response_model=KpiCfgRead)
def update(id: int, p: KpiCfgUpdate, db: Session = Depends(get_db)):
    row = db.get(KpiCfgModel, id)
    if not row:
        raise HTTPException(404, "kpi_cfg not found")
    for k, v in p.model_dump(exclude_unset=True).items():
        setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/{id}", status_code=204)
def delete(id: int, db: Session = Depends(get_db)):
    row = db.get(KpiCfgModel, id)
    if not row:
        raise HTTPException(404, "kpi_cfg not found")
    db.delete(row)
    db.commit()
    return None
