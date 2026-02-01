from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import MaintCfg as MaintCfgModel

from maint_cfg.schemas import MaintCfgCreate, MaintCfgRead, MaintCfgUpdate

router = APIRouter(prefix="/maint_cfg", tags=["maint_cfg"])


@router.get("", response_model=list[MaintCfgRead])
def list_(db: Session = Depends(get_db), skip: int = 0, limit: int = Query(100, le=500)):
    return db.query(MaintCfgModel).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=MaintCfgRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(MaintCfgModel, id)
    if not row:
        raise HTTPException(404, "maint_cfg not found")
    return row


@router.post("", response_model=MaintCfgRead, status_code=201)
def create(p: MaintCfgCreate, db: Session = Depends(get_db)):
    row = MaintCfgModel(maint_type=p.maint_type, description=p.description)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/{id}", response_model=MaintCfgRead)
def update(id: int, p: MaintCfgUpdate, db: Session = Depends(get_db)):
    row = db.get(MaintCfgModel, id)
    if not row:
        raise HTTPException(404, "maint_cfg not found")
    for k, v in p.model_dump(exclude_unset=True).items():
        setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/{id}", status_code=204)
def delete(id: int, db: Session = Depends(get_db)):
    row = db.get(MaintCfgModel, id)
    if not row:
        raise HTTPException(404, "maint_cfg not found")
    db.delete(row)
    db.commit()
    return None
