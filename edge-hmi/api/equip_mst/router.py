from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import EquipMst as EquipMstModel

from equip_mst.schemas import EquipMstCreate, EquipMstRead, EquipMstUpdate

router = APIRouter(prefix="/equip_mst", tags=["equip_mst"])


@router.get("", response_model=list[EquipMstRead])
def list_(db: Session = Depends(get_db), skip: int = 0, limit: int = Query(100, le=500)):
    return db.query(EquipMstModel).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=EquipMstRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(EquipMstModel, id)
    if not row:
        raise HTTPException(404, "equip_mst not found")
    return row


@router.post("", response_model=EquipMstRead, status_code=201)
def create(p: EquipMstCreate, db: Session = Depends(get_db)):
    row = EquipMstModel(line_id=p.line_id, equip_code=p.equip_code, name=p.name, type=p.type)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/{id}", response_model=EquipMstRead)
def update(id: int, p: EquipMstUpdate, db: Session = Depends(get_db)):
    row = db.get(EquipMstModel, id)
    if not row:
        raise HTTPException(404, "equip_mst not found")
    if p.line_id is not None:
        row.line_id = p.line_id
    if p.equip_code is not None:
        row.equip_code = p.equip_code
    if p.name is not None:
        row.name = p.name
    if p.type is not None:
        row.type = p.type
    db.commit()
    db.refresh(row)
    return row


@router.delete("/{id}", status_code=204)
def delete(id: int, db: Session = Depends(get_db)):
    row = db.get(EquipMstModel, id)
    if not row:
        raise HTTPException(404, "equip_mst not found")
    db.delete(row)
    db.commit()
    return None
