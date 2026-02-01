from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import LineMst as LineMstModel

from line_mst.schemas import LineMstCreate, LineMstRead, LineMstUpdate

router = APIRouter(prefix="/line_mst", tags=["line_mst"])


@router.get("", response_model=list[LineMstRead])
def list_(db: Session = Depends(get_db), skip: int = 0, limit: int = Query(100, le=500)):
    return db.query(LineMstModel).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=LineMstRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(LineMstModel, id)
    if not row:
        raise HTTPException(404, "line_mst not found")
    return row


@router.post("", response_model=LineMstRead, status_code=201)
def create(p: LineMstCreate, db: Session = Depends(get_db)):
    row = LineMstModel(line_code=p.line_code, line_name=p.line_name)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/{id}", response_model=LineMstRead)
def update(id: int, p: LineMstUpdate, db: Session = Depends(get_db)):
    row = db.get(LineMstModel, id)
    if not row:
        raise HTTPException(404, "line_mst not found")
    if p.line_code is not None:
        row.line_code = p.line_code
    if p.line_name is not None:
        row.line_name = p.line_name
    db.commit()
    db.refresh(row)
    return row


@router.delete("/{id}", status_code=204)
def delete(id: int, db: Session = Depends(get_db)):
    row = db.get(LineMstModel, id)
    if not row:
        raise HTTPException(404, "line_mst not found")
    db.delete(row)
    db.commit()
    return None
