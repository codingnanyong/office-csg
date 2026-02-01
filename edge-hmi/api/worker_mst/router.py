from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import WorkerMst as WorkerMstModel

from worker_mst.schemas import WorkerMstCreate, WorkerMstRead, WorkerMstUpdate

router = APIRouter(prefix="/worker_mst", tags=["worker_mst"])


@router.get("", response_model=list[WorkerMstRead])
def list_(db: Session = Depends(get_db), skip: int = 0, limit: int = Query(100, le=500)):
    return db.query(WorkerMstModel).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=WorkerMstRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(WorkerMstModel, id)
    if not row:
        raise HTTPException(404, "worker_mst not found")
    return row


@router.post("", response_model=WorkerMstRead, status_code=201)
def create(p: WorkerMstCreate, db: Session = Depends(get_db)):
    row = WorkerMstModel(worker_code=p.worker_code, name=p.name, dept_name=p.dept_name)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/{id}", response_model=WorkerMstRead)
def update(id: int, p: WorkerMstUpdate, db: Session = Depends(get_db)):
    row = db.get(WorkerMstModel, id)
    if not row:
        raise HTTPException(404, "worker_mst not found")
    for k, v in p.model_dump(exclude_unset=True).items():
        setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/{id}", status_code=204)
def delete(id: int, db: Session = Depends(get_db)):
    row = db.get(WorkerMstModel, id)
    if not row:
        raise HTTPException(404, "worker_mst not found")
    db.delete(row)
    db.commit()
    return None
