from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import StatusHis as StatusHisModel

from status_his.schemas import StatusHisCreate, StatusHisRead

router = APIRouter(prefix="/status_his", tags=["status_his"])


@router.get("", response_model=list[StatusHisRead])
def list_(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = Query(100, le=500),
    equip_id: int | None = None,
):
    q = db.query(StatusHisModel)
    if equip_id is not None:
        q = q.filter(StatusHisModel.equip_id == equip_id)
    return q.order_by(StatusHisModel.start_time.desc()).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=StatusHisRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.query(StatusHisModel).filter(StatusHisModel.id == id).first()
    if not row:
        raise HTTPException(404, "status_his not found")
    return row


@router.post("", response_model=StatusHisRead, status_code=201)
def create(p: StatusHisCreate, db: Session = Depends(get_db)):
    row = StatusHisModel(
        equip_id=p.equip_id,
        status_code=p.status_code,
        start_time=p.start_time,
        end_time=p.end_time,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
