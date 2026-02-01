from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import AlarmHis as AlarmHisModel

from alarm_his.schemas import AlarmHisCreate, AlarmHisRead

router = APIRouter(prefix="/alarm_his", tags=["alarm_his"])


@router.get("", response_model=list[AlarmHisRead])
def list_(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = Query(100, le=500),
    equip_id: int | None = None,
):
    q = db.query(AlarmHisModel)
    if equip_id is not None:
        q = q.filter(AlarmHisModel.equip_id == equip_id)
    return q.order_by(AlarmHisModel.time.desc()).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=AlarmHisRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.get(AlarmHisModel, id)
    if not row:
        raise HTTPException(404, "alarm_his not found")
    return row


@router.post("", response_model=AlarmHisRead, status_code=201)
def create(p: AlarmHisCreate, db: Session = Depends(get_db)):
    row = AlarmHisModel(
        time=p.time,
        equip_id=p.equip_id,
        alarm_def_id=p.alarm_def_id,
        trigger_val=p.trigger_val,
        alarm_type=p.alarm_type,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
