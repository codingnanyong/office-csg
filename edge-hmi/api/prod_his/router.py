from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.deps import get_db
from shared.models import ProdHis as ProdHisModel

from prod_his.schemas import ProdHisCreate, ProdHisRead

router = APIRouter(prefix="/prod_his", tags=["prod_his"])


@router.get("", response_model=list[ProdHisRead])
def list_(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = Query(100, le=500),
    equip_id: int | None = None,
):
    q = db.query(ProdHisModel)
    if equip_id is not None:
        q = q.filter(ProdHisModel.equip_id == equip_id)
    return q.order_by(ProdHisModel.time.desc()).offset(skip).limit(limit).all()


@router.get("/{id}", response_model=ProdHisRead)
def get(id: int, db: Session = Depends(get_db)):
    row = db.query(ProdHisModel).filter(ProdHisModel.id == id).first()
    if not row:
        raise HTTPException(404, "prod_his not found")
    return row


@router.post("", response_model=ProdHisRead, status_code=201)
def create(p: ProdHisCreate, db: Session = Depends(get_db)):
    row = ProdHisModel(
        time=p.time,
        equip_id=p.equip_id,
        total_cnt=p.total_cnt,
        good_cnt=p.good_cnt,
        defect_cnt=p.defect_cnt,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
