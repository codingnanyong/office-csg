from sqlalchemy import func
from sqlalchemy.future import select
from app.models.hq.ws_status_model import wsStatusSchema
from app.models.hq.opcd_model import OpCdSchema

wip_columns = [
    wsStatusSchema.op_cd.label("opcd"),
    OpCdSchema.op_name.label("opname"),
    OpCdSchema.op_local_name.label("oplocalname"),
    func.count().label("cnt"),
    func.sum(wsStatusSchema.prod_qty).label("qty")
]

def _wip_filter():
    return (
        wsStatusSchema.prod_date == func.to_char(func.now(), 'YYYYMMDD'),
        wsStatusSchema.status == 'I'
    )

def build_wip(db, opcd: str = None):
    q = db.query(*wip_columns).join(
        OpCdSchema, wsStatusSchema.op_cd == OpCdSchema.op_cd
    ).filter(*_wip_filter())
    
    if opcd:
        q = q.filter(wsStatusSchema.op_cd == opcd)
    
    return q.group_by(
        wsStatusSchema.op_cd, OpCdSchema.op_name, OpCdSchema.op_local_name
    ).order_by(wsStatusSchema.op_cd)

def build_wip_async(opcd: str = None):
    stmt = select(*wip_columns).join(
        OpCdSchema, wsStatusSchema.op_cd == OpCdSchema.op_cd
    ).filter(*_wip_filter())
    
    if opcd:
        stmt = stmt.filter(wsStatusSchema.op_cd == opcd)
    
    return stmt.group_by(
        wsStatusSchema.op_cd, OpCdSchema.op_name, OpCdSchema.op_local_name
    ).order_by(wsStatusSchema.op_cd)
