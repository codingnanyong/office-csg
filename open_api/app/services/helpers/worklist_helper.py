from sqlalchemy import func, case
from sqlalchemy.future import select
from app.models.hq.ws_status_model import wsStatusSchema
from app.models.hq.common_model import CommonSchema
from app.models.hq.opcd_model import OpCdSchema

def get_season_expr():
    return case(
        (CommonSchema.com_cd.isnot(None),
         CommonSchema.com_name.op("||")(func.substring(wsStatusSchema.season_cd, 3, 2))),
        else_=wsStatusSchema.season_cd
    ).label("season")

worklist_columns = [
    wsStatusSchema.ws_no.label("wsno"),
    wsStatusSchema.op_cd.label("opcd"),
    OpCdSchema.op_name.label("opname"),
    OpCdSchema.op_local_name.label("oplocalname"),
    wsStatusSchema.pm.label("pm"),
    wsStatusSchema.model.label("model"),
    wsStatusSchema.style_cd.label("stylecd"),
    get_season_expr(),
    wsStatusSchema.bom_id.label("bom"),
    wsStatusSchema.dev_colorway_id.label("devcolor"),
    wsStatusSchema.plan_date.label("plan_date"),
    wsStatusSchema.prod_date.label("prod_date"),
    wsStatusSchema.status.label("status")
]

today_filter = (
    (wsStatusSchema.plan_date == func.to_char(func.now(), 'YYYYMMDD')) |
    (wsStatusSchema.prod_date == func.to_char(func.now(), 'YYYYMMDD'))
)

def build_worklist(db, opcd: str = None, keyword: str = None):
    q = db.query(*worklist_columns).join(
        OpCdSchema, wsStatusSchema.op_cd == OpCdSchema.op_cd
    ).outerjoin(
        CommonSchema, func.right(wsStatusSchema.season_cd, 2) == CommonSchema.com_cd
    ).filter(today_filter)
    
    if opcd:
        q = q.filter(wsStatusSchema.op_cd == opcd)
    
    if keyword:
        q = q.filter(
            (wsStatusSchema.pm.ilike(f'%{keyword}%')) |
            (wsStatusSchema.model.ilike(f'%{keyword}%')) |
            (wsStatusSchema.style_cd.ilike(f'%{keyword}%')) |
            (wsStatusSchema.bom_id.ilike(f'%{keyword}%')) |
            (wsStatusSchema.season_cd.ilike(f'%{keyword}%')) |
            (wsStatusSchema.dev_colorway_id.ilike(f'%{keyword}%'))
        )
    
    return q.order_by(wsStatusSchema.ws_no)

def build_worklist_async(opcd: str = None, keyword: str = None):
    q = select(*worklist_columns).join(
        OpCdSchema, wsStatusSchema.op_cd == OpCdSchema.op_cd
    ).outerjoin(
        CommonSchema, func.right(wsStatusSchema.season_cd, 2) == CommonSchema.com_cd
    ).filter(today_filter)
    
    if opcd:
        q = q.filter(wsStatusSchema.op_cd == opcd)
    
    if keyword:
        q = q.filter(
            (wsStatusSchema.pm.ilike(f'%{keyword}%')) |
            (wsStatusSchema.model.ilike(f'%{keyword}%')) |
            (wsStatusSchema.style_cd.ilike(f'%{keyword}%')) |
            (wsStatusSchema.bom_id.ilike(f'%{keyword}%')) |
            (wsStatusSchema.season_cd.ilike(f'%{keyword}%')) |
            (wsStatusSchema.dev_colorway_id.ilike(f'%{keyword}%'))
        )
    
    return q.order_by(wsStatusSchema.ws_no)