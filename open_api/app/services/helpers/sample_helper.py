from sqlalchemy import func, case
from sqlalchemy.future import select
from app.models.hq.ws_detail_model import wsDetailSchema
from app.models.hq.ws_his_model import wsHisSchema
from app.models.hq.tags_model import TagsSchema
from app.models.hq.tags_status_model import TagStatusSchema
from app.models.hq.common_model import CommonSchema
from app.models.hq.opcd_model import OpCdSchema
from app.schemas.hq.ws_schema import ws, wsDetail, wsHistory, wsCoordinate

def get_season_expr():
    return case(
        (CommonSchema.com_cd.isnot(None),
         CommonSchema.com_name.op("||")(func.substring(wsDetailSchema.season_cd, 3, 2))),
        else_=wsDetailSchema.season_cd
    ).label("season")

sample_columns = [
    wsDetailSchema.ws_no.label('wsno'),
    wsDetailSchema.pcc_pm.label('pm'),
    get_season_expr(),
    wsDetailSchema.dev_name.label('model'),
    wsDetailSchema.gender.label('gender'),
    wsDetailSchema.dev_colorway_id.label('colorway'),
    wsDetailSchema.style_cd.label('stylecd'),
    wsDetailSchema.model_id.label('modelid'),
    wsDetailSchema.bom_id.label('bom'),
    wsDetailSchema.dev_style_number.label('devstyle'),
    wsDetailSchema.category.label('category'),
    wsDetailSchema.prod_factory.label('prodfactory'),
    wsDetailSchema.sample_size.label('size'),
    wsDetailSchema.sample_qty.label('sampleqty')
]

his_columns = [
    wsHisSchema.ws_no.label('wsno'),
    wsHisSchema.op_cd.label('opcd'),
    OpCdSchema.op_name.label('op_name'),
    OpCdSchema.op_local_name.label('op_local_name'),
    wsHisSchema.plan_date.label('plan_date'),
    wsHisSchema.prod_date.label('prod_date'),
    wsHisSchema.prod_time.label('prod_time'),
    wsHisSchema.prod_qty.label('prod_qty'),
    wsHisSchema.status.label('status'),
]

coordi_columns = [
    TagsSchema.ws_no.label('wsno'),
    TagsSchema.tag_id.label('tagid'),
    TagsSchema.tag_type.label('tagtype'),
    TagStatusSchema.op_cd.label('opcd'),
    TagStatusSchema.status.label('status'),
    TagStatusSchema.floor.label('floor'),
    TagStatusSchema.x.label('x'),
    TagStatusSchema.y.label('y')
]

def get_sample_data(db) -> list:
    q = db.query(*sample_columns, *his_columns)\
         .outerjoin(
             CommonSchema, func.right(wsDetailSchema.season_cd, 2) == CommonSchema.com_cd
         )\
         .outerjoin(
             wsHisSchema, wsDetailSchema.ws_no == wsHisSchema.ws_no
         )\
         .join(
             OpCdSchema, func.lower(wsHisSchema.op_cd) == func.lower(OpCdSchema.op_cd)
         )\
         .filter(wsHisSchema.status != 'X')
    return q.order_by(wsDetailSchema.ws_no).all()

async def get_sample_data_async(db) -> list:
    stmt = select(*sample_columns, *his_columns)\
           .outerjoin(
               CommonSchema, func.right(wsDetailSchema.season_cd, 2) == CommonSchema.com_cd
           )\
           .outerjoin(
               wsHisSchema, wsDetailSchema.ws_no == wsHisSchema.ws_no
           )\
           .join(
               OpCdSchema, func.lower(wsHisSchema.op_cd) == func.lower(OpCdSchema.op_cd)
           )\
           .filter(wsHisSchema.status != 'X')
    stmt = stmt.order_by(wsDetailSchema.ws_no)
    result = await db.execute(stmt)
    return result.all()

def get_coordinate_data(db) -> list:
    q = db.query(*coordi_columns)\
         .outerjoin(
             TagStatusSchema, TagsSchema.ws_no == TagStatusSchema.ws_no
         )
    return q.order_by(TagsSchema.ws_no).all()

async def get_coordinate_data_async(db) -> list:
    stmt = select(*coordi_columns)\
           .outerjoin(
               TagStatusSchema, TagsSchema.ws_no == TagStatusSchema.ws_no
           )
    stmt = stmt.order_by(TagsSchema.ws_no)
    result = await db.execute(stmt)
    return result.all()

def merge_data(sample_rows, coordinate_rows) -> list[ws]:
    result_ws = {}
    history_seen = {}

    detail_keys = [
        'pm', 'season', 'model', 'gender', 'colorway', 
        'stylecd', 'modelid', 'bom', 'devstyle', 'category',
        'prodfactory', 'size', 'sampleqty'
    ]
    history_keys = [
        'opcd', 'op_name', 'op_local_name', 
        'plan_date', 'prod_date', 'prod_time', 'prod_qty', 'status'
    ]
    coordinate_keys = [
        'tagid', 'tagtype', 'opcd', 'status', 'floor', 'x', 'y'
    ]
    default_detail = {key: ("" if key != "sampleqty" else 0.0) for key in detail_keys}

    for row in sample_rows:
        row_dict = dict(row._mapping)
        wsno = row_dict.get('wsno')
        if wsno not in result_ws:
            detail_data = {
                key: row_dict.get(key) if key != 'sampleqty'
                     else (row_dict.get(key) if row_dict.get(key) is not None else 0.0)
                for key in detail_keys
            }
            result_ws[wsno] = ws(
                wsno=wsno,
                detail=wsDetail(**detail_data),
                history=[],
                coordinate=[]
            )
            history_seen[wsno] = set()
        if any(row_dict.get(key) is not None for key in history_keys):
            history_item_data = {key: row_dict.get(key) for key in history_keys}
            his_key = tuple(sorted(history_item_data.items()))
            if his_key not in history_seen[wsno]:
                history_seen[wsno].add(his_key)
                result_ws[wsno].history.append(wsHistory(**history_item_data))
    
    coord_seen = {}
    for row in coordinate_rows:
        row_dict = dict(row._mapping)
        wsno = row_dict.get('wsno')
        if wsno not in result_ws:
            result_ws[wsno] = ws(
                wsno=wsno,
                detail=wsDetail(**default_detail),
                history=[],
                coordinate=[]
            )
            coord_seen[wsno] = set()
        if wsno not in coord_seen:
            coord_seen[wsno] = set()
        if any(row_dict.get(key) is not None for key in coordinate_keys):
            coordinate_item_data = {key: row_dict.get(key) for key in coordinate_keys}
            coord_key = tuple(sorted(coordinate_item_data.items()))
            if coord_key not in coord_seen[wsno]:
                coord_seen[wsno].add(coord_key)
                result_ws[wsno].coordinate.append(wsCoordinate(**coordinate_item_data))
                
    return list(result_ws.values())

def matches_ws(ws_obj, keyword: str) -> bool:
    keyword_lower = keyword.lower()
    fields = [
        ws_obj.wsno,
        getattr(ws_obj.detail, 'colorway', ""),
        getattr(ws_obj.detail, 'modelid', ""),
        getattr(ws_obj.detail, 'model', ""),
        getattr(ws_obj.detail, 'devstyle', ""),
        getattr(ws_obj.detail, 'category', ""),
        getattr(ws_obj.detail, 'st_cd', ""),
        getattr(ws_obj.detail, 'pm', ""),
        getattr(ws_obj.detail, 'stylecd', ""),
        getattr(ws_obj.detail, 'season', "")
    ]
    return any(keyword_lower in str(f).lower() for f in fields if f is not None)

def filter_samples(samples: list[ws], wsno: str = None, keyword: str = None, opcd: str = None, status: str = None) -> list[ws]:
    if wsno:
        samples = [s for s in samples if s.wsno == wsno]
    if keyword:
        samples = [s for s in samples if matches_ws(s, keyword)]
    if opcd and status:
        samples = [s for s in samples if any(coord.opcd == opcd and coord.status == status for coord in s.coordinate)]
    elif opcd:
        samples = [s for s in samples if any(coord.opcd == opcd for coord in s.coordinate)]
    return samples

def get_sample(db, wsno: str = None, keyword: str = None, opcd: str = None, status: str = None) -> list[ws]:
    sample_rows = get_sample_data(db)
    coordinate_rows = get_coordinate_data(db)
    merged = merge_data(sample_rows, coordinate_rows)
    return filter_samples(merged, wsno, keyword, opcd, status)

async def get_sample_async(db, wsno: str = None, keyword: str = None, opcd: str = None, status: str = None) -> list[ws]:
    sample_rows = await get_sample_data_async(db)
    coordinate_rows = await get_coordinate_data_async(db)
    merged = merge_data(sample_rows, coordinate_rows)
    return filter_samples(merged, wsno, keyword, opcd, status)