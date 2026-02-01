from typing import Union, List
from sqlalchemy.future import select
from app.models.hq.tags_model import TagsSchema
from app.models.hq.tags_status_model import TagStatusSchema
from app.schemas.hq.tag_current_schema import CurrentTag,wsCoordinate

tag_colums =[
    TagsSchema.ws_no.label('wsno'),
    TagsSchema.tag_id.label('tagid'),
    TagsSchema.tag_type.label('tagtype'),
    TagStatusSchema.op_cd.label('opcd'),
    TagStatusSchema.status.label('status'),
    TagStatusSchema.zone.label('zone'),
    TagStatusSchema.floor.label('floor'),
    TagStatusSchema.x.label('x'),
    TagStatusSchema.y.label('y')
]

def set_tag(rows) -> List[CurrentTag]:
    tag_keys = ['tagid', 'tagtype', 'opcd', 'status', 'floor', 'x', 'y']
    tag_dict = {}
    for row in rows:
        mapping = row._mapping
        wsno = mapping['wsno']
        tag_obj = wsCoordinate(**{key: mapping.get(key) for key in tag_keys})
        if wsno in tag_dict:
            tag_dict[wsno].append(tag_obj)
        else:
            tag_dict[wsno] = [tag_obj]
    return [CurrentTag(wsno=wsno, coordinate=coords) for wsno, coords in tag_dict.items()]

def filter_tag(tags: List[CurrentTag], wsno: str = None) -> Union[CurrentTag, List[CurrentTag]]:
    if wsno:
        filtered = [s for s in tags if s.wsno == wsno]
        return filtered[0] if filtered else None
    return tags

def get_tag_data(db, wsno: str = None) -> Union[CurrentTag, List[CurrentTag]]:
    q = db.query(*tag_colums)\
         .outerjoin(
            TagStatusSchema, TagsSchema.tag_id == TagStatusSchema.tag_id
         )\
         .order_by(TagsSchema.ws_no)
    rows = q.all()
    tags = set_tag(rows)
    return filter_tag(tags, wsno)

async def get_tag_data_async(db, wsno: str = None) -> Union[CurrentTag, List[CurrentTag]]:
    stmt = select(*tag_colums)\
           .outerjoin(
               TagStatusSchema, TagsSchema.tag_id == TagStatusSchema.tag_id
           )\
           .order_by(TagsSchema.ws_no)
    result = await db.execute(stmt)
    rows = result.all()
    tags = set_tag(rows)
    return filter_tag(tags, wsno)