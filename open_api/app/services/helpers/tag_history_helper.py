from typing import Union, List
from collections import defaultdict
from sqlalchemy.future import select
from app.models.hq.tags_model import TagsSchema
from app.models.hq.tag_his_model import TagHisSchema
from app.schemas.hq.tag_history_schema import TagHistory, Tag
from app.schemas.hq.tag_path_schema import TagPath

tag_histories = [
    TagsSchema.ws_no.label('wsno'),
    TagsSchema.tag_id.label('tagid'),
    TagsSchema.tag_type.label('tagtype'),
    TagHisSchema.op_cd.label('opcd'),
    TagHisSchema.status.label('status'),
    TagHisSchema.floor.label('floor'),
    TagHisSchema.zone.label('zone'),
    TagHisSchema.diff.label('leadtime')
]

def set_tag_history(rows) -> Union[TagHistory, List[TagHistory]]:
    grouped = defaultdict(lambda: defaultdict(list))
    for row in rows:
        mapping = row._mapping
        wsno = mapping['wsno']
        tagid = mapping['tagid']
        tagtype = mapping['tagtype']
        tag_path = TagPath(
            opcd=mapping.get('opcd'),
            status=mapping.get('status'),
            floor=mapping.get('floor'),
            zone=mapping.get('zone'),
            leadtime=mapping.get('leadtime')
        )
        grouped[wsno][(tagid, tagtype)].append(tag_path)
    
    result = []
    for ws_no, tag_group in grouped.items():
        tags = []
        for (tagid, tagtype), paths in tag_group.items():
            tag_obj = Tag(
                tagid=tagid,
                tagtype=tagtype,
                path=paths
            )
            tags.append(tag_obj)
        result.append(TagHistory(wsno=ws_no, tags=tags))
    
    if len(result) == 1:
        return result[0]
    return result

def filter(query, wsno: str = None):
    if wsno:
        query = query.filter(TagsSchema.ws_no == wsno)
    return query

def get_tag_history_data(db, wsno: str = None) -> Union[TagHistory, List[TagHistory]]:
    q = db.query(*tag_histories)\
         .outerjoin(
             TagHisSchema, TagsSchema.ws_no == TagHisSchema.ws_no
         )\
         .order_by(TagsSchema.ws_no, TagsSchema.tag_id, TagsSchema.tag_type)
    
    q = filter(q, wsno)
    rows = q.all()
    return set_tag_history(rows)

async def get_tag_history_data_async(db, wsno: str = None) -> Union[TagHistory, List[TagHistory]]:
    stmt = select(*tag_histories)\
           .outerjoin(
               TagHisSchema, TagsSchema.ws_no == TagHisSchema.ws_no
           )\
           .order_by(TagsSchema.ws_no, TagsSchema.tag_id, TagsSchema.tag_type)
    
    stmt = filter(stmt, wsno)
    result = await db.execute(stmt)
    rows = result.all()
    return set_tag_history(rows)