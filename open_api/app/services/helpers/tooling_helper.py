from typing import List, Union, Optional
from collections import defaultdict
from sqlalchemy.future import select
from sqlalchemy import func
from app.models.hq.tooling_model import ToolingSchema
from app.schemas.hq.tooling_warehouse_schema import Tooling,ToolingWarehouse

tooling_columns = [
    ToolingSchema.purc_no.label('barcode'),
    ToolingSchema.parent_uptl_no.label('parents_tooling_cd'),
    ToolingSchema.part_cd.label('part_cd'),
    ToolingSchema.part_name.label('part_name'),
    ToolingSchema.process_cd.label('process_cd'),
    ToolingSchema.process_name.label('process_name'),
    ToolingSchema.tool_name.label('name'),
    ToolingSchema.tool_size.label('size'),
    ToolingSchema.status.label('status'),
    ToolingSchema.loc_cd.label('loc_cd'),
    ToolingSchema.loc_name.label('loc_name')
]

def filters(query, loc: Optional[str] = None):
    if not loc:
        return query
    query = query.where(
        (ToolingSchema.loc_cd == loc) | (ToolingSchema.loc_name == loc)
    )
    
    return query

def set_toolings(rows) -> Union[ToolingWarehouse,list[ToolingWarehouse]]:

    warehouse_dict = defaultdict(list)

    for row in rows:
        mapping = dict(row)

        toolings = Tooling(
            barcode=mapping['barcode'],
            parents_tooling_cd=mapping['parents_tooling_cd'],
            part_cd=mapping['part_cd'],
            part_name=mapping['part_name'],
            process_cd=mapping['process_cd'],
            process_name=mapping['process_name'],
            name=mapping['name'],
            size=mapping['size'],
            status=mapping['status']
        )

        warehouse_dict[(mapping['loc_cd'], mapping['loc_name'])].append(toolings)

    tooling_warehouses = [
        ToolingWarehouse(loc_cd=loc_cd, loc_name=loc_name, toolings=toolings)
        for (loc_cd, loc_name), toolings in warehouse_dict.items()
    ]

    if len(tooling_warehouses) == 1:
        return tooling_warehouses[0]
    
    return tooling_warehouses

def get_tooling_data(db, loc: Optional[str] = None) -> Union[ToolingWarehouse,list[ToolingWarehouse]]:

    stmt = select(*tooling_columns)
    stmt = filters(stmt, loc) 
    result = db.execute(stmt)
    rows = result.mappings().all()

    return set_toolings(rows)

async def get_tooling_data_async(db,loc: Optional[str] = None) -> Union[ToolingWarehouse,list[ToolingWarehouse]]:
    stmt = select(*tooling_columns)
    stmt = filters(stmt, loc) 
    result = await db.execute(stmt)
    rows = result.mappings().all()

    return set_toolings(rows)