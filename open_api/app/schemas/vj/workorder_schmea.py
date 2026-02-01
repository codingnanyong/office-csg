from pydantic import BaseModel
from .workorder_detail_schema import WorkOrderDetail

class WorkOrder(BaseModel):
    unit_id : str
    detail : list[WorkOrderDetail]
    
    class Config:
        from_attributes = True