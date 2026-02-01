from datetime import datetime
from typing import Optional
from decimal import Decimal
from pydantic import BaseModel

class WorkOrderDetail(BaseModel):
    wo_type : str
    wo_yymm : str
    wo_orgn : str
    wo_no : Decimal  
    wo_class : str
    work_type :str
    wo_status :str
    request_date : Optional[datetime]
    wo_date : Optional[datetime]
    problem_date: Optional[datetime]
    defect_date : Optional[datetime]
    soultion_date : Optional[datetime]
    defect : Optional[str]

    class Config:
        from_attributes = True