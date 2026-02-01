from pydantic import BaseModel
from datetime import timedelta
from typing import Optional

def format_timedelta(td: timedelta) -> str:
    days = td.days
    seconds = td.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    microseconds = td.microseconds
    return f"{days}d {hours:02}:{minutes:02}:{secs:02}.{microseconds:06d}"

class TagPath(BaseModel):
    opcd: Optional[str] = None
    status: Optional[str] = None
    floor: Optional[str] = None
    zone: Optional[str] = None
    leadtime: Optional[timedelta]

    class Config:
        orm_mode = True
        json_encoders = {
            timedelta: format_timedelta
        }