from pydantic import BaseModel, ConfigDict


class WorkerMstBase(BaseModel):
    worker_code: str
    name: str
    dept_name: str | None = None


class WorkerMstCreate(WorkerMstBase):
    pass


class WorkerMstUpdate(BaseModel):
    worker_code: str | None = None
    name: str | None = None
    dept_name: str | None = None


class WorkerMstRead(WorkerMstBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
