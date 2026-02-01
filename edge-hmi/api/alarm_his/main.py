"""FastAPI app for alarm_his only. List + Get + Create."""
from fastapi import FastAPI

from alarm_his.router import router

app = FastAPI(title="edge-hmi alarm_his API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "alarm_his", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
