"""FastAPI app for prod_his only (hypertable). List + Get + Create."""
from fastapi import FastAPI

from prod_his.router import router

app = FastAPI(title="edge-hmi prod_his API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "prod_his", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
