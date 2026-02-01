"""hmi-api: 게이트웨이. Docker-compose 테이블 API 컨테이너(line_mst, equip_mst, …)로 프록시."""
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from hmi_api.config import TABLE_SERVICES, settings
from hmi_api.proxy import fetch_openapi, proxy_to_table

app = FastAPI(
    title="Edge HMI API",
    description="게이트웨이: line_mst, equip_mst 등 테이블 API 컨테이너로 프록시",
    version="1.0.1",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

_STATIC = Path(__file__).parent / "static"
_SWAGGER_UI = _STATIC / "swagger-ui.html"

app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")


@app.get("/")
async def root():
    """루트 = Felt Montrg 스타일 통합 Swagger UI (Docker-compose)."""
    return FileResponse(_SWAGGER_UI, media_type="text/html")


@app.get("/swagger", include_in_schema=False)
async def swagger_alias():
    """Swagger UI 별칭."""
    return FileResponse(_SWAGGER_UI, media_type="text/html")


@app.get("/docs", include_in_schema=False)
async def docs_redirect():
    """FastAPI 기본 docs → Swagger UI 리다이렉트."""
    return RedirectResponse(url="/", status_code=302)


@app.get("/health")
def health():
    return {"status": "ok", "role": "gateway"}


@app.get("/info", include_in_schema=False)
async def service_info():
    """서비스 정보 (기존 동작 서비스 /info 패턴)."""
    return {
        "service": "Edge HMI API Gateway",
        "version": "1.0.1",
        "status": "running",
        "swagger_ui_url": "/",
        "integrated_api_docs": "/openapi.json",
        "integrated_services_count": len(TABLE_SERVICES),
        "services": list(TABLE_SERVICES),
        "available_endpoints": {
            "/": "Swagger UI (main)",
            "/swagger": "Swagger UI (alias)",
            "/info": "Service information",
            "/openapi.json": "OpenAPI specification",
            "/health": "Health check",
        },
    }


@app.get("/openapi.json")
async def openapi_aggregated():
    """각 테이블 서비스 openapi.json 수집 후 병합. (유일한 OpenAPI 소스)"""
    paths: dict = {}
    tags: list = []
    seen_tags: set = set()
    all_schemas: dict = {}
    skip_paths = {"/", "/health", "/openapi.json", "/docs", "/redoc"}
    for svc in TABLE_SERVICES:
        spec = await fetch_openapi(svc)
        if not spec:
            continue
        comp = spec.get("components") or {}
        for name, schema in (comp.get("schemas") or {}).items():
            if name not in all_schemas:
                all_schemas[name] = schema
        for path, path_item in (spec.get("paths") or {}).items():
            if path in skip_paths or not path.startswith(f"/{svc}"):
                continue
            paths[path] = path_item
            for op in (path_item or {}).values():
                if not isinstance(op, dict):
                    continue
                for t in op.get("tags") or []:
                    if t not in seen_tags:
                        seen_tags.add(t)
                        tags.append({"name": t})
    return {
        "openapi": "3.0.3",
        "x-source": "gateway-aggregated",
        "info": {
            "title": "Edge HMI API",
            "version": "1.0.1",
            "description": f"게이트웨이 (테이블 API 프록시). Total {len(tags)} tables integrated.",
        },
        "paths": paths,
        "tags": tags,
        "servers": [],
        "components": {"schemas": all_schemas},
    }


_PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]


async def _proxy_handler(request: Request, service: str, rest: str | None = None):
    path = f"/{service}" + (f"/{rest}" if rest else "")
    return await proxy_to_table(service, request, path)


def _make_proxy_root(svc: str):
    async def _h(request: Request):
        return await _proxy_handler(request, svc, None)
    return _h


def _make_proxy_path(svc: str):
    async def _h(request: Request, rest: str):
        return await _proxy_handler(request, svc, rest)
    return _h


def _register_proxy_routes():
    for svc in TABLE_SERVICES:
        app.add_api_route(
            f"/{svc}",
            _make_proxy_root(svc),
            methods=_PROXY_METHODS,
            name=f"proxy_{svc}",
        )
        app.add_api_route(
            f"/{svc}/{{rest:path}}",
            _make_proxy_path(svc),
            methods=_PROXY_METHODS,
            name=f"proxy_{svc}_path",
        )


_register_proxy_routes()
