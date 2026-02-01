"""Proxy requests to table API containers."""
from typing import Any

import httpx
from fastapi import Request, Response
from fastapi.responses import JSONResponse

from hmi_api.config import TABLE_SERVICES, settings


async def proxy_to_table(service: str, request: Request, path: str) -> Response:
    """Forward request to table service. path includes leading slash."""
    if service not in TABLE_SERVICES:
        return JSONResponse({"detail": f"Unknown table: {service}"}, status_code=404)
    base = settings.base_url(service)
    url = f"{base}{path}"
    headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "connection")}
    try:
        body = await request.body()
    except Exception:
        body = b""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.request(
                request.method,
                url,
                headers=headers,
                content=body,
                params=request.query_params,
            )
    except (httpx.ConnectError, httpx.ConnectTimeout) as e:
        return JSONResponse(
            {"detail": f"Table API '{service}' is not reachable. Is the container running?", "error": str(e)},
            status_code=503,
        )
    out_headers = {k: v for k, v in r.headers.items() if k.lower() not in ("transfer-encoding", "connection")}
    return Response(
        content=r.content,
        status_code=r.status_code,
        headers=out_headers,
        media_type=r.headers.get("content-type"),
    )


async def fetch_openapi(service: str) -> dict[str, Any] | None:
    """Fetch /openapi.json from a table service."""
    base = settings.base_url(service)
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.get(f"{base}/openapi.json")
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None
