import os
from fastapi.testclient import TestClient

# Prevent Oracle initialization error
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.main import app


client = TestClient(app)


def test_healthz_endpoint() -> None:
    response = client.get("/api/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    # Also check node, pod, namespace fields (may vary by environment variables)
    assert "node" in data
    assert "pod" in data
    assert "namespace" in data


def test_readiness_endpoint() -> None:
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


