import sys

# Add consumer src to path
sys.path.append("/app/src")

from fastapi.testclient import TestClient
from consumer_service_app import create_app


def get_client():
    app = create_app()
    return TestClient(app)


def test_health_status_code():
    client = get_client()
    resp = client.get("/health")
    assert resp.status_code in (200, 503)
