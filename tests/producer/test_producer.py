import os
import sys

# Add producer src to path
sys.path.append("/app/src")

import pytest
from fastapi.testclient import TestClient

from producer_service_app import create_app  # now resolvable


def get_client():
    app = create_app()
    return TestClient(app)


def test_health():
    client = get_client()
    resp = client.get("/health")
    assert resp.status_code in (200, 503)


def test_track_event_validation_error():
    client = get_client()
    payload = {
        "user_id": "not-int",
        "event_type": "login",
        "timestamp": "2023-10-27T10:00:00Z",
        "metadata": {},
    }
    resp = client.post("/api/v1/events/track", json=payload)
    # FastAPI returns 422 for body validation errors
    assert resp.status_code == 422



def test_track_event_success(monkeypatch):
    client = get_client()

    class DummyChannel:
        def basic_publish(self, *args, **kwargs):
            return True

        def queue_declare(self, *args, **kwargs):
            return True

    class DummyConn:
        def __init__(self):
            self.is_open = True

    def fake_get_connection():
        return DummyConn(), DummyChannel()

    import producer_service_app

    monkeypatch.setattr(
        producer_service_app, "get_rabbitmq_connection", fake_get_connection
    )

    payload = {
        "user_id": 1,
        "event_type": "login",
        "timestamp": "2023-10-27T10:00:00Z",
        "metadata": {"ip": "127.0.0.1"},
    }
    resp = client.post("/api/v1/events/track", json=payload)
    assert resp.status_code == 202
