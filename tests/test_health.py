import time
from types import SimpleNamespace

from fastapi.testclient import TestClient

import web


def _metrics(updated_at: float) -> dict:
    return {
        key: {"status": "304", "updated_at": updated_at, "elapsed": 0.1}
        for key in web.all_target_keys
    }


def test_health_starting_grace_allows_init():
    state = web.AppState()
    state.poller = SimpleNamespace(metrics={})
    code, payload = web.health_snapshot(state)
    assert code == 200
    assert payload["status"] == "starting"
    assert len(payload["sources"]) == len(web.all_target_keys)
    assert all(source["last_tick_age_seconds"] is None for source in payload["sources"])


def test_health_degraded_when_never_started():
    state = web.AppState()
    state.start_time = time.time() - 700
    state.poller = SimpleNamespace(metrics={})
    code, payload = web.health_snapshot(state)
    assert code == 503
    assert payload["status"] == "degraded"


def test_health_ok_when_all_sources_recent():
    state = web.AppState()
    state.start_time = time.time() - 200
    state.poller = SimpleNamespace(metrics=_metrics(time.time()))
    code, payload = web.health_snapshot(state)
    assert code == 200
    assert payload["status"] == "ok"
    assert all(source["healthy"] for source in payload["sources"])


def test_health_degraded_when_source_is_stale():
    state = web.AppState()
    state.start_time = time.time() - 200
    state.poller = SimpleNamespace(metrics=_metrics(time.time() - 700))
    code, payload = web.health_snapshot(state)
    assert code == 503
    assert payload["status"] == "degraded"
    assert any(not source["healthy"] for source in payload["sources"])


def test_health_route_returns_503_when_degraded():
    state = web.AppState()
    state.start_time = time.time() - 200
    state.poller = SimpleNamespace(metrics=_metrics(time.time() - 700))
    client = TestClient(web.build_app(state))
    response = client.get("/api/health")
    assert response.status_code == 503
    assert response.json()["status"] == "degraded"
