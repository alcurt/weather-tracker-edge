import asyncio
import io
from datetime import datetime

import httpx
from PIL import Image

import poller as poller_module
from config import TZ_UTC8


def _png_bytes() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (100, 100), color=(30, 60, 90)).save(buffer, format="PNG")
    return buffer.getvalue()


class FakeCursor:
    async def update(self, last_modified, content_length, file_hash, file_path):
        self.last_path = file_path


async def _make_poller(handler):
    poller = poller_module.AsyncPoller()
    await poller.client.aclose()
    poller.client = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
    poller.cursors["RadarGZ"] = FakeCursor()
    return poller


def test_fetch_radar_retries_5xx_then_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr(poller_module, "DATA_DIR", tmp_path)
    calls = []
    body = _png_bytes()

    def handler(request):
        calls.append(request.url)
        if len(calls) == 1:
            return httpx.Response(503, headers={"Retry-After": "1"}, content=b"busy")
        return httpx.Response(200, content=body)

    async def scenario():
        poller = await _make_poller(handler)
        try:
            result = await poller._fetch_single_radar(
                "RadarGZ", datetime(2026, 1, 1, 12, 0, tzinfo=TZ_UTC8)
            )
        finally:
            await poller.client.aclose()
        return result

    assert asyncio.run(scenario()) == "200"
    assert len(calls) == 2
    files = list((tmp_path / "RadarGZ").rglob("*.png"))
    assert len(files) == 1
    assert not list((tmp_path / "RadarGZ").rglob("*.part"))


def test_fetch_radar_does_not_retry_permanent_4xx(tmp_path, monkeypatch):
    monkeypatch.setattr(poller_module, "DATA_DIR", tmp_path)
    calls = []

    def handler(request):
        calls.append(request.url)
        return httpx.Response(403, content=b"forbidden")

    async def scenario():
        poller = await _make_poller(handler)
        try:
            return await poller._fetch_single_radar(
                "RadarGZ", datetime(2026, 1, 1, 12, 6, tzinfo=TZ_UTC8)
            )
        finally:
            await poller.client.aclose()

    assert asyncio.run(scenario()) == "ERROR"
    assert len(calls) == 1


def test_backfill_fetch_uses_single_attempt(tmp_path, monkeypatch):
    monkeypatch.setattr(poller_module, "DATA_DIR", tmp_path)
    calls = []

    def handler(request):
        calls.append(request.url)
        return httpx.Response(503, content=b"busy")

    async def scenario():
        poller = await _make_poller(handler)
        try:
            return await poller._fetch_single_radar(
                "RadarGZ", datetime(2026, 1, 1, 12, 12, tzinfo=TZ_UTC8), max_retries=0
            )
        finally:
            await poller.client.aclose()

    assert asyncio.run(scenario()) == "FETCH_ERR"
    assert len(calls) == 1
