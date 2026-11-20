import asyncio
import io
import os
from datetime import datetime
from pathlib import Path

import httpx
from PIL import Image

from config import TZ_UTC8
from frames import FrameCatalog, parse_frame_timestamp
from grid import GridSlot, fetch_grid_slot, grid_slot_key, grid_slots


def test_parse_frame_timestamp_round_trip():
    ts = parse_frame_timestamp("20260101_120000_TempC.png")
    assert ts == datetime(2026, 1, 1, 12, 0, tzinfo=TZ_UTC8)
    assert parse_frame_timestamp("bad.png") is None


def test_frame_catalog_ignores_part_files_and_caches(tmp_path):
    date_dir = tmp_path / "TempC" / "20260101"
    date_dir.mkdir(parents=True)
    (date_dir / "20260101_120000_TempC.png").write_bytes(b"x")
    (date_dir / "20260101_121000_TempC.png.part").write_bytes(b"partial")
    hko_dir = tmp_path / "HKORadar" / "20260101"
    hko_dir.mkdir(parents=True)
    (hko_dir / "20260101_122000_HKORadar.jpg").write_bytes(b"x")

    catalog = FrameCatalog(tmp_path)
    first = catalog.timestamps("TempC", "20260101")
    assert first == (datetime(2026, 1, 1, 12, 0, tzinfo=TZ_UTC8),)

    (date_dir / "20260101_122000_TempC.png").write_bytes(b"x")
    # 显式推进目录 mtime，避免不同文件系统的 mtime 精度影响断言
    later = date_dir.stat().st_mtime + 1
    os.utime(date_dir, (later, later))
    second = catalog.timestamps("TempC", "20260101")
    assert len(second) == 2
    assert catalog.timestamps("TempC", "20260102") == ()
    assert catalog.timestamps("HKORadar", "20260101") == (datetime(2026, 1, 1, 12, 20, tzinfo=TZ_UTC8),)


def test_grid_slot_paths_and_keys():
    slot = GridSlot("RadarGZ", datetime(2026, 1, 2, 5, 6, tzinfo=TZ_UTC8))
    assert slot.key == "RadarGZ:20260102_050600"
    assert slot.file_name == "20260102_050600_RadarGZ.png"
    path = slot.path(Path("root"))
    assert path == Path("root") / "RadarGZ" / "20260102" / "20260102_050600_RadarGZ.png"
    assert grid_slot_key("RadarGZ", slot.ts) == slot.key

    jpg_slot = GridSlot("HKORadar", slot.ts, ".jpg")
    assert jpg_slot.file_name == "20260102_050600_HKORadar.jpg"
    assert jpg_slot.path(Path("root")).name == "20260102_050600_HKORadar.jpg"


def test_grid_slots_newest_first():
    now = datetime(2026, 1, 2, 12, 3, tzinfo=TZ_UTC8)
    slots = grid_slots(now, 10, 0, 60)
    assert slots[0] == datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8)
    assert slots[-1] == datetime(2026, 1, 2, 11, 0, tzinfo=TZ_UTC8)
    assert len(slots) == 7


class FakeCursor:
    def __init__(self):
        self.path = None

    async def update(self, last_modified, content_length, file_hash, file_path):
        self.path = file_path


def _png_bytes() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (100, 100), color=(10, 20, 30)).save(buffer, format="PNG")
    return buffer.getvalue()


def test_fetch_grid_slot_writes_file_atomically(tmp_path):
    cursor = FakeCursor()
    body = _png_bytes()

    def handler(request):
        return httpx.Response(200, content=body)

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            return await fetch_grid_slot(
                client,
                source="RadarGZ",
                target_time=datetime(2026, 1, 2, 5, 6, tzinfo=TZ_UTC8),
                url="http://example.invalid/frame.png",
                data_dir=tmp_path,
                cursor=cursor,
            )

    assert asyncio.run(scenario()) == "200"
    target = tmp_path / "RadarGZ" / "20260102" / "20260102_050600_RadarGZ.png"
    assert target.exists()
    assert not (target.parent / (target.name + ".part")).exists()
    assert cursor.path == str(target)
