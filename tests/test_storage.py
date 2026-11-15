import os

from storage import atomic_write_bytes, atomic_write_json, cleanup_part_files


def test_atomic_write_bytes_replaces_target_without_part(tmp_path):
    target = tmp_path / "sub" / "frame.png"
    atomic_write_bytes(target, b"first")
    atomic_write_bytes(target, b"second")
    assert target.read_bytes() == b"second"
    assert not (tmp_path / "sub" / "frame.png.part").exists()


def test_atomic_write_json_round_trip(tmp_path):
    target = tmp_path / "state" / "cron.json"
    atomic_write_json(target, {"last_done_date": "20260101", "note": "中文"})
    assert '"20260101"' in target.read_text(encoding="utf-8")


def test_cleanup_part_files_respects_age(tmp_path):
    old = tmp_path / "20260101" / "old.png.part"
    fresh = tmp_path / "20260101" / "fresh.png.part"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"x")
    fresh.write_bytes(b"x")

    now = 1_000_000.0
    os.utime(old, (now - 7200, now - 7200))
    os.utime(fresh, (now - 10, now - 10))

    removed = cleanup_part_files(tmp_path, older_than_seconds=600, now=now)
    assert removed == 1
    assert not old.exists()
    assert fresh.exists()


def test_cleanup_missing_root_is_noop(tmp_path):
    assert cleanup_part_files(tmp_path / "missing", now=0.0) == 0
