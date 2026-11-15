import shutil
import sys
from pathlib import Path

import pytest

import synthesizer as synth_module
from frames import parse_frame_timestamp

ROOT = Path(__file__).resolve().parents[1]
FFMPEG = ROOT / ("ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
FRAME_ROOT = ROOT / "data" / "TempC"


@pytest.mark.skipif(not FFMPEG.exists(), reason="本机未放置 ffmpeg 二进制")
@pytest.mark.skipif(not FRAME_ROOT.exists(), reason="本机没有可用的 TempC 样例帧")
def test_generate_video_writes_complete_mp4_atomically(tmp_path, monkeypatch):
    frames = sorted(FRAME_ROOT.glob("*/*.png"))
    if len(frames) < 2:
        pytest.skip("样例帧不足")
    # 取同一日期、时间相邻的三帧
    chosen = frames[:3]
    start = parse_frame_timestamp(chosen[0].name)
    for frame in chosen:
        target = tmp_path / "TempC" / frame.parent.name / frame.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(frame, target)

    monkeypatch.setattr(synth_module, "DATA_DIR", tmp_path)
    synth = synth_module.Synthesizer()
    assert synth.generate_video("TempC", 1, start_time=start) is True

    outputs = list((tmp_path / "output").glob("*.mp4"))
    assert len(outputs) == 1
    assert outputs[0].stat().st_size > 0
    assert not list((tmp_path / "output").rglob("*.part"))
