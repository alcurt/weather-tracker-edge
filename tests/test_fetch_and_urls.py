from datetime import datetime, timezone

from config import TZ_UTC8
from fetch import is_retryable_status, parse_retry_after, retry_delay
from poller import build_nmc_url, build_radar_url


def test_retryable_status_codes():
    for code in (408, 425, 429, 500, 502, 503, 504, 599):
        assert is_retryable_status(code) is True
    for code in (200, 301, 304, 400, 401, 403, 404):
        assert is_retryable_status(code) is False


def test_retry_delay_backoff_sequence():
    assert retry_delay(1) == 5.0
    assert retry_delay(2) == 10.0
    assert retry_delay(3) == 20.0
    assert retry_delay(10) == 60.0


def test_retry_delay_honours_retry_after():
    assert parse_retry_after("30") == 30.0
    assert parse_retry_after("0") == 1.0
    assert parse_retry_after("garbage") is None
    assert parse_retry_after(None) is None
    assert retry_delay(1, "7") == 7.0


def test_retry_after_http_date_in_past_is_clamped():
    assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 1.0


def test_nmc_url_converts_beijing_time_to_utc():
    # 北京时跨日 -> UTC 前一天
    url = build_nmc_url("FY4C", datetime(2026, 1, 1, 3, 0, tzinfo=TZ_UTC8))
    assert "/2025/12/31/" in url
    assert url.endswith("20251231190000000.JPG")


def test_radar_url_uses_beijing_time():
    url = build_radar_url("RadarGZ", datetime(2026, 1, 2, 5, 6, tzinfo=TZ_UTC8))
    assert url == "http://www.tqyb.com.cn/data/swan/mcr/20260102/20260102050600_gz.png"


def test_utc_conversion_is_timezone_aware():
    ts = datetime(2026, 1, 1, 3, 0, tzinfo=TZ_UTC8)
    assert ts.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S") == "20251231190000"
