import asyncio
import importlib.util
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ProxyFailoverTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_env = os.environ.copy()
        os.environ.update(
            {
                "SUBTITLE_CACHE_FILE": str(Path(self.tmp.name) / "cache.db"),
                "YOUTUBE_PAUSE_FILE": str(Path(self.tmp.name) / "youtube_pause_until"),
                "PROXY_LIST": "http://proxy.example:8080",
                "AUTO_PROXY_FAILOVER": "1",
                "DIRECT_BLOCK_THRESHOLD": "2",
                "DIRECT_BLOCK_WINDOW": "300",
                "AUTO_PROXY_PAUSE_SECONDS": "60",
                "AUTO_PROXY_MAX_PAUSE_SECONDS": "120",
                "PAUSE_PROBE_CONFIRM_DELAY": "0",
                "PAUSE_PROBE_BACKOFF_MAX": "120",
                "UPSTREAM_DIRECT_RPM": "6000",
                "UPSTREAM_DIRECT_BURST": "100",
                "UPSTREAM_DIRECT_MAX_WAIT": "1",
                "UPSTREAM_WARM_MAX_WAIT": "1",
                "UPSTREAM_PROXY_RPM": "6000",
                "UPSTREAM_PROXY_BURST": "100",
                "UPSTREAM_PROXY_MAX_WAIT": "1",
                "UPSTREAM_PROXY_HOURLY_CAP": "10",
            }
        )
        spec = importlib.util.spec_from_file_location(f"gateway_under_test_{id(self)}", ROOT / "main.py")
        self.gateway = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(self.gateway)

    def tearDown(self):
        try:
            self.gateway._cache_conn.close()
        except Exception:
            pass
        os.environ.clear()
        os.environ.update(self.old_env)
        self.tmp.cleanup()

    def test_direct_mode_is_default_until_repeated_credible_blocks(self):
        self.assertEqual(self.gateway._proxy_mode(), "direct")

        err = RuntimeError("IpBlocked: YouTube is blocking requests from your IP")
        self.gateway._note_direct_block("video1", "transcript-api", err)
        self.assertEqual(self.gateway._pause_remaining(), 0)

        self.gateway._note_direct_block("video2", "yt-dlp-caption", err)
        self.assertGreater(self.gateway._pause_remaining(), 0)
        self.assertEqual(self.gateway._proxy_mode(), "proxy-degrade")

    def test_non_blocking_errors_do_not_trigger_proxy_failover(self):
        self.gateway._note_direct_block("video1", "transcript-api", RuntimeError("No transcripts were found"))
        self.gateway._note_direct_block("video2", "transcript-api", RuntimeError("사용 가능한 자막이 없습니다."))

        self.assertEqual(self.gateway._pause_remaining(), 0)
        self.assertEqual(self.gateway._proxy_mode(), "direct")

    def test_paused_cache_miss_uses_proxy_degrade_path(self):
        self.gateway._set_pause(60, "test", "video1")
        calls = []

        def fake_degrade(video_id, lang, auto, translate, priority=0):
            calls.append((video_id, lang, auto, translate, priority))
            return {"video_id": video_id, "subtitles": "hello world", "segments": []}

        self.gateway._degrade_fetch_sync = fake_degrade

        async def run():
            return await self.gateway._run_fetch("video1", "ko", True, 0, False, asyncio.get_running_loop())

        result = asyncio.run(run())

        self.assertEqual(result["video_id"], "video1")
        self.assertEqual(calls, [("video1", "ko", True, False, 0)])

    def test_successful_direct_probe_clears_pause(self):
        self.gateway._set_pause(60, "test", "video1")
        calls = []

        def fake_uncached(video_id, lang, auto, priority=0, traffic_class="foreground"):
            calls.append((video_id, lang, auto, priority, traffic_class))
            return {"video_id": video_id, "subtitles": "hello world", "segments": []}

        self.gateway._fetch_subtitles_uncached = fake_uncached

        async def run():
            return await self.gateway._probe_direct_recovery_once(asyncio.get_running_loop(), 0)

        self.assertTrue(asyncio.run(run()))
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], ("dQw4w9WgXcQ", "en", True, 10, "probe"))
        self.assertEqual(self.gateway._pause_remaining(), 0)
        self.assertEqual(self.gateway._proxy_mode(), "direct")
        self.assertEqual(self.gateway._UPSTREAM_SCHEDULER.snapshot()["metrics"]["direct_recoveries_total"], 1)

    def test_scheduler_prioritizes_foreground_over_warm_when_direct_slot_opens(self):
        scheduler = self.gateway.UpstreamScheduler()
        first = scheduler.admit("direct", "foreground", 0, "holder", "held")
        results = []
        errors = []
        lock = threading.Lock()

        def worker(name, traffic_class, priority):
            try:
                with scheduler.admit("direct", traffic_class, priority, name, name):
                    with lock:
                        results.append(name)
            except Exception as exc:
                with lock:
                    errors.append(exc)

        warm = threading.Thread(target=worker, args=("warm", "warm", -1))
        foreground = threading.Thread(target=worker, args=("foreground", "foreground", 5))
        warm.start()
        foreground.start()

        deadline = time.time() + 1
        while scheduler.snapshot()["direct"]["queued"] < 2 and time.time() < deadline:
            time.sleep(0.01)

        first.__exit__(None, None, None)
        warm.join(timeout=1)
        foreground.join(timeout=1)

        self.assertFalse(errors)
        self.assertEqual(results, ["foreground", "warm"])

    def test_scheduler_returns_503_when_direct_rate_budget_wait_exceeds_limit(self):
        self.gateway.UPSTREAM_DIRECT_RPM = 60
        self.gateway.UPSTREAM_DIRECT_BURST = 1
        self.gateway.UPSTREAM_DIRECT_MAX_WAIT = 0.01
        scheduler = self.gateway.UpstreamScheduler()

        with scheduler.admit("direct", "foreground", 0, "first", "video1"):
            pass

        with self.assertRaises(self.gateway.HTTPException) as ctx:
            scheduler.admit("direct", "foreground", 0, "second", "video2")

        self.assertEqual(ctx.exception.status_code, 503)
        self.assertEqual(ctx.exception.headers["Retry-After"], "1")
        self.assertEqual(scheduler.snapshot()["metrics"]["direct_denied_wait_total"], 1)

    def test_proxy_overflow_is_foreground_only_and_requires_direct_pressure(self):
        self.gateway.UPSTREAM_PROXY_OVERFLOW_QUEUE = 1
        self.gateway.UPSTREAM_PROXY_HOURLY_CAP = 10
        scheduler = self.gateway.UpstreamScheduler()

        self.assertFalse(scheduler.proxy_overflow_available("foreground"))

        direct = scheduler.admit("direct", "foreground", 0, "holder", "held")
        try:
            self.assertTrue(scheduler.proxy_overflow_available("foreground"))
            self.assertFalse(scheduler.proxy_overflow_available("warm"))
        finally:
            direct.__exit__(None, None, None)

    def test_proxy_hourly_cap_bounds_paid_overflow(self):
        self.gateway.UPSTREAM_PROXY_HOURLY_CAP = 1
        scheduler = self.gateway.UpstreamScheduler()

        with scheduler.admit("proxy", "foreground", 0, "proxy-first", "video1"):
            pass

        with self.assertRaises(self.gateway.HTTPException) as ctx:
            scheduler.admit("proxy", "foreground", 0, "proxy-second", "video2")

        self.assertEqual(ctx.exception.status_code, 503)
        self.assertEqual(ctx.exception.detail, "proxy hourly budget exhausted")
        self.assertEqual(scheduler.snapshot()["metrics"]["proxy_denied_hourly_cap"], 1)


if __name__ == "__main__":
    unittest.main()
