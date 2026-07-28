import asyncio
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TranscriptsDisabled(Exception):
    """youtube-transcript-api 예외를 이름만 흉내낸다(분류가 타입 이름 기반이므로 충분)."""


class IpBlocked(Exception):
    pass


class NegativeCacheTests(unittest.TestCase):
    """terminal 실패를 캐시해 소비자 무한 재시도가 YouTube(=프록시 GB)에 닿지 않게 하는 장치."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_env = os.environ.copy()
        os.environ.update(
            {
                "SUBTITLE_CACHE_FILE": str(Path(self.tmp.name) / "cache.db"),
                "YOUTUBE_PAUSE_FILE": str(Path(self.tmp.name) / "youtube_pause_until"),
                "DEGRADE_NEGATIVE_TTL": "3600",
            }
        )
        spec = importlib.util.spec_from_file_location(f"gateway_under_test_{id(self)}", ROOT / "main.py")
        self.gateway = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(self.gateway)
        self.HTTPException = self.gateway.HTTPException

    def tearDown(self):
        try:
            self.gateway._cache_conn.close()
        except Exception:
            pass
        os.environ.clear()
        os.environ.update(self.old_env)
        self.tmp.cleanup()

    def test_transient_failures_are_never_cached(self):
        for status in (429, 503, 500):
            self.gateway._negative_cache_put("vid00000001", "ko", True, self.HTTPException(status, "일시적"))
            self.assertIsNone(self.gateway._negative_cache_get("vid00000001", "ko", True))

    def test_direct_no_subtitles_is_cached_permanently(self):
        self.gateway._negative_cache_put("vid00000002", "ko", True, self.HTTPException(422, "사용 가능한 자막이 없습니다."))
        hit = self.gateway._negative_cache_get("vid00000002", "ko", True)
        self.assertEqual(hit.status_code, 422)
        row = self.gateway._cache_conn.execute(
            "SELECT expires_at FROM negative_cache WHERE video_id='vid00000002'"
        ).fetchone()
        self.assertEqual(row[0], 0.0)  # 0 = 무기한

    def test_degrade_verdicts_expire(self):
        """프록시가 봇체크 페이지를 받으면 TranscriptsDisabled로 오보하므로 영구 캐시하면 안 된다
        (2026-07-28: 그렇게 보고된 16개 중 8개가 나중에 정상 수신됨)."""
        terminal = self.gateway._terminal_transcript_error(TranscriptsDisabled("subtitles are disabled"))
        self.assertEqual(terminal.status_code, 422)
        self.assertEqual(terminal.cache_ttl, 3600)

        self.gateway._negative_cache_put("vid00000003", "ko", True, terminal)
        row = self.gateway._cache_conn.execute(
            "SELECT expires_at FROM negative_cache WHERE video_id='vid00000003'"
        ).fetchone()
        self.assertGreater(row[0], 0.0)  # 만료 시각이 있어야 스스로 회복된다

    def test_ip_block_is_not_a_terminal_verdict(self):
        self.assertIsNone(self.gateway._terminal_transcript_error(IpBlocked("blocking requests from your IP")))

    def test_expired_entry_allows_retry(self):
        self.gateway._negative_cache_put("vid00000004", "ko", True, self.gateway.TerminalError(404, "없음", 3600))
        self.gateway._cache_conn.execute("UPDATE negative_cache SET expires_at=1 WHERE video_id='vid00000004'")
        self.gateway._cache_conn.commit()
        self.assertIsNone(self.gateway._negative_cache_get("vid00000004", "ko", True))

    def test_cached_verdict_short_circuits_before_youtube(self):
        """캐시된 terminal 실패는 pause/degrade 분기 이전에 응답 — upstream을 한 번도 안 탄다."""
        self.gateway._negative_cache_put("vid00000005", "ko", True, self.HTTPException(422, "사용 가능한 자막이 없습니다."))
        before = dict(self.gateway._UPSTREAM_SCHEDULER.snapshot()["metrics"])

        with self.assertRaises(self.HTTPException) as ctx:
            asyncio.run(self.gateway.fetch_subtitles("vid00000005", lang="ko", auto=True))

        self.assertEqual(ctx.exception.status_code, 422)
        self.assertEqual(self.gateway._UPSTREAM_SCHEDULER.snapshot()["metrics"], before)

    def test_positive_cache_wins_over_negative(self):
        """자막을 이미 받아둔 영상은 옛 terminal 기록이 있어도 정상 응답해야 한다."""
        self.gateway._subtitle_cache_put("vid00000006", "ko", True, {"video_id": "vid00000006", "subtitles": "hello", "segments": []})
        self.gateway._negative_cache_put("vid00000006", "ko", True, self.HTTPException(422, "사용 가능한 자막이 없습니다."))

        result = asyncio.run(self.gateway.fetch_subtitles("vid00000006", lang="ko", auto=True))
        self.assertEqual(result["subtitles"], "hello")


if __name__ == "__main__":
    unittest.main()
