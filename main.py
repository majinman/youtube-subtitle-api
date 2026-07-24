"""
YouTube Subtitle Extraction API
Powered by yt-dlp
"""

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import GenericProxyConfig
import re
import html
import os
import json
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import asyncio
import threading
import time
import random
import heapq
import itertools
import sqlite3
import collections

app = FastAPI(
    title="YouTube Subtitle API",
    description="yt-dlp 기반 유튜브 자막 추출 API",
    version="3.0.0",
)

# ─────────────────────────────────────────────
# 동시 처리 설정
# ─────────────────────────────────────────────
# 모든 yt-dlp 호출을 단일 직렬 큐로 통과시킨다(동시성 1). 여러 소비자(ReadNThink·tubeletter·guru)가
# 같은 IP로 동시에 때려 429나는 것을 막는 핵심 장치 — 이 게이트웨이가 유일한 yt-dlp 실행 통로다.
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "1"))   # 동시 yt-dlp 처리 수(직렬화로 rate-limit 방지)
executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)

# transcript-api(빠른 경로) 전용 풀. yt-dlp 스로틀 게이트/큐와 완전히 분리해, 빠른 경로가
# 느린 yt-dlp 작업 뒤에 줄서지 않게 한다(timedtext 직접 호출은 가볍고 스로틀 불필요).
FAST_CONCURRENT = int(os.environ.get("FAST_CONCURRENT", "4"))
fast_executor = ThreadPoolExecutor(max_workers=FAST_CONCURRENT)


class PriorityGate:
    """동시성 MAX_CONCURRENT의 우선순위 게이트. 대기 중인 요청을 priority 높은 순으로 깨운다(같으면 FIFO).
    throttle 상황에서 ReadNThink(priority↑) 요청이 tubeletter/importer보다 먼저 처리되게 한다."""
    def __init__(self, capacity: int = 1):
        self._capacity = capacity
        self._active = 0
        self._waiters: list = []  # heap: (-priority, seq, future)
        self._counter = itertools.count()
        self._lock = asyncio.Lock()

    async def acquire(self, priority: int = 0):
        async with self._lock:
            if self._active < self._capacity:
                self._active += 1
                return
            fut = asyncio.get_event_loop().create_future()
            heapq.heappush(self._waiters, (-priority, next(self._counter), fut))
        await fut  # 슬롯이 인계될 때까지 대기(락 밖)

    async def release(self):
        async with self._lock:
            if self._waiters:
                _, _, fut = heapq.heappop(self._waiters)  # 우선순위 높은 대기자에게 슬롯 인계
                if not fut.done():
                    fut.set_result(None)
            else:
                self._active -= 1


gate = PriorityGate(MAX_CONCURRENT)

# ─────────────────────────────────────────────
# YouTube rate-limit(429) 회피: yt-dlp가 YouTube를 때리기 직전 전역적으로 요청을 페이싱한다.
#  - YT_MIN_INTERVAL: 연속 yt-dlp 요청 사이 최소 간격(초). IP 단위 throttle 방지.
#  - YT_COOLDOWN_AFTER_429: 429를 맞으면 이 시간(초)만큼 모든 호출을 멈춰 회복시킨다.
# ─────────────────────────────────────────────
YT_MIN_INTERVAL = float(os.environ.get("YT_MIN_INTERVAL", "12.0"))
# android 클라이언트 스푸핑으로 429가 드물어졌으므로, 한 번 맞아도 전체를 오래 freeze하지
# 않도록 쿨다운을 짧게 둔다(라이브 영상 등 불가피한 429가 정상 요청을 굶기지 않게).
YT_COOLDOWN_AFTER_429 = float(os.environ.get("YT_COOLDOWN_AFTER_429", "30"))

_yt_throttle_lock = threading.Lock()
_yt_last_call = 0.0
_yt_cooldown_until = 0.0


def _yt_throttle():
    """yt-dlp의 YouTube 요청 직전 호출. 전역 처리 예산 → 최소 간격 → 429 쿨다운을 차례로 강제한다.
    락을 잡은 채 대기하므로 동시 요청도 자연히 직렬화되어 IP throttle을 피한다."""
    _yt_budget_acquire()  # 전역 처리 예산(하드 캡) 먼저 소비
    global _yt_last_call
    with _yt_throttle_lock:
        now = time.monotonic()
        wait = max(_yt_cooldown_until - now, _yt_last_call + YT_MIN_INTERVAL - now)
        if wait > 0:
            time.sleep(wait + random.uniform(0, 0.5))  # 지터로 정확한 간격 충돌 방지
        _yt_last_call = time.monotonic()


def _yt_mark_429():
    """429를 감지하면 쿨다운 윈도우를 설정해 후속 호출을 멈춘다."""
    global _yt_cooldown_until
    with _yt_throttle_lock:
        _yt_cooldown_until = time.monotonic() + YT_COOLDOWN_AFTER_429


# transcript-api(빠른 경로/번역/언어목록) 전용 경량 페이싱. yt-dlp와 별개 통로지만 같은 IP로
# YouTube timedtext를 때리므로, 무방비로 두면 연타(다언어 번역·다유저 동시)로 IP throttle에 걸린다.
# yt-dlp의 12s보다 짧은 최소 간격으로 버스트만 눌러 지연을 최소화한다(+실패 시 쿨다운).
TAPI_MIN_INTERVAL = float(os.environ.get("TAPI_MIN_INTERVAL", "2.5"))
TAPI_COOLDOWN_AFTER_FAIL = float(os.environ.get("TAPI_COOLDOWN_AFTER_FAIL", "20"))
_tapi_throttle_lock = threading.Lock()
_tapi_last_call = 0.0
_tapi_cooldown_until = 0.0


def _tapi_throttle():
    """transcript-api가 YouTube를 때리기 직전 호출. 전역 처리 예산 → 최소 간격 + 실패 쿨다운을 강제한다.
    락을 잡은 채 대기하므로 동시 요청도 자연히 직렬화된다."""
    _yt_budget_acquire()  # 전역 처리 예산(하드 캡) 먼저 소비 — yt-dlp 경로와 공유
    global _tapi_last_call
    with _tapi_throttle_lock:
        now = time.monotonic()
        wait = max(_tapi_cooldown_until - now, _tapi_last_call + TAPI_MIN_INTERVAL - now)
        if wait > 0:
            time.sleep(wait + random.uniform(0, 0.3))
        _tapi_last_call = time.monotonic()


def _tapi_mark_fail():
    """transcript-api 실패(빈 응답/파싱 오류=쓰로틀 징후) 시 쿨다운 윈도우를 설정한다."""
    global _tapi_cooldown_until
    with _tapi_throttle_lock:
        _tapi_cooldown_until = time.monotonic() + TAPI_COOLDOWN_AFTER_FAIL


# ── 프록시 degrade 전용 경량 페이싱: pause(직결 IP 밴) 중 프록시 경유 요청은 egress가 집 IP가
#    아니므로, 집 IP 보호용 전역 예산(_yt_budget_acquire)·직결 스로틀(_tapi_throttle)을 소모하면 안 된다.
#    대신 프록시 서버 보호용 최소 간격만 둔다(버스트만 억제). ──
PROXY_MIN_INTERVAL = float(os.environ.get("PROXY_MIN_INTERVAL", "1.0"))
_proxy_pace_lock = threading.Lock()
_proxy_last_call = 0.0


def _proxy_pace():
    """프록시 경유 호출 직전 최소 간격만 강제(전역 예산·직결 스로틀 미소모)."""
    global _proxy_last_call
    with _proxy_pace_lock:
        now = time.monotonic()
        wait = _proxy_last_call + PROXY_MIN_INTERVAL - now
        if wait > 0:
            time.sleep(wait)
        _proxy_last_call = time.monotonic()


# ─────────────────────────────────────────────
# 전역 YouTube 처리 예산(하드 캡, 토큰버킷): yt-dlp·transcript-api 두 경로를 "합쳐서" 분당 최대
# YT_BUDGET_PER_MIN회만 실제로 YouTube를 때리게 한다. 요청은 무제한 받되 실제 처리 rate를 이 예산이
# 강제하므로, 아무리 몰려도 버스트가 안 생겨 IP throttle을 원천 차단한다.
#  - 토큰이 있으면 즉시 소비하고 진행.
#  - 없으면 리필될 때까지 대기하되, 대기가 YT_BUDGET_MAX_WAIT를 넘으면 503(Retry-After)로 넘겨
#    소비자(RT 워커)가 나중에 재시도하게 한다(문서 PENDING 유지 → 유실 없음).
#  - 캐시 적중·번역 등은 스로틀 지점을 안 거치므로 예산을 소모하지 않는다.
# ─────────────────────────────────────────────
YT_BUDGET_PER_MIN = float(os.environ.get("YT_BUDGET_PER_MIN", "60"))   # 분당 실제 YouTube 히트 상한
YT_BUDGET_BURST = float(os.environ.get("YT_BUDGET_BURST", "10"))       # 순간 허용 버스트(토큰 최대치)
YT_BUDGET_MAX_WAIT = float(os.environ.get("YT_BUDGET_MAX_WAIT", "30")) # 이보다 오래 기다려야 하면 503

_budget_lock = threading.Lock()
_budget_tat = time.monotonic()  # GCRA theoretical arrival time(다음 토큰이 나는 가상 시각)
print(f"[budget] YouTube 처리 예산: 분당 {YT_BUDGET_PER_MIN:.0f}회, 버스트 {YT_BUDGET_BURST:.0f}, 최대대기 {YT_BUDGET_MAX_WAIT:.0f}s(초과분 503)", flush=True)


def _yt_budget_acquire():
    """실제 YouTube 히트 직전 호출 — 전역 rate를 GCRA로 강제한다(두 경로 공유).
    정상 분당 YT_BUDGET_PER_MIN회, 순간 버스트 YT_BUDGET_BURST개까지 허용. 예약 대기가
    YT_BUDGET_MAX_WAIT 이내면 그만큼(락 밖에서) 대기 후 진행하고, 넘으면 HTTPException(503)."""
    global _budget_tat
    interval = 60.0 / YT_BUDGET_PER_MIN                    # 토큰당 방출 간격
    burst_tol = max(0.0, YT_BUDGET_BURST - 1.0) * interval  # 버스트 허용폭
    with _budget_lock:
        now = time.monotonic()
        tat = max(_budget_tat, now)
        wait = (tat - burst_tol) - now
        if wait > YT_BUDGET_MAX_WAIT:
            raise HTTPException(
                status_code=503,
                detail=f"처리 예산 소진(분당 {YT_BUDGET_PER_MIN:.0f}회 제한). {int(wait) + 1}s 후 재시도.",
                headers={"Retry-After": str(int(wait) + 1)},
            )
        _budget_tat = tat + interval  # 슬롯 예약(동시 폭주 시 예약이 쌓여 뒤 요청은 503)
    if wait > 0:
        time.sleep(wait)  # 락 밖에서 대기 → 다른 요청은 즉시 자기 예약을 계산(정확한 큐잉)


# ── 수동 일시정지(IP 쿨다운): youtube_pause_until 파일에 epoch(초)를 적으면 그 시각까지
#    자막/언어 조회를 즉시 503으로 막아 YouTube egress를 0으로 만든다. 파일 기반이라
#    재시작 없이 설정/해제되고, 시각이 지나면 자동 재개된다. (소비자는 503을 retryable로 처리)
PAUSE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "youtube_pause_until")


def _pause_remaining() -> float:
    try:
        with open(PAUSE_FILE) as f:
            until = float(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0.0
    return max(0.0, until - time.time())


def _guard_paused():
    remaining = _pause_remaining()
    if remaining > 0:
        raise HTTPException(
            status_code=503,
            detail=f"자막 페치 일시정지 중(IP 쿨다운). {int(remaining)}s 후 재개.",
            headers={"Retry-After": str(int(remaining) + 1)},
        )


# yt-dlp 자체 재시도/요청간 sleep — 일시적 throttle을 내장 백오프로 흡수한다.
# extractor_args: web 클라이언트 + 쿠키 + PO Token(bgutil 로컬 서버 :4416)으로 "인증 요청"을 만들어
# throttle/429를 우회한다(가장 견고). POT는 bgutil-ytdlp-pot-provider 플러그인이 자동 주입하며,
# pm2 `bgutil-pot` 서버가 떠 있어야 한다. 서버가 죽으면 android(쿠키없음·POT불필요) 폴백으로 동작.
_YT_RETRY_OPTS = {
    "retries": 5,
    "extractor_retries": 3,
    "sleep_interval_requests": 1,
    "extractor_args": {"youtube": {"player_client": ["web", "android"]}},
}

# YouTube 쿠키(인증) — web 클라이언트 + PO Token과 함께 쓰면 인증 요청이 되어 throttle/429를 크게 우회한다.
# (예전엔 "쿠키 주면 android가 web으로 떨어져 자동자막 불가"라 비활성했지만, 이제 POT로 web에서 자동자막을
#  받을 수 있으므로 쿠키를 켠다.) cookies.txt(Netscape)를 두면 모든 yt-dlp 호출(라이브러리+subprocess)에 적용.
COOKIES_FILE = os.environ.get("YT_COOKIES_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt"))
if os.path.exists(COOKIES_FILE):
    _YT_RETRY_OPTS["cookiefile"] = COOKIES_FILE
    print(f"[yt-dlp] 쿠키 사용: {COOKIES_FILE}", flush=True)
else:
    print(f"[yt-dlp] 쿠키 파일 없음(쿠키 미사용): {COOKIES_FILE}", flush=True)

# ── 레지덴셜 회전 프록시(egress IP 분산): 프록시는 예비수단이다.
#    기본은 집 IP 직결이며, 프록시 풀은 항상 로드해 둔다(아래 두 용도로 쓰임).
#      1) pause(직결 IP 밴) 중 자동 degrade: 503으로 전면 중단하는 대신, 풀이 있으면 프록시 경유
#         경량 경로(transcript-api)로만 요청을 살려낸다. 밴이 회복되면 자연히 직결로 복귀.
#      2) USE_PROXY_POOL=1: 기존처럼 "항상 프록시 강제" 오버라이드(_proxy_forced).
#    프록시 GB 할당량이 유한하므로, 강제(1)가 아닌 한 평상시엔 프록시를 절대 타지 않는다. ──
PROXY_FILE = os.environ.get("PROXY_LIST_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxies.txt"))
USE_PROXY_POOL = os.environ.get("USE_PROXY_POOL", "").strip().lower() in {"1", "true", "yes", "on"}


def _load_proxy_pool() -> list[str]:
    # USE_PROXY_POOL과 무관하게 항상 로드한다 — pause 중 degrade에 쓰려면 풀이 준비돼 있어야 하기 때문.
    # (강제 여부는 별도로 _proxy_forced()가 판단한다.)
    raw = os.environ.get("PROXY_LIST", "")
    entries = [x.strip() for x in raw.replace(",", "\n").splitlines()]
    if not any(entries) and os.path.exists(PROXY_FILE):
        with open(PROXY_FILE) as f:
            entries = [line.strip() for line in f]
    return [e for e in entries if e and not e.startswith("#")]


_PROXY_POOL = _load_proxy_pool()
if _PROXY_POOL and USE_PROXY_POOL:
    print(f"[proxy] 프록시 풀 {len(_PROXY_POOL)}개 — 항상 프록시 강제(USE_PROXY_POOL=1)", flush=True)
elif _PROXY_POOL:
    print(f"[proxy] 프록시 풀 {len(_PROXY_POOL)}개 로드 — 기본 직결, pause(IP 밴) 중에만 프록시 degrade", flush=True)
else:
    print("[proxy] 프록시 풀 없음 — 기본 직결, pause 중엔 503", flush=True)


def _proxy_forced() -> bool:
    """USE_PROXY_POOL=1 & 풀 존재: 모든 요청을 프록시로 강제(기존 all-or-nothing 오버라이드)."""
    return USE_PROXY_POOL and bool(_PROXY_POOL)


def _pick_proxy() -> Optional[str]:
    """풀에서 무작위 프록시 URL 하나. 비었으면 None(직결)."""
    return random.choice(_PROXY_POOL) if _PROXY_POOL else None


def _is_proxy_quota_error(exc: Exception) -> bool:
    """예비 프록시 quota/결제 문제면 True. 이때는 메인(직결)으로 재시도한다."""
    message = str(exc)
    return (
        "402 Payment Required" in message
        or "Tunnel connection failed: 402" in message
        or "Unable to connect to proxy" in message
    )


def _yt_retry_opts(force_direct: bool = False) -> dict:
    """yt-dlp 옵션 dict. 기본은 직결, USE_PROXY_POOL=1(강제)일 때만 예비 프록시 주입.
    예비 프록시 quota 장애 시 force_direct=True로 메인 직결 재시도.
    프록시를 실제로 태울 때는 옵션을 경량화한다: 재시도 5회는 프록시 GB 낭비의 주범이라 1회로 낮추고,
    쿠키+프록시 조합은 계정 밴 리스크가 커 cookiefile을 뺀다."""
    opts = dict(_YT_RETRY_OPTS)
    proxy = None if force_direct else (_pick_proxy() if _proxy_forced() else None)
    if proxy:
        opts["proxy"] = proxy
        opts["retries"] = 1
        opts["extractor_retries"] = 1
        opts.pop("cookiefile", None)
    return opts


def _ytt(force_direct: bool = False, proxy: Optional[str] = None) -> YouTubeTranscriptApi:
    """transcript-api 인스턴스 팩토리.
    - proxy 명시(pause degrade): 그 프록시로 강제.
    - 미명시: USE_PROXY_POOL=1(강제)일 때만 프록시, force_direct=True면 무조건 직결."""
    if proxy is None and not force_direct and _proxy_forced():
        proxy = _pick_proxy()
    if proxy:
        return YouTubeTranscriptApi(proxy_config=GenericProxyConfig(http_url=proxy, https_url=proxy))
    return YouTubeTranscriptApi()

# ─────────────────────────────────────────────
# 자막 프리워밍(warm) 힌트 큐: 소비자(tubeletter·rt)가 "수요를 감지"해 video_id를 힌트하면,
# 게이트웨이가 인메모리 큐로 받아 백그라운드에서 "최저 우선순위 + 전용 페이싱"으로 미리 캐시한다.
# 설계 원칙: 수요 감지는 각 소비자가(자기 DB를 아니까), 실행·조율은 게이트웨이가(전역 페이싱을 아니까).
#  - 기존 캐시/스로틀/예산/degrade 구조를 우회하지 않고 그대로 통과한다(그 위에 얹기만).
#  - 프리워밍이 throttle을 악화시키면 안 되므로 pause·429 쿨다운·예산소진(503) 시엔 큐를 홀드한다.
#  - pause 중 프록시 degrade 경로로는 절대 태우지 않는다(프록시 GB를 프리워밍에 쓰지 않음).
# ─────────────────────────────────────────────
WARM_MIN_INTERVAL = float(os.environ.get("WARM_MIN_INTERVAL", "20"))   # warm 요청 간 최소 간격(초)
WARM_QUEUE_MAX = int(os.environ.get("WARM_QUEUE_MAX", "500"))          # 큐 상한(초과 시 오래된 것부터 drop)
WARM_MAX_PER_CALL = int(os.environ.get("WARM_MAX_PER_CALL", "50"))     # /warm 1회 최대 video_id 수(초과분 무시)
WARM_PRIORITY = int(os.environ.get("WARM_PRIORITY", "-1"))            # PriorityGate 우선순위(인터랙티브 0보다 낮게)
WARM_HOLD_INTERVAL = float(os.environ.get("WARM_HOLD_INTERVAL", "30"))  # pause/429/예산소진 시 큐 홀드 간격(초)

_warm_queue: collections.deque = collections.deque()  # 항목: (video_id, lang)
_warm_set: set = set()                                 # (video_id, lang) 멤버십(큐 내 중복 제거용)


# ─────────────────────────────────────────────
# API Key
# ─────────────────────────────────────────────
API_KEY = os.environ.get("API_KEY", "yt-dlp-secret-key-change-me")
MIN_SUBTITLE_LENGTH = int(os.environ.get("MIN_SUBTITLE_LENGTH", "80"))


def verify_token(authorization: Optional[str] = Header(None)):
    if authorization is None:
        raise HTTPException(status_code=401, detail="Authorization 헤더가 없습니다.")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token != API_KEY:
        raise HTTPException(status_code=403, detail="유효하지 않은 토큰입니다.")
    return token


# ─────────────────────────────────────────────
# Request 모델
# ─────────────────────────────────────────────

class SubtitleRequest(BaseModel):
    url: str
    lang: str = "ko"
    auto: bool = True
    include_segments: bool = False  # True면 응답에 타임스탬프 cue 배열(segments) 추가 (기본 off로 기존 소비자 응답 불변)
    priority: int = 0  # 높을수록 throttle 대기 큐에서 먼저 처리(ReadNThink 워커가 1로 보냄, 기본 0)
    translate: bool = False  # True면 lang을 "번역 목표 언어"로 보고 자동번역본을 받는다(원본 트랙이 없는 언어용). transcript-api tlang 단일요청 경로만 사용.


class InfoRequest(BaseModel):
    url: str


class WarmRequest(BaseModel):
    video_ids: list[str] = []          # YouTube URL 또는 영상 ID 목록(최대 WARM_MAX_PER_CALL개)
    lang: str = "ko"                    # 프리워밍할 자막 언어(기본 ko)
    source: str = ""                    # 계측용: 어느 소비자가 힌트했는지(tubeletter | rt)


class ChannelVideosRequest(BaseModel):
    channel_url: str          # https://www.youtube.com/@handle or channel ID
    date: str                 # YYYY-MM-DD
    include_shorts: bool = True


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def extract_video_id(url: str) -> str:
    patterns = [r"(?:v=|youtu\.be/|embed/|shorts/)([a-zA-Z0-9_-]{11})"]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    if re.match(r"^[a-zA-Z0-9_-]{11}$", url):
        return url
    raise ValueError(f"유효하지 않은 YouTube URL 또는 ID: {url}")


def parse_vtt(content: str) -> list[dict]:
    lines = content.splitlines()
    entries = []
    i = 0
    while i < len(lines):
        if "-->" in lines[i]:
            times = lines[i].split("-->")
            start = times[0].strip()
            end = times[1].split()[0].strip()
            i += 1
            text_lines = []
            while i < len(lines) and lines[i].strip():
                clean = re.sub(r"<[^>]+>", "", lines[i]).strip()
                if clean:
                    text_lines.append(clean)
                i += 1
            text = " ".join(text_lines)
            if text:
                entries.append({"start": start, "end": end, "text": text})
        else:
            i += 1
    return entries


def _resolve_lang(requested: str, available_subs: list, available_auto: list, original_lang: str) -> str:
    """
    언어 우선순위 결정:
    1. 요청한 lang
    2. 영상 원본 언어 (original_lang)
    3. en
    4. available 중 첫 번째
    """
    all_available = set(available_subs + available_auto)

    def match(lang: str) -> Optional[str]:
        if lang in all_available:
            return lang
        # 변형 매칭 (예: ko-KR, en-US)
        for a in all_available:
            if a.startswith(lang):
                return a
        return None

    for candidate in [requested, original_lang, "en"]:
        if candidate:
            found = match(candidate)
            if found:
                return found

    # fallback: 첫 번째 available
    if available_subs:
        return available_subs[0]
    if available_auto:
        return available_auto[0]

    return requested  # 없으면 그냥 요청값 그대로


_SPEAKER_MARKER_RE = re.compile(r"\s*>{2,}\s*")   # ">>" = 화자 전환 표시(읽기엔 노이즈)
_BRACKET_NOISE_RE = re.compile(r"\[[^\]]{1,40}\]")  # [음악] [Music] [박수] [Applause] 등 비발화 주석


def _clean_caption_text(text: str) -> str:
    """자막 원문 정제: HTML 엔티티 디코드 + 화자 전환 표시(>>) + 비발화 주석([음악] 등) 제거.
    YouTube VTT/timedtext는 화자 전환을 '&gt;&gt;'로, &를 '&amp;'로 인코딩해 그대로 남긴다.
    → &amp;는 디코드해 원문 &를 살리고('S&P 500'), &gt;&gt;는 디코드 후 화자표시로 제거한다.
    이중 인코딩('&amp;gt;')도 대비해 남은 엔티티가 있으면 한 번 더 unescape 한다."""
    if not text:
        return ""
    decoded = html.unescape(text)
    if re.search(r"&#?\w+;", decoded):  # 이중 인코딩 잔여 엔티티 대비
        decoded = html.unescape(decoded)
    decoded = _BRACKET_NOISE_RE.sub(" ", decoded)   # [음악]/[Music] 등 효과음 주석 제거
    return _SPEAKER_MARKER_RE.sub(" ", decoded)


def _sanitize_result(result: dict) -> dict:
    """응답 직전 자막 텍스트 정제. 신규 추출과 레거시(정제 전) 캐시 응답을 모두 커버한다."""
    if isinstance(result.get("subtitles"), str):
        result["subtitles"] = _normalize_subtitle_text(result["subtitles"])
    segments = result.get("segments")
    if isinstance(segments, list):
        for seg in segments:
            if isinstance(seg, dict) and isinstance(seg.get("text"), str):
                seg["text"] = _normalize_subtitle_text(seg["text"])
    return result


def _normalize_subtitle_text(text: str) -> str:
    return re.sub(r"\s+", " ", _clean_caption_text(text)).strip()


def _vtt_to_seconds(ts: str) -> float:
    """VTT 타임스탬프('HH:MM:SS.mmm' 또는 'MM:SS.mmm')를 초(float)로 변환."""
    parts = (ts or "").strip().replace(",", ".").split(":")
    try:
        nums = [float(p) for p in parts]
    except ValueError:
        return 0.0
    seconds = 0.0
    for n in nums:
        seconds = seconds * 60 + n
    return round(seconds, 3)


def _strip_word_overlap(prev_words: list[str], words: list[str]) -> list[str]:
    """words 앞부분이 prev_words 끝부분과 겹치면(롤링 자막) 겹친 만큼 잘라 새 단어만 반환."""
    max_k = min(len(prev_words), len(words))
    for k in range(max_k, 0, -1):
        if prev_words[-k:] == words[:k]:
            return words[k:]
    return words


def _build_segments(entries: list[dict]) -> list[dict]:
    """parse_vtt 결과를 영상 시간 동기화용 cue 배열로 정리한다.
    유튜브 자동자막은 직전 줄들이 다음 cue에 누적·슬라이딩되는 롤링 구조라, 최근 단어
    문맥과의 overlap을 제거해 겹치지 않는 segment만 남긴다. (start/end는 초 단위)
    """
    segments: list[dict] = []
    prev_words: list[str] = []
    for entry in entries:
        text = _normalize_subtitle_text(entry.get("text", ""))
        if not text:
            continue
        words = text.split(" ")
        new_words = _strip_word_overlap(prev_words, words)
        if not new_words:
            continue
        segments.append({
            "start": _vtt_to_seconds(entry.get("start", "")),
            "end": _vtt_to_seconds(entry.get("end", "")),
            "text": " ".join(new_words),
        })
        prev_words = (prev_words + new_words)[-40:]
    return segments


def _ensure_usable_subtitles(text: str) -> str:
    normalized = _normalize_subtitle_text(text)

    if not normalized:
        raise HTTPException(status_code=422, detail="자막이 없습니다.")

    if len(normalized) < MIN_SUBTITLE_LENGTH:
        raise HTTPException(
            status_code=422,
            detail=f"자막이 너무 짧아 요약 품질이 낮습니다. (min={MIN_SUBTITLE_LENGTH})",
        )

    return normalized


def _map_download_error(exc: Exception) -> HTTPException:
    message = str(exc)

    if "HTTP Error 429" in message or "Too Many Requests" in message:
        _yt_mark_429()  # 쿨다운 진입 → 후속 호출이 회복 시간 동안 대기
        return HTTPException(status_code=429, detail=f"자막 추출이 일시적으로 제한되었습니다: {message}")

    if "Private video" in message:
        return HTTPException(status_code=403, detail=f"비공개 영상이거나 접근 권한이 필요합니다: {message}")

    if "This live event will begin" in message:
        return HTTPException(status_code=409, detail=f"라이브 시작 전 영상입니다: {message}")

    return HTTPException(status_code=404, detail=f"영상을 불러올 수 없습니다: {message}")


# ── 영구 트랜스크립트 캐시 (sqlite) ─────────────────────────────────────────
# 트랜스크립트는 영상 게시 후 바뀌지 않으므로 성공 결과를 영구 저장한다.
# 같은 (video_id, lang, auto)는 다시 YouTube를 때리지 않는다 → 429 실수요 자체를 줄임.
SUBTITLE_CACHE_FILE = os.environ.get(
    "SUBTITLE_CACHE_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "subtitle_cache.db"),
)
_cache_lock = threading.Lock()
_cache_conn = sqlite3.connect(SUBTITLE_CACHE_FILE, check_same_thread=False)
_cache_conn.execute(
    "CREATE TABLE IF NOT EXISTS subtitle_cache ("
    " video_id TEXT, lang TEXT, auto INTEGER, payload TEXT, created_at REAL,"
    " PRIMARY KEY (video_id, lang, auto))"
)
_cache_conn.commit()


def _subtitle_cache_get(video_id: str, lang: str, auto: bool):
    with _cache_lock:
        row = _cache_conn.execute(
            "SELECT payload FROM subtitle_cache WHERE video_id=? AND lang=? AND auto=?",
            (video_id, lang, int(auto)),
        ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


def _subtitle_cache_put(video_id: str, lang: str, auto: bool, result: dict):
    try:
        payload = json.dumps(result, ensure_ascii=False)
    except Exception:
        return
    with _cache_lock:
        _cache_conn.execute(
            "INSERT OR REPLACE INTO subtitle_cache (video_id, lang, auto, payload, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (video_id, lang, int(auto), payload, time.time()),
        )
        _cache_conn.commit()


# ── 영구 언어목록 캐시 (sqlite) ──────────────────────────────────────────────
# /languages 응답(네이티브/자동 언어 + translation_targets)은 트랜스크립트처럼 게시 후 사실상
# 불변인데, 리더의 언어 드롭다운이 이걸 반복 호출한다. transcript-api list()는 watch HTML을 통째로
# 받아 영상당 ~345KB나 쓰므로(2026-07-24 실측), video_id 키로 영구 저장해 재호출 시 YouTube를 안 때린다.
# subtitle_cache와 같은 연결/락을 공유한다(같은 DB 파일).
_cache_conn.execute(
    "CREATE TABLE IF NOT EXISTS language_cache ("
    " video_id TEXT PRIMARY KEY, payload TEXT, created_at REAL)"
)
_cache_conn.commit()


def _language_cache_get(video_id: str):
    with _cache_lock:
        row = _cache_conn.execute(
            "SELECT payload FROM language_cache WHERE video_id=?", (video_id,)
        ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


def _language_cache_put(video_id: str, result: dict):
    try:
        payload = json.dumps(result, ensure_ascii=False)
    except Exception:
        return
    with _cache_lock:
        _cache_conn.execute(
            "INSERT OR REPLACE INTO language_cache (video_id, payload, created_at)"
            " VALUES (?, ?, ?)",
            (video_id, payload, time.time()),
        )
        _cache_conn.commit()


def _fetch_subtitles_sync(video_id: str, lang: str, auto: bool, include_segments: bool = False) -> dict:
    """캐시 우선 래퍼. 성공 결과는 영구 캐시(트랜스크립트는 게시 후 불변)되어 같은 영상 재요청 시
    YouTube를 때리지 않는다 — throttle/429의 실수요 자체를 줄이는 핵심 장치."""
    cached = _subtitle_cache_get(video_id, lang, auto)
    if cached is not None:
        result = dict(cached)
    else:
        result = _fetch_subtitles_uncached(video_id, lang, auto)
        _subtitle_cache_put(video_id, lang, auto, result)
        result = dict(result)

    if include_segments:
        result.setdefault("segments", [])
    else:
        result.pop("segments", None)
    return result


def _pick_caption_url(info: dict, resolved_lang: str, auto: bool) -> Optional[str]:
    """1단계 info dict에 이미 담긴 timedtext URL 중 resolved_lang의 vtt URL을 고른다.
    auto=True면 automatic_captions 우선(없으면 subtitles), auto=False면 그 반대.
    정확 매칭 실패 시 변형(en-US 등 prefix) 매칭. vtt 포맷이 있으면 vtt, 없으면 첫 URL.
    → info에 자막 URL이 다 들어있으므로 2차 extract_info(watch/player 재조회) 없이 자막만 직접 받는다."""
    primary = info.get("automatic_captions" if auto else "subtitles") or {}
    other = info.get("subtitles" if auto else "automatic_captions") or {}
    for track_dict in (primary, other):
        fmts = track_dict.get(resolved_lang)
        if not fmts:
            for code in track_dict:  # 변형 매칭(ko-KR/en-US 등)
                if code == resolved_lang or code.startswith(resolved_lang) or resolved_lang.startswith(code):
                    fmts = track_dict[code]
                    break
        if fmts:
            for f in fmts:
                if f.get("ext") == "vtt" and f.get("url"):
                    return f["url"]
            for f in fmts:  # vtt 없으면 아무 포맷 URL(파서가 vtt 전제라 이 케이스는 거의 없음)
                if f.get("url"):
                    return f["url"]
    return None


def _fetch_subtitles_uncached(video_id: str, lang: str, auto: bool) -> dict:
    """blocking yt-dlp 작업 (ThreadPoolExecutor에서 실행)"""
    url = f"https://www.youtube.com/watch?v={video_id}"

    # 1단계: 영상 메타 정보 + 사용 가능한 자막 목록 조회.
    # ignore_no_formats_error: 자막만 필요하므로 영상 포맷이 없어도(로그인 쿠키 등으로 포맷 추출이
    # 실패해도) 메타/자막 목록은 반환하게 한다 — 포맷 없다고 자막 추출을 죽이지 않음.
    # ydl 인스턴스를 with로 즉시 닫지 않고 유지한다 → 3단계에서 같은 세션(쿠키/헤더/프록시/POT)으로
    # info 안의 timedtext URL을 직접 GET하기 위함(2차 extract_info 제거).
    ydl_opts_info = {"skip_download": True, "quiet": True, "no_warnings": True, "ignore_no_formats_error": True, **_yt_retry_opts()}
    _yt_throttle()
    ydl = yt_dlp.YoutubeDL(ydl_opts_info)
    try:
        try:
            info = ydl.extract_info(url, download=False)
        except Exception as exc:
            if not (_PROXY_POOL and _is_proxy_quota_error(exc)):
                raise
            print(f"[proxy] 예비 프록시 실패 → 메인 직결 재시도(info, {video_id}): {exc}", flush=True)
            ydl.close()
            ydl_opts_info = {"skip_download": True, "quiet": True, "no_warnings": True, "ignore_no_formats_error": True, **_yt_retry_opts(force_direct=True)}
            _yt_throttle()
            ydl = yt_dlp.YoutubeDL(ydl_opts_info)
            info = ydl.extract_info(url, download=False)

        title = info.get("title", "")
        channel = info.get("channel", "")
        duration = info.get("duration", 0)
        original_lang = info.get("language") or ""
        available_subs = list(info.get("subtitles", {}).keys())
        available_auto = list(info.get("automatic_captions", {}).keys())

        # 2단계: 우선순위에 따라 실제 사용할 언어 결정
        resolved_lang = _resolve_lang(lang, available_subs, available_auto, original_lang)

        # 3단계: 1단계 info에 담긴 timedtext URL을 같은 ydl로 직접 GET한다.
        # 예전엔 여기서 extract_info를 한 번 더 불러 watch/player HTML(영상당 ~660KB)을 통째로
        # 재조회했는데(실측: 이 재조회가 yt-dlp 경로 바이트의 약 절반), 자막 URL은 이미 info에 다
        # 들어있어 순수 낭비였다. → 자막 파일(수십 KB)만 직접 받아 재조회 ~660KB를 없앤다.
        # 예전 "lang-.*" 대량 번역 문제는 이미 원본 1개만 고르는 것으로 해결됐고, 여기서도 1개만 GET한다.
        cap_url = _pick_caption_url(info, resolved_lang, auto)
        if not cap_url:
            raise HTTPException(status_code=422, detail="사용 가능한 자막이 없습니다.")

        _yt_throttle()  # 자막 GET도 YouTube 히트 → 기존 3단계와 동일하게 페이싱한다.
        try:
            content = ydl.urlopen(cap_url).read().decode("utf-8", "replace")
        except HTTPException:
            raise
        except Exception as exc:
            if _PROXY_POOL and _is_proxy_quota_error(exc):
                print(f"[proxy] 예비 프록시 실패 → 메인 직결 재시도(caption, {video_id}): {exc}", flush=True)
                ydl.close()
                ydl = yt_dlp.YoutubeDL({"skip_download": True, "quiet": True, "no_warnings": True, "ignore_no_formats_error": True, **_yt_retry_opts(force_direct=True)})
                _yt_throttle()
                try:
                    content = ydl.urlopen(cap_url).read().decode("utf-8", "replace")
                except HTTPException:
                    raise
                except Exception as exc2:
                    raise _map_download_error(exc2)  # 429 등 → 적절한 상태코드 매핑 + _yt_mark_429 쿨다운
            else:
                # ydl.urlopen은 DownloadError가 아닌 networking HTTPError(429 등)를 던진다.
                # 예전 extract_info(download=True) 경로가 DownloadError→_map_download_error로 얻던
                # 429 매핑/쿨다운을 여기서도 동일하게 유지한다(그냥 raise하면 상위에서 500이 됨).
                raise _map_download_error(exc)
    finally:
        ydl.close()

    entries = parse_vtt(content)
    subtitle_text = _ensure_usable_subtitles(" ".join(e["text"] for e in entries))

    result = {
        "video_id": video_id,
        "title": title,
        "channel": channel,
        "duration": duration,
        "requested_lang": lang,
        "resolved_lang": resolved_lang,
        "original_lang": original_lang,
        "auto_caption": auto,
        "subtitles": subtitle_text,
        "available_subtitles": available_subs,
        "available_auto_captions": available_auto,
    }
    # segments는 항상 계산해 캐시에 저장한다(응답 포함 여부는 캐시 래퍼에서 결정).
    result["segments"] = _build_segments(entries)
    return result


def _fetch_via_transcript_api(video_id: str, lang: str, auto: bool, proxy: Optional[str] = None) -> dict:
    """빠른 경로: youtube-transcript-api로 timedtext를 직접 1회 요청해 자막을 받는다
    (yt-dlp의 메타 probe + VTT 다운로드보다 훨씬 빠름). 성공 시 yt-dlp 경로와 동일한 result
    딕셔너리를 돌려주고, 실패(자막 없음/IP 차단/네트워크/너무 짧음)하면 예외를 던져 호출부가
    yt-dlp 경로로 폴백하게 한다. title/channel/duration은 이 경로에서 알 수 없어 넣지 않는다
    (ReadNThink는 제목=oEmbed, 길이=segments 끝 → /info 로 보충하므로 회귀 없음).
    proxy 명시(pause degrade): 그 프록시로 강제하고 경량 페이싱만 쓰며, 직결 폴백은 하지 않는다
    (직결 IP가 밴된 상태라 직결 재시도가 무의미)."""
    via_proxy = proxy is not None
    (_proxy_pace if via_proxy else _tapi_throttle)()
    ytt = _ytt(proxy=proxy)
    try:
        tlist = ytt.list(video_id)
    except Exception as exc:
        if via_proxy or not (_PROXY_POOL and _is_proxy_quota_error(exc)):
            raise
        print(f"[proxy] 예비 프록시 실패 → 메인 직결 재시도(transcript-list, {video_id}): {exc}", flush=True)
        _tapi_throttle()
        tlist = _ytt(force_direct=True).list(video_id)

    available_subs: list[str] = []
    available_auto: list[str] = []
    for t in tlist:
        (available_auto if t.is_generated else available_subs).append(t.language_code)

    resolved_lang = _resolve_lang(lang, available_subs, available_auto, "")
    try:
        transcript = tlist.find_transcript([resolved_lang])
    except Exception:
        first = (available_subs or available_auto or [None])[0]
        if not first:
            raise HTTPException(status_code=422, detail="사용 가능한 자막이 없습니다.")
        transcript = tlist.find_transcript([first])
        resolved_lang = first

    raw = transcript.fetch().to_raw_data()  # [{"text", "start", "duration"}] (초 단위)
    # yt-dlp 경로의 parse_vtt 결과와 같은 형태(start/end 문자열)로 변환 → _build_segments 재사용.
    entries = [
        {"start": str(s["start"]), "end": str(s["start"] + s["duration"]), "text": s["text"]}
        for s in raw
    ]
    subtitle_text = _ensure_usable_subtitles(" ".join(s["text"] for s in raw))

    return {
        "video_id": video_id,
        "requested_lang": lang,
        "resolved_lang": resolved_lang,
        "original_lang": "",
        "auto_caption": bool(getattr(transcript, "is_generated", auto)),
        "subtitles": subtitle_text,
        "available_subtitles": available_subs,
        "available_auto_captions": available_auto,
        "segments": _build_segments(entries),
        "source": "transcript-api",
    }


def _fast_fetch_sync(video_id: str, lang: str, auto: bool, include_segments: bool) -> dict:
    """빠른 경로 실행 + 성공 결과 캐시(yt-dlp 경로와 같은 캐시 공유)."""
    result = _fetch_via_transcript_api(video_id, lang, auto)
    _subtitle_cache_put(video_id, lang, auto, result)
    result = dict(result)
    if include_segments:
        result.setdefault("segments", [])
    else:
        result.pop("segments", None)
    return result


def _translate_via_transcript_api(video_id: str, target_lang: str, proxy: Optional[str] = None) -> dict:
    """번역 경로: 영상의 자동/수동 자막 한 트랙을 골라 youtube-transcript-api의 tlang(자동번역)으로
    target_lang 번역본을 1회 요청한다. 원본 언어 트랙만 존재하는 영상(대부분)에서 임의 언어 자막을
    얻기 위한 용도. 예전 yt-dlp "lang-.*" 대량 번역과 달리 목표 언어 1개만 받아 429 부담이 낮다.
    target_lang이 이미 네이티브로 존재하면 번역 없이 그 트랙을 그대로 반환한다.
    proxy 명시(pause degrade): 그 프록시로 강제 + 경량 페이싱, 직결 폴백 없음."""
    via_proxy = proxy is not None
    (_proxy_pace if via_proxy else _tapi_throttle)()
    ytt = _ytt(proxy=proxy)
    try:
        tlist = ytt.list(video_id)
    except Exception as exc:
        if via_proxy or not (_PROXY_POOL and _is_proxy_quota_error(exc)):
            raise
        print(f"[proxy] 예비 프록시 실패 → 메인 직결 재시도(transcript-list, {video_id}): {exc}", flush=True)
        _tapi_throttle()
        tlist = _ytt(force_direct=True).list(video_id)

    available_subs: list[str] = []
    available_auto: list[str] = []
    for t in tlist:
        (available_auto if t.is_generated else available_subs).append(t.language_code)

    # target_lang이 이미 네이티브로 있으면 번역 불필요 — 그 트랙을 그대로 쓴다.
    native = None
    for t in tlist:
        if t.language_code == target_lang or t.language_code.startswith(target_lang):
            native = t
            break

    if native is not None:
        transcript = native.fetch()
        resolved_lang = native.language_code
        translated = False
    else:
        # 번역 가능한 소스 트랙 선택(수동 우선, 없으면 자동). is_translatable=False면 건너뛴다.
        source = None
        for t in tlist:
            if getattr(t, "is_translatable", False):
                source = t
                break
        if source is None:
            raise HTTPException(status_code=422, detail="번역 가능한 자막이 없습니다.")
        transcript = source.translate(target_lang).fetch()
        resolved_lang = target_lang
        translated = True

    raw = transcript.to_raw_data()  # [{"text", "start", "duration"}] (초 단위)
    entries = [
        {"start": str(s["start"]), "end": str(s["start"] + s["duration"]), "text": s["text"]}
        for s in raw
    ]
    subtitle_text = _ensure_usable_subtitles(" ".join(s["text"] for s in raw))

    return {
        "video_id": video_id,
        "requested_lang": target_lang,
        "resolved_lang": resolved_lang,
        "original_lang": "",
        "auto_caption": True,
        "translated": translated,
        "subtitles": subtitle_text,
        "available_subtitles": available_subs,
        "available_auto_captions": available_auto,
        "segments": _build_segments(entries),
        "source": "transcript-api:translate",
    }


def _translate_fetch_sync(video_id: str, target_lang: str, auto: bool, include_segments: bool) -> dict:
    """번역 경로 실행 + 캐시. 번역본은 네이티브 트랙과 캐시 충돌을 피하려 lang 키를 "t:<lang>"로 분리한다.
    transcript-api translate는 YouTube timedtext 쓰로틀/일시 오류로 첫 시도가 빈 응답/실패하는 일이
    잦아 최대 3회 재시도한다(재시도 없으면 리더가 '자막 못 받음' 에러만 뜨고 언어가 안 바뀜)."""
    last_error: Exception | None = None
    result = None
    attempts = 5
    for attempt in range(attempts):
        try:
            result = _translate_via_transcript_api(video_id, target_lang)
            break
        except Exception as error:  # HTTPException(422 등)·네트워크·XML 파싱(쓰로틀 응답) 모두 포함
            last_error = error
            _tapi_mark_fail()  # 실패는 쓰로틀 징후 → 후속 호출 쿨다운
            if attempt < attempts - 1:
                time.sleep(1.5 * (attempt + 1))  # 1.5, 3, 4.5, 6s 점증 백오프(쓰로틀 회복 대기)
    if result is None:
        raise last_error if last_error is not None else HTTPException(status_code=502, detail="자막 번역 실패")
    _subtitle_cache_put(video_id, f"t:{target_lang}", auto, result)
    result = dict(result)
    if include_segments:
        result.setdefault("segments", [])
    else:
        result.pop("segments", None)
    return result


# ── pause(직결 IP 밴) 중 프록시 degrade 경로 ─────────────────────────────────
#    직결이 밴돼 503으로 전면 중단하는 대신, 프록시 풀이 있으면 프록시 경유 "경량" 경로로 요청을 살린다.
#    경량 규칙: transcript-api(빠른/번역)만 사용하고 무거운 yt-dlp 폴백은 절대 타지 않는다.
#    transcript-api가 실패하면 yt-dlp를 프록시로 태우지 않고 retryable 503(Retry-After)으로 넘겨,
#    직결이 회복된 뒤 소비자가 재시도하게 한다(유실 없음). ──
PROXY_DEGRADE_RETRY_AFTER = int(os.environ.get("PROXY_DEGRADE_RETRY_AFTER", "60"))


def _proxy_degrade_log(video_id: str, path: str, nbytes: int, ok: bool):
    """프록시를 경유한 요청마다 한 줄 계측. 다음에 프록시 GB가 또 터지면 원인(어느 경로/얼마나) 추적용."""
    print(f"[proxy-degrade] {video_id} / {path} / {nbytes}B / {'ok' if ok else 'fail'}", flush=True)


def _degrade_fetch_sync(video_id: str, lang: str, auto: bool, translate: bool) -> dict:
    """pause 중 프록시 경유 자막 페치(항상 full result 반환 = segments 포함, 상위에서 정형).
    성공 결과는 캐시에 저장해 이후(직결 회복 후 포함) 재요청 시 YouTube를 안 때리게 한다."""
    proxy = _pick_proxy()
    path = "tapi:translate" if translate else "tapi"
    nbytes, ok = 0, False
    try:
        if translate:
            result = _translate_via_transcript_api(video_id, lang, proxy=proxy)
            _subtitle_cache_put(video_id, f"t:{lang}", auto, result)
        else:
            result = _fetch_via_transcript_api(video_id, lang, auto, proxy=proxy)
            _subtitle_cache_put(video_id, lang, auto, result)
        nbytes = len(result.get("subtitles", "").encode("utf-8"))
        ok = True
        return result
    except HTTPException:
        # 422(자막 없음) 등 의미 있는 상태는 그대로 전파(yt-dlp 폴백 안 함).
        raise
    except Exception as e:
        # transcript-api 실패(쓰로틀/네트워크/파싱) → 무거운 yt-dlp를 프록시로 태우지 않고 retryable 503.
        raise HTTPException(
            status_code=503,
            detail=f"프록시 degrade 자막 페치 실패(직결 회복 후 재시도): {type(e).__name__}",
            headers={"Retry-After": str(PROXY_DEGRADE_RETRY_AFTER)},
        )
    finally:
        _proxy_degrade_log(video_id, path, nbytes, ok)


def _degrade_languages_sync(video_id: str) -> dict:
    """pause 중 프록시 경유 언어목록 조회. 실패 시 yt-dlp /info 폴백 없이 retryable 503."""
    proxy = _pick_proxy()
    ok = False
    try:
        result = _fetch_languages_sync(video_id, proxy=proxy)
        ok = True
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"프록시 degrade 언어조회 실패(직결 회복 후 재시도): {type(e).__name__}",
            headers={"Retry-After": str(PROXY_DEGRADE_RETRY_AFTER)},
        )
    finally:
        _proxy_degrade_log(video_id, "languages", 0, ok)


# 동시 동일요청 합치기(in-flight coalescing): 같은 (video_id, cache_lang, auto)를 여러 요청이
# 동시에 캐시 미스하면 한 번만 실제로 페치하고 나머지는 그 결과를 공유한다. 다유저가 같은 새 영상을
# 동시에 열 때 YouTube 중복 호출/버스트를 없애 IP throttle을 줄이는 핵심 장치.
_inflight: dict = {}


def _shape_result(result: dict, include_segments: bool) -> dict:
    """공유/캐시된 result(항상 segments 포함)를 호출자의 include_segments에 맞춰 정형한다."""
    result = dict(result)
    if include_segments:
        result.setdefault("segments", [])
    else:
        result.pop("segments", None)
    return result


async def _run_fetch(video_id: str, lang: str, auto: bool, priority: int, translate: bool, loop) -> dict:
    """실제 페치(항상 segments 포함으로 받아 캐시·공유용 full result 반환). 번역/빠른/yt-dlp 경로 선택.
    (여기 도달했다는 건 캐시 미스라는 뜻 — 캐시 히트는 상위 fetch_subtitles에서 pause와 무관하게 이미 응답됨.)"""
    # ── pause(직결 IP 밴) 중: 프록시 풀이 있으면 프록시 경유 경량 degrade, 없으면 503(_guard_paused).
    #    직결 프로브 2회 성공 시 자동 회복되므로, 회복되면 이 분기를 빠져 자연히 직결 경로로 복귀한다.
    if _pause_remaining() > 0:
        if not _PROXY_POOL:
            _guard_paused()  # 풀 없음 → 기존처럼 503(Retry-After)
        return await loop.run_in_executor(fast_executor, _degrade_fetch_sync, video_id, lang, auto, translate)

    # ── 번역 경로: transcript-api tlang 단일요청만 사용(원본 트랙이 없는 언어용). yt-dlp 폴백 없음.
    if translate:
        return await loop.run_in_executor(fast_executor, _translate_fetch_sync, video_id, lang, auto, True)

    # ── 빠른 경로: youtube-transcript-api(timedtext 직접). 실패(자막없음/IP차단/네트워크/짧음)하면
    #    아래 yt-dlp 경로(쿠키+POT+스로틀)로 폴백한다.
    try:
        return await loop.run_in_executor(fast_executor, _fast_fetch_sync, video_id, lang, auto, True)
    except HTTPException:
        raise  # 처리 예산 소진(503) 등은 폴백(다른 경로 재시도) 말고 그대로 전파 — 예산 우회 방지
    except Exception as e:
        _tapi_mark_fail()  # 빠른 경로 실패=쓰로틀 징후 → transcript-api 쿨다운
        print(f"[transcript-api] fallback→yt-dlp ({video_id}): {type(e).__name__}: {e}", flush=True)

    await gate.acquire(priority)
    try:
        return await loop.run_in_executor(executor, _fetch_subtitles_sync, video_id, lang, auto, True)
    finally:
        await gate.release()


async def fetch_subtitles(video_id: str, lang: str, auto: bool, include_segments: bool = False, priority: int = 0, translate: bool = False) -> dict:
    """비동기 래퍼: 캐시 우선 → in-flight 합치기 → 실제 페치(우선순위 게이트/스로틀)."""
    # 번역본은 네이티브 트랙과 캐시 충돌을 피하려 lang 키를 "t:<lang>"로 분리한다.
    cache_lang = f"t:{lang}" if translate else lang
    # 캐시 적중은 YouTube를 때리지 않으므로 즉시 응답한다.
    cached = _subtitle_cache_get(video_id, cache_lang, auto)
    if cached is not None:
        return _shape_result(cached, include_segments)

    # 같은 요청이 이미 진행 중이면 그 결과를 공유한다(중복 페치 방지). 단일 이벤트루프라
    # get→create→set 사이에 await가 없어 경쟁 없이 원자적이다.
    key = (video_id, cache_lang, auto)
    inflight = _inflight.get(key)
    if inflight is not None:
        return _shape_result(await inflight, include_segments)

    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    _inflight[key] = fut
    try:
        result = await _run_fetch(video_id, lang, auto, priority, translate, loop)
        if not fut.done():
            fut.set_result(result)
        return _shape_result(result, include_segments)
    except BaseException as e:
        if not fut.done():
            fut.set_exception(e)
        raise
    finally:
        _inflight.pop(key, None)


def _run_ytdlp_for_url(url: str, date_compact: str, start_pos: int, end_pos: int) -> list:
    """단일 URL에 대해 yt-dlp 실행 후 target date 영상만 반환"""
    import subprocess
    args = [
        "--extractor-args", "youtube:player_client=web,android",
        *(["--cookies", COOKIES_FILE] if os.path.exists(COOKIES_FILE) else []),
        "--dump-json",
        "--skip-download",
        "--no-warnings",
        "--quiet",
        "--ignore-errors",
        "--dateafter", date_compact,
        "--playlist-start", str(start_pos),
        "--playlist-end", str(end_pos),
        url,
    ]
    try:
        proc = subprocess.run(["yt-dlp"] + args, capture_output=True, text=True, timeout=120)
        stdout = proc.stdout
    except subprocess.TimeoutExpired:
        return []
    except Exception:
        return []

    items = []
    for line in stdout.strip().split("\n"):
        if not line.strip():
            continue
        try:
            info = json.loads(line)
            vid_id = info.get("id")
            if not vid_id:
                continue
            upload_date = info.get("upload_date", "")
            if upload_date != date_compact:
                continue
            items.append({
                "video_id": vid_id,
                "title": info.get("title", ""),
                "url": f"https://www.youtube.com/watch?v={vid_id}",
                "upload_date": upload_date,
                "duration": info.get("duration", 0),
                "is_short": info.get("duration", 999) <= 60 or "/shorts/" in (info.get("webpage_url") or ""),
                "view_count": info.get("view_count", 0),
            })
        except Exception:
            continue
    return items


def _fetch_channel_videos_sync(channel_url: str, date: str, include_shorts: bool) -> list:
    """특정 날짜의 채널 영상 목록 조회 (regular + shorts 병렬)"""
    import threading
    from datetime import datetime, timezone

    date_compact = date.replace("-", "")

    # 날짜 거리 기반 스캔 윈도우 계산
    # 최신부터 내려가므로: 타겟 날짜 앞쪽(최근) 영상들은 건너뛰고 타겟 주변만 스캔
    today = datetime.now(timezone.utc).date()
    target = datetime.strptime(date, "%Y-%m-%d").date()
    days_diff = max(0, (today - target).days)

    PER_DAY_ESTIMATE = 12  # 채널 하루 평균 업로드 수 (보수적 추정)
    # 타겟 날짜 3일 전부터 스캔 시작 (날짜 순서 불일치 버퍼)
    skip = max(0, (days_diff - 3) * PER_DAY_ESTIMATE)
    start_pos = skip + 1
    window = max(80, PER_DAY_ESTIMATE * 6)  # 최소 80, 하루치 6배 스캔
    end_pos = start_pos + window

    # 채널 URL 정규화
    if channel_url.startswith("UC") and len(channel_url) == 24:
        base = f"https://www.youtube.com/channel/{channel_url}"
    else:
        base = channel_url.rstrip("/")

    urls = [base + "/videos"]
    if include_shorts:
        urls.append(base + "/shorts")

    # /videos 와 /shorts 병렬 실행
    all_results = []
    threads = []
    lock = threading.Lock()

    def fetch_and_collect(url):
        items = _run_ytdlp_for_url(url, date_compact, start_pos, end_pos)
        with lock:
            all_results.extend(items)

    for url in urls:
        t = threading.Thread(target=fetch_and_collect, args=(url,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=130)

    # 중복 제거 (video_id 기준)
    seen = set()
    results = []
    for item in all_results:
        if item["video_id"] not in seen:
            seen.add(item["video_id"])
            results.append(item)

    return results


def _fetch_info_sync(video_id: str) -> dict:
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        ydl_opts = {"skip_download": True, "quiet": True, "no_warnings": True, **_yt_retry_opts()}
        _yt_throttle()
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as exc:
        if not (_PROXY_POOL and _is_proxy_quota_error(exc)):
            raise
        print(f"[proxy] 예비 프록시 실패 → 메인 직결 재시도(info-endpoint, {video_id}): {exc}", flush=True)
        ydl_opts = {"skip_download": True, "quiet": True, "no_warnings": True, **_yt_retry_opts(force_direct=True)}
        _yt_throttle()
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    return {
        "video_id": video_id,
        "title": info.get("title"),
        "channel": info.get("channel"),
        "duration": info.get("duration"),
        "upload_date": info.get("upload_date"),
        "view_count": info.get("view_count"),
        "available_subtitles": list(info.get("subtitles", {}).keys()),
        "available_auto_captions": list(info.get("automatic_captions", {}).keys()),
    }


def _fetch_languages_sync(video_id: str, proxy: Optional[str] = None) -> dict:
    """사용 가능한 자막 언어 목록만 빠르게 조회한다. yt-dlp /info(영상 통째 probe, 수 초~수십 초)보다
    youtube-transcript-api list()가 훨씬 빠르다(timedtext 목록 1회). translation_targets는 번역
    가능한 트랙의 translation_languages(자동번역 대상 전체 목록)에서 모은다(리더 언어 메뉴용).
    proxy 명시(pause degrade): 그 프록시로 강제 + 경량 페이싱, 직결 폴백 없음."""
    via_proxy = proxy is not None
    (_proxy_pace if via_proxy else _tapi_throttle)()
    ytt = _ytt(proxy=proxy)
    try:
        tlist = ytt.list(video_id)
    except Exception as exc:
        if via_proxy or not (_PROXY_POOL and _is_proxy_quota_error(exc)):
            raise
        print(f"[proxy] 예비 프록시 실패 → 메인 직결 재시도(transcript-list, {video_id}): {exc}", flush=True)
        _tapi_throttle()
        tlist = _ytt(force_direct=True).list(video_id)
    native: list[str] = []
    auto: list[str] = []
    targets: dict[str, bool] = {}
    for t in tlist:
        (auto if t.is_generated else native).append(t.language_code)
        if getattr(t, "is_translatable", False):
            for tl in (getattr(t, "translation_languages", None) or []):
                code = tl.get("language_code") if isinstance(tl, dict) else getattr(tl, "language_code", None)
                if code:
                    targets[code] = True
    result = {
        "video_id": video_id,
        "available_subtitles": native,
        "available_auto_captions": auto,
        "translation_targets": list(targets.keys()),
    }
    _language_cache_put(video_id, result)  # 성공 결과 영구 저장(직결/프록시 degrade 공용) → 재조회 시 list() ~345KB 절약
    return result


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {
        "status": "ok",
        "message": "YouTube Subtitle API is running 🎬",
        "max_concurrent": MAX_CONCURRENT,
    }


@app.post("/subtitles", tags=["Subtitles"])
async def get_subtitles(
    body: SubtitleRequest,
    token: str = Depends(verify_token),
):
    """
    YouTube 영상에서 자막을 추출합니다.

    **Headers:** `Authorization: Bearer <API_KEY>`

    **Body:**
    - `url`: YouTube URL 또는 영상 ID
    - `lang`: 자막 언어 코드 (기본값: ko)
    - `auto`: 자동 생성 자막 여부 (기본값: true)
    """
    # pause(IP 밴) 처리는 fetch_subtitles 내부에서 캐시 확인 이후에 한다 — 캐시 히트는 YouTube를
    # 안 때리므로 pause 중에도 즉시 응답하고, 캐시 미스일 때만 프록시 degrade/503으로 분기한다.
    try:
        video_id = extract_video_id(body.url)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        result = await fetch_subtitles(video_id, lang=body.lang, auto=body.auto, include_segments=body.include_segments, priority=body.priority, translate=body.translate)
    except HTTPException:
        raise
    except yt_dlp.utils.DownloadError as e:
        raise _map_download_error(e)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"자막 추출 오류: {str(e)}")

    result = _sanitize_result(result)
    result["url"] = body.url
    return result


@app.post("/warm", tags=["Warm"])
async def warm_subtitles(
    body: WarmRequest,
    token: str = Depends(verify_token),
):
    """자막 프리워밍 힌트: video_id들을 인메모리 큐에 넣고 즉시 응답한다. 실제 페치는 백그라운드
    소비자가 최저 우선순위 + 전용 페이싱으로 처리하며, pause/429/예산소진 시엔 큐를 홀드한다.

    **Headers:** `Authorization: Bearer <API_KEY>`

    **Body:** `{"video_ids": ["..."], "lang": "ko", "source": "tubeletter"|"rt"}`
    **Response:** `{"queued": n, "skipped_cached": m, "queue_size": k}` (+ 초과분 있으면 `skipped_over_limit`)
    """
    ids = list(body.video_ids or [])
    over_limit = 0
    if len(ids) > WARM_MAX_PER_CALL:
        over_limit = len(ids) - WARM_MAX_PER_CALL
        ids = ids[:WARM_MAX_PER_CALL]

    lang = body.lang or "ko"
    source = body.source or "?"
    queued = 0
    skipped_cached = 0

    for raw in ids:
        try:
            video_id = extract_video_id(raw)
        except ValueError:
            continue  # 잘못된 URL/ID는 조용히 무시(warm은 best-effort)
        # 이미 캐시된 건 큐에 넣지 않는다(트랜스크립트는 게시 후 불변).
        if _subtitle_cache_get(video_id, lang, True) is not None:
            skipped_cached += 1
            continue
        key = (video_id, lang)
        if key in _warm_set:
            continue  # 큐에 이미 있음 → 재힌트는 무해한 no-op
        # 큐 상한 초과 시 오래된 것부터 drop.
        while len(_warm_queue) >= WARM_QUEUE_MAX:
            old = _warm_queue.popleft()
            _warm_set.discard(old)
        _warm_queue.append(key)
        _warm_set.add(key)
        queued += 1

    resp = {"queued": queued, "skipped_cached": skipped_cached, "queue_size": len(_warm_queue)}
    if over_limit:
        resp["skipped_over_limit"] = over_limit
    print(
        f"[warm] hint source={source} lang={lang} queued={queued} skipped_cached={skipped_cached} "
        f"queue={len(_warm_queue)}" + (f" over_limit={over_limit}" if over_limit else ""),
        flush=True,
    )
    return resp


@app.post("/channel/videos", tags=["Channel"])
async def get_channel_videos(
    body: ChannelVideosRequest,
    token: str = Depends(verify_token),
):
    """
    특정 날짜에 업로드된 채널 영상 목록 조회 (숏츠 포함).

    **Headers:** `Authorization: Bearer <API_KEY>`

    **Body:**
    - `channel_url`: YouTube 채널 URL (https://www.youtube.com/@handle) 또는 채널 ID (UCxxxxxx)
    - `date`: 조회 날짜 (YYYY-MM-DD)
    - `include_shorts`: 숏츠 포함 여부 (기본값: true)
    """
    # 날짜 형식 검증
    import re as _re
    if not _re.match(r"^\d{4}-\d{2}-\d{2}$", body.date):
        raise HTTPException(status_code=400, detail="date 형식은 YYYY-MM-DD이어야 합니다.")

    try:
        loop = asyncio.get_event_loop()
        videos = await loop.run_in_executor(
            executor,
            _fetch_channel_videos_sync,
            body.channel_url,
            body.date,
            body.include_shorts,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"채널 영상 조회 오류: {str(e)}")

    return {
        "channel_url": body.channel_url,
        "date": body.date,
        "include_shorts": body.include_shorts,
        "count": len(videos),
        "videos": videos,
    }


@app.post("/info", tags=["Info"])
async def get_video_info(
    body: InfoRequest,
    token: str = Depends(verify_token),
):
    """영상 메타정보 및 사용 가능한 자막 목록 조회"""
    try:
        video_id = extract_video_id(body.url)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, _fetch_info_sync, video_id)
    except yt_dlp.utils.DownloadError as e:
        raise _map_download_error(e)

    return result


@app.post("/languages", tags=["Info"])
async def get_languages(
    body: InfoRequest,
    token: str = Depends(verify_token),
):
    """사용 가능한 자막 언어 목록만 빠르게 조회(transcript-api). 실패 시 yt-dlp /info로 폴백.
    리더의 언어 전환 드롭다운이 즉답 받도록 만든 경량 엔드포인트."""
    try:
        video_id = extract_video_id(body.url)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 언어목록 캐시 적중은 YouTube를 안 때리므로 스로틀/예산/pause를 모두 건너뛰고 즉시 응답한다.
    cached = _language_cache_get(video_id)
    if cached is not None:
        return cached

    loop = asyncio.get_event_loop()
    # pause(직결 IP 밴) 중: 프록시 풀이 있으면 프록시 경유 경량 조회(yt-dlp /info 폴백 없음), 없으면 503.
    if _pause_remaining() > 0:
        if not _PROXY_POOL:
            _guard_paused()
        return await loop.run_in_executor(fast_executor, _degrade_languages_sync, video_id)

    try:
        return await loop.run_in_executor(fast_executor, _fetch_languages_sync, video_id)
    except HTTPException:
        raise  # 처리 예산 소진(503) 등은 yt-dlp /info 폴백 말고 그대로 전파
    except Exception as fast_error:
        print(f"[languages] transcript-api 실패→yt-dlp /info 폴백 ({video_id}): {type(fast_error).__name__}", flush=True)
        try:
            result = await loop.run_in_executor(executor, _fetch_info_sync, video_id)
        except yt_dlp.utils.DownloadError as e:
            raise _map_download_error(e)
        # /info엔 translation_targets가 없으니 auto 목록을 대체로 쓴다(yt-dlp auto엔 자동번역 대상이 포함됨).
        result["translation_targets"] = result.get("available_auto_captions", [])
        _language_cache_put(video_id, result)  # /info 폴백 결과도 캐시(다음 조회는 즉답)
        return result


# ─────────────────────────────────────────────
# 일시정지 자동 회복 감시: 정지 중이면 주기적으로 집 IP를 가볍게 찔러보고(캐시 우회=실제 히트),
# 자막이 받아지면(=IP 회복) youtube_pause_until을 지워 자동 재개한다. 밴 상태면 429만 맞고(주기당 1회,
# 무해) 계속 대기. pm2가 살아있는 한 항상 동작하고 재시작에도 자동 시작된다.
# ─────────────────────────────────────────────
PAUSE_PROBE_INTERVAL = float(os.environ.get("PAUSE_PROBE_INTERVAL", "1800"))  # 감시 주기(초, 기본 30분)
PAUSE_PROBE_VIDEO = os.environ.get("PAUSE_PROBE_VIDEO", "dQw4w9WgXcQ")        # 회복 판정용 프로브 영상(항상 자막 있음)


def _clear_pause():
    try:
        os.remove(PAUSE_FILE)
    except FileNotFoundError:
        pass


async def _pause_auto_recover_loop():
    loop = asyncio.get_event_loop()
    while True:
        await asyncio.sleep(PAUSE_PROBE_INTERVAL)
        if _pause_remaining() <= 0:
            continue  # 정지 중이 아니면 스킵(재개 후엔 놀고 있음)
        # 캐시 우회(_fetch_subtitles_uncached 직접) → 실제 YouTube 히트로만 회복 판정.
        # 429는 확률적이라 한 번 우연히 뚫릴 수 있으므로, 20s 간격 2회 연속 성공해야 해제한다(조기해제 방지).
        try:
            await loop.run_in_executor(executor, _fetch_subtitles_uncached, PAUSE_PROBE_VIDEO, "en", True)
        except Exception as e:
            print(f"[pause] 아직 회복 안 됨(계속 대기): {type(e).__name__}", flush=True)
            continue
        await asyncio.sleep(20)
        try:
            await loop.run_in_executor(executor, _fetch_subtitles_uncached, PAUSE_PROBE_VIDEO, "en", True)
        except Exception as e:
            print(f"[pause] 1차 성공했으나 확인 프로브 실패({type(e).__name__}) → 아직 불안정, 대기 유지", flush=True)
            continue
        _clear_pause()
        print("[pause] IP 회복 확인(2회 연속) → 일시정지 자동 해제(자막 페치 재개)", flush=True)


# ─────────────────────────────────────────────
# 자막 프리워밍 큐 소비: 큐에서 하나씩 꺼내 최저 우선순위 + 전용 페이싱으로 미리 캐시한다.
# 기존 캐시/스로틀/예산/degrade를 우회하지 않고 fetch_subtitles를 그대로 재사용한다(그 위에 얹기만).
# throttle을 악화시키지 않도록 pause·429 쿨다운·예산소진(503)이면 처리를 멈추고 큐를 홀드한다.
# ─────────────────────────────────────────────
async def _warm_consumer_loop():
    while True:
        if not _warm_queue:
            await asyncio.sleep(2)
            continue
        # pause(직결 IP 밴) 중엔 warm을 태우지 않는다 — 프록시 degrade로 프리워밍하지 않기 위함(GB 보호).
        if _pause_remaining() > 0:
            await asyncio.sleep(WARM_HOLD_INTERVAL)
            continue
        # 429 쿨다운 중이면 대기(프리워밍이 throttle을 악화시키지 않게).
        with _yt_throttle_lock:
            cooling = _yt_cooldown_until - time.monotonic()
        if cooling > 0:
            await asyncio.sleep(min(cooling + 1, WARM_HOLD_INTERVAL))
            continue

        key = _warm_queue.popleft()
        _warm_set.discard(key)
        video_id, lang = key
        # 큐 대기 중 다른 경로로 이미 캐시됐으면 skip(YouTube 무접촉).
        if _subtitle_cache_get(video_id, lang, True) is not None:
            continue

        try:
            await fetch_subtitles(video_id, lang=lang, auto=True, include_segments=False, priority=WARM_PRIORITY, translate=False)
            print(f"[warm] ok {lang}/{video_id}", flush=True)
        except HTTPException as e:
            if e.status_code in (429, 503):
                # 예산 소진/일시 제한 → 큐 앞으로 되돌리고 잠시 홀드(예산 우회 금지).
                _warm_queue.appendleft(key)
                _warm_set.add(key)
                print(f"[warm] hold {e.status_code} {lang}/{video_id} (재큐)", flush=True)
                await asyncio.sleep(WARM_HOLD_INTERVAL)
                continue  # 홀드 후 재시도 — WARM_MIN_INTERVAL 추가 대기는 생략
            print(f"[warm] fail {e.status_code} {lang}/{video_id}: {e.detail}", flush=True)
        except Exception as e:
            print(f"[warm] fail {lang}/{video_id}: {type(e).__name__}: {e}", flush=True)

        # warm 전용 페이싱: 요청 사이 최소 간격(예산의 '빈틈'만 쓰게 한다).
        await asyncio.sleep(WARM_MIN_INTERVAL)


@app.on_event("startup")
async def _start_pause_watcher():
    asyncio.create_task(_pause_auto_recover_loop())
    print(f"[pause] 자동 회복 감시 시작(주기 {PAUSE_PROBE_INTERVAL:.0f}s)", flush=True)


@app.on_event("startup")
async def _start_warm_consumer():
    asyncio.create_task(_warm_consumer_loop())
    print(f"[warm] warm 큐 소비 시작(min_interval={WARM_MIN_INTERVAL:.0f}s, queue_max={WARM_QUEUE_MAX}, priority={WARM_PRIORITY})", flush=True)
