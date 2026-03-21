"""
YouTube Subtitle Extraction API
Powered by yt-dlp
"""

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
import yt_dlp
import re
import tempfile
import os
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import asyncio

app = FastAPI(
    title="YouTube Subtitle API",
    description="yt-dlp 기반 유튜브 자막 추출 API",
    version="3.0.0",
)

# ─────────────────────────────────────────────
# 동시 처리 설정
# ─────────────────────────────────────────────
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "20"))   # 동시 yt-dlp 처리 수
executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# ─────────────────────────────────────────────
# API Key
# ─────────────────────────────────────────────
API_KEY = os.environ.get("API_KEY", "yt-dlp-secret-key-change-me")


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


class InfoRequest(BaseModel):
    url: str


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


def _fetch_subtitles_sync(video_id: str, lang: str, auto: bool) -> dict:
    """blocking yt-dlp 작업 (ThreadPoolExecutor에서 실행)"""
    url = f"https://www.youtube.com/watch?v={video_id}"

    # 1단계: 영상 메타 정보 + 사용 가능한 자막 목록 조회
    ydl_opts_info = {"skip_download": True, "quiet": True, "no_warnings": True}
    with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
        info = ydl.extract_info(url, download=False)

    title = info.get("title", "")
    channel = info.get("channel", "")
    duration = info.get("duration", 0)
    original_lang = info.get("language") or ""
    available_subs = list(info.get("subtitles", {}).keys())
    available_auto = list(info.get("automatic_captions", {}).keys())

    # 2단계: 우선순위에 따라 실제 사용할 언어 결정
    resolved_lang = _resolve_lang(lang, available_subs, available_auto, original_lang)

    # 3단계: 결정된 언어로 자막 다운로드
    with tempfile.TemporaryDirectory() as tmpdir:
        sub_type = "writeautomaticsub" if auto else "writesubtitles"
        ydl_opts = {
            "skip_download": True,
            sub_type: True,
            "subtitleslangs": [resolved_lang, f"{resolved_lang}-.*"],
            "subtitlesformat": "vtt",
            "outtmpl": os.path.join(tmpdir, "%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.extract_info(url, download=True)

        vtt_files = list(Path(tmpdir).glob(f"{video_id}.{resolved_lang}*.vtt"))
        if not vtt_files:
            vtt_files = list(Path(tmpdir).glob("*.vtt"))

        if not vtt_files:
            return {
                "video_id": video_id,
                "title": title,
                "channel": channel,
                "duration": duration,
                "subtitles": "",
                "available_subtitles": available_subs,
                "available_auto_captions": available_auto,
                "message": "사용 가능한 자막이 없습니다.",
            }

        content = vtt_files[0].read_text(encoding="utf-8")
        entries = parse_vtt(content)

        return {
            "video_id": video_id,
            "title": title,
            "channel": channel,
            "duration": duration,
            "requested_lang": lang,
            "resolved_lang": resolved_lang,
            "original_lang": original_lang,
            "auto_caption": auto,
            "subtitles": " ".join(e["text"] for e in entries),
            "available_subtitles": available_subs,
            "available_auto_captions": available_auto,
        }


async def fetch_subtitles(video_id: str, lang: str, auto: bool) -> dict:
    """비동기 래퍼: 세마포어로 동시 처리 수 제한, executor로 블로킹 회피"""
    async with semaphore:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            executor, _fetch_subtitles_sync, video_id, lang, auto
        )


def _fetch_info_sync(video_id: str) -> dict:
    url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {"skip_download": True, "quiet": True, "no_warnings": True}
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
    try:
        video_id = extract_video_id(body.url)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        result = await fetch_subtitles(video_id, lang=body.lang, auto=body.auto)
    except yt_dlp.utils.DownloadError as e:
        raise HTTPException(status_code=404, detail=f"영상을 불러올 수 없습니다: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"자막 추출 오류: {str(e)}")

    result["url"] = body.url
    return result


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
        raise HTTPException(status_code=404, detail=str(e))

    return result
