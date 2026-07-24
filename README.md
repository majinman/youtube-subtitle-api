# YouTube Subtitle API 🎬

yt-dlp 기반 YouTube 자막 추출 REST API

> ⚠️ **추출 전략을 수정하기 전에 반드시 [`THROTTLE-LEARNINGS.md`](./THROTTLE-LEARNINGS.md)를 먼저 읽을 것.** 429/throttle·쿠키·PO Token은 직관과 반대되는 함정이 많아 잘못 고치면 자막 수신이 깨진다.

### 429/throttle 대책 (2026-06-23 기준, 되돌리지 말 것)
- **PO Token Provider**: web 클라이언트 + 쿠키 + PO Token으로 인증요청화 → throttle 우회. `player_client=["web","android"]`(android는 폴백). POT는 yt-dlp 플러그인 `bgutil-ytdlp-pot-provider`가 **pm2 `bgutil-pot` 서버(:4416)** 에서 자동 주입 → **이 서버가 떠 있어야 함**(죽으면 android 폴백). 상세·설치법은 THROTTLE-LEARNINGS.md.
- **쿠키 활성**: `cookies.txt`가 있어야 web 경로가 동작 (예전엔 android 단독이라 비활성했으나 POT 도입으로 재활성. **다시 끄지 말 것.**)
- **영구 캐시**: `subtitle_cache.db`(sqlite). 성공 결과를 video_id+lang+auto로 영구 저장, 같은 영상 재요청은 YouTube 무접촉. 적중은 ~25ms.

## 설치

```bash
pip install -r requirements.txt
# PO Token 플러그인 (python3.14 환경)
pip install --break-system-packages bgutil-ytdlp-pot-provider==1.3.1
```

## 실행

```bash
uvicorn main:app --reload --port 8000
# + PO Token 서버 (별도 프로세스, pm2 권장)
#   ~/.openclaw/workspace-claude-agent/bgutil-pot-provider/server 에서 node build/main.js (:4416)
```

## 프록시 운용

- 기본값은 프록시 OFF(집 IP 직결)입니다.
- `proxies.txt`(또는 `PROXY_LIST`)는 항상 로드되지만 평상시엔 사용하지 않습니다.
- **IP 밴(pause) 중에만 자동 degrade**: `youtube_pause_until`이 활성일 때 503 대신 프록시 경유 경량 경로(transcript-api만, yt-dlp 폴백 없음, 1s 페이싱)로 요청을 살립니다. 회복 프로브가 직결 성공을 확인하면 자동으로 직결 복귀합니다.
- 캐시 히트는 pause 중에도 즉시 응답합니다(YouTube를 안 때리므로).
- `USE_PROXY_POOL=1`은 "항상 프록시" 강제 오버라이드입니다. 이때도 yt-dlp는 재시도 1회 + 쿠키 제외로 경량 운용됩니다(재시도 5회 × 프록시 조합이 과거 1GB 소진의 주범).
- 프록시 quota/결제 문제(`402 Payment Required`, tunnel proxy 실패) 시 해당 요청은 메인 직결로 한 번 재시도합니다.
- 프록시 경유 요청은 `[proxy-degrade] video_id/경로/바이트/성공여부` 로그로 계측됩니다.
- 트래픽 실측·최적화 이력은 `THROTTLE-LEARNINGS.md` 참고 (영상당 바이트의 정체는 watch/player HTML, 2026-07-24 yt-dlp 경로 50% 절감 + /languages 영구 캐시).

## API 엔드포인트

### `GET /subtitles` — 자막 추출

| 파라미터 | 타입 | 기본값 | 설명 |
|---------|------|--------|------|
| `url` | string | **필수** | YouTube URL 또는 영상 ID |
| `lang` | string | `ko` | 자막 언어 코드 |
| `auto` | bool | `true` | 자동 생성 자막 사용 여부 |

**예시 요청:**
```
GET /subtitles?url=https://www.youtube.com/watch?v=VIDEO_ID&lang=ko&auto=true
```

**응답 예시:**
```json
{
  "video_id": "VIDEO_ID",
  "title": "영상 제목",
  "channel": "채널명",
  "duration": 300,
  "lang": "ko",
  "auto_caption": true,
  "subtitles": [
    { "start": "00:00:01.000", "end": "00:00:04.000", "text": "안녕하세요" }
  ],
  "subtitle_count": 1,
  "available_subtitles": ["ko", "en"],
  "available_auto_captions": ["ko", "en", "ja"]
}
```

### `GET /info` — 영상 정보 조회

자막 추출 없이 영상 메타정보 및 사용 가능한 자막 목록만 확인합니다.

```
GET /info?url=VIDEO_ID
```

## Swagger UI

서버 실행 후 `http://localhost:8000/docs` 접속
