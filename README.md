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
- 모든 YouTube upstream hit(`yt-dlp`, `youtube-transcript-api`, `/info`, `/languages`, `/channel/videos`, `/warm`)는 프로세스 전역 `UpstreamScheduler`를 통과합니다. 기본 direct lane은 `UPSTREAM_DIRECT_CONCURRENCY=1`, `UPSTREAM_DIRECT_RPM=8`, `UPSTREAM_DIRECT_BURST=2`로 집 IP 버스트를 막습니다.
- foreground 요청은 direct lane 포화/대기 중일 때 비용 제한 proxy overflow를 쓸 수 있습니다. proxy lane 기본값은 `UPSTREAM_PROXY_CONCURRENCY=1`, `UPSTREAM_PROXY_RPM=12`, `UPSTREAM_PROXY_BURST=2`, `UPSTREAM_PROXY_HOURLY_CAP=120`입니다.
- warm/background 트래픽은 proxy overflow를 절대 사용하지 않고, direct capacity를 `UPSTREAM_WARM_MAX_WAIT`(기본 2s) 이상 기다려야 하면 retryable 503으로 홀드합니다.
- **IP 밴(pause) 중 degrade**: `youtube_pause_until`이 활성일 때 foreground 캐시 미스는 프록시 경유 경량 경로(transcript-api만, yt-dlp 폴백 없음)로 요청을 살립니다. 회복 프로브가 직결 2회 성공을 확인하면 자동으로 직결 복귀합니다.
- 자동 failover: `AUTO_PROXY_FAILOVER=1`(기본)이고 프록시 풀이 있을 때, 직결 경로에서 credible block(`IpBlocked` 또는 반복 `429`)이 `DIRECT_BLOCK_THRESHOLD`회(`DIRECT_BLOCK_WINDOW`초 안) 관찰되면 `AUTO_PROXY_PAUSE_SECONDS` 동안만 pause를 설정합니다. pause는 `AUTO_PROXY_MAX_PAUSE_SECONDS`를 넘기지 않습니다.
- 캐시 히트는 pause 중에도 즉시 응답합니다(YouTube를 안 때리므로).
- `USE_PROXY_POOL=1`은 "항상 프록시" 강제 오버라이드입니다. 이때도 yt-dlp는 재시도 1회 + 쿠키 제외로 경량 운용됩니다(재시도 5회 × 프록시 조합이 과거 1GB 소진의 주범).
- 프록시 quota/결제 문제(`402 Payment Required`, tunnel proxy 실패) 시 해당 요청은 메인 직결로 한 번 재시도합니다.
- 프록시 경유 요청은 `[proxy-degrade] video_id/경로/바이트/성공여부` 로그로 계측됩니다.
- 자동 전환/복귀와 queue pressure는 `[upstream]`, `[proxy-failover]`, `[pause]` 로그와 `/` health의 `proxy_mode`, `proxy_pool_size`, `pause_remaining_seconds`, `upstream.direct`, `upstream.proxy`, `upstream.metrics`로 확인합니다.
- 트래픽 실측·최적화 이력은 `THROTTLE-LEARNINGS.md` 참고 (영상당 바이트의 정체는 watch/player HTML, 2026-07-24 yt-dlp 경로 50% 절감 + /languages 영구 캐시).

### UpstreamScheduler 설정

| 환경변수 | 기본값 | 설명 |
|---------|--------|------|
| `UPSTREAM_DIRECT_CONCURRENCY` | `1` | 직결 YouTube upstream 동시 실행 수. `yt-dlp`와 transcript-api가 공유합니다. |
| `UPSTREAM_DIRECT_RPM` | `8` | 직결 lane 분당 admit 수. |
| `UPSTREAM_DIRECT_BURST` | `2` | 직결 lane 순간 허용 버스트. |
| `UPSTREAM_DIRECT_MAX_WAIT` | `45` | foreground가 direct lane을 기다릴 최대 시간. 초과 시 503 또는 proxy overflow 후보가 됩니다. |
| `UPSTREAM_WARM_MAX_WAIT` | `2` | warm/preload가 direct lane을 기다릴 최대 시간. 초과 시 503으로 큐를 홀드합니다. |
| `UPSTREAM_PROXY_CONCURRENCY` | `1` | proxy overflow/degrade 동시 실행 수. |
| `UPSTREAM_PROXY_RPM` | `12` | proxy lane 분당 admit 수. |
| `UPSTREAM_PROXY_BURST` | `2` | proxy lane 순간 허용 버스트. |
| `UPSTREAM_PROXY_MAX_WAIT` | `15` | foreground proxy lane 최대 대기 시간. |
| `UPSTREAM_PROXY_HOURLY_CAP` | `120` | 프로세스 메모리 기준 시간당 proxy admit 상한. PM2 재시작 시 카운터는 초기화됩니다. |
| `UPSTREAM_PROXY_OVERFLOW_QUEUE` | `1` | direct active+queued 수가 이 값 이상이면 foreground proxy overflow를 허용합니다. |

`priority` 값이 높을수록 같은 lane 안에서 먼저 admit됩니다. 기본 foreground는 `0`, `/warm` 소비자는 `WARM_PRIORITY=-1`, 회복 probe는 `10`입니다.

### 운영상 한계

- 스케줄러와 proxy hourly cap은 단일 프로세스 메모리 상태입니다. PM2 cluster/multi-process로 늘리면 프로세스 간 공유 cap이 아니므로 별도 외부 락/카운터가 필요합니다.
- `/channel/videos`는 subprocess 실행 단위로 한 번 admit하지만, `yt-dlp` 내부에서 여러 YouTube HTTP 요청을 만들 수 있습니다.
- YouTube는 실제 unban 시각을 제공하지 않습니다. `youtube_pause_until`은 로컬 cooldown과 routing hint일 뿐이며, 회복은 direct probe 2회 성공으로만 확정합니다.
- proxy degrade는 transcript-api 경량 경로만 사용합니다. 영상이 transcript-api로 받을 수 없는 상태면 retryable 503이 정상 동작입니다.

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

### `POST /warm` — 자막 프리워밍 힌트

소비자(tubeletter·rt)가 "새 영상 수요"를 감지해 video_id를 힌트하면, 게이트웨이가 인메모리 큐로 받아
백그라운드에서 **최저 우선순위 + 전용 페이싱(요청 간 `WARM_MIN_INTERVAL`, 기본 20s)**으로 미리 캐시한다.
설계 원칙: **수요 감지는 각 소비자가(자기 DB를 아니까), 실행·조율은 게이트웨이가(전역 페이싱을 아니까).**
소비자 프리워밍 스케줄러들이 서로 모른 채 예산을 두드리는 충돌을 없앤다.

- 기존 캐시/스로틀/예산/degrade를 우회하지 않는다(그 위에 얹기만). 이미 캐시된 영상은 큐에 넣지 않는다.
- **pause(IP 밴)·429 쿨다운·예산소진(503) 시 큐를 홀드**한다(프리워밍이 throttle을 악화시키지 않음).
  pause 중 프록시 degrade 경로로는 **절대 태우지 않는다**(프록시 GB를 프리워밍에 쓰지 않음).
- 큐 상한(`WARM_QUEUE_MAX`, 기본 500) 초과 시 오래된 것부터 drop. 큐 내 중복 video_id는 dedup(재힌트는 no-op).
- 성공/실패는 `[warm] source/lang/video_id/결과` 로그로 계측된다.

**Headers:** `Authorization: Bearer <API_KEY>`

**Body:**

| 필드 | 타입 | 기본값 | 설명 |
|------|------|--------|------|
| `video_ids` | string[] | `[]` | YouTube URL 또는 영상 ID (1회 최대 `WARM_MAX_PER_CALL`=50개, 초과분 무시) |
| `lang` | string | `ko` | 프리워밍할 자막 언어 |
| `source` | string | `""` | 계측용(`tubeletter` | `rt`) |

**응답:** `{"queued": n, "skipped_cached": m, "queue_size": k}` (50개 초과 시 `skipped_over_limit` 추가)

**환경변수:** `WARM_MIN_INTERVAL`(20s) · `WARM_QUEUE_MAX`(500) · `WARM_MAX_PER_CALL`(50) · `WARM_PRIORITY`(-1) · `WARM_HOLD_INTERVAL`(30s)

### `GET /info` — 영상 정보 조회

자막 추출 없이 영상 메타정보 및 사용 가능한 자막 목록만 확인합니다.

```
GET /info?url=VIDEO_ID
```

## Swagger UI

서버 실행 후 `http://localhost:8000/docs` 접속
