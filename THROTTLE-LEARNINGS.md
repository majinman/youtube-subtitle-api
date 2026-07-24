# YouTube 자막 추출 — throttle/쿠키/PO토큰 시행착오 정리

2026-06-23 실측. 이 게이트웨이(youtube-subtitle-api)와 이를 쓰는 모든 소비자(ReadNThink, tubeletter/youtube-summary-service, guru-consensus)가 공통으로 부딪힌 문제와 결론. **수정 전에 이 문서부터 읽을 것.**

## 이 게이트웨이의 역할
모든 yt-dlp 호출을 **단일 통로로 직렬화**해서 여러 소비자가 같은 IP로 동시에 YouTube를 때려 429나는 걸 막는다. PM2 #7, `localhost:8000`, 공개터널 `yt-dlp.whoq.kr`. 소비자는 `SUBTITLE_API_URL`/`YOUTUBE_SUBTITLE_API_URL`로 여기를 가리킨다. **yt-dlp를 직접 부르지 말고 전부 이 API를 거치게 할 것.**

## 핵심 결론 (자동자막 다운로드) — 2026-06-23 PO Token 구축으로 전략 전환
- **현재 전략: `player_client=["web","android"]` + 쿠키 + PO Token.** web 클라이언트로 자동자막을 받되,
  PO Token을 bgutil 로컬 서버가 자동 주입한다 → **인증 요청이라 throttle/429를 크게 우회(가장 견고).**
  POT 서버(`bgutil-pot`)가 죽으면 android(쿠키없음·POT불필요)로 자동 폴백 → 회귀 없음.
- 배경 사실: **web/mweb 등 쿠키호환 클라이언트는 자동자막에 PO Token 필수.** 예전엔 POT가 없어 android(쿠키없음)로만
  자동자막을 받았고, 그래서 쿠키를 끄고(`cookies.txt.disabled`) android 단독으로 갔다. **이제 POT가 있으니 그 제약이 풀려** web+쿠키+POT로 전환했다.

### PO Token Provider 구축 (bgutil-ytdlp-pot-provider 1.3.1)
- **플러그인**: `python3.14 -m pip install --break-system-packages bgutil-ytdlp-pot-provider==1.3.1`
  → `/usr/local/lib/python3.14/site-packages/yt_dlp_plugins`. yt-dlp(라이브러리+CLI)가 자동 로드, web 클라이언트에 POT 자동 주입.
- **토큰 서버**: `~/.openclaw/workspace-claude-agent/bgutil-pot-provider/server` (GitHub 클론 → `npm install` → `npx tsc` → `node build/main.js`).
  **pm2 `bgutil-pot`** 으로 상시 기동, `http://127.0.0.1:4416`. 플러그인이 base_url 미지정 시 이 주소를 기본값으로 씀. `pm2 save` 완료(재부팅 자동기동).
- 검증: web 클라이언트는 POT 없이는 자동자막 불가인데, 구축 후 web으로 자동자막 정상 수신(`[pot:bgutil:http] Generating a gvs PO Token for web client`). 점검: `curl localhost:4416/ping`.

## 적용된 throttle 대책 (이 코드에 이미 들어감)
0. **영구 트랜스크립트 캐시 (sqlite, `subtitle_cache.db`)** — 2026-06-23 추가. 트랜스크립트는 게시 후
   불변이라 성공 결과를 (video_id, lang, auto) 키로 영구 저장. 같은 영상 재요청은 YouTube를 안 때린다
   → **429의 실수요 자체를 줄이는 1차 방어선.** 캐시 적중은 throttle 게이트도 건너뛰어 ~25ms 응답
   (`fetch_subtitles`가 게이트 acquire 전 체크 + 게이트 안쪽 `_fetch_subtitles_sync`도 체크해 동시 미스
   중복 페치 방지). 미스 시 동작은 기존과 동일(회귀 없음). 성공만 캐시(부정 캐시 미적용). override: `SUBTITLE_CACHE_FILE`.
1. **동시성 1 직렬** (`MAX_CONCURRENT=1`) — 버스트 차단.
2. **우선순위 게이트** (`PriorityGate`) — `/subtitles`의 `priority` 필드. ReadNThink 워커가 `priority:1`로 보내 tubeletter/importer(0)보다 먼저 처리. (순서만 정함, throttle 자체는 못 풂)
3. **자막 1개 언어만** — `subtitleslangs=[resolved_lang]`. 예전 `[lang,"lang-.*"]`는 자동번역본 수십 개를 받아 영상당 요청 10배+ → 429 폭증. **이게 가장 큰 레버였음.**
4. **요청 최소간격** `YT_MIN_INTERVAL`(기본 12s) + **429 쿨다운** `YT_COOLDOWN_AFTER_429`(기본 30s).
5. **메타는 yt-dlp `/info` 대신 oEmbed**(소비자 측). throttle 무관, 제목/채널 즉시.

## throttle에 빠졌을 때 회복법
- 429/throttle은 **IP 평판 기반, 요청량 줄이면 시간으로 감쇠**. residential IP는 회복 잘 됨.
- **우선순위로는 안 풀린다** — throttle된 IP는 누가 1등이든 429. **유일한 빠른 회복 = 요청량 확 줄이기/멈추기**(경쟁 소비자 일시정지: `pm2 stop tubeletter 25` → 식으면 재가동).
- 라이브/비공개 영상은 자막 원천 없음 → 무한 재시도 금지(소비자에서 terminal 처리).

## 진짜 견고하게 = PO Token Provider (미구축)
쿠키 + web + **PO Token**이면 인증 요청이라 throttle 우회(최고 견고). 플러그인 `bgutil-ytdlp-pot-provider`(Node 토큰 서버) 띄워 `--extractor-args youtube:po_token=...` 주입. throttle이 고질이면 이걸 구축할 것.

## 기타
- **yt-dlp 최신 유지** (`pip install -U yt-dlp --break-system-packages`, python3.14 환경). YouTube가 자주 바꿔 구버전 깨짐.
- `--cookies-from-browser`는 SSH/헤드리스 macOS에서 키체인 복호화 실패로 0개 추출 → 못 씀. 쿠키 쓰려면 export한 `cookies.txt` 파일로.

---

# 2026-07-24 바이트 실측 — "자막 수십 KB인데 왜 영상당 333KB?" 규명 + yt-dlp 이중 extract_info 제거

## 측정 방법
소켓 단위로 CONNECT 터널 바이트를 host별 집계하는 로컬 counting proxy를 띄우고, 프로덕션 `main._YT_RETRY_OPTS`
그대로(web,android+POT, 쿠키 없음=현재 상태) yt-dlp/transcript-api를 그 프록시로 통과시켜 경로·단계별 wire 바이트
측정(residential 프록시가 청구하는 것과 동일 회계). 샘플 6개 영상(단편~15분), 요청 간 15s. 스크립트는 repo 밖 scratchpad.

## 실측 결과 (영상당, 평균) — **바이트의 정체는 자막이 아니라 watch/player HTML**
| 경로/단계 | 바이트 | 정체 |
|---|---|---|
| transcript-api `list()` (= `/languages`, fast-path 1단계) | **~345 KB** | watch HTML 통째(자막 트랙 목록 파싱용). **관측된 333KB/영상 프록시 소진의 정체가 바로 이것** — degrade(프록시) 기간엔 tapi만 쓰므로 `list()` 1회≈345KB가 그대로 GB로 청구됐다. |
| transcript-api `fetch()` (fast-path 2단계, 실제 자막) | ~2 KB(차단 시) / 자막 XML 수십 KB(정상) | timedtext 직접 — **여기가 진짜 자막**, 가볍다. |
| yt-dlp stage1 `extract_info(download=False)` | **~650 KB** | watch/player HTML. web 클라이언트가 watch HTML(~600KB)을 통째로 받음. |
| yt-dlp stage3 `extract_info(download=True)` (자막 다운로드) | **~650 KB** | **stage1과 거의 동일 = watch/player HTML을 통째로 재조회.** 자막 파일(수십 KB)은 이 위에 얹힐 뿐. |
| **yt-dlp 경로 합(OLD, stage1+stage3)** | **~1,300 KB** | 절반이 순수 재조회 낭비. |

- 즉 "자막 텍스트는 수십 KB인데 나머지"의 정체 = **watch/player HTML(영상당 ~650KB)**. yt-dlp는 이걸 stage1·stage3에서
  **두 번** 받았다(=이중 extract_info). transcript-api `list()`도 같은 watch HTML을 받아 ~345KB.
- player_client 실측: `["web","android"]`=708KB vs **`["android"]` 단독=380KB**(자막 트랙 동일). web 클라이언트의 watch HTML이 바이트 주범.

## 적용한 최적화 — yt-dlp 이중 extract_info 제거 (`_fetch_subtitles_uncached`)
stage1 `info` dict에 timedtext URL(vtt 포함)이 **이미 다 들어있다**. 그래서 stage3에서 extract_info를 다시 부르는 대신,
**같은 ydl 세션(쿠키/헤더/프록시/POT 유지)으로 info 안의 vtt URL을 직접 GET**(`_pick_caption_url` + `ydl.urlopen`)한다.
- 결과(실측): yt-dlp 경로 **1,300KB → ~650KB, 50% 절감**(watch HTML 재조회 ~650KB 제거). 자막 payload(단편 ~2KB, 15분 ~115KB)는 원래대로 1회만 받는다.
- 함정: `ydl.urlopen`은 `DownloadError`가 아니라 `networking.HTTPError(429)`를 던진다 → 그냥 두면 상위에서 500이 되고
  `_yt_mark_429` 쿨다운도 안 걸린다. **caption GET 실패는 `_map_download_error`로 매핑**해 기존 429 처리/쿨다운을 유지.
- 기능 검증: 새 경로 end-to-end로 유효 VTT 파싱 확인(예: Steve Jobs 15분 영상 → 662 cue / 332 segment / 35K자). 프로덕션 라이브에서도
  fast-path 실패→새 yt-dlp 경로 실행 + 429가 올바르게 429(500 아님)로 매핑됨을 로그로 확인.

## 미적용(권고만) — player_client 축소
`["android"]` 단독이면 stage1이 380KB(→ 전체 ~380KB, 추가 42% 절감)로 더 줄지만, **web+POT(인증 요청) throttle 견고성을 포기**하는
전략 변경이라 이번엔 적용하지 않음. throttle GB가 다시 문제되면 "android 단독" 전환을 검토(POT 이점은 web 전용이라 사라짐 유의).

## 남은 큰 비용
- `/languages`(transcript-api `list()`)는 영상당 ~345KB(watch HTML). 리더 언어 메뉴가 반복 호출 → 부담.
  → **해결: `language_cache`(sqlite, video_id 키) 영구 캐시 추가.** /languages·degrade languages·(/info 폴백)에 적용,
  캐시 히트는 스로틀/예산/pause 우회 즉답(실측 10s→2ms). fetch 경로의 list()는 fetch()에 live TranscriptList가
  필요하고 timedtext URL이 서명·만료 파라미터라 캐시 불가 + 결과는 이미 subtitle_cache가 커버 → 미적용.
</content>
