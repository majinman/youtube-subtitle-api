# TODOS — youtube-subtitle-api (자막 게이트웨이)

## 다음 단계
- [ ] **소비자 재시도 백오프 상한** — RT `youtube-extraction.ts`, tubeletter가 게이트웨이 non-2xx를 전부 generic Error로 던져 무한 재큐한다. 네거티브 캐시가 YouTube egress는 막지만 게이트웨이 자체는 계속 두들김. 422/403/404는 terminal로 처리하도록.
- [ ] **집 IP 회복 또는 재발급** — 현재 pause가 30분마다 재무장돼 사실상 100% 프록시 경유(=GB 소모의 근본). 라우터 재부팅으로 동적 IP 재발급 시 timedtext 쿼터가 새로 잡히는지 확인.
- [ ] **프록시 캡을 바이트 기준으로** — 현재 `UPSTREAM_PROXY_HOURLY_CAP=120건/h`은 GB 환산이 없어 최악 ~1GB/일.
- [ ] **인증 토큰 로테이션** — 현재 API_KEY가 코드 기본 플레이스홀더 `yt-dlp-secret-key-change-me`이고, 소비자 3곳(게이트웨이 API_KEY env / tubeletter `SUBTITLE_API_TOKEN` / rt `YOUTUBE_SUBTITLE_API_TOKEN`)이 이 기본값을 공유 중. 공개 도메인(yt-dlp.whoq.kr)에 뜬 API가 누구나 아는 기본 키로만 보호되는 상태 → 실제 랜덤값으로 교체.
  - 순서: 새 토큰 생성 → 게이트웨이 API_KEY 설정 후 재시작 → tubeletter `.env`(로컬) + rt `.env`(Lightsail `~/apps/readnthink/.env`) 동시 갱신 → 각 재시작 → `POST /warm` 200 확인. 세 곳을 원자적으로 못 바꾸면 잠깐 401이 나므로 저트래픽 시간대에.
  - 급하지 않음(기능은 정상). 보안 하드닝 성격.

## 완료
- [x] 네거티브 캐시 — terminal 실패 기록으로 소비자 무한 재시도의 YouTube/프록시 egress 차단 (2026-07-28) — 상세는 THROTTLE-LEARNINGS.md
- [x] 프록시 pause-시 degrade + 트래픽 실측 기반 효율화 (2026-07-24, 커밋 77c0ff3/be0cdd5) — 상세는 THROTTLE-LEARNINGS.md
- [x] `POST /warm` 프리워밍 힌트 API — 소비자(tubeletter·rt)가 video_id 힌트, 게이트웨이가 큐 소유 (2026-07-24)
