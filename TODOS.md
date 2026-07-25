# TODOS — youtube-subtitle-api (자막 게이트웨이)

## 다음 단계
- [ ] **인증 토큰 로테이션** — 현재 API_KEY가 코드 기본 플레이스홀더 `yt-dlp-secret-key-change-me`이고, 소비자 3곳(게이트웨이 API_KEY env / tubeletter `SUBTITLE_API_TOKEN` / rt `YOUTUBE_SUBTITLE_API_TOKEN`)이 이 기본값을 공유 중. 공개 도메인(yt-dlp.whoq.kr)에 뜬 API가 누구나 아는 기본 키로만 보호되는 상태 → 실제 랜덤값으로 교체.
  - 순서: 새 토큰 생성 → 게이트웨이 API_KEY 설정 후 재시작 → tubeletter `.env`(로컬) + rt `.env`(Lightsail `~/apps/readnthink/.env`) 동시 갱신 → 각 재시작 → `POST /warm` 200 확인. 세 곳을 원자적으로 못 바꾸면 잠깐 401이 나므로 저트래픽 시간대에.
  - 급하지 않음(기능은 정상). 보안 하드닝 성격.

## 완료
- [x] 프록시 pause-시 degrade + 트래픽 실측 기반 효율화 (2026-07-24, 커밋 77c0ff3/be0cdd5) — 상세는 THROTTLE-LEARNINGS.md
- [x] `POST /warm` 프리워밍 힌트 API — 소비자(tubeletter·rt)가 video_id 힌트, 게이트웨이가 큐 소유 (2026-07-24)
