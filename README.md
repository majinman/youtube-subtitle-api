# YouTube Subtitle API 🎬

yt-dlp 기반 YouTube 자막 추출 REST API

## 설치

```bash
pip install -r requirements.txt
```

## 실행

```bash
uvicorn main:app --reload --port 8000
```

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
