# YouTube Subtitle Gateway Availability Scheduler

Date: 2026-07-25

## Scope

This gateway is the shared YouTube subtitle upstream for ReadNThink, tubeletter, guru, warm/preload, and local diagnostics. All cache-miss calls that touch YouTube must pass through one scheduler/admission-control layer. Cache hits are outside the scheduler because they do not create upstream load.

## Traffic Classes

- `foreground`: user-demand `/subtitles`, `/languages`, `/info`, and ReadNThink worker requests. Highest priority.
- `warm`: `/warm` background preloads. Best-effort; must defer/drop under pressure and must not use paid proxy overflow.
- `probe`: low-frequency direct recovery checks. Direct only; used to prove direct egress health. Probes currently run with priority `10`.

## State Machine

- `direct`: default. All eligible work uses direct egress under the direct budget.
- `proxy-degrade`: entered only after credible direct-path block evidence or while a local pause file is active. Foreground cache misses may use proxy overflow when direct is unhealthy or saturated. Warm is deferred.
- `direct-recovery`: periodic direct probes run while paused/degraded. Two successful direct probes clear the pause and reset direct health back to `direct`.

YouTube does not expose a real unban time. The local pause is a bounded cooldown and routing hint, not an external guarantee.

## Budgets And Queues

Direct lane defaults are deliberately conservative:

- `UPSTREAM_DIRECT_CONCURRENCY=1`
- `UPSTREAM_DIRECT_RPM=8`
- `UPSTREAM_DIRECT_BURST=2`
- `UPSTREAM_DIRECT_MAX_WAIT=45`
- `UPSTREAM_WARM_MAX_WAIT=2`
- one shared direct pacing/budget bucket for transcript-api, yt-dlp, `/info`, `/languages`, `/channel/videos`, `/warm`, and direct probes
- priority queue by numeric `priority`; default foreground is `0`, warm defaults to `WARM_PRIORITY=-1`, and probes use `10`
- bounded wait; if direct is saturated beyond the wait budget, warm defers with retryable 503 and foreground can use proxy overflow if healthy and configured

Proxy lane is an availability overflow, never a default:

- used only for foreground work; warm/background traffic is denied from proxy overflow
- enabled only when a proxy pool exists and `USE_PROXY_POOL=1` is not forcing all proxy behavior
- `UPSTREAM_PROXY_CONCURRENCY=1`
- `UPSTREAM_PROXY_RPM=12`
- `UPSTREAM_PROXY_BURST=2`
- `UPSTREAM_PROXY_MAX_WAIT=15`
- `UPSTREAM_PROXY_HOURLY_CAP=120`
- `UPSTREAM_PROXY_OVERFLOW_QUEUE=1`
- explicit per-minute rate and in-memory hourly request cap
- proxy-degrade uses transcript-api-only paths; no yt-dlp proxy expansion for regular subtitle fetches

## Retry Behavior

The scheduler records direct block evidence (`IpBlocked`, `429`, `Too Many Requests`, and related IP block strings) and enters bounded pause after `DIRECT_BLOCK_THRESHOLD` events inside `DIRECT_BLOCK_WINDOW`.

It does not retry the same upstream operation repeatedly inside the same lane. A foreground request makes one routing decision before the fetch:

- direct when direct is healthy and not saturated
- proxy overflow when direct is paused or direct active+queued pressure is at least `UPSTREAM_PROXY_OVERFLOW_QUEUE`
- retryable 503 when neither lane is available within the configured wait budget

Warm/preload is not retried through proxy. While pause is active, warm is held via 503 so the warm consumer can sleep and retry later.

Proxy quota/connection failures on the legacy forced-proxy path can retry once through direct. Pause/degrade foreground proxy paths do not escalate to proxy-backed yt-dlp; they return retryable 503 if transcript-api cannot serve the subtitle.

## Observability

Health exposes:

- mode and pause remaining
- direct/proxy queue and active counts
- lane limits and configured budgets
- metrics counter map, including:
  - `{lane}_queued_total`
  - `{lane}_admitted_total`
  - `{lane}_{traffic_class}_admitted_total`
  - `{lane}_denied_wait_total`
  - `{lane}_{traffic_class}_denied_wait_total`
  - `proxy_denied_ineligible`
  - `proxy_denied_no_pool`
  - `proxy_denied_hourly_cap`
  - `warm_deferred_pause`
  - `direct_block_events_total`
  - `direct_recoveries_total`

Logs:

- `[upstream] queued/admit/deny`
- `[proxy-failover]` block observations and pause activation
- `[pause]` direct recovery probe state
- existing `[proxy-degrade]` per proxy request byte/result accounting

Metrics and proxy hourly cap are in-memory process state and reset on PM2 restart.

## Safe Fallbacks

If proxy pool is unavailable, foreground work uses direct when healthy/capacity allows; otherwise it gets retryable 503 during pause/saturation. Warm work is dropped/deferred under pressure.

## Known Limitations

- The scheduler is process-wide, not cluster-wide. Running multiple gateway processes would multiply direct and proxy budgets unless an external shared counter/queue is added.
- `/channel/videos` is admitted at subprocess granularity. `yt-dlp` may perform several YouTube HTTP requests inside that one admitted subprocess call.
- Local pause expiry is not a YouTube unban guarantee. It only permits normal routing to resume; any new direct block evidence can immediately re-enter pause.
- Cache hits intentionally bypass the scheduler because they create no YouTube load, so health queue counts only reflect cache-miss upstream work.
