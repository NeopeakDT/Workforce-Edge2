# Alert System — Frozen Design (pre-Step C)

Status: **frozen**, ready for Step C implementation planning.
Context: Step A1–A4 (catalogue/architecture review) and Step B (type-specific
evaluator engine: `alerts/alert_conditions.py` + `alerts/matchers/*`) are
already implemented, standalone-callable, but **not wired into any
production pipeline**. This document freezes the remaining design decisions
needed before Step C (production wiring) begins. No code, SQL, or schema
changes have been made as part of this document.

Motivating incident: 2026-08-27/28, ~27.5h of zero `activity_detection_event`
rows across all 7 cameras on one farm, while `edge_device_heartbeat`
(board telemetry) stayed continuously healthy the whole time. Root cause:
`workforce-edge.service` (the detector) and `workforce-watchdog.service`
were both down; nothing distinguished "board alive" from "detection
pipeline alive." See git history on branch `scrapping-detection-alerts`
for the full incident investigation and the reliability fixes already
shipped (`edge_watchdog.py` reset-failed fix, `StartLimitBurst` increase,
`RTSP_START_LOCK` → `Semaphore(5)`).

## 1. Frozen alert catalogue

| Category | Alert | Status |
|---|---|---|
| Activity | `ACTIVITY_EARLY` | specified below, not yet implemented |
| Activity | `ACTIVITY_LATE` | specified below — Step B behavior must be **revised** to this spec |
| Activity | `ACTIVITY_MISSED` | existing Step B behavior remains, unchanged |
| Activity | `ACTIVITY_UNSCHEDULED` | existing Step B behavior remains, unchanged |
| Activity | `ACTIVITY_RUNNING_LONG` | existing Step B behavior remains, unchanged |
| Posture | `POSTURE_DATA_STALE` | existing Step B behavior remains, unchanged |
| Posture | `POSTURE_HIGH_RESTING_PERCENTAGE` / `POSTURE_LOW_STANDING_PERCENTAGE` | future, not in Step C |
| Camera | `CAMERA_NOT_CONTRIBUTING`, `CAMERA_OFFLINE`/`CAMERA_STREAM_LOST` | future, not in Step C |
| Infrastructure | `EDGE_DEVICE_OFFLINE` | existing Step B behavior remains, unchanged |
| Infrastructure | `WORKFORCE_DETECTOR_OFFLINE` | specified below — new in Step C |
| Device resources | CPU/GPU temp, disk, memory | future, not in Step C |

**Not in the catalogue:** `ACTIVITY_PIPELINE_STALE` — rejected. It would
fire in lockstep with `WORKFORCE_DETECTOR_OFFLINE` on the same real
failures (redundant), and on its own risks alerting on legitimate
no-activity periods (a healthy detector, genuinely quiet farm). The
underlying "no activity despite detector healthy" fact remains available
as a passive diagnostic (not a user-facing alert, not built in Step C).

## 2. `ACTIVITY_LATE` — final spec

An **operational late-start warning** — must NOT be derived from finalized
`session_classification = 'LATE'` (that remains purely historical/reporting
data, produced by `activity_schedule_resolver.py`, and is a completely
separate timing concept — see §4).

**Trigger:**
```
now_local > ideal_start_time(activity_date, farm timezone) + alert_late_start_min
AND no activity_instance exists for (farm_id, activity_schedule_id, activity_date)
```
Initial threshold: `alert_late_start_min = 30` minutes, stored in
`alert_rule.condition`:
```json
{"metric": "minutes_since_ideal_start", "operator": ">", "value": 30}
```
**Do NOT use `activity_schedule.tolerance_late_min`** — that remains the
timing mechanism for `ACTIVITY_MISSED` only.

**Keying:** schedule-keyed, not instance-keyed (no instance exists yet when
this should first fire) — requires a periodic sweep over
`(farm_id, activity_schedule_id, activity_date)`, not an event-triggered
evaluation.

**Dedup key:** `f"{activity_schedule_id}:{activity_date.isoformat()}"`

**Lifecycle:**
```
no activity_instance after +30 min  → ACTIVITY_LATE = ACTIVE
activity_instance appears (any classification, incl. CANCELLED)
                                     → ACTIVITY_LATE = RESOLVED immediately
```
If `ACTIVITY_MISSED` fires first, it resolves any still-ACTIVE
`ACTIVITY_LATE` for the same `(farm_id, activity_schedule_id, activity_date)`.
If the activity later starts anyway after `MISSED` fired, the historical
`MISSED` classification is untouched — this alert layer never rewrites
`session_classification`.

Finalization with `session_classification='LATE'` must NOT create a second,
separate `ACTIVITY_LATE` alert — the operational alert already resolved
the moment the instance appeared, regardless of how it's later classified.

## 3. `ACTIVITY_EARLY` — final spec

An **operational early-start warning**, point-in-time, evaluated at
`activity_instance` creation.

**Trigger:**
```
actual_start_at < ideal_start_time(activity_date, farm timezone) - alert_early_start_min
```
Initial threshold: `alert_early_start_min = 30` minutes, same
`alert_rule.condition` mechanism as `ACTIVITY_LATE`.

**Keying:** instance-keyed (the instance already exists at evaluation time).
**Dedup key:** `str(activity_instance_id)`

**Lifecycle:** point-in-time fact — insert directly as `RESOLVED`, same
pattern as `ACTIVITY_UNSCHEDULED`'s `immediately_resolve=True`. Do not wait
for finalization.

**Do NOT derive this from `session_classification='EARLY'`** — same
separation-of-concerns reasoning as `ACTIVITY_LATE`.

`ACTIVITY_EARLY` and `ACTIVITY_LATE` must never both be active for the same
occurrence — `ACTIVITY_EARLY` only evaluates once an instance exists, at
which point any `ACTIVITY_LATE` for that same schedule/date must already
have resolved per §2's lifecycle.

Because `activity_instance` creation happens through multiple code paths in
`activity_aggregator.py`, the Step C plan must explicitly identify the
single safest integration point (or all necessary points) rather than
assuming one obvious call site.

## 4. Existing activity alerts — unchanged, kept conceptually separate

Four genuinely different timing concepts must **not** be unified or allowed
to drift into each other during Step C wiring:

1. **Historical EARLY/ON_TIME/LATE classification** — `activity_schedule_resolver.py`, based on actual interval overlap with the ideal window. Reporting only.
2. **`ACTIVITY_MISSED`** — `missed_activity_cron.py`, based on `ideal_end_time + tolerance_late_min`. Remains CRITICAL, remains separate from `ACTIVITY_LATE`.
3. **`ACTIVITY_LATE`** (this doc, §2) — `ideal_start_time + alert_late_start_min`.
4. **`ACTIVITY_EARLY`** (this doc, §3) — `ideal_start_time - alert_early_start_min`.

`ACTIVITY_UNSCHEDULED` (point-in-time, immediately resolved, structural —
not timing-based) and `ACTIVITY_RUNNING_LONG` (evaluated while
`IN_PROGRESS`, uses `ideal_end_time + tolerance_late_min`, resolves on
instance end) keep their existing Step B behavior exactly as implemented.

## 5. `WORKFORCE_DETECTOR_OFFLINE` — final frozen spec

**Definition:** *"The watchdog has not been able to confirm workforce
detection pipeline progress within the configured health interval."*
Not "the Jetson process is offline" — the process can technically still be
running while all useful detection processing is stuck (this is exactly
what the incident investigation found: `/tmp/workforce_edge_alive`'s raw
timestamp can be refreshed by the GPU inference-worker thread's idle poll
loop even when every camera capture thread is fully stuck, because that
write path does not require any camera to have produced a frame).

**Signal chain:**
```
total_frames (genuine per-camera capture progress)
    → is_system_stuck()  [existing watchdog logic — NOT raw file freshness]
    → watchdog's detector-health determination
    → detector-health pulse
    → backend
```

**Pulse payload** (conceptual shape; exact backend persistence decided in
the Step C plan): `detector_healthy: bool` (derived from `is_system_stuck()`),
`total_frames`, `camera_count`, `last_frame_progress_at`.

**Backend alerting rule — pulse absence, not payload content:**
```
No fresh detector-health pulse for > 5 minutes  →  WORKFORCE_DETECTOR_OFFLINE
```
`detector_healthy == false` in a *received* payload is diagnostic context
only — it must **not** itself trigger the alert. The alert is driven purely
by pulse freshness/absence, because a dead watchdog sends nothing (no
final "unhealthy" message), so freshness-of-pulse is the only signal the
backend can rely on for both failure cases:

- **Case A** — `workforce-edge.service` fails/stops → watchdog cannot
  confirm detector progress → pulse stops or reports unhealthy → backend
  eventually detects stale pulse.
- **Case B** — `workforce-watchdog.service` itself dies → no pulse sent at
  all → backend detects absence after timeout.

**Cadence:** 60-second pulse. **Timeout:** 5 minutes. **Severity:** `CRITICAL`.
**Dedup key:** `str(device_id)`.

**First-pulse / initial-state behavior:** a newly registered device has
`detector_last_seen_at = NULL`. This must **not** immediately generate a
CRITICAL alert just because no pulse has arrived yet — the Step C plan
must define an explicit grace period (e.g. skip evaluation while `NULL`,
or treat `NULL` as "not yet observed" rather than "stale").

**Required DB change:** `edge_device.detector_last_seen_at TIMESTAMPTZ NULL`.
No separate detector-heartbeat history table is required for this alert at
this stage (mirrors the reasoning already applied to `EDGE_DEVICE_OFFLINE`,
which only needs `edge_device.last_seen_at`, not per-pulse history, to
function).

**Known, explicitly out-of-scope limitation:** `total_frames` is device-wide.
If 1 of 7 cameras keeps producing frames while the other 6 are dead, the
sum keeps rising and `is_system_stuck()` won't confirm "stuck" — so this
alert will not fire for partial camera degradation, only for whole-subsystem
failure. This is intentionally deferred to the future `CAMERA_NOT_CONTRIBUTING`
work, not solved in Step C.

## 6. Explicit non-goals for Step C

Do NOT, as part of Step C:
- Implement camera alerts (`CAMERA_NOT_CONTRIBUTING`, `CAMERA_OFFLINE`/`STREAM_LOST`)
- Implement device-resource alerts (CPU/GPU temp, disk, memory)
- Implement `ACTIVITY_PIPELINE_STALE` (rejected, §1)
- Change historical `session_classification` semantics or its producers
- Use `tolerance_late_min` for `ACTIVITY_LATE`
- Derive `ACTIVITY_LATE` from finalized `LATE` classification, or `ACTIVITY_EARLY` from finalized `EARLY` classification
- Make `detector_healthy == false` (payload content) itself trigger `WORKFORCE_DETECTOR_OFFLINE` — pulse absence is the trigger
- Add database tables beyond the one column specified in §5
- Modify `notification_dispatcher.py` delivery logic unless a concrete Step C dependency requires it (the previously-identified JOIN→LEFT JOIN issue there is Step D's concern unless the implementation plan finds otherwise)
