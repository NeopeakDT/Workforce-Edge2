-- STEP-6 Add detector-process health tracking, separate from board heartbeat
-- 2026-08-31
--
-- Why: the 2026-08-27/28 incident showed edge_device.last_seen_at (board
-- telemetry, sent by edge_heartbeat_agent.py) staying healthy for ~27h
-- while the detection pipeline (workforce-edge.service) was fully down.
-- This column tracks a SEPARATE signal: the last time workforce-watchdog.py
-- confirmed real detection-pipeline progress (via is_system_stuck()-style
-- logic, not raw /tmp/workforce_edge_alive freshness) and reported it here.
-- See docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md.
--
-- Idempotent: safe to re-run.

ALTER TABLE public.edge_device
  ADD COLUMN IF NOT EXISTS detector_last_seen_at timestamptz;

COMMENT ON COLUMN public.edge_device.detector_last_seen_at IS
  'Last confirmed workforce-detector pipeline progress, reported by edge_watchdog.py. NULL = no pulse ever received (new device or pre-Step-C). Distinct from last_seen_at, which only proves the board/OS is online.';
