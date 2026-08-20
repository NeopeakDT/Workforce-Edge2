-- STEP-4 Add posture_observation_normal view
-- 2026-08-19
--
-- Why this file exists:
-- Dashboard Posture Analytics (Step 3 — Current Status API, Step 4 —
-- Today's Summary) both need "NORMAL-mode only" posture readings: rows
-- written during a scheduled milking window are synthetic all-zero
-- placeholders (metadata->>'mode' = 'MILKING', see
-- jetson/posture/posture_scheduler.py::_write_scheduled_milking_observation)
-- and must never be averaged/peaked/surfaced as real posture data.
--
-- That NORMAL-only rule was initially applied ad hoc per query
-- (WHERE metadata->>'mode' = 'NORMAL' in dashboard/dashboard_query_service.py).
-- This view centralizes it at the DB layer instead, so every current and
-- future analytics query (Current API, Today's Summary, anything later)
-- reads from one canonical relation rather than re-deriving the filter.
--
-- What this does:
-- - Adds a plain (non-materialized) view over posture_observation,
--   filtered to metadata->>'mode' = 'NORMAL'. No new columns, no data
--   migration, no change to posture_observation itself or to how
--   jetson writes rows into it.
--
-- Idempotent: safe to re-run against a DB that already has this object.

CREATE OR REPLACE VIEW public.posture_observation_normal AS
SELECT
    id,
    farm_id,
    zone_id,
    device_id,
    observed_at,
    standing_count,
    feeding_count,
    laying_count,
    standing_percentage,
    laying_percentage,
    metadata,
    activity_type_id,
    created_at
FROM public.posture_observation
WHERE metadata ->> 'mode' = 'NORMAL';
