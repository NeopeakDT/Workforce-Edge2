-- STEP-2 Add merge_processed to activity_detection_event
-- 2026-08-11
--
-- Why this file exists:
-- backend/aggregation/activity_aggregator.py (STEP-4) has depended on a
-- `merge_processed` column on activity_detection_event since before the STEP1
-- baseline was captured, but it was never created via a committed migration --
-- it does not exist on the live Supabase DB (confirmed via information_schema
-- on 2026-08-11) and is absent from STEP1_DATABASE_BASELINE.sql. The aggregator
-- code defends against its absence (`if "merge_processed" not in event_columns`)
-- so nothing crashes, but every code path that depends on this column --
-- the 1-day age gate (AGG_MAX_EVENT_AGE_SEC), mark_event_skipped(), and
-- reconciliation's orphan bookkeeping -- has been silently inert in production.
--
-- What this does:
-- - Adds merge_processed (default FALSE) so events the aggregator gives up on
--   can be permanently excluded from re-selection instead of being retried
--   forever.
-- - Adds a partial index matching the aggregator's main per-loop SELECT
--   (WHERE activity_instance_id IS NULL AND merge_processed = FALSE) so that
--   query stays cheap as the table grows.
--
-- Idempotent: safe to re-run against a DB that already has these objects.

ALTER TABLE public.activity_detection_event
  ADD COLUMN IF NOT EXISTS merge_processed boolean NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_ade_unlinked_unprocessed
  ON public.activity_detection_event (event_time)
  WHERE activity_instance_id IS NULL AND merge_processed = FALSE;
