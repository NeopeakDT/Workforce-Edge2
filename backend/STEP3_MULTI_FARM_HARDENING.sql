-- STEP-3 Multi-farm hardening: drop duplicate indexes, add missing farm index
-- 2026-08-14
--
-- Why this file exists:
-- A multi-farm scalability review of the aggregation pipeline (see
-- backend/aggregation/activity_aggregator.py STEP-4 + event_ingest_api.py)
-- found that STEP1_DATABASE_BASELINE.sql carries several index pairs on
-- activity_instance with column-for-column identical definitions (verified
-- against the baseline SQL, not just by name) -- they add write overhead on
-- every activity_instance insert/update with zero additional query-planning
-- benefit. It also found edge_device has no index on farm_id at all, despite
-- farm-scoped device lookups being a normal admin/dashboard query shape as
-- farm count grows.
--
-- What this does:
-- - Drops 4 confirmed-duplicate indexes on activity_instance:
--   * idx_activity_instance_active is column-for-column identical to the
--     UNIQUE index uniq_active_instance (same columns, same
--     WHERE status = 'IN_PROGRESS' predicate) -- the unique index already
--     serves any query the plain one would.
--   * idx_instance_lookup is column-for-column identical to
--     idx_instance_active_lookup (farm_id, zone_id, activity_type_id, status).
--   * idx_unscheduled_lookup is column-for-column identical to
--     idx_unscheduled_instances (same columns + WHERE activity_schedule_id
--     IS NULL predicate).
--   * idx_activity_instance_session_id is column-for-column identical to
--     idx_activity_instance_session (session_id, no predicate).
-- - Adds idx_edge_device_farm on edge_device(farm_id) -- the only farm-linked
--   table that had no index on its farm_id column at all.
--
-- Deliberately NOT touched here: activity_compliance.farm_id has no FK back
-- to farm(id), but activity_compliance_builder.py's own docstring says the
-- table "has been removed" (Architecture v2, 2026-05-31) while the table
-- still exists in the live schema with no current writers. Resolving that
-- FK gap requires a decision on whether the table is still in use --
-- deliberately left for a separate migration once that's confirmed.
--
-- Idempotent: safe to re-run against a DB that already has these objects.

DROP INDEX IF EXISTS public.idx_activity_instance_active;
DROP INDEX IF EXISTS public.idx_instance_lookup;
DROP INDEX IF EXISTS public.idx_unscheduled_lookup;
DROP INDEX IF EXISTS public.idx_activity_instance_session_id;

CREATE INDEX IF NOT EXISTS idx_edge_device_farm
  ON public.edge_device (farm_id);
