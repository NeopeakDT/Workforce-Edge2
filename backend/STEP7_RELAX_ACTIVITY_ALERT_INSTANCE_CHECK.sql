-- STEP-7 Relax chk_activity_alert_has_instance for schedule-keyed ACTIVITY alerts
-- 2026-09-01
--
-- Why this file exists:
-- STEP5_ADD_ALERT_LIFECYCLE_AND_TYPE.sql added
--     CHECK (alert_type <> 'ACTIVITY' OR activity_instance_id IS NOT NULL)
-- which was correct for every ACTIVITY alert that existed at the time
-- (ACTIVITY_LATE-from-finalization, ACTIVITY_MISSED, ACTIVITY_UNSCHEDULED,
-- ACTIVITY_RUNNING_LONG are all instance-keyed by construction).
--
-- The frozen Step C alert-system design
-- (docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md, S2) adds
-- a genuinely different ACTIVITY_LATE: a schedule-keyed operational warning
-- ("the scheduled activity has not started even though its allowed
-- late-start threshold has passed") that by definition fires BEFORE any
-- activity_instance exists. alerts/matchers/activity_matcher.py's
-- evaluate_late_start_sweep() (Step C, Task 3 -- implemented and reviewed,
-- blocked only by this constraint) creates exactly that row: alert_type=
-- 'ACTIVITY', activity_instance_id=NULL, keyed instead by
-- dedup_key = f"{activity_schedule_id}:{activity_date.isoformat()}".
--
-- What this does:
-- Replaces chk_activity_alert_has_instance with a strictly wider version
-- that additionally permits an ACTIVITY row with a NULL activity_instance_id
-- IF it carries a non-null dedup_key. This is a minimal, purely additive
-- relaxation, not a redesign:
--   - Every existing/other ACTIVITY-alert code path (MISSED, UNSCHEDULED,
--     RUNNING_LONG, and finalized-instance LATE were it ever reintroduced)
--     already sets activity_instance_id, so this change is a no-op for them
--     -- the first disjunct (activity_instance_id IS NOT NULL) still covers
--     every one of those rows exactly as before.
--   - alert_conditions.upsert_active_alert() has always required dedup_key
--     as a mandatory (no-default) keyword argument, and every ACTIVITY-type
--     call site in activity_matcher.py always supplies one -- so this
--     relaxation cannot admit a row that is unidentifiable/undeduplicable;
--     it only widens the constraint enough for the one new, intentional
--     NULL-instance case the Step C design requires.
--
-- Known, deliberately out-of-scope downstream note (not fixed by this
-- migration): dashboard/dashboard_query_service.py::list_recent_alerts()
-- INNER JOINs activity_instance ON ai.id = al.activity_instance_id, so a
-- NULL-instance ACTIVITY_LATE row will not appear in that listing until a
-- later change (Phase 6 dashboard APIs are already disabled by default in
-- main.py, so this has no live effect today). Flagged for whoever wires
-- Phase 6 dashboard APIs or a later alerts-list endpoint.
--
-- Idempotent: safe to re-run (DROP CONSTRAINT IF EXISTS + re-ADD).

DO $$
BEGIN
    ALTER TABLE public.alert_log
      DROP CONSTRAINT IF EXISTS chk_activity_alert_has_instance;

    ALTER TABLE public.alert_log
      ADD CONSTRAINT chk_activity_alert_has_instance
        CHECK (
          alert_type <> 'ACTIVITY'
          OR activity_instance_id IS NOT NULL
          OR dedup_key IS NOT NULL
        );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
