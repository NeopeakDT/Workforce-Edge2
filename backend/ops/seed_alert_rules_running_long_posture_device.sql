-- backend/ops/seed_alert_rules_running_long_posture_device.sql
-- Seeds alert_rule rows for the three families approved for production
-- scope in the 2026-09-04 remaining-alert-families scope/threshold review:
--   ACTIVITY_RUNNING_LONG (WARNING, one rule per active schedule)
--   POSTURE_DATA_STALE    (WARNING, one farm-wide rule)
--   EDGE_DEVICE_OFFLINE   (CRITICAL, one farm-wide rule)
--
-- Explicitly NOT seeded here (deferred per the same review):
--   ACTIVITY_UNSCHEDULED -- no production call site invokes the evaluator
--     for this classification at all (activity_schedule_resolver.py never
--     calls evaluate_finalized_instance()); seeding a rule now would create
--     false confidence. Needs a separate wiring decision first.
--   CAMERA_*, device resource alerts (CPU/GPU/disk/mem) -- no evaluator
--     exists; out of scope per A4.
--
-- ACTIVITY_RUNNING_LONG condition shape: evaluate_in_progress_instance()
-- (alerts/matchers/activity_matcher.py) only checks
-- condition.metric == "elapsed_minutes_since_start" and
-- condition.operator == ">" -- the threshold itself is NOT read from the
-- rule; it is derived entirely from
-- activity_schedule.ideal_end_time + activity_schedule.tolerance_late_min
-- (the same cutoff formula ACTIVITY_MISSED uses). "value" is set to null
-- here to make that explicit rather than seeding a misleading number that
-- the evaluator ignores.
--
-- POSTURE_DATA_STALE / EDGE_DEVICE_OFFLINE: alert_rule has no zone_id or
-- device_id column, so these are inherently farm-wide rules -- the
-- per-zone / per-device dimension comes only from which zone_id/device_id
-- the sweep (aggregation/alerts_cron.py) passes to the evaluator, not from
-- the rule itself. Not a schema change; matches existing evaluator design.
--
-- Idempotent: same NOT EXISTS guard pattern as seed_alert_rules_step_c.sql
-- and seed_alert_rules_activity_missed.sql. Re-running this script is a
-- no-op once these rules exist.
--
-- Does NOT touch: the existing 19 ACTIVITY_EARLY/ACTIVITY_LATE/
-- ACTIVITY_MISSED/WORKFORCE_DETECTOR_OFFLINE rules, alert_log,
-- activity_instance, edge_device, or any other table.

DO $$
DECLARE
  v_farm_id uuid := '608e7a58-d46e-4f6c-bd19-b8c2a8d59050';
  v_schedule record;
BEGIN
  -- ACTIVITY_RUNNING_LONG: one rule per active schedule.
  FOR v_schedule IN
    SELECT id, activity_type_id, label
    FROM activity_schedule
    WHERE farm_id = v_farm_id AND is_active = true
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM alert_rule
      WHERE farm_id = v_farm_id AND activity_schedule_id = v_schedule.id
        AND name = 'ACTIVITY_RUNNING_LONG: ' || v_schedule.label
    ) THEN
      INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                               name, condition, severity, channel, alert_type, is_active)
      VALUES (gen_random_uuid(), v_farm_id, v_schedule.activity_type_id, v_schedule.id,
              'ACTIVITY_RUNNING_LONG: ' || v_schedule.label,
              '{"metric": "elapsed_minutes_since_start", "operator": ">", "value": null}'::jsonb,
              'WARNING', ARRAY['APP'::alert_channel], 'ACTIVITY', true);
    END IF;
  END LOOP;

  -- POSTURE_DATA_STALE: one farm-wide rule.
  IF NOT EXISTS (
    SELECT 1 FROM alert_rule
    WHERE farm_id = v_farm_id AND name = 'POSTURE_DATA_STALE'
  ) THEN
    INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                             name, condition, severity, channel, alert_type, is_active)
    VALUES (gen_random_uuid(), v_farm_id, NULL, NULL,
            'POSTURE_DATA_STALE',
            '{"metric": "observation_age_minutes", "operator": ">", "value": 10}'::jsonb,
            'WARNING', ARRAY['APP'::alert_channel], 'POSTURE', true);
  END IF;

  -- EDGE_DEVICE_OFFLINE: one farm-wide rule.
  IF NOT EXISTS (
    SELECT 1 FROM alert_rule
    WHERE farm_id = v_farm_id AND name = 'EDGE_DEVICE_OFFLINE'
  ) THEN
    INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                             name, condition, severity, channel, alert_type, is_active)
    VALUES (gen_random_uuid(), v_farm_id, NULL, NULL,
            'EDGE_DEVICE_OFFLINE',
            '{"metric": "heartbeat_age_minutes", "operator": ">", "value": 10}'::jsonb,
            'CRITICAL', ARRAY['APP'::alert_channel], 'EDGE_DEVICE', true);
  END IF;
END $$;
