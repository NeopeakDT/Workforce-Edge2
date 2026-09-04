-- backend/ops/seed_alert_rules_activity_missed.sql
-- Seeds alert_rule rows for ACTIVITY_MISSED -- the confirmed gap from the
-- 2026-09-04 Alert Rule / Alert Log / Activity-State consistency audit.
--
-- The Step C design doc (docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md
-- S4) already defines ACTIVITY_MISSED as CRITICAL and separate from
-- ACTIVITY_LATE. The evaluator (alerts/matchers/activity_matcher.py
-- evaluate_finalized_instance(), called from missed_activity_cron.py for
-- every newly-created MISSED activity_instance) already matches
-- condition.metric == "session_classification" against value "MISSED" --
-- only the alert_rule seed was missing. This script adds exactly that,
-- one rule per active schedule, and nothing else.
--
-- Idempotent: same NOT EXISTS guard pattern as
-- seed_alert_rules_step_c.sql (no unique index on (farm_id, name) exists;
-- safety comes from this guard, not a DB constraint). Re-running this
-- script is a no-op once the 6 rules exist.
--
-- Does NOT touch: existing ACTIVITY_LATE/ACTIVITY_EARLY rules,
-- WORKFORCE_DETECTOR_OFFLINE, alert_log, activity_instance, or any other
-- table.

DO $$
DECLARE
  v_farm_id uuid := '608e7a58-d46e-4f6c-bd19-b8c2a8d59050';
  v_schedule record;
BEGIN
  FOR v_schedule IN
    SELECT id, activity_type_id, label
    FROM activity_schedule
    WHERE farm_id = v_farm_id AND is_active = true
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM alert_rule
      WHERE farm_id = v_farm_id AND activity_schedule_id = v_schedule.id
        AND name = 'ACTIVITY_MISSED: ' || v_schedule.label
    ) THEN
      INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                               name, condition, severity, channel, alert_type, is_active)
      VALUES (gen_random_uuid(), v_farm_id, v_schedule.activity_type_id, v_schedule.id,
              'ACTIVITY_MISSED: ' || v_schedule.label,
              '{"metric": "session_classification", "operator": "=", "value": "MISSED"}'::jsonb,
              'CRITICAL', ARRAY['APP'::alert_channel], 'ACTIVITY', true);
    END IF;
  END LOOP;
END $$;
