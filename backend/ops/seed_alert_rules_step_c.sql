-- backend/ops/seed_alert_rules_step_c.sql
-- Seeds alert_rule rows for ACTIVITY_LATE, ACTIVITY_EARLY, WORKFORCE_DETECTOR_OFFLINE.
-- Idempotent: ON CONFLICT DO NOTHING against (farm_id, name) -- no unique index on
-- that pair exists yet, so this relies on name being distinct per run; re-running
-- with the same names is safe (harmless duplicate insert avoided by the NOT EXISTS
-- guard below, not by a DB constraint).

DO $$
DECLARE
  v_farm_id uuid := '608e7a58-d46e-4f6c-bd19-b8c2a8d59050';
  v_schedule record;
BEGIN
  -- ACTIVITY_LATE and ACTIVITY_EARLY: one rule per active schedule.
  FOR v_schedule IN
    SELECT id, activity_type_id, label
    FROM activity_schedule
    WHERE farm_id = v_farm_id AND is_active = true
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM alert_rule
      WHERE farm_id = v_farm_id AND activity_schedule_id = v_schedule.id
        AND name = 'ACTIVITY_LATE: ' || v_schedule.label
    ) THEN
      INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                               name, condition, severity, alert_type, is_active)
      VALUES (gen_random_uuid(), v_farm_id, v_schedule.activity_type_id, v_schedule.id,
              'ACTIVITY_LATE: ' || v_schedule.label,
              '{"metric": "minutes_since_ideal_start", "operator": ">", "value": 30}'::jsonb,
              'WARNING', 'ACTIVITY', true);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM alert_rule
      WHERE farm_id = v_farm_id AND activity_schedule_id = v_schedule.id
        AND name = 'ACTIVITY_EARLY: ' || v_schedule.label
    ) THEN
      INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                               name, condition, severity, alert_type, is_active)
      VALUES (gen_random_uuid(), v_farm_id, v_schedule.activity_type_id, v_schedule.id,
              'ACTIVITY_EARLY: ' || v_schedule.label,
              '{"metric": "minutes_before_ideal_start", "operator": ">", "value": 30}'::jsonb,
              'WARNING', 'ACTIVITY', true);
    END IF;
  END LOOP;

  -- WORKFORCE_DETECTOR_OFFLINE: one device-agnostic, farm-scoped rule.
  IF NOT EXISTS (
    SELECT 1 FROM alert_rule
    WHERE farm_id = v_farm_id AND name = 'WORKFORCE_DETECTOR_OFFLINE'
  ) THEN
    INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                             name, condition, severity, alert_type, is_active)
    VALUES (gen_random_uuid(), v_farm_id, NULL, NULL,
            'WORKFORCE_DETECTOR_OFFLINE',
            '{"metric": "detector_heartbeat_age_minutes", "operator": ">", "value": 5}'::jsonb,
            'CRITICAL', 'EDGE_DEVICE', true);
  END IF;
END $$;
