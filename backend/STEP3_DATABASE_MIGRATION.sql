-- STEP-3 Database Migration — Partial Unique Indexes
-- Required for enforcing "one active instance per key" constraint at DB level
--
-- This index prevents multiple IN_PROGRESS scheduled instances for the same key:
-- - Scheduled: (farm_id, zone_id, activity_schedule_id)
--
-- This ensures merge-on-start logic cannot create duplicates
-- and database enforces correctness even if application logic has bugs.

-- Scheduled activities: one IN_PROGRESS per (farm, zone, schedule)
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_scheduled_instance
ON activity_instance (farm_id, zone_id, activity_schedule_id)
WHERE status = 'IN_PROGRESS';

-- Performance index for UNSCHEDULED instance lookups/recovery paths.
CREATE INDEX IF NOT EXISTS idx_unscheduled_lookup
ON activity_instance (farm_id, zone_id, activity_type_id, activity_date)
WHERE activity_schedule_id IS NULL;

-- Notes:
-- - Partial indexes only include rows matching the WHERE clause
-- - This prevents duplicate IN_PROGRESS scheduled instances at DB level
-- - Critical for merge-on-start correctness in scheduled flows
-- - Safe to run multiple times (IF NOT EXISTS)

-- ------------------------------------------------------------------------------------
-- STEP-3 hardening additions (lifecycle + uniqueness cleanup)
-- ------------------------------------------------------------------------------------

-- Drop legacy strict uniqueness patterns that block multiple completed sessions/day.
-- These names cover known/manual variants and are safe if absent.
ALTER TABLE activity_instance DROP CONSTRAINT IF EXISTS unique_schedule_per_day;
ALTER TABLE activity_instance DROP CONSTRAINT IF EXISTS uq_activity_instance_schedule_per_day;
ALTER TABLE activity_instance DROP CONSTRAINT IF EXISTS uq_instance_schedule_per_day;
ALTER TABLE activity_instance DROP CONSTRAINT IF EXISTS activity_instance_farm_id_zone_id_activity_type_id_activity_schedule_id_activity_date_key;

DROP INDEX IF EXISTS unique_schedule_per_day;
DROP INDEX IF EXISTS uq_activity_instance_schedule_per_day;
DROP INDEX IF EXISTS uq_instance_schedule_per_day;
DROP INDEX IF EXISTS uniq_instance_schedule_per_day;
DROP INDEX IF EXISTS uniq_active_instance_per_day;
DROP INDEX IF EXISTS uq_active_unscheduled_instance;

-- Session-level identity guard: one session_id maps to one instance.
CREATE UNIQUE INDEX IF NOT EXISTS uq_activity_instance_session_id
ON activity_instance (session_id)
WHERE session_id IS NOT NULL;

-- Lifecycle guard: ended rows cannot remain IN_PROGRESS.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'check_valid_lifecycle'
      AND conrelid = 'activity_instance'::regclass
  ) THEN
    ALTER TABLE activity_instance
    ADD CONSTRAINT check_valid_lifecycle
    CHECK (
      (
        status = 'IN_PROGRESS'
        AND actual_end_at IS NULL
      )
      OR (
        status IN (
          'ENDED',
          'EARLY',
          'ON_TIME',
          'LATE',
          'MISSED',
          'UNSCHEDULE',
          'UNSCHEDULED',
          'UNCLEAR',
          'NOISE'
        )
        AND actual_end_at IS NOT NULL
      )
    ) NOT VALID;
  END IF;
END $$;

-- Classification guard: NOISE must never be schedule-bound.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'check_noise_schedule'
      AND conrelid = 'activity_instance'::regclass
  ) THEN
    ALTER TABLE activity_instance
    ADD CONSTRAINT check_noise_schedule
    CHECK (
      NOT (instance_type = 'NOISE' AND activity_schedule_id IS NOT NULL)
    ) NOT VALID;
  END IF;
END $$;

-- Validate after backfilling/cleaning legacy bad rows:
-- ALTER TABLE activity_instance VALIDATE CONSTRAINT check_valid_lifecycle;
-- ALTER TABLE activity_instance VALIDATE CONSTRAINT check_noise_schedule;

-- MISSED idempotency guard: one row per (farm, schedule, local activity_date).
-- Use a full unique index so INSERT ... ON CONFLICT (farm_id, activity_schedule_id, activity_date)
-- works reliably without partial-index conflict-target matching nuances.
DROP INDEX IF EXISTS uq_missed_schedule_per_day;
CREATE UNIQUE INDEX IF NOT EXISTS uq_missed_schedule_per_day
ON activity_instance (farm_id, activity_schedule_id, activity_date);
