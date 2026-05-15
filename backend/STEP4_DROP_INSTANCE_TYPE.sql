-- STEP-4 — Drop instance_type; align lifecycle with simplified model
-- Run AFTER backfilling legacy statuses if needed (see optional UPDATEs below).
--
-- Optional (uncomment if you have legacy rows):
-- UPDATE activity_instance SET status = 'UNSCHEDULED' WHERE status = 'UNSCHEDULE';
-- UPDATE activity_instance SET status = 'UNSCHEDULED', activity_schedule_id = NULL
--   WHERE status IN ('NOISE', 'UNCLEAR') AND activity_schedule_id IS NULL;
-- UPDATE activity_instance SET status = 'ENDED'
--   WHERE status IN ('NOISE', 'UNCLEAR') AND activity_schedule_id IS NOT NULL;

ALTER TABLE activity_instance DROP CONSTRAINT IF EXISTS check_noise_schedule;

ALTER TABLE activity_instance DROP CONSTRAINT IF EXISTS check_valid_lifecycle;

ALTER TABLE activity_instance DROP COLUMN IF EXISTS instance_type;

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
      'UNSCHEDULED'
    )
    AND actual_end_at IS NOT NULL
  )
  OR (status = 'MISSED')
) NOT VALID;

-- After verifying data: ALTER TABLE activity_instance VALIDATE CONSTRAINT check_valid_lifecycle;
