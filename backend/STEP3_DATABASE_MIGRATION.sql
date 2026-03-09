-- STEP-3 Database Migration — Partial Unique Indexes
-- Required for enforcing "one active instance per key" constraint at DB level
--
-- These indexes prevent multiple IN_PROGRESS instances for the same key:
-- - Scheduled: (farm_id, zone_id, activity_schedule_id)
-- - Unscheduled: (farm_id, zone_id, activity_type_id)
--
-- This ensures merge-on-start logic cannot create duplicates
-- and database enforces correctness even if application logic has bugs.

-- Scheduled activities: one IN_PROGRESS per (farm, zone, schedule)
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_scheduled_instance
ON activity_instance (farm_id, zone_id, activity_schedule_id)
WHERE status = 'IN_PROGRESS';

-- Unscheduled activities: one IN_PROGRESS per (farm, zone, activity_type)
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_unscheduled_instance
ON activity_instance (farm_id, zone_id, activity_type_id)
WHERE status = 'IN_PROGRESS'
  AND activity_schedule_id IS NULL;

-- Safety index: prevent cross-day instance reuse
-- Guarantees max 1 active instance per (farm, zone, activity_type, activity_date)
-- Even if aggregator crashes or logic has bugs
CREATE UNIQUE INDEX IF NOT EXISTS uniq_active_instance_per_day
ON activity_instance (
  farm_id,
  zone_id,
  activity_type_id,
  activity_date
)
WHERE status = 'IN_PROGRESS';

-- Notes:
-- - Partial indexes only include rows matching the WHERE clause
-- - This prevents duplicate IN_PROGRESS instances at DB level
-- - Critical for merge-on-start correctness
-- - uniq_active_instance_per_day ensures no cross-day merges
-- - Safe to run multiple times (IF NOT EXISTS)
