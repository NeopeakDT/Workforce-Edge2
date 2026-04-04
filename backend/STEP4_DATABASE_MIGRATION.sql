-- STEP-4 Database Migration
-- Aggregator scale/safety index for unprocessed event scans.
--
-- Why:
-- The STEP-4 worker repeatedly does:
--   WHERE activity_instance_id IS NULL
--   ORDER BY session_id, event_time, id
--   LIMIT <batch>
--
-- This partial composite index keeps that query fast as data grows.

CREATE INDEX IF NOT EXISTS idx_event_unprocessed
ON activity_detection_event (activity_instance_id, session_id, event_time, id)
WHERE activity_instance_id IS NULL;

