-- STEP-6 Database Migration
-- Requested index for faster unassigned-event scans ordered by event_time.

CREATE INDEX IF NOT EXISTS idx_event_unassigned
ON activity_detection_event (activity_instance_id, event_time);

