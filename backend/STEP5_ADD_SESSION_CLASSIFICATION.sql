-- STEP-5 support column for resolver classification output.
-- Stores EARLY / ON_TIME / LATE / UNSCHEDULED classification without mutating lifecycle status.

ALTER TABLE activity_instance
ADD COLUMN IF NOT EXISTS session_classification TEXT;

