-- STEP-2 Database Migration
-- Required schema changes for idempotent, transactional event ingestion

-- 1. Add event_id and session_id columns to activity_detection_event
--    (if they don't exist)
ALTER TABLE activity_detection_event
ADD COLUMN IF NOT EXISTS event_id UUID,
ADD COLUMN IF NOT EXISTS session_id UUID,
ADD COLUMN IF NOT EXISTS merge_processed BOOLEAN DEFAULT FALSE;

-- 2. Add unique constraint on event_id for DB-level idempotency
--    This ensures duplicate events are rejected at DB level
ALTER TABLE activity_detection_event
ADD CONSTRAINT IF NOT EXISTS uq_event_id UNIQUE (event_id);

-- 3. Add session_id column to activity_instance
--    (if it doesn't exist)
ALTER TABLE activity_instance
ADD COLUMN IF NOT EXISTS session_id UUID;

-- 4. Add merged_into_instance_id for merge-on-start tracking
--    (instances are immutable - never delete, only mark as merged)
ALTER TABLE activity_instance
ADD COLUMN IF NOT EXISTS merged_into_instance_id UUID,
ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

-- 5. Create indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_activity_instance_session_id 
ON activity_instance(session_id);

CREATE INDEX IF NOT EXISTS idx_activity_detection_event_session_id 
ON activity_detection_event(session_id);

CREATE INDEX IF NOT EXISTS idx_activity_instance_merged_into 
ON activity_instance(merged_into_instance_id) 
WHERE merged_into_instance_id IS NOT NULL;

-- Notes:
-- - event_id is the primary idempotency mechanism (DB-enforced)
-- - session_id groups events from the same activity run
-- - All operations are transactional (single transaction per event)
-- - merged_into_instance_id tracks merged instances (immutable records)
-- - last_seen_at tracks liveness for stale-close