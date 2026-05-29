-- STEP-5 backfill for legacy rows created before session_classification split.
-- Safe to run multiple times.

-- 1) Carry old classification statuses into session_classification.
UPDATE activity_instance
SET session_classification = status
WHERE status IN ('ON_TIME', 'EARLY', 'LATE')
  AND (session_classification IS NULL OR session_classification = '');

-- 2) Normalize lifecycle status to ENDED for classified sessions.
UPDATE activity_instance
SET status = 'ENDED'
WHERE status IN ('ON_TIME', 'EARLY', 'LATE');

