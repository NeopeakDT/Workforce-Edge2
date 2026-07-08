-- Posture observations: persist feeding-zone standing cows as a first-class metric.
-- herd_size = standing_count (REST) + feeding_count (FEEDING) + laying_count

ALTER TABLE posture_observation
ADD COLUMN IF NOT EXISTS feeding_count INTEGER NOT NULL DEFAULT 0;
