-- STEP-5 Add alert lifecycle/type model and per-channel delivery tracking
-- 2026-08-27
--
-- Why this file exists:
-- The alert system (backend/alerts/alert_evaluator.py, notification_dispatcher.py)
-- currently only understands activity alerts, has no notion of a condition's
-- lifecycle separate from delivery status, and can only record one delivery
-- channel per alert_log row even though alert_rule.channel is an array.
-- See Step A1 audit / A2 data-model / A3 design review (approved 2026-08-27)
-- for full rationale.
--
-- Pre-migration verification (2026-08-27): alert_log and alert_rule were
-- both confirmed EMPTY (0 rows) in the target database before this migration
-- ran. The lifecycle_state backfill-to-RESOLVED step below is therefore a
-- no-op against this database -- there was no genuinely active historical
-- alert at risk of being incorrectly marked RESOLVED. The backfill logic is
-- still included (rather than skipped) so this file remains correct and
-- reusable if ever applied against a database that already has alert_log
-- rows (e.g. a future environment).
--
-- What this does:
-- - Adds alert_type enum (ACTIVITY | POSTURE | CAMERA | EDGE_DEVICE) to both
--   alert_rule and alert_log, defaulted to ACTIVITY so all existing rows
--   backfill correctly (every rule/alert in the system today is activity-only).
-- - Adds alert_log.lifecycle_state (ACTIVE | RESOLVED), resolved_at,
--   recovery_notified_at, zone_id, device_id, dedup_key.
--   IMPORTANT: lifecycle_state is backfilled to 'RESOLVED' for every
--   pre-existing row before the 'ACTIVE' default is attached, specifically
--   so historical alerts are never miscounted as currently active once
--   dashboard/API queries move from status='SENT' to lifecycle_state='ACTIVE'.
-- - Adds a partial unique index (alert_rule_id, dedup_key) WHERE
--   lifecycle_state = 'ACTIVE' to prevent alert storms from a continuously
--   true condition. dedup_key is intentionally left NULLable here for
--   backward compatibility with the existing evaluator's INSERT statement --
--   Step B MUST populate a non-null, deterministic dedup_key on every new
--   alert occurrence it creates; this migration does not and cannot enforce
--   that (a NOT NULL constraint is deferred to a later step once Step B
--   ships).
-- - Adds alert_delivery for per-channel delivery tracking (one row per
--   channel attempt), since alert_rule.channel[] is multi-valued but
--   alert_log.channel is a single column. The channel column supports the
--   full alert_channel enum (APP/EMAIL/SMS/WHATSAPP) for forward
--   compatibility, but APP is the only channel actually implemented by any
--   current dispatcher code -- EMAIL/SMS/WHATSAPP delivery logic is explicit
--   future work (Step F+), not part of this migration.
-- - Enables RLS on alert_delivery with a farm-scoped read policy joined
--   through alert_log.farm_id (alert_delivery has no farm_id of its own),
--   mirroring the existing user_read_alert_log policy. No write policy is
--   added -- writes go through the backend's service-role DB connection,
--   consistent with how alert_log/alert_rule already work.
-- - Adds chk_activity_alert_has_instance: an ACTIVITY-typed alert_log row
--   must carry an activity_instance_id.
--
-- Idempotent: safe to re-run against a DB that already has these objects.

-- 1. alert_type enum -------------------------------------------------------
DO $$
BEGIN
    CREATE TYPE public.alert_type AS ENUM (
        'ACTIVITY',
        'POSTURE',
        'CAMERA',
        'EDGE_DEVICE'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- 2. alert_rule.alert_type --------------------------------------------------
ALTER TABLE public.alert_rule
  ADD COLUMN IF NOT EXISTS alert_type public.alert_type NOT NULL DEFAULT 'ACTIVITY';

-- 3. alert_log.alert_type ---------------------------------------------------
ALTER TABLE public.alert_log
  ADD COLUMN IF NOT EXISTS alert_type public.alert_type NOT NULL DEFAULT 'ACTIVITY';

-- 4-7. alert_log.lifecycle_state, with correct historical backfill ---------
ALTER TABLE public.alert_log
  ADD COLUMN IF NOT EXISTS lifecycle_state text;

-- Backfill EVERY pre-existing row to RESOLVED before attaching the ACTIVE
-- default -- prevents historical alerts from reading as currently active.
UPDATE public.alert_log
  SET lifecycle_state = 'RESOLVED'
  WHERE lifecycle_state IS NULL;

-- Keep historical rows internally consistent: a RESOLVED row should carry a
-- resolved_at. Backfill it from triggered_at for rows that predate this
-- column (there is no better historical signal available).
ALTER TABLE public.alert_log
  ADD COLUMN IF NOT EXISTS resolved_at timestamptz;

UPDATE public.alert_log
  SET resolved_at = triggered_at
  WHERE lifecycle_state = 'RESOLVED' AND resolved_at IS NULL;

ALTER TABLE public.alert_log
  ALTER COLUMN lifecycle_state SET DEFAULT 'ACTIVE';

ALTER TABLE public.alert_log
  ALTER COLUMN lifecycle_state SET NOT NULL;

DO $$
BEGIN
    ALTER TABLE public.alert_log
      ADD CONSTRAINT chk_alert_log_lifecycle_state
        CHECK (lifecycle_state IN ('ACTIVE', 'RESOLVED'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- 8. Remaining alert_log columns --------------------------------------------
ALTER TABLE public.alert_log
  ADD COLUMN IF NOT EXISTS recovery_notified_at timestamptz,
  ADD COLUMN IF NOT EXISTS zone_id   uuid REFERENCES public.farm_zone(id),
  ADD COLUMN IF NOT EXISTS device_id uuid REFERENCES public.edge_device(id),
  ADD COLUMN IF NOT EXISTS dedup_key text;
  -- dedup_key intentionally nullable -- see header note. Step B is
  -- responsible for always populating it on new inserts.

-- 9. alert_delivery table ---------------------------------------------------
CREATE TABLE IF NOT EXISTS public.alert_delivery (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_log_id uuid NOT NULL REFERENCES public.alert_log(id) ON DELETE CASCADE,
    channel      public.alert_channel NOT NULL,
    status       public.alert_status NOT NULL DEFAULT 'SENT',
    sent_at      timestamptz DEFAULT now(),
    error        text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- 10. Indexes -----------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_rule_dedup_active
  ON public.alert_log (alert_rule_id, dedup_key)
  WHERE lifecycle_state = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_alert_log_farm_lifecycle
  ON public.alert_log (farm_id, lifecycle_state);

CREATE INDEX IF NOT EXISTS idx_alert_log_type_farm_triggered
  ON public.alert_log (alert_type, farm_id, triggered_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_delivery_alert_log
  ON public.alert_delivery (alert_log_id);

CREATE INDEX IF NOT EXISTS idx_alert_log_pending_recovery
  ON public.alert_log (farm_id)
  WHERE lifecycle_state = 'RESOLVED' AND recovery_notified_at IS NULL;

-- 11. Activity-type guardrail -------------------------------------------
DO $$
BEGIN
    ALTER TABLE public.alert_log
      ADD CONSTRAINT chk_activity_alert_has_instance
        CHECK (alert_type <> 'ACTIVITY' OR activity_instance_id IS NOT NULL);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- 12. RLS for alert_delivery ----------------------------------------------
ALTER TABLE public.alert_delivery ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    CREATE POLICY user_read_alert_delivery ON public.alert_delivery
      FOR SELECT USING (
        (EXISTS (
          SELECT 1 FROM public.user_profile up
          WHERE up.id = auth.uid() AND up.role = 'ADMIN' AND up.is_active = true
        ))
        OR
        (EXISTS (
          SELECT 1
          FROM public.alert_log al
          JOIN public.user_farm_access ufa ON ufa.farm_id = al.farm_id
          WHERE al.id = alert_delivery.alert_log_id
            AND ufa.user_id = auth.uid()
        ))
      );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
