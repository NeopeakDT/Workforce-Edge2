-- STEP-1 Database Baseline
-- Reconstructed from a live schema-only pg_dump of the Supabase project on 2026-08-11.
--
-- Why this file exists:
-- STEP2 through STEP7 are all incremental ALTER TABLE / CREATE INDEX migrations that
-- assume the tables below already exist. No CREATE TABLE for them was ever checked into
-- this repo -- the base schema was created directly in Supabase (dashboard / ad-hoc SQL)
-- and never captured in git. This file closes that gap so the database is reconstructable
-- from source control, in the same STEP{N} sequence as everything that follows it.
--
-- Scope:
-- - Schema-only (no data), public schema only. Supabase-managed schemas (auth, storage,
--   realtime, extensions) are NOT included and are assumed already provisioned by Supabase
--   on any project this is run against.
-- - `user_profile.id` has a FOREIGN KEY to auth.users(id) -- that table is Supabase-managed
--   and out of scope here, but must exist first.
-- - gen_random_uuid() is used throughout and assumed available (built into Postgres 13+ /
--   provided by Supabase's default pgcrypto extension) -- no CREATE EXTENSION needed.
--
-- Ordering: types -> functions -> tables (+ owned sequences) -> column defaults ->
-- primary key / unique / check constraints -> indexes -> triggers -> foreign keys ->
-- row level security (enable + policies). This mirrors dependency order, not edit order --
-- keep it if you extend this file.
--
-- Idempotency: unlike STEP2+, this is a first-run baseline against an EMPTY public schema
-- (a fresh Supabase project). It is not written to be safely re-run against a database that
-- already has these objects (no IF NOT EXISTS on most statements, matching how they exist
-- live today). If you need to re-run it against a partially-provisioned DB, add guards as
-- needed rather than assuming this is idempotent like STEP2+.


CREATE SCHEMA IF NOT EXISTS public;

COMMENT ON SCHEMA public IS 'standard public schema';

-- ------------------------------------------------------------------------------------
-- ENUM TYPES
-- ------------------------------------------------------------------------------------
CREATE TYPE public.activity_final_status AS ENUM (
    'ON_TIME',
    'EARLY',
    'LATE',
    'MISSED',
    'UNSCHEDULED'
);

CREATE TYPE public.activity_frequency AS ENUM (
    'DAILY',
    'WEEKLY',
    'CUSTOM'
);

CREATE TYPE public.activity_source AS ENUM (
    'AI',
    'MANUAL',
    'AI_WITH_MANUAL_OVERRIDE',
    'SYSTEM'
);

CREATE TYPE public.activity_status AS ENUM (
    'IN_PROGRESS',
    'EARLY',
    'ON_TIME',
    'LATE',
    'MISSED',
    'CANCELLED',
    'START_PENDING',
    'END_PENDING',
    'MERGED',
    'UNSCHEDULED',
    'UNCLEAR',
    'NOISE',
    'ENDED'
);

CREATE TYPE public.activity_status_new AS ENUM (
    'IN_PROGRESS',
    'EARLY',
    'ON_TIME',
    'LATE',
    'MISSED',
    'ENDED',
    'UNSCHEDULED'
);

CREATE TYPE public.alert_channel AS ENUM (
    'APP',
    'EMAIL',
    'SMS',
    'WHATSAPP'
);

CREATE TYPE public.alert_severity AS ENUM (
    'INFO',
    'WARNING',
    'CRITICAL'
);

CREATE TYPE public.alert_status AS ENUM (
    'SENT',
    'FAILED',
    'ACKED'
);

CREATE TYPE public.detection_event_type AS ENUM (
    'START_CANDIDATE',
    'FRAME_AGGREGATE',
    'END_CANDIDATE'
);

CREATE TYPE public.execution_quality_type AS ENUM (
    'CLEAN',
    'FRAGMENTED',
    'NO_ACTIVITY'
);

CREATE TYPE public.posture_summary_type AS ENUM (
    'DAILY',
    'WEEKLY',
    'MONTHLY',
    'YEARLY'
);

CREATE TYPE public.task_status AS ENUM (
    'PENDING',
    'COMPLETED',
    'SKIPPED'
);


-- ------------------------------------------------------------------------------------
-- FUNCTIONS
-- ------------------------------------------------------------------------------------
CREATE FUNCTION public.check_user_role(allowed_roles text[]) RETURNS boolean
    LANGUAGE plpgsql STABLE SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
    user_role text;
    current_user_id uuid;
BEGIN
    -- Get the current authenticated user ID
    current_user_id := auth.uid();
    
    -- If no user ID, return false
    IF current_user_id IS NULL THEN
        RETURN false;
    END IF;
    
    -- Get the user's role from user_profile
    -- SECURITY DEFINER allows this to bypass RLS on user_profile
    -- Use explicit schema to avoid any search_path issues
    SELECT role INTO user_role
    FROM public.user_profile
    WHERE id = current_user_id;
    
    -- Debug: Log what we found (remove in production)
    -- RAISE NOTICE 'check_user_role: user_id=%, role=%', current_user_id, user_role;
    
    -- If no role found, return false
    IF user_role IS NULL THEN
        RETURN false;
    END IF;
    
    -- Check if role matches any of the allowed roles
    -- Use case-insensitive comparison to handle variations
    RETURN EXISTS (
        SELECT 1 
        FROM unnest(allowed_roles) AS allowed_role
        WHERE UPPER(TRIM(user_role)) = UPPER(TRIM(allowed_role))
    );
    
EXCEPTION
    WHEN OTHERS THEN
        -- If anything goes wrong, return false (fail secure)
        RETURN false;
END;
$$;

CREATE FUNCTION public.get_managed_farm_user_ids(p_user_id uuid) RETURNS SETOF uuid
    LANGUAGE sql STABLE SECURITY DEFINER
    AS $$
  -- If user is global ADMIN, return ALL user IDs
  SELECT id FROM public.user_profile
  WHERE EXISTS (
    SELECT 1 FROM public.user_profile 
    WHERE id = p_user_id AND role = 'ADMIN'
  )
  UNION
  -- Otherwise return users on farms where user is OWNER
  SELECT DISTINCT ufa.user_id 
  FROM public.user_farm_access ufa
  WHERE ufa.farm_id IN (
    SELECT farm_id FROM public.user_farm_access 
    WHERE user_id = p_user_id AND role = 'OWNER'
  );
$$;

CREATE FUNCTION public.get_posture_trend(p_farm_id uuid, p_summary_type text, p_limit integer DEFAULT 30) RETURNS TABLE(period_start timestamp with time zone, observation_count bigint, standing_percentage numeric, feeding_percentage numeric, laying_percentage numeric)
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
  SELECT
    date_trunc(
      CASE p_summary_type
        WHEN 'DAILY' THEN 'day'
        WHEN 'WEEKLY' THEN 'week'
        WHEN 'MONTHLY' THEN 'month'
        ELSE 'year'
      END,
      observed_at
    ) AS period_start,
    count(*) AS observation_count,
    round(100.0 * sum(standing_count) / NULLIF(sum(standing_count + feeding_count + laying_count), 0), 2) AS standing_percentage,
    round(100.0 * sum(feeding_count)  / NULLIF(sum(standing_count + feeding_count + laying_count), 0), 2) AS feeding_percentage,
    round(100.0 * sum(laying_count)   / NULLIF(sum(standing_count + feeding_count + laying_count), 0), 2) AS laying_percentage
  FROM public.posture_observation
  WHERE farm_id = p_farm_id
    AND (
      farm_id IN (SELECT public.get_user_managed_farm_ids(auth.uid()))
      OR farm_id IN (SELECT farm_id FROM public.user_farm_access WHERE user_id = auth.uid())
    )
  GROUP BY 1
  ORDER BY 1 DESC
  LIMIT p_limit
$$;

CREATE FUNCTION public.get_user_managed_farm_ids(p_user_id uuid) RETURNS SETOF uuid
    LANGUAGE sql STABLE SECURITY DEFINER
    AS $$
  -- If user is global ADMIN, return ALL farm IDs
  SELECT id FROM public.farm
  WHERE EXISTS (
    SELECT 1 FROM public.user_profile 
    WHERE id = p_user_id AND role = 'ADMIN'
  )
  UNION
  -- Otherwise return farms where user is OWNER
  SELECT farm_id 
  FROM public.user_farm_access 
  WHERE user_id = p_user_id AND role = 'OWNER';
$$;

CREATE FUNCTION public.grant_creator_farm_owner() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
BEGIN
  INSERT INTO public.user_farm_access (user_id, farm_id, role)
  VALUES (auth.uid(), NEW.id, 'OWNER')
  ON CONFLICT (user_id, farm_id) DO NOTHING;

  RETURN NEW;
END;
$$;

CREATE FUNCTION public.handle_new_user_profile() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
BEGIN
  -- Skip if this is an invited user (they haven't accepted yet)
  -- user_profile will be created by accept-invite Edge Function
  IF EXISTS (SELECT 1 FROM public.farm_invite WHERE email = LOWER(NEW.email) AND status = 'PENDING') THEN
    RETURN NEW;
  END IF;

  -- Only create user_profile for direct signups (not invites)
  INSERT INTO public.user_profile (id, role, is_active, timezone, metadata)
  VALUES (
    NEW.id,
    COALESCE(NEW.raw_user_meta_data->>'role', 'OWNER'),
    true,
    COALESCE(NEW.raw_user_meta_data->>'timezone', 'Asia/Kolkata'),
    jsonb_build_object(
      'role', COALESCE(NEW.raw_user_meta_data->>'role', 'OWNER'),
      'email', NEW.email,
      'phone', COALESCE(NEW.raw_user_meta_data->>'phone', '')
    )
  )
  ON CONFLICT (id) DO NOTHING;

  RETURN NEW;
END;
$$;

CREATE FUNCTION public.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE FUNCTION public.set_updated_at_activity_compliance() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

CREATE FUNCTION public.sync_user_profile_phone() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if new.phone is null then
    new.phone := new.metadata->>'phone';
  end if;
  return new;
end;
$$;


-- ------------------------------------------------------------------------------------
-- TABLES (+ owned sequences)
-- ------------------------------------------------------------------------------------
CREATE TABLE public.activity_compliance (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    activity_schedule_id uuid NOT NULL,
    activity_date date NOT NULL,
    final_status text NOT NULL,
    primary_instance_id uuid,
    total_sessions integer DEFAULT 0,
    early_sessions integer DEFAULT 0,
    on_time_sessions integer DEFAULT 0,
    late_sessions integer DEFAULT 0,
    first_activity_at timestamp with time zone,
    last_activity_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    activity_type_id uuid,
    CONSTRAINT activity_compliance_final_status_check CHECK ((final_status = ANY (ARRAY['ON_TIME'::text, 'EARLY'::text, 'LATE'::text, 'MISSED'::text, 'UNSCHEDULED'::text])))
);

CREATE TABLE public.activity_detection_event (
    id bigint NOT NULL,
    farm_id uuid NOT NULL,
    activity_type_id smallint NOT NULL,
    activity_instance_id uuid,
    device_id uuid,
    camera_id uuid,
    event_type public.detection_event_type NOT NULL,
    event_time timestamp with time zone NOT NULL,
    ai_confidence numeric(5,2),
    payload jsonb,
    created_at timestamp with time zone DEFAULT now(),
    zone_id uuid,
    event_id uuid NOT NULL,
    session_id uuid NOT NULL
);

CREATE SEQUENCE public.activity_detection_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.activity_detection_event_id_seq OWNED BY public.activity_detection_event.id;

CREATE TABLE public.activity_instance (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    activity_type_id smallint NOT NULL,
    activity_schedule_id uuid,
    activity_date date NOT NULL,
    actual_start_at timestamp with time zone,
    actual_end_at timestamp with time zone,
    actual_duration_sec integer,
    started_offset_min integer,
    ended_offset_min integer,
    within_ideal_window boolean,
    status public.activity_status,
    source public.activity_source DEFAULT 'AI'::public.activity_source,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    zone_id uuid,
    last_seen_at timestamp with time zone,
    session_id uuid,
    session_classification text,
    CONSTRAINT activity_duration_non_negative CHECK (((actual_duration_sec IS NULL) OR (actual_duration_sec >= 0))),
    CONSTRAINT chk_activity_instance_session_classification CHECK (((session_classification IS NULL) OR (session_classification = ANY (ARRAY['EARLY'::text, 'ON_TIME'::text, 'LATE'::text, 'MISSED'::text, 'UNSCHEDULED'::text])))),
    CONSTRAINT noise_has_no_schedule CHECK ((NOT ((status = 'NOISE'::public.activity_status) AND (activity_schedule_id IS NOT NULL))))
);

CREATE TABLE public.activity_schedule (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    activity_type_id smallint NOT NULL,
    label text NOT NULL,
    ideal_start_time time without time zone NOT NULL,
    ideal_end_time time without time zone,
    frequency public.activity_frequency DEFAULT 'DAILY'::public.activity_frequency,
    days_of_week integer[],
    tolerance_early_min integer DEFAULT 15,
    tolerance_late_min integer DEFAULT 15,
    is_active boolean DEFAULT true,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now(),
    activity_type_id_uuid uuid
);

CREATE TABLE public.activity_type (
    id smallint NOT NULL,
    code text NOT NULL,
    display_name text NOT NULL,
    description text,
    icon text,
    color text,
    created_at timestamp with time zone DEFAULT now(),
    id_uuid uuid DEFAULT gen_random_uuid()
);

CREATE SEQUENCE public.activity_type_id_seq
    AS smallint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.activity_type_id_seq OWNED BY public.activity_type.id;

CREATE TABLE public.alert_log (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid,
    alert_rule_id uuid,
    activity_instance_id uuid,
    triggered_at timestamp with time zone DEFAULT now(),
    channel public.alert_channel,
    status public.alert_status DEFAULT 'SENT'::public.alert_status,
    message text,
    details jsonb
);

CREATE TABLE public.alert_rule (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid,
    activity_type_id smallint,
    activity_schedule_id uuid,
    name text NOT NULL,
    condition jsonb NOT NULL,
    severity public.alert_severity DEFAULT 'CRITICAL'::public.alert_severity,
    channel public.alert_channel[] DEFAULT ARRAY['APP'::public.alert_channel],
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now()
);

CREATE TABLE public.app_settings (
    key text NOT NULL,
    value jsonb,
    description text,
    updated_at timestamp with time zone DEFAULT now()
);

CREATE TABLE public.audit_log (
    id bigint NOT NULL,
    user_id uuid,
    action text NOT NULL,
    entity_type text,
    entity_id uuid,
    details jsonb,
    ip_address text,
    created_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE public.audit_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.audit_log_id_seq OWNED BY public.audit_log.id;

CREATE TABLE public.camera_activity_zone (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    camera_id uuid NOT NULL,
    activity_type_id smallint NOT NULL,
    zone_id uuid NOT NULL,
    roi jsonb,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.camera_stream_config (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    camera_id uuid NOT NULL,
    resolution text,
    fps_target integer DEFAULT 5,
    roi jsonb,
    motion_sensitivity integer DEFAULT 50,
    created_at timestamp with time zone DEFAULT now()
);

CREATE TABLE public.dashboard_config (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    farm_id uuid,
    config jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

CREATE TABLE public.device_model_assignment (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    device_id uuid NOT NULL,
    ml_model_version_id uuid NOT NULL,
    assigned_at timestamp with time zone DEFAULT now(),
    effective_from timestamp with time zone DEFAULT now(),
    notes text
);

CREATE TABLE public.edge_device (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    name text NOT NULL,
    code text NOT NULL,
    api_key_hash text NOT NULL,
    timezone text DEFAULT 'Asia/Kolkata'::text,
    metadata jsonb,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    last_seen_at timestamp with time zone
);

CREATE TABLE public.edge_device_heartbeat (
    id bigint NOT NULL,
    device_id uuid NOT NULL,
    heartbeat_time timestamp with time zone DEFAULT now(),
    cpu_temp_c numeric(6,2),
    gpu_temp_c numeric(6,2),
    disk_usage_pct numeric(5,2),
    memory_usage_pct numeric(5,2),
    notes text,
    created_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE public.edge_device_heartbeat_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.edge_device_heartbeat_id_seq OWNED BY public.edge_device_heartbeat.id;

CREATE TABLE public.farm (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    code text NOT NULL,
    location text,
    timezone text DEFAULT 'Asia/Kolkata'::text NOT NULL,
    is_active boolean DEFAULT true,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now()
);

CREATE TABLE public.farm_camera (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    name text NOT NULL,
    code text,
    rtsp_url text,
    nvr_channel text,
    is_active boolean DEFAULT true,
    "position" jsonb,
    created_at timestamp with time zone DEFAULT now(),
    stream_type text,
    nvr_rtsp_base text
);

CREATE TABLE public.farm_invite (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    email text NOT NULL,
    role text NOT NULL,
    invited_by uuid NOT NULL,
    token_hash text NOT NULL,
    status text DEFAULT 'PENDING'::text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    accepted_at timestamp with time zone,
    accepted_by uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    email_norm text,
    CONSTRAINT farm_invite_role_check CHECK ((role = ANY (ARRAY['OWNER'::text, 'USER'::text]))),
    CONSTRAINT farm_invite_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'ACCEPTED'::text, 'REVOKED'::text, 'EXPIRED'::text])))
);

CREATE TABLE public.farm_zone (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    name text NOT NULL,
    type text DEFAULT 'OTHER'::text,
    description text,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT farm_zone_type_check CHECK ((type = ANY (ARRAY['FEEDING_ALLEY'::text, 'SHED'::text, 'MILKING_PARLOUR'::text, 'SCRAPPING_SHED'::text, 'OTHER'::text])))
);

CREATE TABLE public.ml_model_version (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    activity_type_id smallint,
    storage_path text,
    checksum text,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now(),
    is_active boolean DEFAULT true
);

CREATE TABLE public.posture_observation (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    zone_id uuid NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    standing_count integer DEFAULT 0 NOT NULL,
    laying_count integer DEFAULT 0 NOT NULL,
    standing_percentage numeric(5,2),
    laying_percentage numeric(5,2),
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now(),
    device_id uuid,
    activity_type_id smallint,
    feeding_count integer DEFAULT 0 NOT NULL
);

CREATE TABLE public.posture_summary (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    zone_id uuid,
    summary_type public.posture_summary_type NOT NULL,
    period_start timestamp with time zone NOT NULL,
    period_end timestamp with time zone NOT NULL,
    avg_standing_count numeric(6,2) NOT NULL,
    avg_feeding_count numeric(6,2) NOT NULL,
    avg_laying_count numeric(6,2) NOT NULL,
    avg_standing_percentage numeric(5,2) NOT NULL,
    avg_feeding_percentage numeric(5,2) NOT NULL,
    avg_laying_percentage numeric(5,2) NOT NULL,
    max_standing_count integer NOT NULL,
    max_feeding_count integer NOT NULL,
    max_laying_count integer NOT NULL,
    min_standing_count integer NOT NULL,
    min_feeding_count integer NOT NULL,
    min_laying_count integer NOT NULL,
    source_observation_count integer NOT NULL,
    expected_observations integer NOT NULL,
    received_observations integer NOT NULL,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.task_log (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    task_schedule_id uuid,
    farm_id uuid NOT NULL,
    scheduled_for timestamp with time zone,
    completed_at timestamp with time zone,
    completed_by_user_id uuid,
    status public.task_status DEFAULT 'PENDING'::public.task_status,
    notes text,
    created_at timestamp with time zone DEFAULT now()
);

CREATE TABLE public.task_schedule (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    farm_id uuid NOT NULL,
    name text NOT NULL,
    description text,
    activity_type_id smallint,
    schedule_spec text,
    is_active boolean DEFAULT true,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now()
);

CREATE TABLE public.user_farm_access (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    farm_id uuid NOT NULL,
    role text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT user_farm_access_role_chk CHECK ((role = ANY (ARRAY['OWNER'::text, 'USER'::text])))
);

CREATE TABLE public.user_profile (
    id uuid NOT NULL,
    full_name text,
    role text DEFAULT 'USER'::text,
    phone text,
    timezone text DEFAULT 'Asia/Kolkata'::text,
    is_active boolean DEFAULT true,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT user_profile_role_check CHECK ((role = ANY (ARRAY['ADMIN'::text, 'OWNER'::text, 'USER'::text])))
);


-- ------------------------------------------------------------------------------------
-- SEQUENCE-BACKED COLUMN DEFAULTS
-- ------------------------------------------------------------------------------------
ALTER TABLE ONLY public.activity_detection_event ALTER COLUMN id SET DEFAULT nextval('public.activity_detection_event_id_seq'::regclass);

ALTER TABLE ONLY public.activity_type ALTER COLUMN id SET DEFAULT nextval('public.activity_type_id_seq'::regclass);

ALTER TABLE ONLY public.audit_log ALTER COLUMN id SET DEFAULT nextval('public.audit_log_id_seq'::regclass);

ALTER TABLE ONLY public.edge_device_heartbeat ALTER COLUMN id SET DEFAULT nextval('public.edge_device_heartbeat_id_seq'::regclass);


-- ------------------------------------------------------------------------------------
-- PRIMARY KEY / UNIQUE / CHECK CONSTRAINTS
-- ------------------------------------------------------------------------------------
ALTER TABLE ONLY public.activity_compliance
    ADD CONSTRAINT activity_compliance_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.activity_detection_event
    ADD CONSTRAINT activity_detection_event_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.activity_instance
    ADD CONSTRAINT activity_instance_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.activity_schedule
    ADD CONSTRAINT activity_schedule_farm_id_activity_type_id_label_key UNIQUE (farm_id, activity_type_id, label);

ALTER TABLE ONLY public.activity_schedule
    ADD CONSTRAINT activity_schedule_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.activity_type
    ADD CONSTRAINT activity_type_code_key UNIQUE (code);

ALTER TABLE ONLY public.activity_type
    ADD CONSTRAINT activity_type_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.alert_log
    ADD CONSTRAINT alert_log_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.alert_rule
    ADD CONSTRAINT alert_rule_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.app_settings
    ADD CONSTRAINT app_settings_pkey PRIMARY KEY (key);

ALTER TABLE ONLY public.audit_log
    ADD CONSTRAINT audit_log_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.camera_activity_zone
    ADD CONSTRAINT camera_activity_zone_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.camera_stream_config
    ADD CONSTRAINT camera_stream_config_camera_id_unique UNIQUE (camera_id);

ALTER TABLE ONLY public.camera_stream_config
    ADD CONSTRAINT camera_stream_config_pkey PRIMARY KEY (id);

ALTER TABLE public.activity_instance
    ADD CONSTRAINT check_valid_lifecycle CHECK ((NOT ((actual_end_at IS NOT NULL) AND (status = 'IN_PROGRESS'::public.activity_status)))) NOT VALID;

ALTER TABLE ONLY public.dashboard_config
    ADD CONSTRAINT dashboard_config_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.device_model_assignment
    ADD CONSTRAINT device_model_assignment_device_id_ml_model_version_id_effec_key UNIQUE (device_id, ml_model_version_id, effective_from);

ALTER TABLE ONLY public.device_model_assignment
    ADD CONSTRAINT device_model_assignment_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.edge_device
    ADD CONSTRAINT edge_device_code_key UNIQUE (code);

ALTER TABLE ONLY public.edge_device
    ADD CONSTRAINT edge_device_code_unique UNIQUE (code);

ALTER TABLE ONLY public.edge_device_heartbeat
    ADD CONSTRAINT edge_device_heartbeat_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.edge_device
    ADD CONSTRAINT edge_device_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.farm_camera
    ADD CONSTRAINT farm_camera_code_key UNIQUE (code);

ALTER TABLE ONLY public.farm_camera
    ADD CONSTRAINT farm_camera_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.farm
    ADD CONSTRAINT farm_code_key UNIQUE (code);

ALTER TABLE ONLY public.farm_invite
    ADD CONSTRAINT farm_invite_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.farm
    ADD CONSTRAINT farm_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.farm_zone
    ADD CONSTRAINT farm_zone_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.ml_model_version
    ADD CONSTRAINT ml_model_version_name_version_key UNIQUE (name, version);

ALTER TABLE ONLY public.ml_model_version
    ADD CONSTRAINT ml_model_version_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.posture_observation
    ADD CONSTRAINT posture_observation_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.posture_summary
    ADD CONSTRAINT posture_summary_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.task_log
    ADD CONSTRAINT task_log_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.task_schedule
    ADD CONSTRAINT task_schedule_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.activity_compliance
    ADD CONSTRAINT uq_activity_compliance UNIQUE (farm_id, activity_schedule_id, activity_date);

ALTER TABLE ONLY public.posture_summary
    ADD CONSTRAINT uq_posture_summary UNIQUE (farm_id, zone_id, summary_type, period_start);

ALTER TABLE ONLY public.user_farm_access
    ADD CONSTRAINT user_farm_access_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.user_farm_access
    ADD CONSTRAINT user_farm_access_unique UNIQUE (user_id, farm_id);

ALTER TABLE ONLY public.user_profile
    ADD CONSTRAINT user_profile_pkey PRIMARY KEY (id);


-- ------------------------------------------------------------------------------------
-- INDEXES
-- ------------------------------------------------------------------------------------
CREATE INDEX farm_invite_token_hash_idx ON public.farm_invite USING btree (token_hash);

CREATE UNIQUE INDEX farm_invite_unique_pending ON public.farm_invite USING btree (farm_id, email_norm) WHERE (status = 'PENDING'::text);

CREATE INDEX idx_ac_farm_date ON public.activity_compliance USING btree (farm_id, activity_date);

CREATE INDEX idx_ac_primary_instance ON public.activity_compliance USING btree (primary_instance_id);

CREATE INDEX idx_ac_schedule ON public.activity_compliance USING btree (activity_schedule_id);

CREATE INDEX idx_activity_instance_active ON public.activity_instance USING btree (farm_id, zone_id, activity_type_id, activity_date) WHERE (status = 'IN_PROGRESS'::public.activity_status);

CREATE INDEX idx_activity_instance_schedule_day ON public.activity_instance USING btree (farm_id, activity_schedule_id, activity_date);

CREATE INDEX idx_activity_instance_session ON public.activity_instance USING btree (session_id);

CREATE INDEX idx_activity_instance_session_id ON public.activity_instance USING btree (session_id);

CREATE INDEX idx_activity_instance_status ON public.activity_instance USING btree (status);

CREATE INDEX idx_ade_unlinked ON public.activity_detection_event USING btree (activity_instance_id, event_time) WHERE (activity_instance_id IS NULL);

CREATE INDEX idx_ai_recovery_lookup ON public.activity_instance USING btree (farm_id, activity_type_id, activity_date, status, actual_end_at);

CREATE INDEX idx_ai_status ON public.activity_instance USING btree (status);

CREATE INDEX idx_ai_zone ON public.activity_instance USING btree (zone_id);

CREATE INDEX idx_alert_log_farm_triggered ON public.alert_log USING btree (farm_id, triggered_at DESC);

CREATE INDEX idx_audit_log_entity ON public.audit_log USING btree (entity_type, entity_id);

CREATE INDEX idx_audit_log_time ON public.audit_log USING btree (created_at);

CREATE INDEX idx_camera_activity_zone_lookup ON public.camera_activity_zone USING btree (farm_id, camera_id, activity_type_id) WHERE (is_active = true);

CREATE INDEX idx_camera_zone_lookup ON public.camera_activity_zone USING btree (farm_id, camera_id, activity_type_id, is_active);

CREATE INDEX idx_dashboard_config_user ON public.dashboard_config USING btree (user_id);

CREATE INDEX idx_device_model_assignment_device ON public.device_model_assignment USING btree (device_id);

CREATE INDEX idx_device_model_assignment_effective ON public.device_model_assignment USING btree (device_id, effective_from DESC);

CREATE INDEX idx_edge_device_api_key_hash ON public.edge_device USING btree (api_key_hash);

CREATE INDEX idx_event_farm_type_time ON public.activity_detection_event USING btree (farm_id, activity_type_id, event_time);

CREATE INDEX idx_event_farm_type_zone_time ON public.activity_detection_event USING btree (farm_id, activity_type_id, zone_id, event_time);

CREATE INDEX idx_event_unprocessed ON public.activity_detection_event USING btree (activity_instance_id, event_time);

CREATE INDEX idx_farm_camera_farm ON public.farm_camera USING btree (farm_id);

CREATE INDEX idx_farm_zone_farm ON public.farm_zone USING btree (farm_id);

CREATE INDEX idx_instance_active_lookup ON public.activity_instance USING btree (farm_id, zone_id, activity_type_id, status);

CREATE INDEX idx_instance_farm_type_zone_date ON public.activity_instance USING btree (farm_id, activity_type_id, zone_id, activity_date);

CREATE INDEX idx_instance_lookup ON public.activity_instance USING btree (farm_id, zone_id, activity_type_id, status);

CREATE INDEX idx_instance_recent_end ON public.activity_instance USING btree (farm_id, zone_id, activity_type_id, actual_end_at);

CREATE INDEX idx_instance_stale_lookup ON public.activity_instance USING btree (status, last_seen_at);

CREATE INDEX idx_ml_model_version_active ON public.ml_model_version USING btree (is_active);

CREATE INDEX idx_posture_observation_device_time ON public.posture_observation USING btree (device_id, observed_at);

CREATE INDEX idx_posture_observation_farm_time ON public.posture_observation USING btree (farm_id, observed_at);

CREATE INDEX idx_posture_observation_zone_time ON public.posture_observation USING btree (zone_id, observed_at);

CREATE INDEX idx_posture_summary_farm ON public.posture_summary USING btree (farm_id);

CREATE INDEX idx_posture_summary_farm_type_period ON public.posture_summary USING btree (farm_id, summary_type, period_start);

CREATE INDEX idx_posture_summary_period ON public.posture_summary USING btree (period_start);

CREATE INDEX idx_posture_summary_type ON public.posture_summary USING btree (summary_type);

CREATE INDEX idx_posture_summary_zone ON public.posture_summary USING btree (zone_id);

CREATE INDEX idx_task_log_farm_time ON public.task_log USING btree (farm_id, scheduled_for);

CREATE INDEX idx_task_log_schedule_time ON public.task_log USING btree (task_schedule_id, scheduled_for);

CREATE INDEX idx_task_schedule_farm ON public.task_schedule USING btree (farm_id);

CREATE INDEX idx_unscheduled_instances ON public.activity_instance USING btree (farm_id, zone_id, activity_type_id, activity_date) WHERE (activity_schedule_id IS NULL);

CREATE INDEX idx_unscheduled_lookup ON public.activity_instance USING btree (farm_id, zone_id, activity_type_id, activity_date) WHERE (activity_schedule_id IS NULL);

CREATE UNIQUE INDEX uniq_active_instance ON public.activity_instance USING btree (farm_id, zone_id, activity_type_id, activity_date) WHERE (status = 'IN_PROGRESS'::public.activity_status);

CREATE UNIQUE INDEX uniq_active_unscheduled_activity_per_zone ON public.activity_instance USING btree (farm_id, zone_id, activity_type_id) WHERE ((status = 'IN_PROGRESS'::public.activity_status) AND (actual_end_at IS NULL) AND (activity_schedule_id IS NULL));

CREATE UNIQUE INDEX uq_activity_detection_event_event_id ON public.activity_detection_event USING btree (event_id);

CREATE UNIQUE INDEX uq_activity_instance_session_id ON public.activity_instance USING btree (session_id) WHERE (session_id IS NOT NULL);

CREATE UNIQUE INDEX uq_alert_rule_activity ON public.alert_log USING btree (alert_rule_id, activity_instance_id);

CREATE UNIQUE INDEX uq_camera_activity_zone ON public.camera_activity_zone USING btree (camera_id, activity_type_id, zone_id) WHERE (is_active = true);

CREATE UNIQUE INDEX uq_edge_device_code ON public.edge_device USING btree (code);

CREATE UNIQUE INDEX uq_missed_schedule_per_day ON public.activity_instance USING btree (farm_id, activity_schedule_id, activity_date);


-- ------------------------------------------------------------------------------------
-- TRIGGERS
-- ------------------------------------------------------------------------------------
CREATE TRIGGER trg_ac_updated_at BEFORE UPDATE ON public.activity_compliance FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_activity_compliance();

CREATE TRIGGER trg_activity_instance_updated_at BEFORE UPDATE ON public.activity_instance FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_camera_activity_zone_updated_at BEFORE UPDATE ON public.camera_activity_zone FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_dashboard_config_updated_at BEFORE UPDATE ON public.dashboard_config FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_grant_creator_farm_owner AFTER INSERT ON public.farm FOR EACH ROW EXECUTE FUNCTION public.grant_creator_farm_owner();

CREATE TRIGGER trg_sync_user_profile_phone BEFORE INSERT OR UPDATE ON public.user_profile FOR EACH ROW EXECUTE FUNCTION public.sync_user_profile_phone();


-- ------------------------------------------------------------------------------------
-- FOREIGN KEY CONSTRAINTS
-- ------------------------------------------------------------------------------------
ALTER TABLE ONLY public.activity_compliance
    ADD CONSTRAINT activity_compliance_primary_instance_id_fkey FOREIGN KEY (primary_instance_id) REFERENCES public.activity_instance(id);

ALTER TABLE ONLY public.activity_detection_event
    ADD CONSTRAINT activity_detection_event_activity_instance_id_fkey FOREIGN KEY (activity_instance_id) REFERENCES public.activity_instance(id);

ALTER TABLE ONLY public.activity_detection_event
    ADD CONSTRAINT activity_detection_event_activity_type_id_fkey FOREIGN KEY (activity_type_id) REFERENCES public.activity_type(id);

ALTER TABLE ONLY public.activity_detection_event
    ADD CONSTRAINT activity_detection_event_camera_id_fkey FOREIGN KEY (camera_id) REFERENCES public.farm_camera(id);

ALTER TABLE ONLY public.activity_detection_event
    ADD CONSTRAINT activity_detection_event_device_id_fkey FOREIGN KEY (device_id) REFERENCES public.edge_device(id);

ALTER TABLE ONLY public.activity_detection_event
    ADD CONSTRAINT activity_detection_event_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id);

ALTER TABLE ONLY public.activity_detection_event
    ADD CONSTRAINT activity_detection_event_zone_id_fkey FOREIGN KEY (zone_id) REFERENCES public.farm_zone(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.activity_instance
    ADD CONSTRAINT activity_instance_activity_schedule_id_fkey FOREIGN KEY (activity_schedule_id) REFERENCES public.activity_schedule(id);

ALTER TABLE ONLY public.activity_instance
    ADD CONSTRAINT activity_instance_activity_type_id_fkey FOREIGN KEY (activity_type_id) REFERENCES public.activity_type(id);

ALTER TABLE ONLY public.activity_instance
    ADD CONSTRAINT activity_instance_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.activity_instance
    ADD CONSTRAINT activity_instance_zone_id_fkey FOREIGN KEY (zone_id) REFERENCES public.farm_zone(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.activity_schedule
    ADD CONSTRAINT activity_schedule_activity_type_id_fkey FOREIGN KEY (activity_type_id) REFERENCES public.activity_type(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.activity_schedule
    ADD CONSTRAINT activity_schedule_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.alert_log
    ADD CONSTRAINT alert_log_activity_instance_id_fkey FOREIGN KEY (activity_instance_id) REFERENCES public.activity_instance(id);

ALTER TABLE ONLY public.alert_log
    ADD CONSTRAINT alert_log_alert_rule_id_fkey FOREIGN KEY (alert_rule_id) REFERENCES public.alert_rule(id);

ALTER TABLE ONLY public.alert_log
    ADD CONSTRAINT alert_log_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id);

ALTER TABLE ONLY public.alert_rule
    ADD CONSTRAINT alert_rule_activity_schedule_id_fkey FOREIGN KEY (activity_schedule_id) REFERENCES public.activity_schedule(id);

ALTER TABLE ONLY public.alert_rule
    ADD CONSTRAINT alert_rule_activity_type_id_fkey FOREIGN KEY (activity_type_id) REFERENCES public.activity_type(id);

ALTER TABLE ONLY public.alert_rule
    ADD CONSTRAINT alert_rule_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id);

ALTER TABLE ONLY public.audit_log
    ADD CONSTRAINT audit_log_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.user_profile(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.camera_activity_zone
    ADD CONSTRAINT camera_activity_zone_activity_type_id_fkey FOREIGN KEY (activity_type_id) REFERENCES public.activity_type(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.camera_activity_zone
    ADD CONSTRAINT camera_activity_zone_camera_id_fkey FOREIGN KEY (camera_id) REFERENCES public.farm_camera(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.camera_activity_zone
    ADD CONSTRAINT camera_activity_zone_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.camera_activity_zone
    ADD CONSTRAINT camera_activity_zone_zone_id_fkey FOREIGN KEY (zone_id) REFERENCES public.farm_zone(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.camera_stream_config
    ADD CONSTRAINT camera_stream_config_camera_id_fkey FOREIGN KEY (camera_id) REFERENCES public.farm_camera(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.dashboard_config
    ADD CONSTRAINT dashboard_config_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.dashboard_config
    ADD CONSTRAINT dashboard_config_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.user_profile(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.device_model_assignment
    ADD CONSTRAINT device_model_assignment_device_id_fkey FOREIGN KEY (device_id) REFERENCES public.edge_device(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.device_model_assignment
    ADD CONSTRAINT device_model_assignment_ml_model_version_id_fkey FOREIGN KEY (ml_model_version_id) REFERENCES public.ml_model_version(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.edge_device
    ADD CONSTRAINT edge_device_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.edge_device_heartbeat
    ADD CONSTRAINT edge_device_heartbeat_device_id_fkey FOREIGN KEY (device_id) REFERENCES public.edge_device(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.farm_camera
    ADD CONSTRAINT farm_camera_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.farm_invite
    ADD CONSTRAINT farm_invite_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.farm_zone
    ADD CONSTRAINT farm_zone_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.activity_compliance
    ADD CONSTRAINT fk_compliance_schedule FOREIGN KEY (activity_schedule_id) REFERENCES public.activity_schedule(id);

ALTER TABLE ONLY public.posture_observation
    ADD CONSTRAINT fk_posture_activity_type FOREIGN KEY (activity_type_id) REFERENCES public.activity_type(id);

ALTER TABLE ONLY public.ml_model_version
    ADD CONSTRAINT ml_model_version_activity_type_id_fkey FOREIGN KEY (activity_type_id) REFERENCES public.activity_type(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.posture_observation
    ADD CONSTRAINT posture_observation_device_id_fkey FOREIGN KEY (device_id) REFERENCES public.edge_device(id);

ALTER TABLE ONLY public.posture_observation
    ADD CONSTRAINT posture_observation_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id);

ALTER TABLE ONLY public.posture_observation
    ADD CONSTRAINT posture_observation_zone_id_fkey FOREIGN KEY (zone_id) REFERENCES public.farm_zone(id);

ALTER TABLE ONLY public.posture_summary
    ADD CONSTRAINT posture_summary_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id);

ALTER TABLE ONLY public.posture_summary
    ADD CONSTRAINT posture_summary_zone_id_fkey FOREIGN KEY (zone_id) REFERENCES public.farm_zone(id);

ALTER TABLE ONLY public.task_log
    ADD CONSTRAINT task_log_completed_by_user_id_fkey FOREIGN KEY (completed_by_user_id) REFERENCES public.user_profile(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.task_log
    ADD CONSTRAINT task_log_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.task_log
    ADD CONSTRAINT task_log_task_schedule_id_fkey FOREIGN KEY (task_schedule_id) REFERENCES public.task_schedule(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.task_schedule
    ADD CONSTRAINT task_schedule_activity_type_id_fkey FOREIGN KEY (activity_type_id) REFERENCES public.activity_type(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.task_schedule
    ADD CONSTRAINT task_schedule_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.user_farm_access
    ADD CONSTRAINT user_farm_access_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farm(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.user_farm_access
    ADD CONSTRAINT user_farm_access_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.user_profile(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.user_profile
    ADD CONSTRAINT user_profile_id_fkey FOREIGN KEY (id) REFERENCES auth.users(id) ON DELETE CASCADE;


-- ------------------------------------------------------------------------------------
-- ROW LEVEL SECURITY (RLS) - policies and per-table enablement
-- Note: a handful of policies below are functionally redundant with others on the same
-- table (e.g. activity_schedule has overlapping select/insert/update policies from what
-- look like two different iterations). Captured as-is from the live DB; not resolved here.
-- ------------------------------------------------------------------------------------
CREATE POLICY "Allow authenticated read activity_instance" ON public.activity_instance FOR SELECT TO authenticated USING (true);

CREATE POLICY "Owners can view farm invites" ON public.farm_invite FOR SELECT USING ((farm_id IN ( SELECT public.get_user_managed_farm_ids(auth.uid()) AS get_user_managed_farm_ids)));

CREATE POLICY "Owners can view farm members" ON public.user_farm_access FOR SELECT USING (((user_id = auth.uid()) OR (farm_id IN ( SELECT public.get_user_managed_farm_ids(auth.uid()) AS get_user_managed_farm_ids))));

CREATE POLICY "Owners can view profiles of farm members" ON public.user_profile FOR SELECT USING (((id = auth.uid()) OR (id IN ( SELECT public.get_managed_farm_user_ids(auth.uid()) AS get_managed_farm_user_ids))));

CREATE POLICY "Users can read their own profile" ON public.user_profile FOR SELECT TO authenticated USING ((id = auth.uid()));

ALTER TABLE public.activity_compliance ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.activity_detection_event ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.activity_instance ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.activity_schedule ENABLE ROW LEVEL SECURITY;

CREATE POLICY activity_schedule_delete_for_admin_owner ON public.activity_schedule FOR DELETE TO authenticated USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND ((up.role = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])) OR ((up.metadata ->> 'role'::text) = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])))))) AND (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id))))));

CREATE POLICY activity_schedule_insert_for_admin_owner ON public.activity_schedule FOR INSERT TO authenticated WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND ((up.role = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])) OR ((up.metadata ->> 'role'::text) = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])))))) AND (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id))))));

CREATE POLICY activity_schedule_select_for_farm_members ON public.activity_schedule FOR SELECT TO authenticated USING ((EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id)))));

CREATE POLICY activity_schedule_select_members_or_admin ON public.activity_schedule FOR SELECT USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id))))));

CREATE POLICY activity_schedule_update_for_admin_owner ON public.activity_schedule FOR UPDATE TO authenticated USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND ((up.role = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])) OR ((up.metadata ->> 'role'::text) = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])))))) AND (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND ((up.role = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])) OR ((up.metadata ->> 'role'::text) = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])))))) AND (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id))))));

CREATE POLICY activity_schedule_write_owner_or_admin_del ON public.activity_schedule FOR DELETE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY activity_schedule_write_owner_or_admin_ins ON public.activity_schedule FOR INSERT WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY activity_schedule_write_owner_or_admin_upd ON public.activity_schedule FOR UPDATE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = activity_schedule.farm_id) AND (ufa.role = 'OWNER'::text))))));

ALTER TABLE public.activity_type ENABLE ROW LEVEL SECURITY;

CREATE POLICY activity_type_admin_write_del ON public.activity_type FOR DELETE USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

CREATE POLICY activity_type_admin_write_ins ON public.activity_type FOR INSERT WITH CHECK ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

CREATE POLICY activity_type_admin_write_upd ON public.activity_type FOR UPDATE USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true))))) WITH CHECK ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

CREATE POLICY activity_type_read_all ON public.activity_type FOR SELECT USING (true);

CREATE POLICY admin_read_audit_log ON public.audit_log FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

ALTER TABLE public.alert_log ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.alert_rule ENABLE ROW LEVEL SECURITY;

CREATE POLICY alert_rule_select_members_or_admin ON public.alert_rule FOR SELECT USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = alert_rule.farm_id))))));

CREATE POLICY alert_rule_write_owner_or_admin_del ON public.alert_rule FOR DELETE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = alert_rule.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY alert_rule_write_owner_or_admin_ins ON public.alert_rule FOR INSERT WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = alert_rule.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY alert_rule_write_owner_or_admin_upd ON public.alert_rule FOR UPDATE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = alert_rule.farm_id) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = alert_rule.farm_id) AND (ufa.role = 'OWNER'::text))))));

ALTER TABLE public.app_settings ENABLE ROW LEVEL SECURITY;

CREATE POLICY app_settings_admin_only ON public.app_settings TO authenticated USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true))))) WITH CHECK ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

ALTER TABLE public.audit_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY block_user_activity_instance_update ON public.activity_instance FOR UPDATE USING (false);

CREATE POLICY block_user_activity_instance_write ON public.activity_instance FOR INSERT WITH CHECK (false);

ALTER TABLE public.camera_activity_zone ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.camera_stream_config ENABLE ROW LEVEL SECURITY;

CREATE POLICY camera_stream_config_select_members_or_admin ON public.camera_stream_config FOR SELECT USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM (public.farm_camera fc
     JOIN public.user_farm_access ufa ON ((ufa.farm_id = fc.farm_id)))
  WHERE ((fc.id = camera_stream_config.camera_id) AND (ufa.user_id = auth.uid()))))));

CREATE POLICY camera_stream_config_write_owner_or_admin_del ON public.camera_stream_config FOR DELETE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM (public.farm_camera fc
     JOIN public.user_farm_access ufa ON ((ufa.farm_id = fc.farm_id)))
  WHERE ((fc.id = camera_stream_config.camera_id) AND (ufa.user_id = auth.uid()) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY camera_stream_config_write_owner_or_admin_ins ON public.camera_stream_config FOR INSERT WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM (public.farm_camera fc
     JOIN public.user_farm_access ufa ON ((ufa.farm_id = fc.farm_id)))
  WHERE ((fc.id = camera_stream_config.camera_id) AND (ufa.user_id = auth.uid()) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY camera_stream_config_write_owner_or_admin_upd ON public.camera_stream_config FOR UPDATE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM (public.farm_camera fc
     JOIN public.user_farm_access ufa ON ((ufa.farm_id = fc.farm_id)))
  WHERE ((fc.id = camera_stream_config.camera_id) AND (ufa.user_id = auth.uid()) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM (public.farm_camera fc
     JOIN public.user_farm_access ufa ON ((ufa.farm_id = fc.farm_id)))
  WHERE ((fc.id = camera_stream_config.camera_id) AND (ufa.user_id = auth.uid()) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY caz_all_owner_or_admin ON public.camera_activity_zone TO authenticated USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = camera_activity_zone.farm_id) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = camera_activity_zone.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY caz_select_farm_members ON public.camera_activity_zone FOR SELECT TO authenticated USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = camera_activity_zone.farm_id))))));

ALTER TABLE public.dashboard_config ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.device_model_assignment ENABLE ROW LEVEL SECURITY;

CREATE POLICY dma_all_owner_or_admin ON public.device_model_assignment TO authenticated USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM (public.edge_device d
     JOIN public.user_farm_access ufa ON ((ufa.farm_id = d.farm_id)))
  WHERE ((d.id = device_model_assignment.device_id) AND (ufa.user_id = auth.uid()) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM (public.edge_device d
     JOIN public.user_farm_access ufa ON ((ufa.farm_id = d.farm_id)))
  WHERE ((d.id = device_model_assignment.device_id) AND (ufa.user_id = auth.uid()) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY dma_select_farm_members ON public.device_model_assignment FOR SELECT TO authenticated USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM (public.edge_device d
     JOIN public.user_farm_access ufa ON ((ufa.farm_id = d.farm_id)))
  WHERE ((d.id = device_model_assignment.device_id) AND (ufa.user_id = auth.uid()))))));

CREATE POLICY edge_block_event_reads ON public.activity_detection_event FOR SELECT USING (false);

ALTER TABLE public.edge_device ENABLE ROW LEVEL SECURITY;

CREATE POLICY edge_device_admin_write_del ON public.edge_device FOR DELETE USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

CREATE POLICY edge_device_admin_write_upd ON public.edge_device FOR UPDATE USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true))))) WITH CHECK ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

ALTER TABLE public.edge_device_heartbeat ENABLE ROW LEVEL SECURITY;

CREATE POLICY edge_device_insert_admin_owner ON public.edge_device FOR INSERT TO authenticated WITH CHECK ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = ANY (ARRAY['ADMIN'::text, 'OWNER'::text]))))));

CREATE POLICY edge_device_select_admin_owner ON public.edge_device FOR SELECT TO authenticated USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = ANY (ARRAY['ADMIN'::text, 'OWNER'::text]))))));

CREATE POLICY edge_insert_detection_event ON public.activity_detection_event FOR INSERT WITH CHECK (((auth.role() = 'authenticated'::text) AND (device_id = ((auth.jwt() ->> 'device_id'::text))::uuid)));

CREATE POLICY edge_insert_heartbeat ON public.edge_device_heartbeat FOR INSERT WITH CHECK (((auth.role() = 'authenticated'::text) AND (device_id = ((auth.jwt() ->> 'device_id'::text))::uuid)));

ALTER TABLE public.farm ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.farm_camera ENABLE ROW LEVEL SECURITY;

CREATE POLICY farm_camera_select_members_or_admin ON public.farm_camera FOR SELECT USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm_camera.farm_id))))));

CREATE POLICY farm_camera_write_owner_or_admin_del ON public.farm_camera FOR DELETE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm_camera.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY farm_camera_write_owner_or_admin_ins ON public.farm_camera FOR INSERT TO authenticated WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile
  WHERE ((user_profile.id = auth.uid()) AND ((upper(user_profile.role) = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text])) OR (upper((user_profile.metadata ->> 'role'::text)) = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text])))))) OR ((EXISTS ( SELECT 1
   FROM public.user_profile
  WHERE ((user_profile.id = auth.uid()) AND ((upper(user_profile.role) = 'OWNER'::text) OR (upper((user_profile.metadata ->> 'role'::text)) = 'OWNER'::text))))) AND (EXISTS ( SELECT 1
   FROM public.user_farm_access
  WHERE ((user_farm_access.user_id = auth.uid()) AND (user_farm_access.farm_id = farm_camera.farm_id)))))));

CREATE POLICY farm_camera_write_owner_or_admin_upd ON public.farm_camera FOR UPDATE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm_camera.farm_id) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm_camera.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY farm_delete_admin_only ON public.farm FOR DELETE USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

CREATE POLICY farm_insert_admin_owner ON public.farm FOR INSERT TO authenticated WITH CHECK ((EXISTS ( SELECT 1
   FROM public.user_profile
  WHERE ((user_profile.id = auth.uid()) AND ((upper(user_profile.role) = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])) OR (upper((user_profile.metadata ->> 'role'::text)) = ANY (ARRAY['ADMIN'::text, 'SUPER_ADMIN'::text, 'OWNER'::text])))))));

ALTER TABLE public.farm_invite ENABLE ROW LEVEL SECURITY;

CREATE POLICY farm_select_admin_owner ON public.farm FOR SELECT TO authenticated USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = ANY (ARRAY['ADMIN'::text, 'OWNER'::text]))))));

CREATE POLICY farm_update_admin_or_farm_owner ON public.farm FOR UPDATE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm.id) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm.id) AND (ufa.role = 'OWNER'::text))))));

ALTER TABLE public.farm_zone ENABLE ROW LEVEL SECURITY;

CREATE POLICY farm_zone_insert_admin_owner ON public.farm_zone FOR INSERT TO authenticated WITH CHECK ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = ANY (ARRAY['ADMIN'::text, 'OWNER'::text]))))));

CREATE POLICY farm_zone_select_admin_owner ON public.farm_zone FOR SELECT TO authenticated USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = ANY (ARRAY['ADMIN'::text, 'OWNER'::text]))))));

CREATE POLICY farm_zone_write_owner_or_admin_del ON public.farm_zone FOR DELETE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm_zone.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY farm_zone_write_owner_or_admin_upd ON public.farm_zone FOR UPDATE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm_zone.farm_id) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = farm_zone.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY "insert own profile" ON public.user_profile FOR INSERT TO authenticated WITH CHECK ((auth.uid() = id));

ALTER TABLE public.ml_model_version ENABLE ROW LEVEL SECURITY;

CREATE POLICY mmv_admin_only ON public.ml_model_version TO authenticated USING ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true))))) WITH CHECK ((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))));

ALTER TABLE public.posture_observation ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.posture_summary ENABLE ROW LEVEL SECURITY;

CREATE POLICY posture_summary_select_farm_members ON public.posture_summary FOR SELECT TO authenticated USING (((farm_id IN ( SELECT public.get_user_managed_farm_ids(auth.uid()) AS get_user_managed_farm_ids)) OR (farm_id IN ( SELECT user_farm_access.farm_id
   FROM public.user_farm_access
  WHERE (user_farm_access.user_id = auth.uid())))));

CREATE POLICY "read own profile" ON public.user_profile FOR SELECT TO authenticated USING ((auth.uid() = id));

ALTER TABLE public.task_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY task_log_all_owner_or_admin ON public.task_log TO authenticated USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = task_log.farm_id) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = task_log.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY task_log_select_farm_members ON public.task_log FOR SELECT TO authenticated USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = task_log.farm_id))))));

ALTER TABLE public.task_schedule ENABLE ROW LEVEL SECURITY;

CREATE POLICY task_schedule_select_members_or_admin ON public.task_schedule FOR SELECT USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = task_schedule.farm_id))))));

CREATE POLICY task_schedule_write_owner_or_admin_del ON public.task_schedule FOR DELETE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = task_schedule.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY task_schedule_write_owner_or_admin_ins ON public.task_schedule FOR INSERT WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = task_schedule.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY task_schedule_write_owner_or_admin_upd ON public.task_schedule FOR UPDATE USING (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = task_schedule.farm_id) AND (ufa.role = 'OWNER'::text)))))) WITH CHECK (((EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text) AND (up.is_active = true)))) OR (EXISTS ( SELECT 1
   FROM public.user_farm_access ufa
  WHERE ((ufa.user_id = auth.uid()) AND (ufa.farm_id = task_schedule.farm_id) AND (ufa.role = 'OWNER'::text))))));

CREATE POLICY "update own profile" ON public.user_profile FOR UPDATE TO authenticated USING ((auth.uid() = id)) WITH CHECK ((auth.uid() = id));

ALTER TABLE public.user_farm_access ENABLE ROW LEVEL SECURITY;

CREATE POLICY user_farm_access_select_self_or_admin ON public.user_farm_access FOR SELECT USING (((user_id = auth.uid()) OR (EXISTS ( SELECT 1
   FROM public.user_profile up
  WHERE ((up.id = auth.uid()) AND (up.role = 'ADMIN'::text))))));

CREATE POLICY user_manage_dashboard_config ON public.dashboard_config USING ((user_id = auth.uid())) WITH CHECK ((user_id = auth.uid()));

ALTER TABLE public.user_profile ENABLE ROW LEVEL SECURITY;

CREATE POLICY user_profile_insert_own ON public.user_profile FOR INSERT TO authenticated WITH CHECK ((id = auth.uid()));

CREATE POLICY user_profile_insert_self_admin_owner ON public.user_profile FOR INSERT TO authenticated WITH CHECK (((id = auth.uid()) AND (role = ANY (ARRAY['ADMIN'::text, 'OWNER'::text]))));

CREATE POLICY user_profile_read_own ON public.user_profile FOR SELECT TO authenticated USING ((id = auth.uid()));

CREATE POLICY user_profile_select_self ON public.user_profile FOR SELECT TO authenticated USING ((id = auth.uid()));

CREATE POLICY user_profile_update_own ON public.user_profile FOR UPDATE TO authenticated USING ((id = auth.uid())) WITH CHECK ((id = auth.uid()));

CREATE POLICY user_read_activity_instance ON public.activity_instance FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.user_farm_access
  WHERE ((user_farm_access.user_id = auth.uid()) AND (user_farm_access.farm_id = activity_instance.farm_id)))));

CREATE POLICY user_read_alert_log ON public.alert_log FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.user_farm_access
  WHERE ((user_farm_access.user_id = auth.uid()) AND (user_farm_access.farm_id = alert_log.farm_id)))));

