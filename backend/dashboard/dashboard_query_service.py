"""
STEP 6.3 — Dashboard Query Service

Read-only queries for UI dashboards.
NO writes.
NO inference.
NO detection tables.
"""

from common.db import get_cursor


# -------------------------------------------------
# Farm overview
# -------------------------------------------------

def get_farm_overview(farm_id: str):
    """
    High-level farm dashboard numbers.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                f.id AS farm_id,
                f.name AS farm_name,

                COUNT(ai.id) FILTER (WHERE ai.status = 'IN_PROGRESS') AS in_progress_activities,
                COUNT(ai.id) FILTER (WHERE ai.status = 'MISSED') AS missed_activities,
                COUNT(al.id) FILTER (WHERE al.status = 'SENT') AS active_alerts

            FROM farm f
            LEFT JOIN activity_instance ai ON ai.farm_id = f.id
            LEFT JOIN alert_log al ON al.farm_id = f.id

            WHERE f.id = %s
            GROUP BY f.id
            """,
            (farm_id,),
        )
        return cur.fetchone()


# -------------------------------------------------
# Activity lists
# -------------------------------------------------

def list_today_activities(farm_id: str, activity_date):
    """
    Activities for a given date (already computed in Phase 5).
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                activity_type_id,
                status,
                actual_start_at,
                actual_end_at,
                started_offset_min,
                ended_offset_min,
                source
            FROM activity_instance
            WHERE farm_id = %s
              AND activity_date = %s
            ORDER BY COALESCE(actual_start_at, created_at)
            """,
            (farm_id, activity_date),
        )
        return cur.fetchall()


def list_in_progress_activities(farm_id: str):
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                activity_type_id,
                actual_start_at,
                status
            FROM activity_instance
            WHERE farm_id = %s
              AND status = 'IN_PROGRESS'
            ORDER BY actual_start_at
            """,
            (farm_id,),
        )
        return cur.fetchall()


def list_missed_activities(farm_id: str, limit=20):
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                activity_type_id,
                activity_date,
                status
            FROM activity_instance
            WHERE farm_id = %s
              AND status = 'MISSED'
            ORDER BY activity_date DESC
            LIMIT %s
            """,
            (farm_id, limit),
        )
        return cur.fetchall()


# -------------------------------------------------
# Posture
# -------------------------------------------------

# Canonical "NORMAL-only" analytics relation.
#
# public.posture_observation_normal (STEP4_ADD_POSTURE_OBSERVATION_NORMAL_VIEW.sql)
# is a plain view over posture_observation filtered to
# metadata->>'mode' = 'NORMAL'. It bakes the MILKING-exclusion rule in
# once at the DB layer so every posture analytics query — Current API
# here, Today's Summary in Step 4, anything later — reads from one
# canonical relation instead of re-deriving the filter per query.
POSTURE_NORMAL_RELATION = "public.posture_observation_normal"


def get_current_posture_status(farm_id: str, zone_id: str):
    """
    Latest NORMAL-mode posture observation for one farm/zone.

    Reads from posture_observation_normal, so MILKING-mode rows
    (schedule-only, all-zero placeholder rows written by
    jetson/posture/posture_scheduler.py during milking windows — see its
    metadata.mode field) are already excluded by the view. Returns None
    if no NORMAL observation exists yet; callers must not fabricate zero
    values for that case.
    """
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                farm_id,
                zone_id,
                device_id,
                observed_at,
                standing_count,
                feeding_count,
                laying_count,
                standing_percentage,
                laying_percentage,
                metadata
            FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %s
              AND zone_id = %s
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (farm_id, zone_id),
        )
        return cur.fetchone()


def get_farm_timezone(farm_id: str):
    """
    Farm-local IANA timezone name, or None if farm_id doesn't exist.
    """
    with get_cursor() as cur:
        cur.execute(
            "SELECT timezone FROM public.farm WHERE id = %s",
            (farm_id,),
        )
        row = cur.fetchone()
        return row["timezone"] if row else None


# feeding_percentage is not a stored column (see posture_models.PostureObservation
# — only standing_percentage/laying_percentage are persisted). Computed per-row
# from that observation's OWN metadata.herd_size, never a current/config herd
# size, so historical rows stay correct if herd size changes over time.
_FEEDING_PERCENTAGE_SQL = (
    "(feeding_count::numeric / NULLIF((metadata ->> 'herd_size')::numeric, 0) * 100)"
)
_EXPECTED_CAMERAS_SQL = "(metadata ->> 'expected_cameras')::int"
_RECEIVED_CAMERAS_SQL = "(metadata ->> 'received_cameras')::int"


def get_posture_daily_summary(farm_id: str, zone_id: str, day_start_utc, day_end_utc):
    """
    Today's (or any single farm-local day's) posture summary for one
    farm/zone, computed entirely in Postgres: averages, peaks, peak
    timestamps (ALL of them, not just one — ties are expected and
    meaningful), observation count, partial-camera-coverage count, and
    average camera coverage.

    Two queries total (no N+1): one aggregate query, one query for the
    timestamps matching each peak. The peak-timestamp query re-filters
    on the exact same unrounded SQL expressions as the aggregate query
    (not the rounded display values), so equality comparisons are exact
    — Postgres numeric arithmetic, no floating-point drift.

    Returns None if there are zero NORMAL observations in the window;
    callers must not fabricate zero values for that case.
    """
    with get_cursor() as cur:
        cur.execute(
            f"""
            WITH scoped AS (
                SELECT
                    observed_at,
                    standing_percentage,
                    laying_percentage,
                    {_FEEDING_PERCENTAGE_SQL} AS feeding_percentage,
                    {_EXPECTED_CAMERAS_SQL} AS expected_cameras,
                    {_RECEIVED_CAMERAS_SQL} AS received_cameras
                FROM {POSTURE_NORMAL_RELATION}
                WHERE farm_id = %s
                  AND zone_id = %s
                  AND observed_at >= %s
                  AND observed_at < %s
            )
            SELECT
                count(*) AS observation_count,
                avg(feeding_percentage) AS avg_feeding_percentage,
                avg(standing_percentage) AS avg_standing_percentage,
                avg(laying_percentage) AS avg_resting_percentage,
                max(feeding_percentage) AS peak_feeding_percentage,
                max(standing_percentage) AS peak_standing_percentage,
                max(laying_percentage) AS peak_resting_percentage,
                count(*) FILTER (WHERE received_cameras < expected_cameras)
                    AS partial_camera_observations,
                avg(received_cameras::numeric / NULLIF(expected_cameras, 0) * 100)
                    AS avg_camera_coverage_percentage
            FROM scoped
            """,
            (farm_id, zone_id, day_start_utc, day_end_utc),
        )
        agg = cur.fetchone()

        if not agg or not agg["observation_count"]:
            return None

        cur.execute(
            f"""
            SELECT observed_at, 'feeding' AS peak_type
            FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %s AND zone_id = %s
              AND observed_at >= %s AND observed_at < %s
              AND {_FEEDING_PERCENTAGE_SQL} = %s

            UNION ALL

            SELECT observed_at, 'standing' AS peak_type
            FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %s AND zone_id = %s
              AND observed_at >= %s AND observed_at < %s
              AND standing_percentage = %s

            UNION ALL

            SELECT observed_at, 'resting' AS peak_type
            FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %s AND zone_id = %s
              AND observed_at >= %s AND observed_at < %s
              AND laying_percentage = %s

            ORDER BY peak_type, observed_at
            """,
            (
                farm_id, zone_id, day_start_utc, day_end_utc, agg["peak_feeding_percentage"],
                farm_id, zone_id, day_start_utc, day_end_utc, agg["peak_standing_percentage"],
                farm_id, zone_id, day_start_utc, day_end_utc, agg["peak_resting_percentage"],
            ),
        )
        peak_rows = cur.fetchall()

    peak_times = {"feeding": [], "standing": [], "resting": []}
    for row in peak_rows:
        peak_times[row["peak_type"]].append(row["observed_at"])

    agg = dict(agg)
    agg["peak_feeding_times"] = peak_times["feeding"]
    agg["peak_standing_times"] = peak_times["standing"]
    agg["peak_resting_times"] = peak_times["resting"]
    return agg


def get_posture_24h_buckets(farm_id: str, zone_id: str, window_start_utc, window_end_utc):
    """
    Hourly-bucketed posture data for a farm-local 24-hour window.

    Two queries total (no N+1), one round trip:

    1. NORMAL-mode hourly averages, from posture_observation_normal —
       same canonical NORMAL-only relation as Step 3/4.
    2. MILKING-mode hourly presence, from the RAW posture_observation
       table. posture_observation_normal exists specifically to exclude
       MILKING rows, so it cannot answer "was this hour a scheduled
       milking window" — that requires the raw table. This is the only
       function in this module that reads posture_observation directly,
       and only to detect MILKING presence, never to pull posture values
       out of it (those always come from the NORMAL-only relation).

    bucket_index is farm-local-hour-aligned: floor((observed_at -
    window_start_utc) / 1 hour). window_start_utc is the caller-computed
    UTC instant of the farm-local window's first hour boundary (handles
    fixed non-whole-hour offsets like Asia/Kolkata's UTC+5:30 correctly,
    since every subsequent bucket is exactly 1 hour from that anchor —
    this does not account for a DST transition occurring mid-window).

    Returns (normal_by_bucket: dict[int, dict], milking_buckets: set[int]).
    Never returns None — an empty/no-data window still yields empty
    dict/set, and it's the caller's job to render all 24 hours (NO_DATA
    where neither NORMAL nor MILKING evidence exists), not to 404: this
    is a time-series view, not a single observation.
    """
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                floor(extract(epoch FROM (observed_at - %(start)s)) / 3600)::int AS bucket_index,
                avg({_FEEDING_PERCENTAGE_SQL}) AS avg_feeding_percentage,
                avg(standing_percentage) AS avg_standing_percentage,
                avg(laying_percentage) AS avg_resting_percentage,
                count(*) AS observation_count,
                count(*) FILTER (WHERE {_RECEIVED_CAMERAS_SQL} < {_EXPECTED_CAMERAS_SQL})
                    AS partial_camera_observations
            FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
            GROUP BY bucket_index
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": window_start_utc, "end": window_end_utc},
        )
        normal_by_bucket = {row["bucket_index"]: row for row in cur.fetchall()}

        cur.execute(
            """
            SELECT DISTINCT
                floor(extract(epoch FROM (observed_at - %(start)s)) / 3600)::int AS bucket_index
            FROM public.posture_observation
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
              AND metadata ->> 'mode' = 'MILKING'
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": window_start_utc, "end": window_end_utc},
        )
        milking_buckets = {row["bucket_index"] for row in cur.fetchall()}

    return normal_by_bucket, milking_buckets


def get_posture_7d_buckets(farm_id: str, zone_id: str, window_start_utc, window_end_utc):
    """
    Daily-bucketed NORMAL-mode posture averages for a farm-local 7-day
    window (today + previous 6 farm-local calendar days).

    One query, no N+1. Unlike Step 5 (24h), this does NOT also query the
    raw posture_observation table for MILKING detection — MILKING is an
    intra-day exclusion (a normal operating day can contain a milking
    window), not a state a whole day can be in, so there is no MILKING
    daily status to derive. Only NORMAL vs NO_DATA, decided purely by
    whether posture_observation_normal has any row for that day.

    bucket_index is farm-local-day-aligned: floor((observed_at -
    window_start_utc) / 1 day), where window_start_utc is the caller-
    computed UTC instant of the farm-local window's first midnight
    (same anchoring technique as Step 5's hourly buckets, just at a
    24-hour period instead of 1-hour — does not account for a DST
    transition occurring mid-window).

    Returns dict[bucket_index -> row]; a missing key means that day has
    zero NORMAL observations (NO_DATA), not a fabricated zero.
    """
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                floor(extract(epoch FROM (observed_at - %(start)s)) / 86400)::int AS bucket_index,
                avg({_FEEDING_PERCENTAGE_SQL}) AS avg_feeding_percentage,
                avg(standing_percentage) AS avg_standing_percentage,
                avg(laying_percentage) AS avg_resting_percentage,
                count(*) AS observation_count,
                count(*) FILTER (WHERE {_RECEIVED_CAMERAS_SQL} < {_EXPECTED_CAMERAS_SQL})
                    AS partial_camera_observations,
                avg({_RECEIVED_CAMERAS_SQL}::numeric / NULLIF({_EXPECTED_CAMERAS_SQL}, 0) * 100)
                    AS avg_camera_coverage_percentage
            FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
            GROUP BY bucket_index
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": window_start_utc, "end": window_end_utc},
        )
        return {row["bucket_index"]: row for row in cur.fetchall()}


# -------------------------------------------------
# Alerts
# -------------------------------------------------

def list_recent_alerts(farm_id: str, limit=20):
    """
    Recent alerts with context for dashboard.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                al.id,
                al.triggered_at,
                al.status AS alert_status,
                al.message,
                al.channel,

                ar.severity,
                ar.name AS rule_name,

                ai.activity_type_id,
                ai.status AS activity_status

            FROM alert_log al
            JOIN alert_rule ar ON ar.id = al.alert_rule_id
            JOIN activity_instance ai ON ai.id = al.activity_instance_id

            WHERE al.farm_id = %s
            ORDER BY al.triggered_at DESC
            LIMIT %s
            """,
            (farm_id, limit),
        )
        return cur.fetchall()
