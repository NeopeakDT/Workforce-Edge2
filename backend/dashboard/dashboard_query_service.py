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


def get_camera_names(farm_id: str, zone_id: str):
    """
    Cameras configured for POSTURE (activity_type_id=4) at this zone.

    Returns dict[camera_code -> {"camera_id": ..., "camera_name": ...}].
    camera_name is farm_camera.name verbatim -- no reformatting/cleanup.
    The DB value is authoritative; the frontend decides how to display it.

    This is the authoritative "expected camera" set for this zone (used
    for expected_camera_count in the summary endpoint), independent of
    what any single observation's metadata happened to report.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT fc.code AS camera_code, fc.id AS camera_id, fc.name AS camera_name
            FROM public.camera_activity_zone caz
            JOIN public.farm_camera fc ON fc.id = caz.camera_id
            WHERE caz.farm_id = %s
              AND caz.zone_id = %s
              AND caz.activity_type_id = 4
              AND caz.is_active = true
            """,
            (farm_id, zone_id),
        )
        return {
            row["camera_code"]: {"camera_id": row["camera_id"], "camera_name": row["camera_name"]}
            for row in cur.fetchall()
        }


def get_camera_trend_buckets(farm_id: str, zone_id: str, window_start_utc, window_end_utc):
    """
    Daily-bucketed per-camera feeding/standing averages for a farm-local
    window, unnesting metadata.cameras (a JSONB object keyed by camera
    code) via jsonb_each() + LATERAL -- one row per camera per
    observation, computed in Postgres. One query, no N+1, no per-camera
    round trips.

    Only feeding/standing -- no per-camera resting figure exists
    anywhere in this pipeline (see Step 8A audit), so none is computed
    here.

    Returns dict[(bucket_index, camera_code) -> row]. A missing key
    means that camera had zero observations in that bucket -- NO_DATA
    for that camera on that day, independent of the zone-wide status
    Step 6/7 would report for the same day.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                floor(extract(epoch FROM (observed_at - %(start)s)) / 86400)::int AS bucket_index,
                cam.key AS camera_code,
                avg((cam.value ->> 'feeding')::numeric) AS avg_feeding,
                avg((cam.value ->> 'standing')::numeric) AS avg_standing,
                count(*) AS observation_count
            FROM public.posture_observation_normal,
                 LATERAL jsonb_each(metadata -> 'cameras') AS cam(key, value)
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
            GROUP BY bucket_index, cam.key
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": window_start_utc, "end": window_end_utc},
        )
        return {(row["bucket_index"], row["camera_code"]): row for row in cur.fetchall()}


def get_camera_summary(farm_id: str, zone_id: str, window_start_utc, window_end_utc):
    """
    Window-averaged per-camera feeding/standing stats, for ranking and
    presence-quality reporting.

    Two queries, one round trip: total_observations (the same
    NORMAL-only row count pattern as Steps 4/6/7 -- the denominator for
    presence_percentage), and the per-camera jsonb_each aggregate (same
    unnesting technique as get_camera_trend_buckets, just grouped by
    camera only, no day bucketing).

    total_observations = count of NORMAL posture_observation rows for
    this farm/zone/window (i.e. count(*) from posture_observation_normal
    -- NOT a sum of per-camera counts, since a camera can be missing
    from some rows).

    Returns (total_observations: int, per_camera: dict[camera_code -> row]).
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT count(*) AS total_observations
            FROM public.posture_observation_normal
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": window_start_utc, "end": window_end_utc},
        )
        total_observations = cur.fetchone()["total_observations"]

        cur.execute(
            """
            SELECT
                cam.key AS camera_code,
                avg((cam.value ->> 'feeding')::numeric) AS avg_feeding,
                avg((cam.value ->> 'standing')::numeric) AS avg_standing,
                count(*) AS observation_count
            FROM public.posture_observation_normal,
                 LATERAL jsonb_each(metadata -> 'cameras') AS cam(key, value)
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
            GROUP BY cam.key
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": window_start_utc, "end": window_end_utc},
        )
        per_camera = {row["camera_code"]: row for row in cur.fetchall()}

    return total_observations, per_camera


# Empirically confirmed in the Step 9 audit: NORMAL-observation cadence
# is ~5.03 min median, matching posture_scheduler.py's
# DB_WRITE_INTERVAL_SECONDS=300 flush interval exactly. Used as the
# grounding constant for two first-pass classification heuristics below
# (gap classification's "not really a gap" floor, and device freshness) --
# both are deliberately derived from this already-established, code-level
# constant rather than an invented number, but are still heuristics, not
# a business threshold decision. Revisit once real GOOD/WARNING/CRITICAL
# bands are defined (see Step 9 audit, section 14).
_NORMAL_CADENCE_MINUTES = 5.0


def get_data_quality_period(farm_id: str, zone_id: str, period_start_utc, period_end_utc):
    """
    Coverage / cadence / completeness / mode-distribution metrics for one
    farm-local period (a single calendar day in the current caller).

    Several small, bounded queries (period-scoped, not "load all
    history"): a coverage aggregate over posture_observation_normal, the
    NORMAL observed_at list for that period (cadence + gap detection),
    and the raw posture_observation (observed_at, mode) rows for that
    period (mode distribution + gap-classification evidence). None scan
    beyond the requested period.

    completeness/coverage/mode_distribution counts stay strictly
    period-scoped (a day's completeness must only count that day's own
    rows). Gap DETECTION, however, also fetches the single NORMAL row
    immediately before period_start and immediately after period_end
    (two tiny indexed LIMIT-1 queries) as boundary context -- discovered
    necessary while validating against real data: a genuine ~20h
    anomalous gap (Aug 3->4 in the Step 9 audit) straddles a farm-local
    midnight, so day-scoped-only gap detection would silently split it
    into two ordinary-looking smaller in-day gaps and never surface the
    real anomaly on either day's report. These boundary rows are used
    ONLY for gap/cadence detection in the API layer, never counted
    toward observation_count/completeness.

    Returns a dict with keys: observation_count, partial_camera_observations,
    coverage_percentage, typical_received_cameras, normal_timestamps
    (list, period-scoped only), boundary_before/boundary_after
    (single timestamp or None, just outside the period), milking_timestamps
    (period-scoped, widened to the boundary rows' span so a
    boundary-crossing gap's MILKING evidence isn't missed), normal_count,
    milking_count, legacy_unknown_count.
    """
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                count(*) AS observation_count,
                count(*) FILTER (WHERE {_RECEIVED_CAMERAS_SQL} < {_EXPECTED_CAMERAS_SQL})
                    AS partial_camera_observations,
                avg({_RECEIVED_CAMERAS_SQL}::numeric / NULLIF({_EXPECTED_CAMERAS_SQL}, 0) * 100)
                    AS coverage_percentage,
                mode() WITHIN GROUP (ORDER BY {_RECEIVED_CAMERAS_SQL}) AS typical_received_cameras
            FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": period_start_utc, "end": period_end_utc},
        )
        coverage = cur.fetchone()

        cur.execute(
            f"""
            SELECT observed_at
            FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
            ORDER BY observed_at
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": period_start_utc, "end": period_end_utc},
        )
        normal_timestamps = [row["observed_at"] for row in cur.fetchall()]

        cur.execute(
            f"""
            SELECT observed_at FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %(farm_id)s AND zone_id = %(zone_id)s AND observed_at < %(start)s
            ORDER BY observed_at DESC LIMIT 1
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": period_start_utc},
        )
        row = cur.fetchone()
        boundary_before = row["observed_at"] if row else None

        cur.execute(
            f"""
            SELECT observed_at FROM {POSTURE_NORMAL_RELATION}
            WHERE farm_id = %(farm_id)s AND zone_id = %(zone_id)s AND observed_at >= %(end)s
            ORDER BY observed_at ASC LIMIT 1
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "end": period_end_utc},
        )
        row = cur.fetchone()
        boundary_after = row["observed_at"] if row else None

        cur.execute(
            """
            SELECT observed_at, metadata ->> 'mode' AS mode
            FROM public.posture_observation
            WHERE farm_id = %(farm_id)s
              AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s
              AND observed_at < %(end)s
            ORDER BY observed_at
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": period_start_utc, "end": period_end_utc},
        )
        raw_rows = [(row["observed_at"], row["mode"]) for row in cur.fetchall()]

        # Widen the MILKING-evidence fetch to the boundary rows' span, so
        # a gap crossing the period edge can still see MILKING rows that
        # fall just outside [period_start, period_end).
        milking_span_start = boundary_before if boundary_before is not None else period_start_utc
        milking_span_end = boundary_after if boundary_after is not None else period_end_utc
        cur.execute(
            """
            SELECT observed_at FROM public.posture_observation
            WHERE farm_id = %(farm_id)s AND zone_id = %(zone_id)s
              AND observed_at >= %(start)s AND observed_at <= %(end)s
              AND metadata ->> 'mode' = 'MILKING'
            ORDER BY observed_at
            """,
            {"farm_id": farm_id, "zone_id": zone_id, "start": milking_span_start, "end": milking_span_end},
        )
        milking_timestamps = [row["observed_at"] for row in cur.fetchall()]

    normal_count = sum(1 for _, m in raw_rows if m == "NORMAL")
    milking_count = sum(1 for _, m in raw_rows if m == "MILKING")
    legacy_unknown_count = sum(1 for _, m in raw_rows if m is None)

    return {
        "observation_count": coverage["observation_count"],
        "partial_camera_observations": coverage["partial_camera_observations"],
        "coverage_percentage": coverage["coverage_percentage"],
        "typical_received_cameras": coverage["typical_received_cameras"],
        "normal_timestamps": normal_timestamps,
        "boundary_before": boundary_before,
        "boundary_after": boundary_after,
        "milking_timestamps": milking_timestamps,
        "normal_count": normal_count,
        "milking_count": milking_count,
        "legacy_unknown_count": legacy_unknown_count,
    }


def get_max_configured_milking_gap_minutes(farm_id: str):
    """
    Largest plausible MILKING-related gap duration, derived from the
    farm's OWN configured milking schedules (activity_schedule,
    activity_type_id=1) rather than an invented constant: for each
    active milking schedule, ideal_end - ideal_start + both tolerances.
    Returns None if no active milking schedule is configured (gap
    classification then falls back to MILKING_ADJACENT_ANOMALY whenever
    any MILKING evidence exists, since there's no configured expectation
    to compare against).
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT ideal_start_time, ideal_end_time, tolerance_early_min, tolerance_late_min
            FROM public.activity_schedule
            WHERE farm_id = %s AND activity_type_id = 1 AND is_active = true
            """,
            (farm_id,),
        )
        rows = cur.fetchall()

    if not rows:
        return None

    max_span = None
    for row in rows:
        if row["ideal_end_time"] is None:
            continue
        start_min = row["ideal_start_time"].hour * 60 + row["ideal_start_time"].minute
        end_min = row["ideal_end_time"].hour * 60 + row["ideal_end_time"].minute
        if end_min <= start_min:
            end_min += 24 * 60  # window crosses midnight
        span = (
            (end_min - start_min)
            + (row["tolerance_early_min"] or 0)
            + (row["tolerance_late_min"] or 0)
        )
        max_span = span if max_span is None else max(max_span, span)

    return max_span


def get_latest_observation_and_heartbeat(farm_id: str, zone_id: str):
    """
    Global (not period-scoped) freshness anchor: freshness answers "is
    the pipeline healthy right now", independent of which historical
    period the caller is otherwise inspecting.

    Two small queries: the most recent posture_observation row of ANY
    mode (so a heavy MILKING window doesn't look "stale" just because no
    NORMAL row has landed recently -- the pipeline is still alive and
    writing), which also gives device_id; then that device's most recent
    heartbeat.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at, device_id
            FROM public.posture_observation
            WHERE farm_id = %s AND zone_id = %s
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (farm_id, zone_id),
        )
        latest_obs = cur.fetchone()

        latest_heartbeat = None
        if latest_obs and latest_obs["device_id"]:
            cur.execute(
                """
                SELECT heartbeat_time
                FROM public.edge_device_heartbeat
                WHERE device_id = %s
                ORDER BY heartbeat_time DESC
                LIMIT 1
                """,
                (latest_obs["device_id"],),
            )
            latest_heartbeat = cur.fetchone()

    return (
        latest_obs["observed_at"] if latest_obs else None,
        latest_heartbeat["heartbeat_time"] if latest_heartbeat else None,
    )


# -------------------------------------------------
# Alerts
# -------------------------------------------------

def list_recent_alerts(farm_id: str, lifecycle_state: str = "ACTIVE", limit=20):
    """
    Recent alerts with context for dashboard.

    STEP D — D1: `activity_instance` is now a LEFT JOIN (was INNER JOIN).
    An INNER JOIN silently dropped every alert whose `activity_instance_id`
    is NULL -- which is every WORKFORCE_DETECTOR_OFFLINE, EDGE_DEVICE_OFFLINE,
    POSTURE_DATA_STALE, and schedule-keyed ACTIVITY_LATE row (Step C). Those
    alert types have no activity_instance at all by design (device/zone/
    schedule-keyed, not instance-keyed), so `ai.*` fields are NULL for them
    -- expected, not a data-quality problem.

    `lifecycle_state` defaults to 'ACTIVE' (current-alerts view -- an
    operational alert screen should not show resolved history as if it
    were a current problem); pass 'RESOLVED' explicitly for history, or
    None for both. This filters server-side by design (Step D decision)
    rather than leaving every caller to filter the full result client-side.
    """
    with get_cursor() as cur:
        query = """
            SELECT
                al.id,
                al.triggered_at,
                al.resolved_at,
                al.status AS alert_status,
                al.lifecycle_state,
                al.alert_type,
                al.dedup_key,
                al.message,
                al.channel,
                al.activity_instance_id,
                al.zone_id,
                al.device_id,

                ar.severity,
                ar.name AS rule_name,

                ai.activity_type_id,
                ai.activity_schedule_id,
                ai.status AS activity_status

            FROM alert_log al
            JOIN alert_rule ar ON ar.id = al.alert_rule_id
            LEFT JOIN activity_instance ai ON ai.id = al.activity_instance_id

            WHERE al.farm_id = %s
        """
        params = [farm_id]
        if lifecycle_state is not None:
            query += " AND al.lifecycle_state = %s"
            params.append(lifecycle_state)
        query += " ORDER BY al.triggered_at DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, tuple(params))
        return cur.fetchall()
