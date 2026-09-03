"""
backend/dashboard/dashboard_api.py
Dashboard API (Phase 6, read-only)

Step 3 — Posture Current-Status endpoint.
Step 4 — Posture Today's Summary endpoint.
Step 5 — Posture 24-Hour Trend endpoint.
Step 6 — Posture 7-Day Trend endpoint.
Step 7 — Posture 30-Day Trend endpoint.

Responsibilities:
- Authenticate the caller via their Supabase JWT (common/auth.py)
- Authorize the caller against the requested farm (common/auth.py)
- Delegate all queries to dashboard_query_service.py (no SQL here)

This is the first router under dashboard/ — it establishes the pattern
(FastAPI router + JWT dependency + farm-authorization check) for future
dashboard endpoints. See ADR discussion in main.py's Phase 6 comment
block for why this differs from the Postgres-RPC/RLS pattern used by
get_posture_trend() in STEP1_DATABASE_BASELINE.sql: this router runs on
the backend's service-role DB pool, which has no auth.uid() context, so
farm authorization is re-checked in Python via
common.auth.user_can_access_farm().
"""

from datetime import date as date_type, datetime, time as time_type, timedelta
from typing import Literal, Optional

import pytz
from fastapi import APIRouter, Header, HTTPException, Query

from common.auth import parse_auth_header, user_can_access_farm
from common.time_utils import build_utc_from_local_date_time
from dashboard.dashboard_query_service import (
    get_camera_names,
    get_camera_summary,
    get_camera_trend_buckets,
    get_current_posture_status,
    get_data_quality_period,
    get_farm_timezone,
    get_latest_observation_and_heartbeat,
    get_max_configured_milking_gap_minutes,
    get_posture_7d_buckets,
    get_posture_24h_buckets,
    get_posture_daily_summary,
    list_recent_alerts,
)

router = APIRouter()


@router.get("/posture/current")
def posture_current(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    authorization: str = Header(None),
):
    # -------------------------------------------------
    # 1. Authenticate (existing common/auth.py JWT flow)
    # -------------------------------------------------
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    # -------------------------------------------------
    # 2. Authorize — caller must have access to this farm
    # -------------------------------------------------
    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    # -------------------------------------------------
    # 3. Query latest NORMAL-mode observation
    # -------------------------------------------------
    observation = get_current_posture_status(farm_id, zone_id)

    if observation is None:
        raise HTTPException(
            status_code=404,
            detail="No posture observation found for this farm/zone",
        )

    return _build_response(observation)


@router.get("/posture/summary/today")
def posture_summary_today(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    date: date_type = Query(
        None,
        description="Farm-local calendar date (YYYY-MM-DD). Defaults to farm-local today.",
    ),
    authorization: str = Header(None),
):
    # -------------------------------------------------
    # 1. Authenticate — same as Step 3
    # -------------------------------------------------
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    # -------------------------------------------------
    # 2. Authorize — same as Step 3, no new mechanism
    # -------------------------------------------------
    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    # -------------------------------------------------
    # 3. Resolve the farm-local calendar day into a UTC window.
    #    "Today" must mean the farm's local calendar day, not UTC's —
    #    a farm in IST is ~5.5h into tomorrow while UTC is still on
    #    yesterday, and vice versa near local midnight.
    # -------------------------------------------------
    farm_tz_name = get_farm_timezone(farm_id)
    if farm_tz_name is None:
        raise HTTPException(status_code=404, detail="Farm not found")

    if date is not None:
        target_date = date
    else:
        target_date = datetime.now(pytz.utc).astimezone(pytz.timezone(farm_tz_name)).date()

    day_start_utc = build_utc_from_local_date_time(target_date, time_type.min, farm_tz_name)
    day_end_utc = build_utc_from_local_date_time(
        target_date + timedelta(days=1), time_type.min, farm_tz_name
    )

    # -------------------------------------------------
    # 4. Query the day's NORMAL-mode observations (aggregate + peak
    #    timestamps — two queries total, computed in Postgres)
    # -------------------------------------------------
    summary = get_posture_daily_summary(farm_id, zone_id, day_start_utc, day_end_utc)

    if summary is None:
        raise HTTPException(
            status_code=404,
            detail="No posture observations found for this farm/zone/date",
        )

    return _build_summary_response(summary, target_date, farm_tz_name)


def _build_summary_response(summary: dict, target_date, farm_tz_name: str) -> dict:
    """
    Map the raw aggregate row (+ peak timestamp lists) onto the same
    semantic style as Step 3's current-status response.
    """

    def _pct(value):
        return round(float(value), 2) if value is not None else None

    return {
        "date": target_date.isoformat(),
        "timezone": farm_tz_name,
        "observation_count": summary["observation_count"],
        "posture": {
            "feeding": {
                "average_percentage": _pct(summary["avg_feeding_percentage"]),
                "peak_percentage": _pct(summary["peak_feeding_percentage"]),
                "peak_times": summary["peak_feeding_times"],
            },
            "standing": {
                "average_percentage": _pct(summary["avg_standing_percentage"]),
                "peak_percentage": _pct(summary["peak_standing_percentage"]),
                "peak_times": summary["peak_standing_times"],
            },
            "resting": {
                "average_percentage": _pct(summary["avg_resting_percentage"]),
                "peak_percentage": _pct(summary["peak_resting_percentage"]),
                "peak_times": summary["peak_resting_times"],
            },
        },
        "data_quality": {
            "partial_camera_observations": summary["partial_camera_observations"],
            "average_camera_coverage_percentage": _pct(
                summary["avg_camera_coverage_percentage"]
            ),
        },
    }


@router.get("/posture/trend/24h")
def posture_trend_24h(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    authorization: str = Header(None),
):
    # -------------------------------------------------
    # 1. Authenticate — same as Step 3/4
    # -------------------------------------------------
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    # -------------------------------------------------
    # 2. Authorize — same as Step 3/4, no new mechanism
    # -------------------------------------------------
    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    # -------------------------------------------------
    # 3. Resolve the rolling farm-local 24-hour window: the current
    #    farm-local hour (inclusive, even if partial) back through the
    #    23 preceding full hours. Computed in Python against farm.timezone
    #    (same source as Step 4), not hardcoded or assumed from UTC.
    # -------------------------------------------------
    farm_tz_name = get_farm_timezone(farm_id)
    if farm_tz_name is None:
        raise HTTPException(status_code=404, detail="Farm not found")

    tz = pytz.timezone(farm_tz_name)
    now_local = datetime.now(pytz.utc).astimezone(tz)
    current_hour_local = now_local.replace(minute=0, second=0, microsecond=0)
    window_start_local = current_hour_local - timedelta(hours=23)
    window_start_utc = window_start_local.astimezone(pytz.utc)
    window_end_utc = window_start_utc + timedelta(hours=24)

    # -------------------------------------------------
    # 4. Two queries (NORMAL hourly averages + MILKING hourly presence),
    #    combined into a 24-point timeline in Python. Never 404s — this
    #    is a time-series view; an all-NO_DATA window still returns all
    #    24 points, just each one flagged NO_DATA.
    # -------------------------------------------------
    normal_by_bucket, milking_buckets = get_posture_24h_buckets(
        farm_id, zone_id, window_start_utc, window_end_utc
    )

    return _build_trend_response(farm_tz_name, window_start_local, normal_by_bucket, milking_buckets)


def _build_trend_response(
    farm_tz_name: str,
    window_start_local,
    normal_by_bucket: dict,
    milking_buckets: set,
) -> dict:
    """
    Merge the two raw bucket sources into exactly 24 timeline points.

    Status per hour, in priority order:
    - NORMAL:  at least one NORMAL observation landed in this hour (even
               if a milking window also touched part of it — real
               posture data beats an inferred label).
    - MILKING: no NORMAL observation, but the raw table shows a MILKING
               row in this hour.
    - NO_DATA: neither — no evidence of any kind for this hour.
    Never fabricates 0 for a status other than NORMAL.
    """

    def _pct(value):
        return round(float(value), 2) if value is not None else None

    points = []
    for i in range(24):
        bucket_local = window_start_local + timedelta(hours=i)
        normal = normal_by_bucket.get(i)

        if normal and normal["observation_count"]:
            status = "NORMAL"
            feeding = _pct(normal["avg_feeding_percentage"])
            standing = _pct(normal["avg_standing_percentage"])
            resting = _pct(normal["avg_resting_percentage"])
            observation_count = normal["observation_count"]
            partial_camera_observations = normal["partial_camera_observations"]
        else:
            status = "MILKING" if i in milking_buckets else "NO_DATA"
            feeding = standing = resting = None
            observation_count = 0
            partial_camera_observations = 0

        points.append({
            "time": bucket_local.strftime("%H:%M"),
            "observed_at": bucket_local.isoformat(),
            "status": status,
            "feeding": feeding,
            "standing": standing,
            "resting": resting,
            "observation_count": observation_count,
            "partial_camera_observations": partial_camera_observations,
        })

    return {
        "range": "24h",
        "timezone": farm_tz_name,
        "points": points,
    }


@router.get("/posture/trend/7d")
def posture_trend_7d(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    authorization: str = Header(None),
):
    # -------------------------------------------------
    # 1. Authenticate — same as Step 3/4/5
    # -------------------------------------------------
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    # -------------------------------------------------
    # 2. Authorize — same as Step 3/4/5, no new mechanism
    # -------------------------------------------------
    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    # -------------------------------------------------
    # 3. Resolve the window: today (farm-local) + previous 6 farm-local
    #    calendar days = exactly 7 dates, oldest first. Same
    #    farm.timezone source as Step 4/5, not hardcoded/assumed UTC.
    # -------------------------------------------------
    farm_tz_name = get_farm_timezone(farm_id)
    if farm_tz_name is None:
        raise HTTPException(status_code=404, detail="Farm not found")

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(farm_tz_name)).date()
    dates = [today_local - timedelta(days=d) for d in range(6, -1, -1)]  # oldest -> today

    window_start_utc = build_utc_from_local_date_time(dates[0], time_type.min, farm_tz_name)
    window_end_utc = build_utc_from_local_date_time(dates[-1] + timedelta(days=1), time_type.min, farm_tz_name)

    # -------------------------------------------------
    # 4. One query, grouped into daily buckets. Never 404s — a
    #    timeline endpoint, not a single observation; an all-NO_DATA
    #    week still returns all 7 points.
    # -------------------------------------------------
    daily_buckets = get_posture_7d_buckets(farm_id, zone_id, window_start_utc, window_end_utc)

    return _build_7d_trend_response(farm_tz_name, dates, daily_buckets)


def _build_7d_trend_response(farm_tz_name: str, dates: list, daily_buckets: dict) -> dict:
    """
    Merge the daily aggregate buckets into exactly 7 timeline points,
    one per farm-local calendar date, oldest first.

    Status is only ever NORMAL or NO_DATA — MILKING is never a whole-day
    state (a normal day can contain a milking window; that's already
    excluded row-by-row by posture_observation_normal, same as Step 4).
    NO_DATA days never carry fabricated feeding/standing/resting values.
    """

    def _pct(value):
        return round(float(value), 2) if value is not None else None

    points = []
    for i, day in enumerate(dates):
        bucket = daily_buckets.get(i)

        if bucket and bucket["observation_count"]:
            status = "NORMAL"
            feeding = _pct(bucket["avg_feeding_percentage"])
            standing = _pct(bucket["avg_standing_percentage"])
            resting = _pct(bucket["avg_resting_percentage"])
            observation_count = bucket["observation_count"]
            partial_camera_observations = bucket["partial_camera_observations"]
            avg_camera_coverage_percentage = _pct(bucket["avg_camera_coverage_percentage"])
        else:
            status = "NO_DATA"
            feeding = standing = resting = None
            observation_count = 0
            partial_camera_observations = 0
            avg_camera_coverage_percentage = None

        points.append({
            "date": day.isoformat(),
            "status": status,
            "feeding": feeding,
            "standing": standing,
            "resting": resting,
            "observation_count": observation_count,
            "partial_camera_observations": partial_camera_observations,
            "average_camera_coverage_percentage": avg_camera_coverage_percentage,
        })

    return {
        "range": "7d",
        "timezone": farm_tz_name,
        "points": points,
    }


@router.get("/posture/trend/30d")
def posture_trend_30d(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    authorization: str = Header(None),
):
    """
    30-day daily trend. Mechanically identical to Step 6's 7-day trend —
    same window-agnostic day-bucketing query (get_posture_7d_buckets
    groups by day-bucket-index against whatever window it's given; the
    "7d" in its name describes Step 6's caller, not a hardcoded day
    count in the SQL), just called with a 30-day window instead of a
    7-day one. Reused as-is rather than duplicated, and Step 6's
    function/endpoint are untouched, per instruction.
    """
    # -------------------------------------------------
    # 1. Authenticate — same as Step 3/4/5/6
    # -------------------------------------------------
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    # -------------------------------------------------
    # 2. Authorize — same as Step 3/4/5/6, no new mechanism
    # -------------------------------------------------
    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    # -------------------------------------------------
    # 3. Resolve the window: today (farm-local) + previous 29 farm-local
    #    calendar days = exactly 30 dates, oldest first.
    # -------------------------------------------------
    farm_tz_name = get_farm_timezone(farm_id)
    if farm_tz_name is None:
        raise HTTPException(status_code=404, detail="Farm not found")

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(farm_tz_name)).date()
    dates = [today_local - timedelta(days=d) for d in range(29, -1, -1)]  # oldest -> today

    window_start_utc = build_utc_from_local_date_time(dates[0], time_type.min, farm_tz_name)
    window_end_utc = build_utc_from_local_date_time(dates[-1] + timedelta(days=1), time_type.min, farm_tz_name)

    # -------------------------------------------------
    # 4. Same single query as Step 6 — no new SQL, no new view.
    #    posture_observation_normal is already the canonical NORMAL-only
    #    source; MILKING is intra-day (never a whole-day state), so
    #    there's nothing here that needs the raw table, same as Step 6.
    #    Never 404s — a gap like Jul 28-31 below still returns all 30
    #    points, each correctly flagged NO_DATA rather than omitted or
    #    fabricated as zero.
    # -------------------------------------------------
    daily_buckets = get_posture_7d_buckets(farm_id, zone_id, window_start_utc, window_end_utc)

    return _build_30d_trend_response(farm_tz_name, dates, daily_buckets)


def _build_30d_trend_response(farm_tz_name: str, dates: list, daily_buckets: dict) -> dict:
    """
    Same merge logic as Step 6's _build_7d_trend_response (NORMAL if the
    day has any NORMAL observation, else NO_DATA — MILKING is never a
    whole-day status), duplicated rather than shared so that Step 6's
    function stays untouched. Only differences: "range": "30d" and 30
    points instead of 7.
    """

    def _pct(value):
        return round(float(value), 2) if value is not None else None

    points = []
    for i, day in enumerate(dates):
        bucket = daily_buckets.get(i)

        if bucket and bucket["observation_count"]:
            status = "NORMAL"
            feeding = _pct(bucket["avg_feeding_percentage"])
            standing = _pct(bucket["avg_standing_percentage"])
            resting = _pct(bucket["avg_resting_percentage"])
            observation_count = bucket["observation_count"]
            partial_camera_observations = bucket["partial_camera_observations"]
            avg_camera_coverage_percentage = _pct(bucket["avg_camera_coverage_percentage"])
        else:
            status = "NO_DATA"
            feeding = standing = resting = None
            observation_count = 0
            partial_camera_observations = 0
            avg_camera_coverage_percentage = None

        points.append({
            "date": day.isoformat(),
            "status": status,
            "feeding": feeding,
            "standing": standing,
            "resting": resting,
            "observation_count": observation_count,
            "partial_camera_observations": partial_camera_observations,
            "average_camera_coverage_percentage": avg_camera_coverage_percentage,
        })

    return {
        "range": "30d",
        "timezone": farm_tz_name,
        "points": points,
    }


@router.get("/posture/camera/current")
def posture_camera_current(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    authorization: str = Header(None),
):
    """
    Latest per-camera feeding/standing counts.

    Reuses Step 3's get_current_posture_status() as-is -- verified
    (before writing this) that its SELECT already includes `metadata`,
    which embeds metadata.cameras, so this needs zero new queries beyond
    the one small camera-name lookup below. Only feeding/standing are
    exposed per camera; no per-camera resting figure or herd percentage
    exists anywhere in this pipeline (see Step 8A audit) and none is
    fabricated here.
    """
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    observation = get_current_posture_status(farm_id, zone_id)

    if observation is None:
        raise HTTPException(
            status_code=404,
            detail="No posture observation found for this farm/zone",
        )

    camera_names = get_camera_names(farm_id, zone_id)

    return _build_camera_current_response(observation, camera_names)


def _build_camera_current_response(observation: dict, camera_names: dict) -> dict:
    """
    camera_names is the authoritative camera set for this zone (from
    camera_activity_zone/farm_camera, not from this one observation's
    metadata) -- every configured camera appears in the response even if
    it happened to be missing from this specific row, with null counts
    rather than a fabricated 0.
    """
    metadata = observation["metadata"] or {}
    cam_data = metadata.get("cameras", {})

    cameras = []
    for camera_code, names in camera_names.items():
        c = cam_data.get(camera_code)
        cameras.append({
            "camera_code": camera_code,
            "camera_name": names["camera_name"],
            "feeding_count": c["feeding"] if c else None,
            "standing_count": c["standing"] if c else None,
        })

    return {
        "observed_at": observation["observed_at"],
        "cameras": cameras,
    }


@router.get("/posture/camera/trend/7d")
def posture_camera_trend_7d(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    authorization: str = Header(None),
):
    """
    Daily per-camera feeding/standing trend, 7 farm-local days
    (today + previous 6), same window convention as Step 6.
    """
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    farm_tz_name = get_farm_timezone(farm_id)
    if farm_tz_name is None:
        raise HTTPException(status_code=404, detail="Farm not found")

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(farm_tz_name)).date()
    dates = [today_local - timedelta(days=d) for d in range(6, -1, -1)]  # oldest -> today

    window_start_utc = build_utc_from_local_date_time(dates[0], time_type.min, farm_tz_name)
    window_end_utc = build_utc_from_local_date_time(dates[-1] + timedelta(days=1), time_type.min, farm_tz_name)

    camera_names = get_camera_names(farm_id, zone_id)
    trend_buckets = get_camera_trend_buckets(farm_id, zone_id, window_start_utc, window_end_utc)

    return _build_camera_trend_response(farm_tz_name, dates, camera_names, trend_buckets)


def _build_camera_trend_response(
    farm_tz_name: str,
    dates: list,
    camera_names: dict,
    trend_buckets: dict,
) -> dict:
    """
    Per-camera NO_DATA is independent of the zone-wide day status Step 6
    reports for the same day: a day can be NORMAL at the zone level
    while one specific camera has zero rows that day (e.g. a camera
    outage), and that must show up here rather than being hidden behind
    the zone-level status.
    """

    def _pct(value):
        return round(float(value), 2) if value is not None else None

    cameras = []
    for camera_code, names in camera_names.items():
        points = []
        for i, day in enumerate(dates):
            bucket = trend_buckets.get((i, camera_code))

            if bucket and bucket["observation_count"]:
                status = "NORMAL"
                feeding = _pct(bucket["avg_feeding"])
                standing = _pct(bucket["avg_standing"])
                observation_count = bucket["observation_count"]
            else:
                status = "NO_DATA"
                feeding = standing = None
                observation_count = 0

            points.append({
                "date": day.isoformat(),
                "status": status,
                "feeding_average_count": feeding,
                "standing_average_count": standing,
                "observation_count": observation_count,
            })

        cameras.append({
            "camera_code": camera_code,
            "camera_name": names["camera_name"],
            "points": points,
        })

    return {
        "range": "7d",
        "timezone": farm_tz_name,
        "cameras": cameras,
    }


@router.get("/posture/camera/summary")
def posture_camera_summary(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    days: int = Query(7, description="Summary window in days. One of 1, 7, 30."),
    authorization: str = Header(None),
):
    """
    Window-averaged per-camera feeding/standing stats, with independent
    feeding/standing rankings and presence-quality reporting.
    """
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    if days not in (1, 7, 30):
        raise HTTPException(status_code=400, detail="days must be one of 1, 7, 30")

    farm_tz_name = get_farm_timezone(farm_id)
    if farm_tz_name is None:
        raise HTTPException(status_code=404, detail="Farm not found")

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(farm_tz_name)).date()
    window_start_date = today_local - timedelta(days=days - 1)

    window_start_utc = build_utc_from_local_date_time(window_start_date, time_type.min, farm_tz_name)
    window_end_utc = build_utc_from_local_date_time(today_local + timedelta(days=1), time_type.min, farm_tz_name)

    camera_names = get_camera_names(farm_id, zone_id)
    total_observations, per_camera = get_camera_summary(farm_id, zone_id, window_start_utc, window_end_utc)

    return _build_camera_summary_response(days, farm_tz_name, total_observations, camera_names, per_camera)


def _competition_rank(values: dict) -> dict:
    """
    Standard competition ranking (1, 1, 3): ties share a rank, the next
    distinct value skips ranks accordingly. Cameras with a None average
    (zero observations in the window) are excluded from ranking
    entirely -- rank None, never ranked last.
    """
    ranked_codes = [code for code, v in values.items() if v is not None]
    ranked_codes.sort(key=lambda code: values[code], reverse=True)

    ranks = {}
    for i, code in enumerate(ranked_codes):
        if i > 0 and values[code] == values[ranked_codes[i - 1]]:
            ranks[code] = ranks[ranked_codes[i - 1]]
        else:
            ranks[code] = i + 1

    for code, v in values.items():
        if v is None:
            ranks[code] = None

    return ranks


def _build_camera_summary_response(
    window_days: int,
    farm_tz_name: str,
    total_observations: int,
    camera_names: dict,
    per_camera: dict,
) -> dict:
    """
    total_observations = count of NORMAL posture_observation rows for
    this farm/zone/window (i.e. from posture_observation_normal) -- NOT
    a sum of per-camera observation counts, since a camera can be
    missing from some rows. presence_percentage divides each camera's
    own observation_count by this same shared denominator.
    """

    def _pct(value):
        return round(float(value), 2) if value is not None else None

    feeding_avgs = {}
    standing_avgs = {}
    per_camera_stats = {}

    for camera_code in camera_names:
        row = per_camera.get(camera_code)
        observation_count = row["observation_count"] if row else 0

        presence_percentage = (
            round(observation_count / total_observations * 100, 2)
            if total_observations
            else 0.0
        )

        feeding_avg = _pct(row["avg_feeding"]) if observation_count else None
        standing_avg = _pct(row["avg_standing"]) if observation_count else None

        feeding_avgs[camera_code] = feeding_avg
        standing_avgs[camera_code] = standing_avg
        per_camera_stats[camera_code] = {
            "observation_count": observation_count,
            "presence_percentage": presence_percentage,
        }

    feeding_ranks = _competition_rank(feeding_avgs)
    standing_ranks = _competition_rank(standing_avgs)

    cameras = []
    for camera_code, names in camera_names.items():
        stats = per_camera_stats[camera_code]
        cameras.append({
            "camera_code": camera_code,
            "camera_name": names["camera_name"],
            "observation_count": stats["observation_count"],
            "presence_percentage": stats["presence_percentage"],
            "feeding": {
                "average_count": feeding_avgs[camera_code],
                "rank": feeding_ranks[camera_code],
            },
            "standing": {
                "average_count": standing_avgs[camera_code],
                "rank": standing_ranks[camera_code],
            },
        })

    return {
        "window_days": window_days,
        "timezone": farm_tz_name,
        "data_quality": {
            "total_observations": total_observations,
            "expected_camera_count": len(camera_names),
        },
        "cameras": cameras,
    }


# Grounding constants for the two first-pass classification heuristics
# below (see dashboard_query_service._NORMAL_CADENCE_MINUTES's docstring
# comment for the full rationale) -- both derived from already-
# established, code-level constants (posture_scheduler.py's
# DB_WRITE_INTERVAL_SECONDS, jetson/edge_heartbeat_agent.py's INTERVAL,
# both 300s / 5min, confirmed empirically in the Step 9 audit), not
# invented business thresholds. Revisit once real GOOD/WARNING/CRITICAL
# bands are defined.
_NORMAL_CADENCE_MINUTES = 5.0
_HEARTBEAT_INTERVAL_MINUTES = 5.0


@router.get("/posture/data-quality")
def posture_data_quality(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    date: date_type = Query(
        None,
        description="Farm-local calendar date (YYYY-MM-DD). Defaults to farm-local today.",
    ),
    authorization: str = Header(None),
):
    """
    Data-quality metrics for one farm-local day: coverage, cadence,
    completeness, freshness, mode distribution.

    Never 404s for "no data" -- a data-quality report exists precisely
    to surface an unhealthy/empty period, not hide it behind a 404 the
    way a single-observation endpoint (Step 3) would.
    """
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    farm_tz_name = get_farm_timezone(farm_id)
    if farm_tz_name is None:
        raise HTTPException(status_code=404, detail="Farm not found")

    if date is not None:
        target_date = date
    else:
        target_date = datetime.now(pytz.utc).astimezone(pytz.timezone(farm_tz_name)).date()

    period_start_utc = build_utc_from_local_date_time(target_date, time_type.min, farm_tz_name)
    period_end_utc = build_utc_from_local_date_time(
        target_date + timedelta(days=1), time_type.min, farm_tz_name
    )

    now_utc = datetime.now(pytz.utc)
    is_complete = period_end_utc <= now_utc

    period_data = get_data_quality_period(farm_id, zone_id, period_start_utc, period_end_utc)
    max_milking_gap_minutes = get_max_configured_milking_gap_minutes(farm_id)
    latest_observation_at, latest_heartbeat_at = get_latest_observation_and_heartbeat(farm_id, zone_id)
    expected_cameras = len(get_camera_names(farm_id, zone_id))

    return _build_data_quality_response(
        farm_id=farm_id,
        zone_id=zone_id,
        farm_tz_name=farm_tz_name,
        target_date=target_date,
        period_start_utc=period_start_utc,
        period_end_utc=period_end_utc,
        is_complete=is_complete,
        period_data=period_data,
        max_milking_gap_minutes=max_milking_gap_minutes,
        latest_observation_at=latest_observation_at,
        latest_heartbeat_at=latest_heartbeat_at,
        now_utc=now_utc,
        expected_cameras=expected_cameras,
    )


def _cadence_stats(timestamps: list):
    """Median/p95 interval (minutes) between consecutive NORMAL rows in the period."""
    if len(timestamps) < 2:
        return None, None

    intervals = sorted(
        (timestamps[i] - timestamps[i - 1]).total_seconds() / 60
        for i in range(1, len(timestamps))
    )
    n = len(intervals)
    median = (
        intervals[n // 2]
        if n % 2
        else (intervals[n // 2 - 1] + intervals[n // 2]) / 2
    )
    p95 = intervals[min(int(0.95 * n), n - 1)]
    return round(median, 2), round(p95, 2)


def _largest_gap_and_classification(
    normal_timestamps: list,
    milking_timestamps: list,
    max_milking_gap_minutes,
    boundary_before=None,
    boundary_after=None,
):
    """
    Largest gap between consecutive NORMAL rows in the period, and a
    classification -- deliberately NOT "any MILKING row inside the gap
    -> MILKING", per the review's correction (a 20-hour gap containing
    24 MILKING rows is milking-ADJACENT, not a legitimate milking gap):

    - NORMAL_CADENCE: the "gap" is within ~2x the established ~5min
      cadence -- not really a gap at all.
    - MILKING_EXPECTED: MILKING evidence exists in the gap AND its
      duration fits within the farm's own configured milking-window
      span (activity_schedule ideal window + tolerances).
    - MILKING_ADJACENT_ANOMALY: MILKING evidence exists in the gap, but
      the duration exceeds the configured span (or no schedule is
      configured to compare against).
    - UNEXPLAINED: no MILKING evidence in the gap at all.

    boundary_before/boundary_after (the nearest NORMAL rows just outside
    the period, from get_data_quality_period) extend gap detection past
    the period's own edges -- without them, a gap that straddles a
    farm-local midnight (confirmed in the Step 9 audit: the real ~20h
    Aug 3->4 anomaly does exactly this) would be silently split into two
    ordinary in-day gaps and never surface on either day's report.
    """
    timestamps = list(normal_timestamps)
    if boundary_before is not None:
        timestamps = [boundary_before] + timestamps
    if boundary_after is not None:
        timestamps = timestamps + [boundary_after]

    if len(timestamps) < 2:
        return None, None

    largest_start, largest_end, largest_minutes = None, None, -1
    for i in range(1, len(timestamps)):
        start, end = timestamps[i - 1], timestamps[i]
        minutes = (end - start).total_seconds() / 60
        if minutes > largest_minutes:
            largest_start, largest_end, largest_minutes = start, end, minutes

    if largest_minutes <= 2 * _NORMAL_CADENCE_MINUTES:
        classification = "NORMAL_CADENCE"
    else:
        milking_in_gap = any(largest_start < ts < largest_end for ts in milking_timestamps)
        if not milking_in_gap:
            classification = "UNEXPLAINED"
        elif max_milking_gap_minutes is not None and largest_minutes <= max_milking_gap_minutes:
            classification = "MILKING_EXPECTED"
        else:
            classification = "MILKING_ADJACENT_ANOMALY"

    return round(largest_minutes, 2), classification


def _completeness_block(period_minutes: float, period_data: dict, is_complete: bool):
    """
    Period-aware completeness, per the review's correction: a partial
    (in-progress) day must never be scored against a full day's expected
    count, and a period touching the pre-mode-field legacy window must
    say so explicitly rather than silently producing a number that
    excludes data it can't see.
    """
    normal_count = period_data["normal_count"]

    if not is_complete:
        return {
            "observation_count": normal_count,
            "expected_observation_count": None,
            "completeness_percentage": None,
            "milking_minutes_excluded": None,
            "calculation_status": "PARTIAL_PERIOD",
        }

    if period_data["legacy_unknown_count"] > 0:
        return {
            "observation_count": normal_count,
            "expected_observation_count": None,
            "completeness_percentage": None,
            "milking_minutes_excluded": None,
            "calculation_status": "LEGACY_DATA_PRESENT",
        }

    milking_minutes = period_data["milking_count"] * _NORMAL_CADENCE_MINUTES
    expected = round((period_minutes - milking_minutes) / _NORMAL_CADENCE_MINUTES)
    completeness_percentage = (
        round(normal_count / expected * 100, 2) if expected else None
    )

    return {
        "observation_count": normal_count,
        "expected_observation_count": expected,
        "completeness_percentage": completeness_percentage,
        "milking_minutes_excluded": round(milking_minutes, 1),
        "calculation_status": "VALID",
    }


def _build_data_quality_response(
    *,
    farm_id: str,
    zone_id: str,
    farm_tz_name: str,
    target_date,
    period_start_utc,
    period_end_utc,
    is_complete: bool,
    period_data: dict,
    max_milking_gap_minutes,
    latest_observation_at,
    latest_heartbeat_at,
    now_utc,
    expected_cameras: int,
) -> dict:
    def _pct(value):
        return round(float(value), 2) if value is not None else None

    def _age_minutes(ts):
        return round((now_utc - ts).total_seconds() / 60, 2) if ts is not None else None

    median_interval, p95_interval = _cadence_stats(period_data["normal_timestamps"])
    largest_gap_minutes, largest_gap_classification = _largest_gap_and_classification(
        period_data["normal_timestamps"],
        period_data["milking_timestamps"],
        max_milking_gap_minutes,
        boundary_before=period_data["boundary_before"],
        boundary_after=period_data["boundary_after"],
    )

    period_minutes = (period_end_utc - period_start_utc).total_seconds() / 60
    completeness = _completeness_block(period_minutes, period_data, is_complete)

    latest_observation_age = _age_minutes(latest_observation_at)
    latest_heartbeat_age = _age_minutes(latest_heartbeat_at)

    if latest_heartbeat_at is None:
        device_status = "UNKNOWN"
    elif latest_heartbeat_age <= 3 * _HEARTBEAT_INTERVAL_MINUTES:
        device_status = "ONLINE"
    else:
        device_status = "OFFLINE"

    return {
        "farm_id": farm_id,
        "zone_id": zone_id,
        "period": {
            "date": target_date.isoformat(),
            "start": period_start_utc.isoformat(),
            "end": period_end_utc.isoformat(),
            "timezone": farm_tz_name,
            "is_complete": is_complete,
        },
        "coverage": {
            "expected_cameras": expected_cameras,
            # Most frequently reported received-camera count this period
            # (SQL mode()) -- a single representative figure, since
            # per-observation received_cameras can vary within a period;
            # partial_camera_observations/coverage_percentage below are
            # the precise, non-approximated signals.
            "received_cameras": period_data["typical_received_cameras"],
            "coverage_percentage": _pct(period_data["coverage_percentage"]),
            "partial_camera_observations": period_data["partial_camera_observations"],
        },
        "cadence": {
            "median_interval_minutes": median_interval,
            "p95_interval_minutes": p95_interval,
            "largest_gap_minutes": largest_gap_minutes,
            "largest_gap_classification": largest_gap_classification,
        },
        "completeness": completeness,
        "freshness": {
            "latest_observation_at": latest_observation_at,
            "latest_observation_age_minutes": latest_observation_age,
            "latest_device_heartbeat_at": latest_heartbeat_at,
            "latest_device_heartbeat_age_minutes": latest_heartbeat_age,
            "device_status": device_status,
        },
        "mode_distribution": {
            "normal_count": period_data["normal_count"],
            "milking_count": period_data["milking_count"],
            "legacy_unknown_count": period_data["legacy_unknown_count"],
        },
    }


@router.get("/posture/dashboard")
def posture_dashboard(
    farm_id: str = Query(..., description="Farm UUID"),
    zone_id: str = Query(..., description="Farm zone UUID"),
    authorization: str = Header(None),
):
    """
    Composed dashboard: current + today's summary + 24h/7d/30d trends +
    camera current/trend/summary + data quality, in one response.

    This calls Steps 3-9's query-service functions and private response
    builders directly as Python function calls within this module --
    it never calls this router's own HTTP endpoints internally, and it
    does not modify or wrap any of them. A handful of naturally-
    identical lookups are fetched ONCE and reused across sections
    instead of once per section (farm.timezone, camera_names, the
    latest-NORMAL-observation row) -- this changes nothing about any
    section's OUTPUT (every _build_*_response call below is byte-for-
    byte the same function Steps 3-9's own endpoints call), it only
    avoids re-running identical queries multiple times within this one
    composed request. See the implementation report for the exact query
    count and where each query comes from.
    """
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    farm_tz_name = get_farm_timezone(farm_id)
    if farm_tz_name is None:
        raise HTTPException(status_code=404, detail="Farm not found")

    tz = pytz.timezone(farm_tz_name)
    now_local = datetime.now(pytz.utc).astimezone(tz)
    now_utc = datetime.now(pytz.utc)
    today_local = now_local.date()

    # Shared across multiple sections below -- see docstring.
    observation = get_current_posture_status(farm_id, zone_id)
    camera_names = get_camera_names(farm_id, zone_id)

    # --- Step 3: current ---
    current = _build_response(observation) if observation is not None else None

    # --- Step 4: today's summary ---
    day_start_utc = build_utc_from_local_date_time(today_local, time_type.min, farm_tz_name)
    day_end_utc = build_utc_from_local_date_time(
        today_local + timedelta(days=1), time_type.min, farm_tz_name
    )
    daily_summary = get_posture_daily_summary(farm_id, zone_id, day_start_utc, day_end_utc)
    today_summary = (
        _build_summary_response(daily_summary, today_local, farm_tz_name)
        if daily_summary is not None
        else None
    )

    # --- Step 5: 24h trend ---
    current_hour_local = now_local.replace(minute=0, second=0, microsecond=0)
    window_start_local_24h = current_hour_local - timedelta(hours=23)
    window_start_utc_24h = window_start_local_24h.astimezone(pytz.utc)
    window_end_utc_24h = window_start_utc_24h + timedelta(hours=24)
    normal_by_bucket, milking_buckets = get_posture_24h_buckets(
        farm_id, zone_id, window_start_utc_24h, window_end_utc_24h
    )
    trend_24h = _build_trend_response(farm_tz_name, window_start_local_24h, normal_by_bucket, milking_buckets)

    # --- Step 6: 7d trend (bounds reused below by camera trend/summary too) ---
    dates_7d = [today_local - timedelta(days=d) for d in range(6, -1, -1)]
    window_start_utc_7d = build_utc_from_local_date_time(dates_7d[0], time_type.min, farm_tz_name)
    window_end_utc_7d = build_utc_from_local_date_time(
        dates_7d[-1] + timedelta(days=1), time_type.min, farm_tz_name
    )
    buckets_7d = get_posture_7d_buckets(farm_id, zone_id, window_start_utc_7d, window_end_utc_7d)
    trend_7d = _build_7d_trend_response(farm_tz_name, dates_7d, buckets_7d)

    # --- Step 7: 30d trend ---
    dates_30d = [today_local - timedelta(days=d) for d in range(29, -1, -1)]
    window_start_utc_30d = build_utc_from_local_date_time(dates_30d[0], time_type.min, farm_tz_name)
    window_end_utc_30d = build_utc_from_local_date_time(
        dates_30d[-1] + timedelta(days=1), time_type.min, farm_tz_name
    )
    buckets_30d = get_posture_7d_buckets(farm_id, zone_id, window_start_utc_30d, window_end_utc_30d)
    trend_30d = _build_30d_trend_response(farm_tz_name, dates_30d, buckets_30d)

    # --- Step 8: camera current / trend / summary ---
    camera_current = (
        _build_camera_current_response(observation, camera_names)
        if observation is not None
        else None
    )

    camera_trend_buckets = get_camera_trend_buckets(
        farm_id, zone_id, window_start_utc_7d, window_end_utc_7d
    )
    camera_trend_7d = _build_camera_trend_response(farm_tz_name, dates_7d, camera_names, camera_trend_buckets)

    camera_summary_total, camera_summary_per_camera = get_camera_summary(
        farm_id, zone_id, window_start_utc_7d, window_end_utc_7d
    )
    camera_summary = _build_camera_summary_response(
        7, farm_tz_name, camera_summary_total, camera_names, camera_summary_per_camera
    )

    # --- Step 9: data quality (today) ---
    is_complete = day_end_utc <= now_utc
    dq_period_data = get_data_quality_period(farm_id, zone_id, day_start_utc, day_end_utc)
    max_milking_gap_minutes = get_max_configured_milking_gap_minutes(farm_id)
    latest_observation_at, latest_heartbeat_at = get_latest_observation_and_heartbeat(farm_id, zone_id)
    data_quality = _build_data_quality_response(
        farm_id=farm_id,
        zone_id=zone_id,
        farm_tz_name=farm_tz_name,
        target_date=today_local,
        period_start_utc=day_start_utc,
        period_end_utc=day_end_utc,
        is_complete=is_complete,
        period_data=dq_period_data,
        max_milking_gap_minutes=max_milking_gap_minutes,
        latest_observation_at=latest_observation_at,
        latest_heartbeat_at=latest_heartbeat_at,
        now_utc=now_utc,
        expected_cameras=len(camera_names),
    )

    return {
        "farm_id": farm_id,
        "zone_id": zone_id,
        "timezone": farm_tz_name,
        "generated_at": now_utc.isoformat(),
        "current": current,
        "today_summary": today_summary,
        "trend_24h": trend_24h,
        "trend_7d": trend_7d,
        "trend_30d": trend_30d,
        "camera_current": camera_current,
        "camera_trend_7d": camera_trend_7d,
        "camera_summary": camera_summary,
        "data_quality": data_quality,
    }


def _build_response(observation: dict) -> dict:
    """
    Map a raw posture_observation row (+ its metadata jsonb) onto the
    frontend's semantic contract. Database column names (laying_count,
    farm_id/zone_id/device_id, metadata.*) stay internal to this
    function; nothing outside it should read the raw row shape.
    """
    metadata = observation["metadata"] or {}

    herd_size = metadata.get("herd_size")
    feeding_count = observation["feeding_count"]

    feeding_percentage = (
        round(feeding_count / herd_size * 100, 2)
        if herd_size
        else None
    )

    expected_cameras = metadata.get("expected_cameras")
    received_cameras = metadata.get("received_cameras")

    coverage_percentage = (
        round(received_cameras / expected_cameras * 100, 2)
        if expected_cameras
        else None
    )

    return {
        "observed_at": observation["observed_at"],
        "herd": {
            "size": herd_size,
        },
        "posture": {
            "feeding": {
                "count": feeding_count,
                "percentage": feeding_percentage,
            },
            "standing": {
                "count": observation["standing_count"],
                "percentage": observation["standing_percentage"],
            },
            "resting": {
                "count": observation["laying_count"],
                "percentage": observation["laying_percentage"],
            },
        },
        "data_quality": {
            "expected_cameras": expected_cameras,
            "received_cameras": received_cameras,
            "coverage_percentage": coverage_percentage,
        },
    }


# -------------------------------------------------
# Step D2 — Alerts read endpoint
# -------------------------------------------------
# Pull/read model only (Step D frozen decision): this endpoint exposes
# alert_log via the existing dashboard auth/authorization pattern. It does
# not create, resolve, or otherwise mutate any alert -- alert_log remains
# exclusively Step C's write surface. No dispatcher, no alert_delivery, no
# push transport; see docs history for the Step D design freeze.
#
# lifecycle_state and alert_type are typed as Literal[...] rather than
# `str` so FastAPI/Pydantic reject any value outside the supported set at
# the request-validation layer (422) before this function body ever runs
# -- no arbitrary string is ever interpolated into SQL. This intentionally
# does not expose dashboard_query_service.list_recent_alerts()'s internal
# lifecycle_state=None ("both states") capability over HTTP; the frozen
# API surface offers exactly ACTIVE or RESOLVED, per the Step D decision.

@router.get("/alerts")
def list_alerts(
    farm_id: str = Query(..., description="Farm UUID"),
    lifecycle_state: Literal["ACTIVE", "RESOLVED"] = Query(
        "ACTIVE", description="Alert lifecycle state to return."
    ),
    alert_type: Optional[Literal["ACTIVITY", "EDGE_DEVICE", "POSTURE"]] = Query(
        None, description="Restrict to one alert type. Omit for all types."
    ),
    limit: int = Query(20, ge=1, le=100, description="Max rows to return."),
    authorization: str = Header(None),
):
    # -------------------------------------------------
    # 1. Authenticate (existing common/auth.py JWT flow)
    # -------------------------------------------------
    try:
        ctx = parse_auth_header(authorization)
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))

    # -------------------------------------------------
    # 2. Authorize — caller must have access to this farm
    # -------------------------------------------------
    if not user_can_access_farm(ctx.user_id, farm_id):
        raise HTTPException(
            status_code=403,
            detail="Not authorized for this farm",
        )

    # -------------------------------------------------
    # 3. Delegate to the query service (no SQL here)
    # -------------------------------------------------
    alerts = list_recent_alerts(
        farm_id,
        lifecycle_state=lifecycle_state,
        alert_type=alert_type,
        limit=limit,
    )

    return {
        "farm_id": farm_id,
        "lifecycle_state": lifecycle_state,
        "alert_type": alert_type,
        "count": len(alerts),
        "alerts": alerts,
    }
