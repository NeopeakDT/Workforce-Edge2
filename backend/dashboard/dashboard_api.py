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

import pytz
from fastapi import APIRouter, Header, HTTPException, Query

from common.auth import parse_auth_header, user_can_access_farm
from common.time_utils import build_utc_from_local_date_time
from dashboard.dashboard_query_service import (
    get_current_posture_status,
    get_farm_timezone,
    get_posture_7d_buckets,
    get_posture_24h_buckets,
    get_posture_daily_summary,
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
