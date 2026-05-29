"""
backend/common/time_utils.py
Timezone Utilities (uses UTC as the base timezone)
Provides timezone-safe datetime operations.
All timestamps are stored in UTC and converted based on farm timezone.

Enforces UTC everywhere
Converts Jetson timestamps safely
Prevents aggregation drift across farms/timezones
Supabase stores timestamptz → UTC internally.
"""
# datatime.now(timezone.utc),isoformat()  ---check this from jetson
# common/time_utils.py

from datetime import datetime, date, time, timezone
import pytz


def build_utc_from_local_date_time(
    local_date: date,
    local_time: time,
    tz_name: str
) -> datetime:
    naive_local = datetime.combine(local_date, local_time)
    return local_to_utc(naive_local, tz_name)


def utc_now() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(microsecond=0)


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("Naive datetime not allowed")
    return dt.astimezone(timezone.utc)


def from_epoch_ms(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def to_epoch_ms(dt: datetime) -> int:
    dt = to_utc(dt)
    return int(dt.timestamp() * 1000)


def local_to_utc(dt: datetime, tz_name: str) -> datetime:
    tz = pytz.timezone(tz_name)

    if dt.tzinfo is not None:
        raise ValueError("local_to_utc expects naive local datetime")

    localized = tz.localize(dt)
    return localized.astimezone(timezone.utc)

# ----------------------------------------------------------------------------------------------------------
# Below is the older version(12-2-26)
# from datetime import datetime, timezone
# import pytz
# from datetime import date, time, datetime

# def build_utc_from_local_date_time(
#     local_date: date,
#     local_time: time,
#     tz_name: str
# ) -> datetime:
#     naive_local = datetime.combine(local_date, local_time)
#     return local_to_utc(naive_local, tz_name)


# def utc_now() -> datetime:
#     """
#     Get current UTC time, rounded to seconds (no microseconds).
    
#     Returns timestamps in format: 2026-01-29 11:46:11+00:00
#     (instead of: 2026-01-29 11:46:11.660875+00:00)
#     """
#     now = datetime.now(timezone.utc)
#     # Round to seconds by replacing microseconds with 0
#     return now.replace(microsecond=0)

# def to_utc(dt: datetime) -> datetime:
#     """
#     Convert datetime to UTC.
    
#     ⚠️ WARNING: NOT for DB writes!
#     All timestamps written to DB must already be UTC.
#     This function is for display/logging purposes only.
#     """
#     if dt.tzinfo is None:
#         raise ValueError("Naive datetime not allowed")
#     return dt.astimezone(timezone.utc)

# def from_epoch_ms(ts_ms: int) -> datetime:
#     return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

# def to_epoch_ms(dt: datetime) -> int:
#     dt = to_utc(dt)
#     return int(dt.timestamp() * 1000)

# def local_to_utc(dt: datetime, tz_name: str) -> datetime:
#     """
#     Convert local datetime to UTC.
    
#     ⚠️ WARNING: NOT for DB writes!
#     All timestamps written to DB must already be UTC.
#     This function is for display/logging purposes only.
#     """
#     tz = pytz.timezone(tz_name)
#     return tz.localize(dt).astimezone(timezone.utc)
