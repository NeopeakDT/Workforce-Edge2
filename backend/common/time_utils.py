"""
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

from datetime import datetime, timezone
import pytz

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

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
    return tz.localize(dt).astimezone(timezone.utc)
