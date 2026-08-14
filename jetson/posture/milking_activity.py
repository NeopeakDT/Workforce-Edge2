"""
Farm-level milking activity registry.


Updated by the milking camera pipeline via START and FRAME_AGGREGATE
heartbeats while milking activity is active.


Provides live milking activity state for consumers that explicitly
need it.


PostureScheduler currently uses configured milking schedules as the
authoritative MILKING mode boundary and does not depend on this registry.
"""

from __future__ import annotations

import time
from threading import Lock
from typing import Dict, Optional, Set

HEARTBEAT_TIMEOUT_SEC = 30.0
SUSTAINED_MILKING_SEC = 30.0

_lock = Lock()
_active: Dict[str, float] = {}
_farm_active_since: Optional[float] = None


def _prune_stale_locked(
    now: float,
    timeout: float = HEARTBEAT_TIMEOUT_SEC,
) -> None:
    global _farm_active_since

    stale = [
        camera_id
        for camera_id, last_seen in _active.items()
        if now - last_seen >= timeout
    ]

    for camera_id in stale:
        del _active[camera_id]

    if not _active:
        _farm_active_since = None


def set_milking_camera_active(camera_id: str, active: bool) -> None:
    global _farm_active_since

    now = time.monotonic()

    with _lock:
        if active:
            _active[camera_id] = now
            if _farm_active_since is None:
                _farm_active_since = now
            return

        _active.pop(camera_id, None)
        _prune_stale_locked(now)


def is_milking_active(timeout: float = HEARTBEAT_TIMEOUT_SEC) -> bool:
    now = time.monotonic()

    with _lock:
        _prune_stale_locked(now, timeout)
        return bool(_active)


def is_milking_sustained(
    sustain_sec: float = SUSTAINED_MILKING_SEC,
    timeout: float = HEARTBEAT_TIMEOUT_SEC,
) -> bool:
    now = time.monotonic()

    with _lock:
        _prune_stale_locked(now, timeout)

        if not _active or _farm_active_since is None:
            return False

        return (now - _farm_active_since) >= sustain_sec


def milking_active_duration_sec(
    timeout: float = HEARTBEAT_TIMEOUT_SEC,
) -> float:
    now = time.monotonic()

    with _lock:
        _prune_stale_locked(now, timeout)

        if not _active or _farm_active_since is None:
            return 0.0

        return now - _farm_active_since


def active_milking_camera_ids(
    timeout: float = HEARTBEAT_TIMEOUT_SEC,
) -> Set[str]:
    now = time.monotonic()

    with _lock:
        _prune_stale_locked(now, timeout)
        return set(_active.keys())
