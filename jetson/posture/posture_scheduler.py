"""
jetson/posture/posture_scheduler.py

Scheduler for Cow Posture Detection.

Responsibilities
----------------
- Collect per-camera samples into minute snapshots
- Buffer minute snapshots until DB flush
- Persist aggregated pen observations

Milking mode is schedule-only (± tolerance).
No milking-detector dependency.
No YOLO inference during scheduled milking windows.

No YOLO inference in this module (delegated to detector).
No ROI logic.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, time as dt_time, timezone
from threading import Lock
from typing import Dict, Optional, Set
from zoneinfo import ZoneInfo

from .posture_models import (
    MinuteAggregation,
    PenAggregationBuffer,
    PostureObservation,
    PostureSample,
)

logger = logging.getLogger(__name__)


class PostureScheduler:
    """
    Frame-driven posture sampling and periodic pen-wide DB flush.
    """

    # Sample every 15 seconds
    SAMPLE_INTERVAL_SECONDS = 15

    # Aggregate every 5 minutes (20 samples at 15s intervals)
    DB_WRITE_INTERVAL_SECONDS = 300

    def __init__(
        self,
        runtime_config: dict,
        detector,
        db,
    ):
        self.runtime_config = runtime_config
        self.detector = detector
        self.db = db

        self.pen_buffer = PenAggregationBuffer()

        self.current_minute_samples: Dict[str, PostureSample] = {}

        self.current_minute_started_at: Optional[float] = None

        self.posture_camera_ids: Set[str] = {
            camera["camera_id"]
            for camera in runtime_config["cameras"]
            if "POSTURE" in camera.get("activity_zones", {})
        }

        self.pen_rest_zone_id: Optional[str] = None

        for camera in runtime_config["cameras"]:

            posture = camera.get("activity_zones", {}).get("POSTURE")

            if posture is None:
                continue

            rest_zone = next(
                (
                    z
                    for z in posture["zones"]
                    if z["zone_type"] == "REST"
                ),
                None,
            )

            if rest_zone is not None:
                self.pen_rest_zone_id = rest_zone["zone_id"]
                break

        self.last_flush: Optional[datetime] = None

        # Serialize access to the current per-camera snapshot window.
        self._minute_lock = Lock()

        # Serialize scheduled milking state transitions.
        self._milking_state_lock = Lock()

        # Serialize pen-buffer flush / DB writes.
        self._flush_lock = Lock()

        # True while inside a scheduled milking window (cleared once on enter).
        self._in_milking_window = False

        # camera_id -> last sample monotonic time
        self.last_sample: Dict[str, float] = {}

        posture_runtime = runtime_config.get("posture_runtime", {})
        self.milking_schedules = posture_runtime.get(
            "milking_schedules",
            [],
        )

    @property
    def total_samples(self) -> int:
        with self.pen_buffer.lock:
            return self.pen_buffer.size

    def _parse_schedule_time(self, value) -> dt_time:
        if isinstance(value, dt_time):
            return value
        return datetime.strptime(str(value), "%H:%M:%S").time()

    def _active_milking_schedule(self, now: datetime) -> Optional[dict]:
        """
        Return the matching milking schedule dict if now is inside
        ideal_start/end ± tolerance, else None.
        """
        if not self.milking_schedules:
            return None

        farm_tz_name = self.runtime_config.get("farm_timezone", "UTC")
        farm_tz = ZoneInfo(farm_tz_name)

        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        local_now = now.astimezone(farm_tz)
        activity_date = local_now.date()

        for schedule in self.milking_schedules:
            ideal_start = datetime.combine(
                activity_date,
                self._parse_schedule_time(schedule["start_time"]),
                tzinfo=farm_tz,
            )
            ideal_end = datetime.combine(
                activity_date,
                self._parse_schedule_time(schedule["end_time"]),
                tzinfo=farm_tz,
            )

            if ideal_end <= ideal_start:
                ideal_end += timedelta(days=1)

            early_min = schedule.get("tolerance_early_min", 0) or 0
            late_min = schedule.get("tolerance_late_min", 0) or 0

            window_start = ideal_start - timedelta(minutes=early_min)
            window_end = ideal_end + timedelta(minutes=late_min)

            if window_start <= local_now <= window_end:
                return schedule

        return None

    def start(self):
        """
        No-op: sampling is frame-driven via process_frame().
        """

    def stop(self):
        """
        Flush remaining buffer on shutdown.
        """

        self.flush_all()

        logger.info("Posture scheduler stopped.")

    def process_frame(
        self,
        frame,
        camera: dict,
    ):
        """
        Sample posture from one camera frame when the interval has elapsed.

        During scheduled milking windows, skips YOLO posture inference and
        only writes synthetic MILKING observations on the flush interval.
        """

        camera_id = camera["camera_id"]

        # Monotonic clock is used only for interval/duration calculations.
        now_mono = time.monotonic()

        observed_at = datetime.now(timezone.utc)

        schedule = self._active_milking_schedule(observed_at)

        if schedule is not None:
            with self._milking_state_lock:
                entering_milking = not self._in_milking_window

                if entering_milking:
                    self._in_milking_window = True

            if entering_milking:
                self._clear_minute_window()

                with self.pen_buffer.lock:
                    self.pen_buffer.clear()

                if self.last_flush is None:
                    self.last_flush = observed_at

                logger.info(
                    "[POSTURE] Entered scheduled milking window: %s",
                    schedule.get("label"),
                )

            self.flush_if_due()
            return

        with self._milking_state_lock:
            leaving_milking = self._in_milking_window

            if leaving_milking:
                self._in_milking_window = False

        if leaving_milking:
            logger.info(
                "[POSTURE] Left scheduled milking window; resuming posture"
            )

        # Finalize an expired partial snapshot before collecting this frame.
        self._maybe_finalize_minute(now_mono)

        last = self.last_sample.get(camera_id, 0.0)

        if now_mono - last < self.SAMPLE_INTERVAL_SECONDS:
            return

        self.last_sample[camera_id] = now_mono

        # YOLO inference happens OUTSIDE _minute_lock.
        sample = self.detector.detect(
            frame,
            camera,
            self.runtime_config["farm_id"],
            self.runtime_config["device_id"],
            observed_at,
        )

        snapshot_to_build = None

        with self._minute_lock:
            if not self.current_minute_samples:
                self.current_minute_started_at = now_mono

            logger.debug(
                "[SCHEDULER] %s standing=%d feeding=%d",
                sample.camera_code,
                sample.standing_count,
                sample.feeding_count,
            )

            self.current_minute_samples[sample.camera_id] = sample

            if len(self.current_minute_samples) == len(
                self.posture_camera_ids
            ):
                snapshot_to_build = self._take_minute_window_locked()

        # Build snapshot OUTSIDE _minute_lock.
        if snapshot_to_build is not None:
            self.build_minute_snapshot(snapshot_to_build)

        self.flush_if_due()

    def _maybe_finalize_minute(self, now_mono: float):
        """
        Finalize a partial snapshot when the window expires.

        The current snapshot state is detached atomically under _minute_lock.
        Snapshot construction happens after releasing the lock.
        """

        snapshot_to_build = None

        with self._minute_lock:
            if not self.current_minute_samples:
                return

            if self.current_minute_started_at is None:
                return

            if (
                now_mono - self.current_minute_started_at
            ) < self.SAMPLE_INTERVAL_SECONDS:
                return

            snapshot_to_build = self._take_minute_window_locked()

        if snapshot_to_build is not None:
            self.build_minute_snapshot(snapshot_to_build)

    def _take_minute_window(self) -> Optional[Dict[str, PostureSample]]:
        """
        Atomically detach the current snapshot window.

        The shared minute-window state is copied and cleared while holding
        _minute_lock. Snapshot construction happens after the lock is released.
        """

        with self._minute_lock:
            if not self.current_minute_samples:
                return None

            samples = dict(self.current_minute_samples)

            self.current_minute_samples.clear()
            self.current_minute_started_at = None

            return samples

    def _take_minute_window_locked(
        self,
    ) -> Optional[Dict[str, PostureSample]]:
        """
        Detach the current snapshot window.

        Caller MUST already hold _minute_lock.
        """

        if not self.current_minute_samples:
            return None

        samples = dict(self.current_minute_samples)

        self.current_minute_samples.clear()
        self.current_minute_started_at = None

        return samples

    def _clear_minute_window(self):
        """
        Safely discard the current snapshot window.

        This is retained for state-reset paths such as scheduled milking.
        """

        with self._minute_lock:
            self.current_minute_samples.clear()
            self.current_minute_started_at = None

    def build_minute_snapshot(
        self,
        samples: Dict[str, PostureSample],
    ):
        """
        Merge all camera samples for this minute into one snapshot.

        `samples` is an immutable snapshot of the shared minute-window state.
        The caller must not modify it while this method is running.
        """

        standing = 0
        feeding = 0
        detected_laying = 0
        camera_breakdown: Dict[str, dict] = {}

        for sample in samples.values():

            standing += sample.standing_count
            feeding += sample.feeding_count
            detected_laying += sample.detected_laying_count

            camera_breakdown[sample.camera_code] = {
                "camera_id": sample.camera_id,
                "standing": sample.standing_count,
                "feeding": sample.feeding_count,
            }

        expected = len(self.posture_camera_ids)
        received = len(samples)

        if received < expected:

            logger.warning(
                "[POSTURE] Partial minute snapshot: %d/%d cameras",
                received,
                expected,
            )

        herd_size = max(
            s.herd_size
            for s in samples.values()
        )

        observed_at = max(
            s.observed_at
            for s in samples.values()
        )

        snapshot = MinuteAggregation(
            observed_at=observed_at,
            standing_count=standing,
            feeding_count=feeding,
            detected_laying_count=detected_laying,
            herd_size=herd_size,
            camera_breakdown=camera_breakdown,
        )

        with self.pen_buffer.lock:
            self.pen_buffer.add(snapshot)
            buffer_size = self.pen_buffer.size

        if self.last_flush is None:
            self.last_flush = observed_at

        logger.info(
            "[POSTURE][1MIN] %s | standing=%d feeding=%d | cameras=%d/%d | buffer=%d/20",
            observed_at.strftime("%H:%M"),
            standing,
            feeding,
            received,
            expected,
            buffer_size,
        )

    def flush_if_due(self):
        """
        Write aggregated pen observation every
        DB_WRITE_INTERVAL_SECONDS.

        Locked so concurrent camera threads cannot insert duplicates.
        """

        with self._flush_lock:
            now = datetime.now(timezone.utc)

            if self.last_flush is None:
                return

            if (
                now - self.last_flush
            ).total_seconds() < self.DB_WRITE_INTERVAL_SECONDS:
                return

            try:

                self.aggregate_pen()

            except Exception:

                logger.exception(
                    "Failed flushing posture pen buffer"
                )

                return

            self.last_flush = now

    def flush_all(self):
        """
        Flush pen buffer (e.g. on shutdown).
        """

        with self._flush_lock:
            self.aggregate_pen()
            self.last_flush = datetime.now(timezone.utc)

    def aggregate_pen(self):
        """
        Aggregate pen buffer into one DB observation.

        During scheduled milking: write zeros with mode=MILKING.
        Otherwise: normal standing/feeding/laying aggregation.
        """

        now = datetime.now(timezone.utc)
        schedule = self._active_milking_schedule(now)

        if schedule is not None:
            self._write_scheduled_milking_observation(now, schedule)
            with self.pen_buffer.lock:
                self.pen_buffer.clear()
            return

        with self.pen_buffer.lock:

            if not self.pen_buffer.snapshots:

                logger.debug(
                    "No minute snapshots in pen buffer"
                )

                return

            snapshots = list(self.pen_buffer.snapshots)

        logger.debug(
            "[POSTURE] Aggregating %d snapshots",
            len(snapshots),
        )

        for i, snapshot in enumerate(snapshots, start=1):
            logger.debug(
                "[SNAPSHOT %02d] standing=%d feeding=%d cameras=%s",
                i,
                snapshot.standing_count,
                snapshot.feeding_count,
                snapshot.camera_breakdown,
            )

        snapshot_count = len(snapshots)

        avg_standing = round(
            sum(s.standing_count for s in snapshots)
            / snapshot_count
        )

        avg_feeding = round(
            sum(s.feeding_count for s in snapshots)
            / snapshot_count
        )

        herd_size = snapshots[0].herd_size

        laying_count = max(
            herd_size - avg_standing - avg_feeding,
            0,
        )

        if herd_size == 0:
            standing_percentage = 0.0
            laying_percentage = 0.0
        else:
            standing_percentage = round(
                avg_standing / herd_size * 100,
                2,
            )
            laying_percentage = round(
                laying_count / herd_size * 100,
                2,
            )

        camera_stats: Dict[str, dict] = {}

        for snapshot in snapshots:

            for camera_code, counts in snapshot.camera_breakdown.items():

                if camera_code not in camera_stats:

                    camera_stats[camera_code] = {
                        "camera_id": counts["camera_id"],
                        "standing": 0,
                        "feeding": 0,
                    }

                camera_stats[camera_code]["standing"] += counts["standing"]
                camera_stats[camera_code]["feeding"] += counts["feeding"]

        for stats in camera_stats.values():

            stats["standing"] = round(
                stats["standing"] / snapshot_count
            )

            stats["feeding"] = round(
                stats["feeding"] / snapshot_count
            )

        expected_cameras = len(self.posture_camera_ids)

        received_cameras = round(
            sum(
                len(s.camera_breakdown)
                for s in snapshots
            )
            / snapshot_count
        )

        observation = PostureObservation(
            farm_id=self.runtime_config["farm_id"],
            zone_id=self.pen_rest_zone_id,
            device_id=self.runtime_config["device_id"],
            observed_at=snapshots[-1].observed_at,
            standing_count=avg_standing,
            feeding_count=avg_feeding,
            laying_count=laying_count,
            standing_percentage=standing_percentage,
            laying_percentage=laying_percentage,
            metadata={
                "mode": "NORMAL",
                "herd_size": herd_size,
                "expected_cameras": expected_cameras,
                "received_cameras": received_cameras,
                "aggregation_window_minutes": snapshot_count,
                "cameras": camera_stats,
            },
        )

        logger.info(
            "[POSTURE][MODE] inside_schedule=False mode=NORMAL"
        )

        logger.info(
            "[POSTURE][10MIN] Writing observation -> "
            "standing=%d feeding=%d laying=%d "
            "standing%%=%.1f laying%%=%.1f mode=NORMAL",
            avg_standing,
            avg_feeding,
            laying_count,
            standing_percentage,
            laying_percentage,
        )

        logger.info(
            "[POSTURE FLUSH] standing=%d feeding=%d laying=%d",
            avg_standing,
            avg_feeding,
            laying_count,
        )

        self.db.insert_observation(observation)

        with self.pen_buffer.lock:
            self.pen_buffer.clear()

    def _write_scheduled_milking_observation(
        self,
        observed_at: datetime,
        schedule: dict,
    ) -> None:
        """
        Write a synthetic MILKING posture row (all counts zero).
        """

        label = schedule.get("label", "Milking")

        observation = PostureObservation(
            farm_id=self.runtime_config["farm_id"],
            zone_id=self.pen_rest_zone_id,
            device_id=self.runtime_config["device_id"],
            observed_at=observed_at,
            standing_count=0,
            feeding_count=0,
            laying_count=0,
            standing_percentage=0.0,
            laying_percentage=0.0,
            metadata={
                "mode": "MILKING",
                "reason": "scheduled_milking",
                "inside_schedule": True,
                "schedule_label": label,
            },
        )

        logger.info(
            "[POSTURE][MODE] inside_schedule=True "
            "schedule_label=%s mode=MILKING reason=scheduled_milking",
            label,
        )

        logger.info(
            "[POSTURE][10MIN] Writing observation -> "
            "standing=0 feeding=0 laying=0 "
            "standing%%=0.0 laying%%=0.0 mode=MILKING"
        )

        logger.info(
            "[POSTURE FLUSH] standing=0 feeding=0 laying=0"
        )

        self.db.insert_observation(observation)
