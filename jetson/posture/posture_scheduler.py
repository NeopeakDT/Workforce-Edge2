"""
jetson/posture/posture_scheduler.py

Scheduler for Cow Posture Detection.

Responsibilities
----------------
- Collect per-camera samples into minute snapshots
- Buffer minute snapshots until DB flush
- Persist aggregated pen observations

No YOLO inference.
No ROI logic.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Set

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

        # camera_id -> last sample monotonic time
        self.last_sample: Dict[str, float] = {}

    @property
    def total_samples(self) -> int:
        with self.pen_buffer.lock:
            return self.pen_buffer.size

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
        """

        camera_id = camera["camera_id"]

        now_mono = time.time()

        self._maybe_finalize_minute(now_mono)

        last = self.last_sample.get(camera_id, 0.0)

        if now_mono - last < self.SAMPLE_INTERVAL_SECONDS:
            return

        self.last_sample[camera_id] = now_mono

        observed_at = datetime.now(timezone.utc)

        sample = self.detector.detect(
            frame,
            camera,
            self.runtime_config["farm_id"],
            self.runtime_config["device_id"],
            observed_at,
        )

        if not self.current_minute_samples:
            self.current_minute_started_at = now_mono

        logger.info(
            "[SCHEDULER] %s standing=%d feeding=%d",
            sample.camera_code,
            sample.standing_count,
            sample.feeding_count,
        )

        self.current_minute_samples[sample.camera_id] = sample

        if len(self.current_minute_samples) == len(self.posture_camera_ids):

            self.build_minute_snapshot()

            self._clear_minute_window()

        self.flush_if_due()

    def _maybe_finalize_minute(self, now_mono: float):
        """
        Finalize a partial minute when the window expires.
        """

        if not self.current_minute_samples:
            return

        if self.current_minute_started_at is None:
            return

        if (
            now_mono - self.current_minute_started_at
        ) < self.SAMPLE_INTERVAL_SECONDS:
            return

        self.build_minute_snapshot()

        self._clear_minute_window()

    def _clear_minute_window(self):
        self.current_minute_samples.clear()
        self.current_minute_started_at = None

    def build_minute_snapshot(self):
        """
        Merge all camera samples for this minute into one snapshot.
        """

        standing = 0
        feeding = 0
        camera_breakdown: Dict[str, dict] = {}

        for sample in self.current_minute_samples.values():

            standing += sample.standing_count
            feeding += sample.feeding_count

            camera_breakdown[sample.camera_code] = {
                "camera_id": sample.camera_id,
                "standing": sample.standing_count,
                "feeding": sample.feeding_count,
            }

        expected = len(self.posture_camera_ids)
        received = len(self.current_minute_samples)

        if received < expected:

            logger.warning(
                "[POSTURE] Partial minute snapshot: %d/%d cameras",
                received,
                expected,
            )

        herd_size = max(
            s.herd_size
            for s in self.current_minute_samples.values()
        )

        observed_at = max(
            s.observed_at
            for s in self.current_minute_samples.values()
        )

        snapshot = MinuteAggregation(
            observed_at=observed_at,
            standing_count=standing,
            feeding_count=feeding,
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
        """

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

        self.aggregate_pen()

    def aggregate_pen(self):
        """
        Aggregate pen buffer into one DB observation.
        """

        with self.pen_buffer.lock:

            if not self.pen_buffer.snapshots:

                logger.debug(
                    "No minute snapshots in pen buffer"
                )

                return

            snapshots = list(self.pen_buffer.snapshots)

        logger.info(
            "[POSTURE] Aggregating %d snapshots",
            len(snapshots),
        )

        for i, snapshot in enumerate(snapshots, start=1):
            logger.info(
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

        logger.info(
            "[POSTURE] Average standing=%d feeding=%d",
            avg_standing,
            avg_feeding,
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
                "herd_size": herd_size,
                "expected_cameras": expected_cameras,
                "received_cameras": received_cameras,
                "aggregation_window_minutes": snapshot_count,
                "cameras": camera_stats,
            },
        )

        logger.info(
            "[POSTURE][10MIN] Writing observation -> "
            "standing=%d feeding=%d laying=%d "
            "standing%%=%.1f laying%%=%.1f",
            avg_standing,
            avg_feeding,
            laying_count,
            standing_percentage,
            laying_percentage,
        )

        self.db.insert_observation(
            observation
        )

        with self.pen_buffer.lock:
            self.pen_buffer.clear()
