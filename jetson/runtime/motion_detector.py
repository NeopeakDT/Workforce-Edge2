"""
runtime/motion_detector.py

Reusable motion detector for tracked objects.
No dependency on model, ROI, or activity logic.

Tracks centroid + area velocity per object_id.
"""

import time
import math


class MotionDetector:
    def __init__(
        self,
        velocity_threshold_px=8,
        area_velocity_threshold=1500,
        min_motion_duration_sec=3.0,
    ):
        self.velocity_threshold = velocity_threshold_px
        self.area_velocity_threshold = area_velocity_threshold
        self.min_duration = min_motion_duration_sec

        # memory structure:
        # {
        #   object_id: {
        #       prev_centroid,
        #       prev_area,
        #       prev_ts,
        #       moving_since
        #   }
        # }
        self.memory = {}

    # ---------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------

    @staticmethod
    def _centroid(bbox):
        x1, y1, x2, y2 = bbox
        return ((x1 + x2) / 2, (y1 + y2) / 2)

    @staticmethod
    def _euclidean(a, b):
        return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

    @staticmethod
    def _area(bbox):
        x1, y1, x2, y2 = bbox
        return max(0, (x2 - x1)) * max(0, (y2 - y1))

    # ---------------------------------------------------------
    # Core API
    # ---------------------------------------------------------

    def update(self, object_id, bbox, current_ts=None):
        """
        Update motion state for a single object.

        Args:
            object_id: stable tracking ID (required for production)
            bbox: [x1, y1, x2, y2]
            current_ts: timestamp (defaults to time.time())

        Returns:
            is_moving_sustained: bool
        """

        if current_ts is None:
            current_ts = time.time()

        cx, cy = self._centroid(bbox)
        current_area = self._area(bbox)

        mem = self.memory.get(object_id)

        # First observation
        if mem is None:
            self.memory[object_id] = {
                "prev_centroid": (cx, cy),
                "prev_area": current_area,
                "prev_ts": current_ts,
                "moving_since": None,
            }
            return False

        delta_t = max(current_ts - mem["prev_ts"], 1e-6)

        displacement = self._euclidean(mem["prev_centroid"], (cx, cy))
        velocity = displacement / delta_t

        area_delta = abs(current_area - mem["prev_area"])
        area_velocity = area_delta / delta_t

        # Update memory
        mem["prev_centroid"] = (cx, cy)
        mem["prev_area"] = current_area
        mem["prev_ts"] = current_ts

        # FIX 1: Safe motion detection with null checks (production-grade)
        translation_motion = (
            velocity is not None and
            self.velocity_threshold is not None and
            velocity >= self.velocity_threshold
        )
        area_motion = (
            area_velocity is not None and
            self.area_velocity_threshold is not None and
            area_velocity >= self.area_velocity_threshold
        )

        if translation_motion or area_motion:
            if mem["moving_since"] is None:
                mem["moving_since"] = current_ts
            elif current_ts - mem["moving_since"] >= self.min_duration:
                return True
        else:
            mem["moving_since"] = None

        return False

    # ---------------------------------------------------------
    # Cleanup
    # ---------------------------------------------------------

    def cleanup(self, active_ids):
        """
        Remove stale objects not detected in current frame.
        """
        for oid in list(self.memory.keys()):
            if oid not in active_ids:
                self.memory.pop(oid)
