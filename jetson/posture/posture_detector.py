"""
jetson/posture/posture_detector.py

Cow Posture Detector.

Responsibilities
----------------
- Run posture model inference
- Assign standing cows to REST / FEEDING ROIs
- Build PostureSample (per-camera counts)

No scheduling.
No database access.
No aggregation.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import List

from runtime.model_loader import ModelRunner

from .posture_models import (
    PostureSample,
    StandingCow,
)

from .posture_utils import (
    assign_detection_to_zone,
)

logger = logging.getLogger(__name__)

STANDING_CLASSES = {
    "cow_standing",
    "standing",
}

CONFIDENCE_THRESHOLD = 0.50


class PostureDetector:
    """
    Detect standing cows for one camera.
    """

    def __init__(
        self,
        model_runner: ModelRunner,
        herd_size: int,
        confidence_threshold: float = CONFIDENCE_THRESHOLD,
    ):
        self.model = model_runner

        self.herd_size = herd_size

        self.confidence_threshold = confidence_threshold

        logger.info(
            "Posture detector initialized "
            "(herd_size=%d, confidence=%.2f)",
            herd_size,
            confidence_threshold,
        )

    def detect(
        self,
        frame,
        camera: dict,
        farm_id: str,
        device_id: str,
        observed_at: datetime,
    ) -> PostureSample:
        """
        Detect standing cows for one camera.
        """

        detections = self.model.infer(frame)

        standing_cows = self._filter_standing_detections(
            detections
        )

        posture = camera["activity_zones"].get("POSTURE")

        if posture is None:
            raise ValueError(
                f"Camera {camera['code']} has no POSTURE configuration."
            )

        frame_height, frame_width = frame.shape[:2]

        standing_count = 0
        feeding_count = 0

        for cow in standing_cows:

            zone = assign_detection_to_zone(
                {
                    "bbox": cow.bbox,
                },
                posture,
                frame_width=frame_width,
                frame_height=frame_height,
            )

            zone_type = zone.get("zone_type")

            if zone_type == "REST":
                standing_count += 1

            elif zone_type == "FEEDING":
                feeding_count += 1

        logger.info(
            "[POSTURE] %s standing=%d feeding=%d",
            camera["code"],
            standing_count,
            feeding_count,
        )

        return PostureSample(
            farm_id=farm_id,
            device_id=device_id,
            camera_id=camera["camera_id"],
            camera_code=camera["code"],
            observed_at=observed_at,
            herd_size=self.herd_size,
            standing_count=standing_count,
            feeding_count=feeding_count,
        )

    def _filter_standing_detections(
        self,
        detections: List[dict],
    ) -> List[StandingCow]:
        """
        Keep standing-class detections above confidence threshold.
        """

        cows: List[StandingCow] = []

        for det in detections:

            confidence = float(det.get("confidence", 0.0))

            if confidence < self.confidence_threshold:
                continue

            class_name = str(det.get("class", "")).lower()

            if class_name not in STANDING_CLASSES:
                continue

            cows.append(
                StandingCow(
                    bbox=det["bbox"],
                    confidence=confidence,
                )
            )

        return cows
