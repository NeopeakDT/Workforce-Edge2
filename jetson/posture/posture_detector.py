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

LAYING_CLASSES = {
    "cow_laying",
    "cow_lying",
    "lying",
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

        detected_laying_count = sum(
            1
            for det in detections
            if (
                str(det["class"]).lower() in LAYING_CLASSES
                and float(det["confidence"]) >= self.confidence_threshold
            )
        )

        logger.debug(
            "[POSTURE RAW] %s total_detections=%d",
            camera["code"],
            len(detections),
        )

        for det in detections:
            logger.debug(
                "[RAW] class=%s conf=%.2f bbox=%s",
                det["class"],
                det["confidence"],
                det["bbox"],
            )

        standing_cows = self._filter_standing_detections(
            detections
        )

        logger.debug(
            "[POSTURE FILTER] %s standing=%d",
            camera["code"],
            len(standing_cows),
        )

        posture = camera["activity_zones"].get("POSTURE")

        if posture is None:
            raise ValueError(
                f"Camera {camera['code']} has no POSTURE configuration."
            )

        frame_height, frame_width = frame.shape[:2]

        classified_cows = []

        for cow in standing_cows:

            zone = assign_detection_to_zone(
                {
                    "bbox": cow.bbox,
                    "camera_code": camera["code"],
                },
                posture,
                frame_width=frame_width,
                frame_height=frame_height,
            )

            logger.debug(
                "[ROI] %s conf=%.2f rest=%.2f feeding=%.2f final=%s",
                cow.bbox,
                cow.confidence,
                zone["rest_overlap"],
                zone["feeding_overlap"],
                zone["zone_type"],
            )

            classified_cows.append(
                {
                    "bbox": cow.bbox,
                    "confidence": cow.confidence,
                    "zone": zone["zone_type"],
                    "rest_overlap": zone["rest_overlap"],
                    "feeding_overlap": zone["feeding_overlap"],
                }
            )

        standing_count = sum(
            1 for c in classified_cows if c["zone"] == "REST"
        )

        feeding_count = sum(
            1 for c in classified_cows if c["zone"] == "FEEDING"
        )

        logger.debug(
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
            detected_laying_count=detected_laying_count,
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
