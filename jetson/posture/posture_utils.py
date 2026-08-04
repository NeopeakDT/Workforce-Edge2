"""
jetson/posture/posture_utils.py

Stateless helper functions for Cow Posture Detection.

Responsibilities
----------------
- ROI conversion
- Zone assignment (REST / FEEDING)

No model loading.
No database access.
No scheduler logic.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import cv2
import numpy as np

logger = logging.getLogger(__name__)

# Fraction of bbox area that must overlap the ROI polygon.
FEEDING_OVERLAP_THRESHOLD = 0.18
REST_OVERLAP_THRESHOLD = 0.30

# ---------------------------------------------------------
# ROI Helpers
# ---------------------------------------------------------


def build_pixel_roi(
    normalized_roi: List[Dict],
    frame_width: int,
    frame_height: int,
) -> List[Tuple[int, int]]:
    """
    Convert normalized ROI into pixel coordinates.
    """

    return [
        (
            int(point["x"] * frame_width),
            int(point["y"] * frame_height),
        )
        for point in normalized_roi
    ]


# ---------------------------------------------------------
# Bounding Box Helpers
# ---------------------------------------------------------


def bbox_center(bbox):
    """
    Returns center point of bbox.
    """

    x1, y1, x2, y2 = bbox

    return (
        (x1 + x2) / 2,
        (y1 + y2) / 2,
    )


def point_inside_polygon(
    point,
    polygon,
):
    """
    Returns True if point lies inside polygon.
    """

    return (
        cv2.pointPolygonTest(
            np.asarray(polygon, dtype=np.int32),
            point,
            False,
        )
        >= 0
    )


def polygon_overlap_ratio(
    bbox,
    polygon,
):
    """
    Returns the fraction of bbox area lying inside ROI.
    """

    x1, y1, x2, y2 = map(int, bbox)

    bbox_mask = np.zeros(
        (
            max(y2 + 5, polygon[:, 1].max() + 5),
            max(x2 + 5, polygon[:, 0].max() + 5),
        ),
        dtype=np.uint8,
    )

    roi_mask = np.zeros_like(bbox_mask)

    cv2.rectangle(
        bbox_mask,
        (x1, y1),
        (x2, y2),
        255,
        -1,
    )

    cv2.fillPoly(
        roi_mask,
        [polygon.astype(np.int32)],
        255,
    )

    intersection = cv2.bitwise_and(
        bbox_mask,
        roi_mask,
    )

    bbox_pixels = cv2.countNonZero(bbox_mask)

    if bbox_pixels == 0:
        return 0.0

    overlap = cv2.countNonZero(intersection)

    return overlap / bbox_pixels


# ---------------------------------------------------------
# Zone Assignment
# ---------------------------------------------------------


def _posture_zone_list(posture_zones) -> List[Dict[str, Any]]:
    """Accept POSTURE activity block or a plain zone list."""
    if isinstance(posture_zones, dict):
        return posture_zones.get("zones") or []
    return posture_zones or []


def _zone_polygon(
    zone: Dict[str, Any],
    frame_width: Optional[int],
    frame_height: Optional[int],
):
    pixel_polygon = zone.get("polygon")
    if pixel_polygon is not None:
        return pixel_polygon

    roi = zone.get("roi")
    if (
        roi is not None
        and frame_width is not None
        and frame_height is not None
    ):
        return build_pixel_roi(roi, frame_width, frame_height)

    return None


def assign_detection_to_zone(
    detection,
    posture_zones,
    *,
    frame_width=None,
    frame_height=None,
):

    bbox = detection["bbox"]

    feeding_overlap = 0.0
    rest_overlap = 0.0

    feeding_zone = None
    rest_zone = None

    for zone in _posture_zone_list(posture_zones):

        polygon = _zone_polygon(
            zone,
            frame_width,
            frame_height,
        )

        if polygon is None:
            continue

        overlap = polygon_overlap_ratio(
            bbox,
            np.asarray(polygon),
        )

        zone_type = zone["zone_type"].upper()

        if zone_type == "FEEDING":

            if overlap > feeding_overlap:
                feeding_overlap = overlap
                feeding_zone = zone

        elif zone_type == "REST":

            if overlap > rest_overlap:
                rest_overlap = overlap
                rest_zone = zone

    # Temporary debug: confirm whether FEEDING ROI ever intersects bboxes.
    logger.info(
        "[POSTURE][ROI_DEBUG] camera=%s "
        "bbox=%s "
        "feeding_zone=%s "
        "feeding_overlap=%.3f",
        detection.get("camera_code"),
        bbox,
        feeding_zone["zone_id"] if feeding_zone else None,
        feeding_overlap,
    )

    #
    # Decision
    #

    final_zone = None
    final_zone_id = None

    if (
        feeding_zone is not None
        and feeding_overlap >= FEEDING_OVERLAP_THRESHOLD
    ):
        final_zone = "FEEDING"
        final_zone_id = feeding_zone["zone_id"]

    elif (
        rest_zone is not None
        and rest_overlap >= REST_OVERLAP_THRESHOLD
    ):
        final_zone = "REST"
        final_zone_id = rest_zone["zone_id"]

    # Temporary debug: inspect real overlaps before changing thresholds.
    logger.info(
        "[POSTURE][ROI] "
        "camera=%s "
        "feeding_overlap=%.2f (threshold=%.2f) "
        "rest_overlap=%.2f (threshold=%.2f) "
        "assigned=%s "
        "feeding_zone=%s "
        "rest_zone=%s",
        detection.get("camera_code"),
        feeding_overlap,
        FEEDING_OVERLAP_THRESHOLD,
        rest_overlap,
        REST_OVERLAP_THRESHOLD,
        final_zone,
        feeding_zone["zone_id"] if feeding_zone else None,
        rest_zone["zone_id"] if rest_zone else None,
    )

    return {
        "zone_id": final_zone_id,
        "zone_type": final_zone,
        "feeding_overlap": feeding_overlap,
        "rest_overlap": rest_overlap,
    }
