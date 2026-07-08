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

from typing import Any, Dict, List, Optional, Tuple

import cv2
import numpy as np

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


def detection_inside_roi(
    bbox,
    polygon,
):
    """
    Uses bbox center as ROI check.
    """

    return point_inside_polygon(
        bbox_center(bbox),
        polygon,
    )


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
    frame_width: Optional[int] = None,
    frame_height: Optional[int] = None,
) -> Dict[str, Optional[str]]:
    """
    Map one detection to a posture zone.

    posture_zones: POSTURE block from local_cache
        {"zones": [{zone_id, zone_type, roi|polygon}, ...]}
        or a plain list of zone dicts.

    Returns:
        {"zone_id": "...", "zone_type": "REST"|"FEEDING"|"DEFAULT"|None}

    When a detection falls in multiple zones, FEEDING wins over REST.
    """

    bbox = detection["bbox"]
    matches: List[Dict[str, Any]] = []

    for zone in _posture_zone_list(posture_zones):
        pixel_polygon = _zone_polygon(zone, frame_width, frame_height)
        if pixel_polygon is None:
            continue
        if detection_inside_roi(bbox, pixel_polygon):
            matches.append(zone)

    if not matches:
        return {
            "zone_id": None,
            "zone_type": None,
        }

    priority = {
        "FEEDING": 0,
        "REST": 1,
        "DEFAULT": 2,
    }
    best = min(
        matches,
        key=lambda z: priority.get(str(z.get("zone_type", "DEFAULT")).upper(), 99),
    )

    return {
        "zone_id": best.get("zone_id"),
        "zone_type": str(best.get("zone_type", "DEFAULT")).upper(),
    }
