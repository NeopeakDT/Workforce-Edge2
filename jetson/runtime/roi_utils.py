import cv2
import numpy as np


def filter_by_roi(detections, roi_polygon):
    """
    Filter detections inside polygon.
    Uses OpenCV pointPolygonTest (FAST, no shapely).
    """

    if not roi_polygon:
        return detections

    poly = np.array(roi_polygon, dtype=np.int32)

    filtered = []

    for d in detections:
        x1, y1, x2, y2 = d["bbox"]
        cx = int((x1 + x2) / 2)
        cy = int((y1 + y2) / 2)

        # pointPolygonTest returns:
        # >0 inside
        # 0  on edge
        # <0 outside
        inside = cv2.pointPolygonTest(poly, (cx, cy), False)

        if inside >= 0:
            filtered.append(d)

    return filtered
