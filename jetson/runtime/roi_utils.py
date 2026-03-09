# jetson/runtime/roi_utils.py
import cv2
import numpy as np


def bbox_roi_overlap(box, roi_polygon, frame_shape, min_overlap_ratio=0.02, roi_mask=None):
    """
    Fast bbox-ROI overlap using mask intersection.

    If roi_mask is provided, it is used directly to avoid re-allocating
    a full-frame ROI mask for every detection.
    """
    h, w = frame_shape[:2]
    x1, y1, x2, y2 = map(int, box)

    x1 = max(0, min(w - 1, x1))
    y1 = max(0, min(h - 1, y1))
    x2 = max(0, min(w, x2))
    y2 = max(0, min(h, y2))

    if x2 <= x1 or y2 <= y1:
        return False

    if roi_mask is None:
        roi_mask = np.zeros((h, w), dtype=np.uint8)
        cv2.fillPoly(roi_mask, [roi_polygon], 1)

    intersection = int(roi_mask[y1:y2, x1:x2].sum())
    bbox_area = (x2 - x1) * (y2 - y1)

    if bbox_area == 0:
        return False

    overlap_ratio = intersection / bbox_area
    return overlap_ratio >= min_overlap_ratio


def filter_by_roi(detections, roi_polygon, frame_shape, min_overlap_ratio=0.03, roi_mask=None):
    """
    Filter detections using bbox overlap logic.

    If roi_mask is provided, it is reused for all detections in the frame.
    """
    if not roi_polygon:
        return detections

    filtered = []

    if roi_mask is not None:
        for d in detections:
            if bbox_roi_overlap(
                d["bbox"],
                roi_polygon,
                frame_shape,
                min_overlap_ratio=min_overlap_ratio,
                roi_mask=roi_mask,
            ):
                filtered.append(d)
    else:
        poly = np.array(roi_polygon, dtype=np.int32)
        for d in detections:
            if bbox_roi_overlap(
                d["bbox"],
                poly,
                frame_shape,
                min_overlap_ratio=min_overlap_ratio,
                roi_mask=None,
            ):
                filtered.append(d)

    return filtered
