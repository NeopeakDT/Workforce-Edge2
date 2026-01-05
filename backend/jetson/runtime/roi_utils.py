"""
ROI (Region of Interest) Utilities

Pure geometry filtering for detections. Filters detections to only process
relevant regions of the frame using polygon-based ROI.

Key Features:
    - Geometry-only filtering (no class semantics)
    - Deterministic point-in-polygon checks
    - Filters detections by bounding box center point

Usage:
    from runtime.roi_utils import filter_by_roi
    
    roi_polygon = [(0, 0), (640, 0), (640, 480), (0, 480)]  # Full frame
    filtered = filter_by_roi(detections, roi_polygon)
"""

from shapely.geometry import Point, Polygon


def filter_by_roi(detections, roi_polygon):
    """
    Filter detections to only include those within ROI polygon.
    
    Uses bounding box center point for point-in-polygon check.
    Pure geometry - no class semantics, deterministic.
    
    Args:
        detections: List of detection dictionaries with "bbox" key
        roi_polygon: List of (x, y) tuples defining polygon vertices
        
    Returns:
        List of detection dictionaries that are within ROI
        
    Example:
        >>> detections = [
        ...     {"bbox": [100, 50, 200, 150], "class": "milking", ...},
        ...     {"bbox": [500, 300, 600, 400], "class": "scraping", ...}
        ... ]
        >>> roi = [(0, 0), (400, 0), (400, 300), (0, 300)]  # Left half
        >>> filtered = filter_by_roi(detections, roi)
        >>> # Returns only first detection (center at 150, 100 - inside ROI)
    """
    poly = Polygon(roi_polygon)
    out = []
    
    for d in detections:
        x1, y1, x2, y2 = d["bbox"]
        cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
        
        if poly.contains(Point(cx, cy)):
            out.append(d)
    
    return out
