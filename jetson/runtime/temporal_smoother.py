"""
jetson/runtime/temporal_smoother.py
Temporal Smoother

START/END debouncing for detection signals. Converts noisy frame-level detections
into stable activity signals using sliding window approach.

Key Features:
    - Converts noisy frames → stable signals
    - START/END debouncing (prevents false positives)
    - Sliding window buffer for temporal consistency
    - No activities, no timestamps (pure signal processing)

Usage:
    from runtime.temporal_smoother import TemporalSmoother
    
    smoother = TemporalSmoother(window=5)
    result = smoother.update(detections)
    # Returns: "START", "END", or None (semantic signals)
"""

from collections import deque


class TemporalSmoother:
    """
    Temporal smoothing for detection signals.
    
    Uses sliding window to debounce START/END events and prevent false positives
    from noisy frame-level detections.
    """
    
    def __init__(self, window=5):
        """
        Initialize temporal smoother.
        
        Args:
            window: Size of sliding window buffer (default: 5 frames)
        """
        self.window = window
        self.buffer = deque(maxlen=window)
        self.active = False
    
    def update(self, detections):
        """
        Update smoother with new frame detections.
        
        Converts noisy frame-level detections into stable activity signals.
        Returns semantic signal when activity state changes.
        
        Args:
            detections: List of detections (empty list = no detection)
            
        Returns:
            "START": Activity likely started (semantic signal)
            "END": Activity likely ended (semantic signal)
            None: No state change
        
        Logic:
            - START: Need 4+ detections in window (out of 5)
            - END: Need 1 or fewer detections in window (out of 5)
            - Prevents false positives from single-frame noise
        """
        present = bool(detections)
        self.buffer.append(present)
        
        if not self.active and sum(self.buffer) >= 4:
            self.active = True
            return "START"
        
        if self.active and sum(self.buffer) <= 1:
            self.active = False
            return "END"
        
        return None
