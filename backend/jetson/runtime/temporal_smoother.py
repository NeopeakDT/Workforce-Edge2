"""
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
    # Returns: {"type": "START_CANDIDATE"}, {"type": "END_CANDIDATE"}, 
    #          {"type": "FRAME_AGGREGATE"}, or None
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
        Returns event type when activity state changes.
        
        Args:
            detections: List of detections (empty list = no detection)
            
        Returns:
            Dict with "type" key:
                - "START_CANDIDATE": Activity likely started
                - "END_CANDIDATE": Activity likely ended
                - "FRAME_AGGREGATE": Activity ongoing
            None: No significant signal
        
        Logic:
            - START: Need 3+ detections in window (out of 5)
            - END: Need 1 or fewer detections in window (out of 5)
            - Prevents false positives from single-frame noise
        """
        present = bool(detections)
        self.buffer.append(present)
        
        if not self.active and sum(self.buffer) >= 3:
            self.active = True
            return {"type": "START_CANDIDATE"}
        
        if self.active and sum(self.buffer) <= 1:
            self.active = False
            return {"type": "END_CANDIDATE"}
        
        if self.active:
            return {"type": "FRAME_AGGREGATE"}
        
        return None
