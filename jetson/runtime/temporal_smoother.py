"""
jetson/runtime/temporal_smoother.py
Temporal Smoother (Time-Based)

Time-based START/END debouncing for activity signals.
Deterministic, FPS-independent, works for both LIVE and BATCH processing.

Key Features:
    - Time-based debouncing (not frame-based)
    - Configurable START and END thresholds (seconds)
    - Deterministic: same input → same output
    - Works for both LIVE (real-time) and BATCH (video file) modes
    - Pure signal processing (no activity tracking)

Usage:
    from runtime.temporal_smoother import TemporalSmoother
    
    smoother = TemporalSmoother(start_sec=3.0, end_sec=5.0)
    result = smoother.update(condition, timestamp)
    # Returns: "START", "END", or None (semantic signals)
"""

import time


class TemporalSmoother:
    """
    Time-based temporal smoothing for activity signals.
    
    Debounces START/END events based on duration, not frame count.
    Prevents false positives from transient detections.
    """
    
    def __init__(self, start_sec=3.0, end_sec=5.0):
        """
        Initialize temporal smoother with time-based thresholds.
        
        Args:
            start_sec: Duration (seconds) condition must be true to trigger START (default: 3.0)
            end_sec: Duration (seconds) condition must be false to trigger END (default: 5.0)
        """
        self.start_sec = start_sec
        self.end_sec = end_sec
        self.active = False
        self.true_since = None
        self.false_since = None

    def update(self, condition, timestamp=None):
        """
        Update smoother with new condition state.
        
        Time-based debouncing:
        - START: Condition stays true for start_sec seconds
        - END: Condition stays false for end_sec seconds
        - FPS-independent (uses wall-clock or video timestamp)
        
        Args:
            condition: Boolean condition (True = activity present, False = activity absent)
            timestamp: Optional timestamp (defaults to time.time() if not provided)
            
        Returns:
            "START": Activity transitioned from absent → present (sustained start_sec)
            "END": Activity transitioned from present → absent (sustained end_sec)
            None: No state change
        """
        if timestamp is None:
            timestamp = time.time()

        # --- START logic: transition from INACTIVE to ACTIVE ---
        if not self.active:
            if condition:
                if self.true_since is None:
                    self.true_since = timestamp
                elif timestamp - self.true_since >= self.start_sec:
                    self.active = True
                    self.true_since = None
                    self.false_since = None
                    return "START"
            else:
                self.true_since = None

        # --- END logic: transition from ACTIVE to INACTIVE ---
        else:
            if not condition:
                if self.false_since is None:
                    self.false_since = timestamp
                elif timestamp - self.false_since >= self.end_sec:
                    self.active = False
                    self.false_since = None
                    self.true_since = None
                    return "END"
            else:
                self.false_since = None

        return None
