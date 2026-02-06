#!/usr/bin/env python3
"""
FROZEN PARAMETERS — STEP 0

These constants are frozen and should NOT be tuned yet.
All timing parameters are in seconds unless otherwise specified.
"""

# ------------------------------------------------------------------
# Edge Processing Parameters
# ------------------------------------------------------------------
# Current FPS for testing purposes, to be changed later
PROCESS_FPS = 10

# ------------------------------------------------------------------
# Activity Lifecycle Parameters (seconds)
# ------------------------------------------------------------------
START_CONFIRM_SEC = 3
END_CONFIRM_SEC = 5
FRAME_AGGREGATE_INTERVAL_SEC = 10

# ------------------------------------------------------------------
# Backend Aggregation Parameters
# ------------------------------------------------------------------
MERGE_GAP_MIN = 10  # minutes
STALE_CLOSE_MIN = 15  # minutes
