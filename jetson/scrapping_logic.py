#!/usr/bin/env python3
"""
jetson/scrapping_logic.py

Pure, dependency-free SCRAPPING detection logic - no GStreamer/CUDA/queue/
threading imports, so it can be unit-tested in isolation and exercised
against real footage before touching the production edge loop.

Used by:
    - jetson/test_scrapping_logic.py       (unit tests, no video/model)
    - jetson/test_scrapping_video_cpu.py   (video smoke test, real YOLO)
    - jetson/edge_detector.py              (production, once validated)

Fix for the recurring false-positive UNSCHEDULED scrapping activity
(reviewed on the `scrapping-detection-alerts` branch, Aug 2026):

    detect_scrapping() previously confirmed "real evidence" from raw
    person+tool bbox proximity alone, gated only by the generic YOLO
    inference confidence floor (~0.65). Both confirmed false positives in
    production sat at 0.68/0.70 confidence - directly on top of that floor.
    A per-class confidence floor for `scrapping_tool` (SCRAP_TOOL_MIN_CONFIDENCE)
    is now enforced here, tighter than the generic model threshold.

    Separately, the state machine's start confirmation
    (SCRAP_START_CONFIRM_SEC + SCRAP_START_GAP_TOLERANCE_SEC) could be
    satisfied by as few as one or two isolated detections spaced up to
    SCRAP_START_GAP_TOLERANCE_SEC apart - elapsed time, not sustained
    evidence. SCRAP_START_MIN_HITS now requires the evidence to actually
    recur a minimum number of times within the confirm window before a
    START_CANDIDATE is confirmed.

    SCRAP_END_GRACE_SEC is left unchanged (30.0s) - it was raised on
    purpose (see git history: commit c12db3f) to avoid fragmenting real
    multi-camera scrapes into multiple short instances. Loosening the
    START condition instead of touching the END condition avoids
    reopening that problem.
"""

import math

# ------------------------------------------------------------------
# Frozen-ish parameters (mirrors jetson/edge_detector.py SCRAP_* constants)
# ------------------------------------------------------------------
SCRAP_START_CONFIRM_SEC = 3.0
SCRAP_START_GAP_TOLERANCE_SEC = 2.0
SCRAP_END_GRACE_SEC = 30.0

# New in the Aug 2026 false-positive fix.
SCRAP_TOOL_MIN_CONFIDENCE = 0.75
SCRAP_START_MIN_HITS = 3


def bbox_center(b):
    x1, y1, x2, y2 = b
    return ((x1 + x2) / 2.0, (y1 + y2) / 2.0)


def euclidean(a, b):
    return math.hypot(a[0] - b[0], a[1] - b[1])


def detect_scrapping(
    detections,
    person_classes,
    tool_classes,
    max_dist,
    tool_min_confidence=SCRAP_TOOL_MIN_CONFIDENCE,
):
    """
    Detect real scrapping evidence.

    A raw scrapping hit requires:
      1. A person detection
      2. A scrapping_tool / shovel detection with confidence >= tool_min_confidence
      3. Person center within max_dist pixels of tool center

    Returns:
        (detected, evidence_detections)
    """
    persons = [
        d for d in (detections or [])
        if str(d.get("class", "")).lower() in person_classes
    ]

    tools = [
        d for d in (detections or [])
        if str(d.get("class", "")).lower() in tool_classes
        and float(d.get("confidence", 0.0)) >= tool_min_confidence
    ]

    for person in persons:
        person_center = bbox_center(person["bbox"])

        for tool in tools:
            tool_center = bbox_center(tool["bbox"])

            if euclidean(person_center, tool_center) <= max_dist:
                return True, [person, tool]

    return False, []


def new_scrapping_state():
    """Fresh per-camera SCRAPPING state dict."""
    return {
        "state": "INACTIVE",
        "candidate_since": None,
        "candidate_last_detected": None,
        "candidate_hit_count": 0,
        "end_candidate_since": None,
        "last_evidence_detections": [],
    }


def step_scrapping_state(
    state,
    ts,
    scrapping_detected,
    evidence=None,
    start_confirm_sec=SCRAP_START_CONFIRM_SEC,
    gap_tolerance_sec=SCRAP_START_GAP_TOLERANCE_SEC,
    end_grace_sec=SCRAP_END_GRACE_SEC,
    start_min_hits=SCRAP_START_MIN_HITS,
):
    """
    Advance the SCRAPPING state machine by one detection poll.

    Returns (state, event) where event is one of:
        "START_CANDIDATE", "END_CANDIDATE", or None

    FRAME_AGGREGATE is intentionally not modeled here - in production it's
    a wall-clock timer fired independently of detection state
    (FRAME_AGGREGATE_INTERVAL_SEC in edge_detector.py), not part of this
    detection-driven state transition.
    """
    if scrapping_detected:
        state["candidate_last_detected"] = ts
        state["last_evidence_detections"] = evidence or []

    if state["state"] == "INACTIVE":
        if scrapping_detected:
            if state["candidate_since"] is None:
                state["candidate_since"] = ts
                state["candidate_hit_count"] = 0

            state["candidate_hit_count"] += 1
            candidate_duration = ts - state["candidate_since"]

            if (
                candidate_duration >= start_confirm_sec
                and state["candidate_hit_count"] >= start_min_hits
            ):
                state["state"] = "ACTIVE"
                state["candidate_since"] = None
                state["candidate_last_detected"] = None
                state["candidate_hit_count"] = 0
                state["end_candidate_since"] = None
                return state, "START_CANDIDATE"
        else:
            if (
                state["candidate_since"] is not None
                and state["candidate_last_detected"] is not None
            ):
                gap = ts - state["candidate_last_detected"]

                if gap > gap_tolerance_sec:
                    state["candidate_since"] = None
                    state["candidate_last_detected"] = None
                    state["candidate_hit_count"] = 0
                    state["last_evidence_detections"] = []

        return state, None

    if state["state"] == "ACTIVE":
        if scrapping_detected:
            state["end_candidate_since"] = None
        else:
            if state["end_candidate_since"] is None:
                state["end_candidate_since"] = ts

        if state["end_candidate_since"] is not None:
            absence_duration = ts - state["end_candidate_since"]

            if absence_duration >= end_grace_sec:
                state["state"] = "INACTIVE"
                state["end_candidate_since"] = None
                return state, "END_CANDIDATE"

        return state, None

    return state, None
