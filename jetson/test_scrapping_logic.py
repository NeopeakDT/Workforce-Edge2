#!/usr/bin/env python3
"""
jetson/test_scrapping_logic.py

Standalone unit test for the SCRAPPING false-positive fix (Aug 2026).
No model / video / GPU dependency - pure state-machine logic only.

Run directly:
    python jetson/test_scrapping_logic.py

Convention: this repo has no pytest/unittest suite (see CLAUDE.md /
IMPORT_GUIDE.md) - standalone scripts with manual asserts, run directly,
matching jetson/test_activity_motion_detection*.py.

Fixtures below are built from the real production event data reviewed on
the `scrapping-detection-alerts` branch:
    - False positive 1: activity_instance 41ea2edb (2026-08-24 05:23, cam
      80461940) - single person+tool frame at conf 0.68, then every
      FRAME_AGGREGATE empty for the rest of the session.
    - False positive 2: activity_instance df665f98 (2026-08-22 04:21, cam
      f209c8ba) - single person+tool frame at conf 0.71, then only `cow`
      objects (no person/tool) for the rest of the session.
    - Real session: one of the 8 camera segments that make up the correct
      842cdfff instance (2026-08-23 09:58:57-09:59:35, cam 6d85c1fb) -
      person+tool evidence recurring across multiple polls, well above the
      confidence floor.

Note: production only emits FRAME_AGGREGATE every 10s (FRAME_AGGREGATE_INTERVAL_SEC),
so the exact sub-second poll cadence that fed the real edge state machine isn't
recoverable from logged events. The fixtures approximate realistic poll timing
(edge processes several frames/sec) to exercise the hit-count logic; the video
smoke test (test_scrapping_video_cpu.py) is the authentic empirical check.
"""

import sys

from scrapping_logic import (
    detect_scrapping,
    new_scrapping_state,
    step_scrapping_state,
    SCRAP_TOOL_MIN_CONFIDENCE,
)

PERSON_CLASSES = {"person"}
TOOL_CLASSES = {"scrapping_tool", "shovel"}
MAX_DIST = 120

PASS = []
FAIL = []


def check(name, condition, detail=""):
    if condition:
        PASS.append(name)
        print(f"[PASS] {name}")
    else:
        FAIL.append(name)
        print(f"[FAIL] {name} {detail}")


def person(x=100, y=100, conf=0.85):
    return {"class": "person", "bbox": [x, y, x + 40, y + 100], "confidence": conf}


def tool(x=110, y=100, conf=0.68):
    return {"class": "scrapping_tool", "bbox": [x, y, x + 30, y + 60], "confidence": conf}


# ------------------------------------------------------------------
# detect_scrapping(): confidence-floor unit tests
# ------------------------------------------------------------------

def test_low_confidence_tool_rejected():
    detected, evidence = detect_scrapping(
        [person(), tool(conf=0.68)], PERSON_CLASSES, TOOL_CLASSES, MAX_DIST
    )
    check(
        "low-confidence tool (0.68) rejected below floor",
        detected is False,
        f"got detected={detected}",
    )


def test_high_confidence_tool_accepted():
    detected, evidence = detect_scrapping(
        [person(), tool(conf=0.85)], PERSON_CLASSES, TOOL_CLASSES, MAX_DIST
    )
    check(
        "high-confidence tool (0.85) accepted",
        detected is True,
        f"got detected={detected}",
    )


def test_confidence_floor_boundary():
    detected, _ = detect_scrapping(
        [person(), tool(conf=SCRAP_TOOL_MIN_CONFIDENCE)],
        PERSON_CLASSES, TOOL_CLASSES, MAX_DIST,
    )
    check(
        f"tool exactly at floor ({SCRAP_TOOL_MIN_CONFIDENCE}) accepted (inclusive)",
        detected is True,
        f"got detected={detected}",
    )


# ------------------------------------------------------------------
# State machine: replay the real event sequences
# ------------------------------------------------------------------

def run_ticks(ticks):
    """ticks: list of (ts, scrapping_detected, evidence). Returns list of events."""
    state = new_scrapping_state()
    events = []
    for ts, detected, evidence in ticks:
        state, event = step_scrapping_state(state, ts, detected, evidence)
        if event:
            events.append((ts, event))
    return events


def test_false_positive_1_single_flicker_no_start():
    # 41ea2edb: person+tool seen once (below confidence floor at 0.68 -> would
    # already be rejected by detect_scrapping, but even ignoring confidence,
    # a single isolated hit must not satisfy the hit-count requirement).
    ticks = [
        (0.0, True, [person(), tool(conf=0.68)]),
        (0.5, False, None),
        (1.0, False, None),
        (1.5, False, None),
        (2.0, False, None),
        (2.5, False, None),
        (3.0, False, None),
    ]
    events = run_ticks(ticks)
    check(
        "false positive 1 (single flicker) never confirms START",
        not any(e == "START_CANDIDATE" for _, e in events),
        f"got events={events}",
    )


def test_false_positive_2_seed_then_unrelated_objects():
    # df665f98: person+tool once, then only `cow` (no person/tool) afterward.
    ticks = [
        (0.0, True, [person(), tool(conf=0.71)]),
        (0.5, False, None),
        (1.0, False, None),
        (1.5, False, None),
        (2.0, False, None),
    ]
    events = run_ticks(ticks)
    check(
        "false positive 2 (seed then unrelated objects) never confirms START",
        not any(e == "START_CANDIDATE" for _, e in events),
        f"got events={events}",
    )


def test_real_session_confirms_start():
    # 842cdfff / session 484432bc: sustained person+tool evidence recurring
    # across multiple polls (realistic ~2Hz effective detection cadence)
    # well above the confidence floor.
    ticks = [
        (0.0, True, [person(), tool(conf=0.80)]),
        (0.6, True, [person(), tool(conf=0.82)]),
        (1.2, True, [person(), tool(conf=0.79)]),
        (1.8, True, [person(), tool(conf=0.81)]),
        (2.4, True, [person(), tool(conf=0.83)]),
        (3.0, True, [person(), tool(conf=0.80)]),
    ]
    events = run_ticks(ticks)
    check(
        "real sustained session confirms START",
        any(e == "START_CANDIDATE" for _, e in events),
        f"got events={events}",
    )


def test_real_session_survives_brief_gap():
    # Gap tolerance (<=2s) should still allow a real, mostly-continuous
    # session to confirm even with a couple of missed polls.
    ticks = [
        (0.0, True, [person(), tool(conf=0.80)]),
        (0.5, False, None),
        (1.0, True, [person(), tool(conf=0.78)]),
        (1.5, False, None),
        (2.0, True, [person(), tool(conf=0.81)]),
        (2.5, False, None),
        (3.0, True, [person(), tool(conf=0.80)]),
    ]
    events = run_ticks(ticks)
    check(
        "real session with brief detection gaps still confirms START",
        any(e == "START_CANDIDATE" for _, e in events),
        f"got events={events}",
    )


def test_end_after_grace_period():
    state = new_scrapping_state()
    # Confirm start first.
    for ts in [0.0, 0.6, 1.2, 1.8, 2.4, 3.0]:
        state, event = step_scrapping_state(state, ts, True, [person(), tool(conf=0.8)])
    check("session reached ACTIVE", state["state"] == "ACTIVE", f"state={state['state']}")

    # end_candidate_since starts counting from the first no-evidence poll.
    state, event = step_scrapping_state(state, 10.0, False, None)
    check(
        "end_candidate_since anchored, no premature END",
        event is None,
        f"event={event}",
    )

    # < grace period since end_candidate_since (10.0) -> no END yet.
    state, event = step_scrapping_state(state, 25.0, False, None)
    check("no premature END before grace period elapses", event is None, f"event={event}")

    # >= grace period (30s) since end_candidate_since (10.0) -> END_CANDIDATE.
    state, event = step_scrapping_state(state, 41.0, False, None)
    check("END_CANDIDATE fires after grace period", event == "END_CANDIDATE", f"event={event}")


def main():
    tests = [
        test_low_confidence_tool_rejected,
        test_high_confidence_tool_accepted,
        test_confidence_floor_boundary,
        test_false_positive_1_single_flicker_no_start,
        test_false_positive_2_seed_then_unrelated_objects,
        test_real_session_confirms_start,
        test_real_session_survives_brief_gap,
        test_end_after_grace_period,
    ]
    for t in tests:
        t()

    print("-" * 60)
    print(f"{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        sys.exit(1)


if __name__ == "__main__":
    main()
