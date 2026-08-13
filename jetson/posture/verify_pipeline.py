#!/usr/bin/env python3
"""
End-to-end posture pipeline verification (no GPU, no live DB).

Run from repo root:
    PYTHONPATH=jetson python3 jetson/posture/verify_pipeline.py
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import numpy as np

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "jetson"))

from posture.posture_detector import PostureDetector  # noqa: E402
from posture.posture_models import PostureObservation, PostureSample  # noqa: E402
from posture.posture_scheduler import PostureScheduler  # noqa: E402


class MockRunner:
  def infer(self, frame):
    return [
      {"class": "cow_standing", "confidence": 0.9, "bbox": [100, 100, 200, 200]},
      {"class": "cow_standing", "confidence": 0.85, "bbox": [300, 300, 400, 400]},
    ]


class MockDB:
  def __init__(self):
    self.observations: List[PostureObservation] = []

  def insert_observation(self, observation: PostureObservation):
    self.observations.append(observation)


def load_runtime_config() -> dict:
  path = REPO / "jetson" / "config" / "local_cache.json"
  with open(path) as f:
    return json.load(f)


def posture_cameras(cfg: dict) -> list:
  return [
    c
    for c in cfg["cameras"]
    if "POSTURE" in c.get("activity_zones", {})
  ]


def verify_detector(cfg: dict) -> PostureSample:
  camera = posture_cameras(cfg)[0]
  detector = PostureDetector(MockRunner(), herd_size=33)
  frame = np.zeros((360, 640, 3), dtype=np.uint8)

  sample = detector.detect(
    frame,
    camera,
    cfg["farm_id"],
    cfg["device_id"],
    datetime.now(timezone.utc),
  )

  assert isinstance(sample, PostureSample)
  assert sample.camera_code == camera["code"]
  assert sample.herd_size == 33
  assert sample.standing_count >= 0
  assert sample.feeding_count >= 0
  print(f"  OK PostureDetector → PostureSample ({sample.camera_code})")
  return sample


def verify_scheduler_to_db(cfg: dict):
  """
  Drive the data path explicitly (bypasses 60s camera/minute timers).
  """
  db = MockDB()
  detector = PostureDetector(MockRunner(), herd_size=33)
  scheduler = PostureScheduler(cfg, detector, db)

  cameras = posture_cameras(cfg)
  assert len(scheduler.posture_camera_ids) == len(cameras)
  assert scheduler.pen_rest_zone_id is not None

  frame = np.zeros((360, 640, 3), dtype=np.uint8)
  now = datetime.now(timezone.utc)

  for minute in range(10):
    for camera in cameras:
      sample = detector.detect(
        frame,
        camera,
        cfg["farm_id"],
        cfg["device_id"],
        now,
      )
      scheduler.current_minute_samples[sample.camera_id] = sample

    samples = scheduler._take_minute_window()
    assert samples is not None
    scheduler.build_minute_snapshot(samples)

  assert scheduler.total_samples == 10, (
    f"expected 10 minute snapshots, got {scheduler.total_samples}"
  )

  scheduler.aggregate_pen()

  assert scheduler.pen_buffer.size == 0
  assert len(db.observations) == 1

  obs = db.observations[0]
  assert isinstance(obs, PostureObservation)
  assert obs.zone_id == scheduler.pen_rest_zone_id
  assert (
    obs.standing_count + obs.feeding_count + obs.laying_count
    <= obs.metadata["herd_size"]
  )
  assert obs.metadata["expected_cameras"] == len(cameras)
  assert obs.metadata["aggregation_window_minutes"] == 10
  assert "cameras" in obs.metadata

  for code in (c["code"] for c in cameras):
    assert code in obs.metadata["cameras"]
    assert "camera_id" in obs.metadata["cameras"][code]

  print(
    f"  OK PostureSample×{len(cameras)} → MinuteAggregation×10 → "
    f"PenBuffer → aggregate_pen → PostureObservation "
    f"(standing={obs.standing_count} feeding={obs.feeding_count} "
    f"laying={obs.laying_count})"
  )
  return obs


def verify_process_frame_wiring(cfg: dict):
  """Confirm process_frame calls detector and reaches minute snapshot."""
  db = MockDB()
  detector = PostureDetector(MockRunner(), herd_size=33)
  scheduler = PostureScheduler(cfg, detector, db)
  scheduler.SAMPLE_INTERVAL_SECONDS = 0
  scheduler.last_sample.clear()

  frame = np.zeros((360, 640, 3), dtype=np.uint8)
  cameras = posture_cameras(cfg)

  # Disable minute expiry between rapid camera passes in this test
  noop_finalize = lambda now: None
  real_finalize = scheduler._maybe_finalize_minute
  scheduler._maybe_finalize_minute = noop_finalize

  try:
    for camera in cameras:
      scheduler.process_frame(frame, camera)
  finally:
    scheduler._maybe_finalize_minute = real_finalize

  # All 3 cameras in one pass → one complete minute snapshot
  assert scheduler.total_samples == 1
  assert len(scheduler.current_minute_samples) == 0
  print("  OK process_frame() → detector → minute snapshot (3/3 cameras)")

  # Partial-minute path: one camera only, then expire window
  scheduler.current_minute_samples = {
    cameras[0]["camera_id"]: detector.detect(
      frame,
      cameras[0],
      cfg["farm_id"],
      cfg["device_id"],
      datetime.now(timezone.utc),
    )
  }
  scheduler.current_minute_started_at = 0.0
  scheduler._maybe_finalize_minute(time.monotonic())

  assert scheduler.total_samples == 2
  print("  OK partial minute snapshot (1/3 cameras, fault-tolerant)")


def verify_edge_integration():
  edge = REPO / "jetson" / "edge_detector.py"
  text = edge.read_text()
  checks = [
    "from posture.posture_detector import PostureDetector",
    "from posture.posture_scheduler import PostureScheduler",
    "posture_scheduler.process_frame(",
    "PostureScheduler(",
    "posture_scheduler.stop()",
  ]
  for token in checks:
    assert token in text, f"missing in edge_detector.py: {token}"
  print("  OK edge_detector.py integration hooks present")


def main():
  print("Posture pipeline verification\n")

  cfg = load_runtime_config()
  n = len(posture_cameras(cfg))
  print(f"Runtime config: {n} POSTURE camera(s)\n")

  print("Step 1 — PostureDetector")
  verify_detector(cfg)

  print("\nStep 2 — Scheduler data path → DB")
  verify_scheduler_to_db(cfg)

  print("\nStep 3 — process_frame wiring")
  verify_process_frame_wiring(cfg)

  print("\nStep 4 — edge_detector integration")
  verify_edge_integration()

  print("\n✅ Pipeline verification passed")


if __name__ == "__main__":
  main()
