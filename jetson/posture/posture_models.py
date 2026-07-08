"""
jetson/posture/posture_models.py

Data models for Cow Posture Detection.

Flow

Detector
    ↓
PostureSample (per camera)
    ↓
Scheduler
    ↓
MinuteAggregation (one minute, all cameras)
    ↓
PenAggregationBuffer
    ↓
Aggregator
    ↓
PostureObservation
    ↓
Database
"""

from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List

POSTURE_ACTIVITY_TYPE_ID = 4


# ---------------------------------------------------------
# Single Cow
# ---------------------------------------------------------

@dataclass(slots=True)
class StandingCow:
    """
    Represents one standing cow after ROI filtering.
    """

    bbox: List[float]
    confidence: float
    zone_type: str | None = None
    zone_id: str | None = None


# ---------------------------------------------------------
# Single Camera Sample
# ---------------------------------------------------------

@dataclass
class PostureSample:
    """
    One observation from one camera.
    """

    farm_id: str
    device_id: str

    camera_id: str
    camera_code: str

    observed_at: datetime

    herd_size: int

    standing_count: int
    feeding_count: int


# ---------------------------------------------------------
# One-Minute Pen Snapshot
# ---------------------------------------------------------

@dataclass
class MinuteAggregation:
    """
    One minute aggregated posture snapshot
    across all posture cameras.
    """

    observed_at: datetime

    standing_count: int
    feeding_count: int

    herd_size: int

    camera_breakdown: Dict[str, dict]


# ---------------------------------------------------------
# Final Aggregated Observation
# ---------------------------------------------------------

@dataclass(slots=True)
class PostureObservation:
    """
    Final value inserted into posture_observation.

    Generated every flush interval across all posture cameras.
    """

    farm_id: str

    zone_id: str | None

    device_id: str

    observed_at: datetime

    standing_count: int

    feeding_count: int

    laying_count: int

    standing_percentage: float

    laying_percentage: float

    metadata: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------
# Pen Aggregation Buffer
# ---------------------------------------------------------

@dataclass
class PenAggregationBuffer:
    """
    Temporary RAM buffer holding one-minute
    posture snapshots until DB flush.
    """

    snapshots: List[MinuteAggregation] = field(
        default_factory=list
    )

    lock: Lock = field(default_factory=Lock)

    @property
    def size(self):
        return len(self.snapshots)

    def add(self, snapshot: MinuteAggregation):
        self.snapshots.append(snapshot)

    def clear(self):
        self.snapshots.clear()
