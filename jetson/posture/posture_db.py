"""
jetson/posture/posture_db.py

Database layer for Cow Posture Detection.

Responsibilities
----------------
- Insert posture observations
- Bulk insert observations
- Keep database logic separate from detector/scheduler

No inference.
No aggregation.
No ROI logic.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Iterable

from psycopg2.extras import Json

# common.db lives under backend/; edge runs with jetson/ on sys.path only.
_backend_dir = Path(__file__).resolve().parents[2] / "backend"
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

from common.db import get_cursor

from .posture_models import PostureObservation, POSTURE_ACTIVITY_TYPE_ID

logger = logging.getLogger(__name__)


class PostureDB:
    """
    Database helper for posture_observation table.
    """

    @staticmethod
    def insert_observation(
        observation: PostureObservation,
    ) -> None:
        """
        Insert one posture observation.
        """

        try:
            with get_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO posture_observation
                    (
                        farm_id,
                        zone_id,
                        device_id,
                        activity_type_id,
                        observed_at,
                        standing_count,
                        feeding_count,
                        laying_count,
                        standing_percentage,
                        laying_percentage,
                        metadata
                    )
                    VALUES
                    (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    """,
                    (
                        observation.farm_id,
                        observation.zone_id,
                        observation.device_id,
                        POSTURE_ACTIVITY_TYPE_ID,
                        observation.observed_at,
                        observation.standing_count,
                        observation.feeding_count,
                        observation.laying_count,
                        observation.standing_percentage,
                        observation.laying_percentage,
                        Json(observation.metadata),
                    ),
                )

            logger.info(
                "[POSTURE][DB] Observation stored successfully at %s",
                observation.observed_at.isoformat(),
            )
        except Exception:
            logger.exception(
                "Failed to insert posture observation "
                "zone_id=%s observed_at=%s",
                observation.zone_id,
                observation.observed_at,
            )
            raise

    @staticmethod
    def bulk_insert(
        observations: Iterable[PostureObservation],
    ) -> None:
        """
        Insert multiple observations in one transaction.
        """

        observations = list(observations)

        if not observations:
            return

        try:
            with get_cursor() as cur:
                for observation in observations:
                    cur.execute(
                        """
                        INSERT INTO posture_observation
                        (
                            farm_id,
                            zone_id,
                            device_id,
                            activity_type_id,
                            observed_at,
                            standing_count,
                            feeding_count,
                            laying_count,
                            standing_percentage,
                            laying_percentage,
                            metadata
                        )
                        VALUES
                        (
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                        )
                        """,
                        (
                            observation.farm_id,
                            observation.zone_id,
                            observation.device_id,
                            POSTURE_ACTIVITY_TYPE_ID,
                            observation.observed_at,
                            observation.standing_count,
                            observation.feeding_count,
                            observation.laying_count,
                            observation.standing_percentage,
                            observation.laying_percentage,
                            Json(observation.metadata),
                        ),
                    )
        except Exception:
            logger.exception(
                "Failed bulk insert of %d posture observation(s)",
                len(observations),
            )
            raise

    @staticmethod
    def health_check() -> bool:
        """
        Verify database connectivity.
        """

        try:
            with get_cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            logger.exception("Posture database health check failed")
            return False
