"""
Model Registry Sync (One-Time)
Registers a model and assigns it to a device.

#Purpose:
Register models once and assign them to devices.

#This script:
- Registers a model artifact (YOLO/TensorRT/etc.)
- Assigns it to a device
- Allows deterministic rollout and rollback
- No auto-deployment. No inference logic.

#Usage:
python ops/model_registry_sync.py \
    --model-name wf_yolo_v1.2 \
    --model-type YOLO \
    --version 1.2.0 \
    --artifact-uri s3://wf-models/yolo/wf_v1.2.engine \
    --device-id <uuid>

Run this dummy-
python ops/model_registry_sync.py --model-name wf_test --model-type YOLO --version 0.0.1 --artifact-uri s3://dummy --device-id 6eb487b5-17c4-4f62-a613-84db27397621
"""

import argparse
import sys
from pathlib import Path

# -------------------------------------------------
# Setup path for imports
# -------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


def register_and_assign_model(
    activity_type_id: int,
    model_name: str,
    version: str,
    storage_path: str,
    device_id: str,
):
    with get_cursor() as cur:
        # 1. Register model version (idempotent)
        cur.execute(
            """
            INSERT INTO ml_model_version (
                activity_type_id,
                name,
                version,
                storage_path,
                is_active,
                created_at
            )
            VALUES (%s, %s, %s, %s, true, %s)
            ON CONFLICT (activity_type_id, name, version)
            DO UPDATE SET
                storage_path = EXCLUDED.storage_path,
                is_active = true
            RETURNING id
            """,
            (
                activity_type_id,
                model_name,
                version,
                storage_path,
                utc_now(),
            ),
        )

        row = cur.fetchone()
        model_id = row["id"]

        # 2. Assign model to device
        cur.execute(
            """
            INSERT INTO device_model_assignment (
                device_id,
                model_id,
                assigned_at,
                is_active
            )
            VALUES (%s, %s, %s, true)
            ON CONFLICT (device_id)
            DO UPDATE SET
                model_id = EXCLUDED.model_id,
                assigned_at = EXCLUDED.assigned_at,
                is_active = true
            """,
            (
                device_id,
                model_id,
                utc_now(),
            ),
        )

    print("✅ Model registered and assigned")
    print(f"Model ID        : {model_id}")
    print(f"Activity Type   : {activity_type_id}")
    print(f"Device ID       : {device_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--activity-type-id", type=int, required=True)
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--storage-path", required=True)
    parser.add_argument("--device-id", required=True)

    args = parser.parse_args()

    register_and_assign_model(
        activity_type_id=args.activity_type_id,
        model_name=args.model_name,
        version=args.version,
        storage_path=args.storage_path,
        device_id=args.device_id,
    )
