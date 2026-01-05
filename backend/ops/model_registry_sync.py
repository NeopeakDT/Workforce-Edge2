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
"""

import argparse
from common.db import get_cursor
from common.time_utils import utc_now


def register_and_assign_model(
    model_name: str,
    model_type: str,
    version: str,
    artifact_uri: str,
    device_id: str,
):
    with get_cursor() as cur:
        # 1. Register model (idempotent)
        cur.execute(
            """
            INSERT INTO model (
                name,
                model_type,
                version,
                artifact_uri,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name, version)
            DO UPDATE SET artifact_uri = EXCLUDED.artifact_uri
            RETURNING id
            """,
            (
                model_name,
                model_type,
                version,
                artifact_uri,
                utc_now(),
            ),
        )
        model_id = cur.fetchone()[0]

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
    print(f"Model ID  : {model_id}")
    print(f"Device ID : {device_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--model-type", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--artifact-uri", required=True)
    parser.add_argument("--device-id", required=True)

    args = parser.parse_args()

    register_and_assign_model(
        model_name=args.model_name,
        model_type=args.model_type,
        version=args.version,
        artifact_uri=args.artifact_uri,
        device_id=args.device_id,
    )
