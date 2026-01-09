#!/usr/bin/env python3
"""
Model Registry Sync (One-Time)

Registers an ML model version and assigns it to a device.

- Model is activity-agnostic
- activity_type_id is optional (NULL)
- Metadata is stored as JSONB
- Uses name + version as model identity

in terminal - to run 

python ops/model_registry_sync.py \
  --model-name farm_multi_activity_detector \
  --version 1.0.3 \
  --storage-path models/farm_multi_activity_detector/1.0.3/model.engine \
  --checksum sha256:9a4f...c2d1 \
  --metadata-file metadata.json \
  --device-id <uuid>

Example -
python ops/model_registry_sync.py `
  --model-name farm_multi_activity_detector `
  --version 1.0.3 `
  --storage-path models/farm_multi_activity_detector/1.0.3/best.pt `
  --checksum sha256:9a4f...c2d1 `
  --metadata-file metadata.json `
  --device-id 6eb487b5-17c4-4f62-a613-84db27397621


"""

import argparse
import json
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


def load_metadata(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def register_and_assign_model(
    model_name: str,
    version: str,
    storage_path: str,
    checksum: str,
    metadata: dict,
    device_id: str,
):
    with get_cursor() as cur:
        # -------------------------------------------------
        # 1. Register / update model version
        # -------------------------------------------------
        cur.execute(
            """
            INSERT INTO ml_model_version (
                name,
                version,
                activity_type_id,
                storage_path,
                checksum,
                metadata,
                is_active,
                created_at
            )
            VALUES (%s, %s, NULL, %s, %s, %s::jsonb, true, %s)
            ON CONFLICT (name, version)
            DO UPDATE SET
                storage_path = EXCLUDED.storage_path,
                checksum = EXCLUDED.checksum,
                metadata = EXCLUDED.metadata,
                is_active = true
            RETURNING id
            """,
            (
                model_name,
                version,
                storage_path,
                checksum,
                json.dumps(metadata),
                utc_now(),
            ),
        )

        row = cur.fetchone()
        model_id = row["id"]

        # -------------------------------------------------
        # 2. Remove any previous assignment for device
        # -------------------------------------------------
        cur.execute(
            """
            DELETE FROM device_model_assignment
            WHERE device_id = %s
            """,
            (device_id,),
        )

        # -------------------------------------------------
        # 3. Assign model to device
        # -------------------------------------------------
        cur.execute(
            """
            INSERT INTO device_model_assignment (
                device_id,
                ml_model_version_id,
                assigned_at,
                effective_from,
                notes
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                device_id,
                model_id,
                utc_now(),
                utc_now(),
                "Assigned via ops/model_registry_sync.py",
            ),
        )

    print("✅ Model registered and assigned")
    print(f"Model ID   : {model_id}")
    print(f"Name       : {model_name}")
    print(f"Version    : {version}")
    print(f"Device ID  : {device_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register ML model and assign to device")
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--storage-path", required=True)
    parser.add_argument("--checksum", required=True)
    parser.add_argument("--metadata-file", required=True)
    parser.add_argument("--device-id", required=True)

    args = parser.parse_args()

    metadata = load_metadata(args.metadata_file)

    register_and_assign_model(
        model_name=args.model_name,
        version=args.version,
        storage_path=args.storage_path,
        checksum=args.checksum,
        metadata=metadata,
        device_id=args.device_id,
    )
