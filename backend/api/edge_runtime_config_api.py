"""
Edge Runtime Config API (Phase 3)

Responds to runtime configuration requests from Jetson devices.

Responsibilities:
- Authenticate device via X-DEVICE-CODE
- Respond with runtime configuration
- Lightweight & high-frequency safe
"""
from fastapi import APIRouter, Request, HTTPException
from common.db import get_cursor

router = APIRouter()


@router.get("/edge/runtime-config")
def edge_runtime_config(request: Request):
    device_code = request.headers.get("X-DEVICE-CODE")
    if not device_code:
        raise HTTPException(status_code=400, detail="Missing device code")

    with get_cursor() as cur:
        # -------------------------------------------------
        # 1. Resolve device → farm
        # -------------------------------------------------
        cur.execute(
            """
            SELECT id, farm_id
            FROM edge_device
            WHERE code = %s AND is_active = true
            """,
            (device_code,),
        )
        device = cur.fetchone()
        if not device:
            raise HTTPException(status_code=404, detail="Device not found")

        device_id = device["id"]
        farm_id = device["farm_id"]

        # -------------------------------------------------
        # 2. Farm timezone
        # -------------------------------------------------
        cur.execute(
            """
            SELECT timezone
            FROM farm
            WHERE id = %s
            """,
            (farm_id,),
        )
        farm = cur.fetchone()
        if not farm:
            raise HTTPException(status_code=500, detail="Farm not found")

        farm_timezone = farm["timezone"]

        # -------------------------------------------------
        # 3. Cameras + stream config (JOIN)
        # -------------------------------------------------
        cur.execute(
            """
            SELECT
                fc.id                AS camera_id,
                fc.code              AS camera_code,
                fc.rtsp_url,
                fc.nvr_channel,
                fc.zone_id,
                csc.resolution,
                csc.fps_target,
                csc.roi,
                csc.motion_sensitivity
            FROM farm_camera fc
            LEFT JOIN camera_stream_config csc
                ON csc.camera_id = fc.id
            WHERE fc.farm_id = %s
              AND fc.is_active = true
            """,
            (farm_id,),
        )

        rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=400, detail="No active cameras found")

        cameras = []
        for r in rows:
            cameras.append({
                "camera_id": r["camera_id"],
                "code": r["camera_code"],
                "rtsp_url": r["rtsp_url"],
                "nvr_channel": r["nvr_channel"],
                "zone_id": r["zone_id"],
                "resolution": r["resolution"],
                "fps": r["fps_target"] if r["fps_target"] is not None else 5,
                "roi_polygon": r["roi"],
                "motion_sensitivity": r["motion_sensitivity"],
            })

        # -------------------------------------------------
        # 4. Device → model assignment
        # -------------------------------------------------
        cur.execute(
            """
            SELECT ml_model_version_id
            FROM device_model_assignment
            WHERE device_id = %s
            ORDER BY assigned_at DESC
            LIMIT 1
            """,
            (device_id,),
        )
        assignment = cur.fetchone()
        if not assignment:
            raise HTTPException(status_code=400, detail="No model assigned to device")

        model_version_id = assignment["ml_model_version_id"]

        # -------------------------------------------------
        # 5. Model version details
        # -------------------------------------------------
        cur.execute(
            """
            SELECT
                id,
                name,
                version,
                storage_path,
                checksum,
                metadata
            FROM ml_model_version
            WHERE id = %s AND is_active = true
            """,
            (model_version_id,),
        )
        model = cur.fetchone()
        if not model:
            raise HTTPException(status_code=500, detail="Model version not found")

    # -------------------------------------------------
    # METADATA VALIDATION
    # -------------------------------------------------
    metadata = model["metadata"] or {}

    if "class_map" not in metadata:
        raise HTTPException(
            status_code=500,
            detail="Model metadata missing class_map"
        )

    # -------------------------------------------------
    # FINAL RUNTIME CONFIG PAYLOAD
    # -------------------------------------------------
    return {
        "device_id": device_id,
        "farm_id": farm_id,
        "farm_timezone": farm_timezone,

        "cameras": cameras,

        "device_model_assignment": {
            "ml_model_version_id": model_version_id
        },

        "ml_model_version": {
            "id": model["id"],
            "name": model["name"],
            "version": model["version"],
            "model_path": model["storage_path"],
            "checksum": model["checksum"],
            "class_map": metadata["class_map"],
            "activity_thresholds": metadata.get("activity_thresholds", {}),
        }
    }
