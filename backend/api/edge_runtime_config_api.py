"""
backend/api/edge_runtime_config_api.py
Edge Runtime Config API (Phase 3)

Responds to runtime configuration requests from Jetson devices.

Responsibilities:
- Authenticate device via X-DEVICE-CODE
- Respond with runtime configuration
- Lightweight & high-frequency safe

to run in terminal: 
curl -X GET http://localhost:8000/api/v1/edge/runtime-config \
  -H "X-DEVICE-CODE: HD_TEST_01"
eg: curl -X GET http://localhost:8000/api/v1/edge/runtime-config \
  -H "X-DEVICE-CODE: HD_TEST_01"



"""
"""
Edge Runtime Config API (Phase 3)

Responds to runtime configuration requests from Jetson devices.

Responsibilities:
- Authenticate device via X-DEVICE-CODE
- Respond with runtime configuration
- Lightweight & high-frequency safe
"""

from fastapi import APIRouter, Request, HTTPException
from collections import defaultdict
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
        # 3. Cameras + stream config
        # -------------------------------------------------
        cur.execute(
            """
            SELECT
                fc.id                AS camera_id,
                fc.code              AS camera_code,
                fc.stream_type,
                fc.rtsp_url,
                fc.nvr_rtsp_base,
                fc.nvr_channel,
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
        camera_index = {}

        for r in rows:
            cam = {
                "camera_id": r["camera_id"],
                "code": r["camera_code"],

                "stream_type": r["stream_type"],

                "rtsp_url": r["rtsp_url"],

                # NVR
                "nvr_rtsp_base": r["nvr_rtsp_base"],
                "nvr_channel": r["nvr_channel"],

                "resolution": r["resolution"],
                "fps": r["fps_target"] if r["fps_target"] is not None else 5,
                "roi_polygon": r["roi"],
                "motion_sensitivity": r["motion_sensitivity"],

                # Will be filled next
                "activity_zones": {},
            }

            cameras.append(cam)
            camera_index[r["camera_id"]] = cam

        # -------------------------------------------------
        # 3.1 Camera → Activity → Zone mapping
        # -------------------------------------------------
        cur.execute(
            """
            SELECT
                caz.camera_id,
                at.code AS activity_code,
                caz.zone_id,
                fz.name AS zone_name,
                caz.roi
            FROM camera_activity_zone caz
            JOIN activity_type at
                ON at.id = caz.activity_type_id
            JOIN farm_zone fz
                ON fz.id = caz.zone_id
            WHERE caz.farm_id = %s
              AND caz.is_active = true
            """,
            (farm_id,),
        )

        rows = cur.fetchall()

        for r in rows:
            cam = camera_index.get(r["camera_id"])
            if not cam:
                continue

            activity_code = r["activity_code"].upper()

            zone_name = r["zone_name"]

            zone_type = "DEFAULT"

            if "rest" in zone_name.lower():
                zone_type = "REST"

            elif "feeding" in zone_name.lower():
                zone_type = "FEEDING"

            # -------------------------------------------------
            # POSTURE supports multiple ROIs
            # -------------------------------------------------
            if activity_code == "POSTURE":

                if activity_code not in cam["activity_zones"]:
                    cam["activity_zones"][activity_code] = {
                        "zones": []
                    }

                cam["activity_zones"][activity_code]["zones"].append({
                    "zone_id": r["zone_id"],
                    "zone_name": zone_name,
                    "zone_type": zone_type,
                    "roi": r["roi"],
                })

            # -------------------------------------------------
            # Existing activities remain unchanged
            # -------------------------------------------------
            else:

                cam["activity_zones"][activity_code] = {
                    "zone_id": r["zone_id"],
                    "roi": r["roi"],
                }

        # -------------------------------------------------
        # Optional strict check: fail-fast if camera has no activity zones
        # (enable in production farms only)
        # -------------------------------------------------
        for cam in cameras:
            if not cam["activity_zones"]:
                raise HTTPException(
                    status_code=500,
                    detail=f"Camera {cam['code']} has no activity_zone mapping"
                )

        # -------------------------------------------------
        # Validate camera stream config (fail-fast)
        # -------------------------------------------------
        for cam in cameras:
            st = cam["stream_type"]

            if st == "RTSP" and not cam.get("rtsp_url"):
                raise HTTPException(
                    status_code=500,
                    detail=f"RTSP camera {cam['code']} missing rtsp_url"
                )

            if st == "NVR_CHANNEL" and (
                not cam.get("nvr_rtsp_base") or not cam.get("nvr_channel")
            ):
                raise HTTPException(
                    status_code=500,
                    detail=f"NVR_CHANNEL camera {cam['code']} missing nvr_rtsp_base or nvr_channel"
                )

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
        # 6. Activity schedules (milking / feeding / scrapping)
        # -------------------------------------------------
        cur.execute(
            """
            SELECT
                id,
                activity_type_id,
                label,
                ideal_start_time,
                ideal_end_time,
                tolerance_early_min,
                tolerance_late_min
            FROM activity_schedule
            WHERE farm_id = %s
              AND is_active = true
            ORDER BY activity_type_id, ideal_start_time
            """,
            (farm_id,),
        )
        schedule_rows = cur.fetchall()

        def _time_str(value):
            if value is None:
                return None
            if hasattr(value, "strftime"):
                return value.strftime("%H:%M:%S")
            return str(value)

        activity_schedules = [
            {
                "id": row["id"],
                "activity_type_id": row["activity_type_id"],
                "label": row["label"],
                "ideal_start_time": _time_str(row["ideal_start_time"]),
                "ideal_end_time": _time_str(row["ideal_end_time"]),
                "tolerance_early_min": row["tolerance_early_min"],
                "tolerance_late_min": row["tolerance_late_min"],
            }
            for row in schedule_rows
        ]

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
        },

        "activity_schedules": activity_schedules,
    }
