"""Camera → inference pipeline routing (local edge only, not from DB)."""


def is_milking_camera(camera_cfg):
    """
    Detect whether this camera should use the milking inference pipeline.
    """
    code = (camera_cfg.get("code") or "").lower()
    return "milking" in code
