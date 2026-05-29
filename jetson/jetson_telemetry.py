"""
Jetson Orin Telemetry Module

Reads device health metrics directly from kernel/sysfs.
NO tegrastats
NO shell parsing
NO blocking calls

Compatible with:
- Jetson Orin
- Orin Nano
- Orin NX

All values returned are safe to insert directly into DB.
"""

import os
import shutil
import psutil


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def _read_temp_from_zone(zone_type_keywords):
    """
    Read temperature (°C) from thermal zones matching keywords.
    """
    base = "/sys/devices/virtual/thermal"
    if not os.path.exists(base):
        return None

    for zone in os.listdir(base):
        zone_path = os.path.join(base, zone)
        try:
            with open(os.path.join(zone_path, "type")) as f:
                zone_type = f.read().strip().lower()

            if any(k in zone_type for k in zone_type_keywords):
                with open(os.path.join(zone_path, "temp")) as f:
                    return int(f.read().strip()) / 1000.0
        except Exception:
            continue

    return None


# --------------------------------------------------
# Telemetry Readers
# --------------------------------------------------

def get_cpu_temp_c():
    """
    CPU temperature in °C
    """
    return _read_temp_from_zone(
        zone_type_keywords=["cpu", "soc", "package"]
    )


def get_gpu_temp_c():
    """
    GPU temperature in °C
    """
    return _read_temp_from_zone(
        zone_type_keywords=["gpu"]
    )


def get_disk_usage_pct(path="/"):
    """
    Disk usage percentage
    """
    try:
        usage = shutil.disk_usage(path)
        return round((usage.used / usage.total) * 100, 2)
    except Exception:
        return None


def get_memory_usage_pct():
    """
    RAM usage percentage
    """
    try:
        return round(psutil.virtual_memory().percent, 2)
    except Exception:
        return None


# --------------------------------------------------
# Public API
# --------------------------------------------------

def collect_telemetry():
    """
    Collect all telemetry metrics.

    Returns:
        dict suitable for HeartbeatIn schema
    """
    return {
        "cpu_temp_c": get_cpu_temp_c(),
        "gpu_temp_c": get_gpu_temp_c(),
        "disk_usage_pct": get_disk_usage_pct(),
        "memory_usage_pct": get_memory_usage_pct(),
        "notes": "jetson heartbeat",
    }
