"""
Edge Device Telemetry Module

Reads device health metrics for the current edge box. Originally written
for the Jetson Orin/Nano SoC line (thermal zones exposed under
/sys/devices/virtual/thermal with type strings like "cpu-thermal" /
"gpu-thermal"). The fleet has since moved to PC-class "Edge2" hardware
(x86_64 + a discrete NVIDIA GPU) where that sysfs layout doesn't apply:
the CPU package zone is typically named "x86_pkg_temp" (no "cpu"/"soc"/
"package" substring match), and a discrete NVIDIA GPU exposes no generic
Linux thermal zone at all — its temperature only comes from the NVIDIA
driver itself.

CPU temp now prefers `psutil.sensors_temperatures()` (already a hard
dependency here; reads /sys/class/hwmon directly, no lm-sensors CLI
required) and GPU temp now prefers `nvidia-smi` (ships with every NVIDIA
driver install; already assumed present anywhere this project runs
CUDA/TensorRT inference). Both fall back to the original raw sysfs
thermal-zone scan, so this still works unmodified on real Jetson
hardware if it's ever redeployed there.

All values returned are safe to insert directly into DB (None if
unavailable, never raises).
"""

import os
import shutil
import subprocess
import psutil


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def _read_temp_from_zone(zone_type_keywords):
    """
    Read temperature (°C) from thermal zones matching keywords.
    Jetson/Tegra-style fallback — kept for compatibility with SoC hardware.
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


def _read_cpu_temp_psutil():
    """
    CPU package temperature via psutil.sensors_temperatures() — works on
    standard x86 Linux (coretemp/k10temp hwmon drivers) without needing
    the lm-sensors CLI installed. Prefers a "package"-labeled entry
    (whole-CPU temp) over individual per-core readings.
    """
    try:
        sensors = psutil.sensors_temperatures()
    except Exception:
        return None
    if not sensors:
        return None

    for group_name in ("coretemp", "k10temp", "cpu_thermal", "soc_thermal"):
        entries = sensors.get(group_name)
        if entries:
            for entry in entries:
                if entry.label and "package" in entry.label.lower():
                    return round(entry.current, 2)
            return round(entries[0].current, 2)

    # Unknown sensor group naming — take whatever's first rather than nothing.
    for entries in sensors.values():
        if entries:
            return round(entries[0].current, 2)
    return None


def _read_gpu_temp_nvidia_smi():
    """
    Discrete NVIDIA GPU temperature via `nvidia-smi`. There is no generic
    Linux thermal-zone equivalent for a discrete GPU — this is the only
    reliable source on non-Jetson hardware.
    """
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=temperature.gpu", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=3,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    first_line = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
    try:
        return float(first_line)
    except ValueError:
        return None


# --------------------------------------------------
# Telemetry Readers
# --------------------------------------------------

def get_cpu_temp_c():
    """
    CPU temperature in °C. psutil (x86/PC) first, Jetson/Tegra sysfs
    zone scan as fallback.
    """
    temp = _read_cpu_temp_psutil()
    if temp is not None:
        return temp
    return _read_temp_from_zone(
        zone_type_keywords=["cpu", "soc", "package"]
    )


def get_gpu_temp_c():
    """
    GPU temperature in °C. nvidia-smi (discrete NVIDIA GPU) first,
    Jetson/Tegra sysfs zone scan as fallback (integrated Tegra GPU does
    expose a "gpu"-typed thermal zone).
    """
    temp = _read_gpu_temp_nvidia_smi()
    if temp is not None:
        return temp
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
