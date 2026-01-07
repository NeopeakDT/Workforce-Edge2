"""
Device Authentication Test Script for device_auth.py

Tests device authentication using X-DEVICE-KEY header.
Validates that device API keys resolve to correct device identity.

Usage:
    Set DEVICE_API_KEY variable with a valid device API key,
    then run: python scripts/test_device_auth_api.py
"""

import sys
from pathlib import Path

# Setup path for imports (allows script to run from any directory)
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()

from common.device_auth import resolve_device_from_headers, DeviceAuthError

# PASTE A REAL DEVICE API KEY HERE
# Get it from device_provisioning.py output when provisioning a device
DEVICE_API_KEY = "5c399d607fe890ea61d3405b0fc40d6b17a15dac6751d6c49e61fd78ce0992a8"


def main():
    print("🔐 Testing Device Authentication API...\n")

    if DEVICE_API_KEY == "PASTE_DEVICE_API_KEY_HERE":
        print("❌ ERROR: Please paste a REAL device API key.\n")
        print("💡 How to get a device API key:")
        print("   Run device_provisioning.py to provision a device")
        print("   Copy the API key from the output\n")
        sys.exit(1)

    try:
        # Simulate request headers
        headers = {"X-DEVICE-KEY": DEVICE_API_KEY}
        
        print("Testing device authentication...")
        device_ctx = resolve_device_from_headers(headers)

        print("✅ Device authentication OK\n")
        print("Device ID:", device_ctx["device_id"])
        print("Farm ID:", device_ctx["farm_id"])
        print("Device Name:", device_ctx["device_name"])
        print()

    except DeviceAuthError as e:
        print(f"❌ Authentication Error: {str(e)}\n")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {str(e)}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
