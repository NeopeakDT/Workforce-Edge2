# Edge Config Sync Execution Timeline

## When to Run `edge_config_sync.py`

**Answer:** It runs exactly once per boot — before any inference starts.

---

## 🚦 Correct Execution Timeline (PHASE-3)

### 🟢 Jetson First-Time Setup (Factory / Farm Install)

1. **Flash Jetson** - Install OS and base system
2. **Install Python deps** - `pip install -r jetson_requirements.txt`
3. **Export EDGE_API_BASE + EDGE_TOKEN** - Set environment variables
4. **Run `edge_config_sync.py`** ← **REQUIRED**
5. **Verify `local_cache.json`** - Ensure config was downloaded successfully
6. **Enable systemd services** - Set up auto-start on boot

---

### 🔁 Every Jetson Boot (Normal Operation)

```
BOOT
 │
 ├─▶ edge_config_sync.py        (must succeed)
 │   └─▶ Fetches config from backend
 │       └─▶ Creates/updates local_cache.json
 │
 ├─▶ edge_heartbeat_agent.py    (background)
 │   └─▶ Sends periodic heartbeats
 │
 └─▶ edge_detector.py
     └─▶ Starts inference only if config sync succeeded
```

**Critical:** If `edge_config_sync.py` fails, the device must NOT proceed to inference.

---

## ❌ What must NOT happen

| Scenario | Allowed | Reason |
|----------|---------|--------|
| Run detector without config | ❌ | Device needs valid config to operate |
| Use stale `local_cache.json` | ❌ | Config may be outdated, causing errors |
| Retry forever | ❌ | Prevents device from failing fast |
| Silent fallback | ❌ | Hides critical configuration errors |
| Skip config sync on boot | ❌ | Device may have stale/invalid config |

---

## ✅ Correct Behavior

### On Successful Config Sync:
1. ✅ `local_cache.json` is created/updated
2. ✅ All required keys are validated
3. ✅ Device proceeds to start `edge_detector.py`
4. ✅ `edge_heartbeat_agent.py` starts in background

### On Failed Config Sync:
1. ❌ **Device must NOT start inference**
2. ❌ **No heartbeat spam** (don't retry forever)
3. ❌ **No silent fallback** (fail loudly)
4. ✅ **Log error clearly** (for debugging)
5. ✅ **Exit with error code** (so systemd/init knows it failed)

---

## Implementation Requirements

### Systemd Service Example

```ini
[Unit]
Description=Workforce Config Sync
After=network.target
Before=edge_detector.service

[Service]
Type=oneshot
Environment="BACKEND_API_URL=https://api.example.com"
ExecStart=/usr/bin/python3 /path/to/edge_config_sync.py
RemainAfterExit=yes
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### Startup Script Example

```bash
#!/bin/bash
# /etc/init.d/workforce-startup

set -e  # Exit on any error

echo "Starting Workforce Detection System..."

# Step 1: Sync configuration (MUST succeed)
echo "Syncing configuration..."
python3 /path/to/edge_config_sync.py || {
    echo "FATAL: Config sync failed. Device cannot start."
    exit 1
}

# Step 2: Verify config exists
if [ ! -f "config/local_cache.json" ]; then
    echo "FATAL: local_cache.json not found after sync."
    exit 1
fi

# Step 3: Start heartbeat agent (background)
echo "Starting heartbeat agent..."
python3 /path/to/edge_heartbeat_agent.py &

# Step 4: Start detector (only if config sync succeeded)
echo "Starting edge detector..."
python3 /path/to/edge_detector.py
```

---

## Error Handling

### Config Sync Failure Scenarios:

1. **Network Error:**
   - ❌ Don't retry forever
   - ✅ Fail immediately with clear error
   - ✅ Log to syslog/journal

2. **Invalid Response:**
   - ❌ Don't use stale config
   - ✅ Fail with validation error
   - ✅ Show which keys are missing

3. **Authentication Error:**
   - ❌ Don't proceed without auth
   - ✅ Fail with 401/403 error
   - ✅ Check EDGE_TOKEN is set

---

## Verification Checklist

Before device is considered "ready":

- [ ] `edge_config_sync.py` completed successfully
- [ ] `local_cache.json` exists and is valid JSON
- [ ] All required keys present: `farm_camera`, `camera_stream_config`, `device_model_assignment`, `ml_model_version`
- [ ] `edge_detector.py` can load config without errors
- [ ] `edge_heartbeat_agent.py` is running
- [ ] Device can communicate with backend API

---

## Summary

**`edge_config_sync.py` is a MANDATORY bootstrap step that:**
- Runs once per boot
- Must succeed before any inference starts
- Fetches fresh configuration from backend
- Validates all required configuration keys
- Fails fast if anything goes wrong

**Never skip this step or use stale configuration.**

<!---------------------------------------------------------------------------------------------- -->

BOOT SEQUENCE (MANDATORY)
1. python edge_config_sync.py
2. python edge_heartbeat_agent.py &
3. python edge_detector.py