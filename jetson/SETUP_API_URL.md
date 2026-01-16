# How to Set BACKEND_API_URL

This guide shows you exactly how to set the `BACKEND_API_URL` environment variable for the Jetson device.

## Step-by-Step Instructions

### **Option 1: Environment Variable (Recommended for Production)**

#### **On Linux/Jetson Device:**

**Method A: Export in Current Session**
```bash
# Set for current terminal session
export BACKEND_API_URL=https://api.workforce.example.com

# Verify it's set
echo $BACKEND_API_URL

# Run config sync
python edge_config_sync.py
```

**Method B: Add to Shell Profile (Permanent)**
```bash
# Edit your shell profile (choose one based on your shell)
nano ~/.bashrc        # For bash
# OR
nano ~/.zshrc         # For zsh

# Add this line at the end:
export BACKEND_API_URL=https://api.workforce.example.com

# Save and exit (Ctrl+X, then Y, then Enter)

# Reload the profile
source ~/.bashrc
# OR
source ~/.zshrc

# Verify
echo $BACKEND_API_URL
```

**Method C: Systemd Service (For Auto-Start)**
```bash
# Create service file
sudo nano /etc/systemd/system/workforce-config-sync.service

# Add this content:
[Unit]
Description=Workforce Config Sync
After=network.target

[Service]
Type=oneshot
Environment="BACKEND_API_URL=https://api.workforce.example.com"
ExecStart=/usr/bin/python3 /path/to/edge_config_sync.py
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target

# Enable and start
sudo systemctl enable workforce-config-sync.service
sudo systemctl start workforce-config-sync.service
```

#### **On Windows (Development/Testing):**

**Method A: PowerShell (Current Session)**
```powershell
# Set for current PowerShell session
$env:BACKEND_API_URL="https://api.workforce.example.com"

# Verify
echo $env:BACKEND_API_URL

# Run config sync
python edge_config_sync.py
```

**Method B: Command Prompt (Current Session)**
```cmd
# Set for current CMD session
set BACKEND_API_URL=https://api.workforce.example.com

# Verify
echo %BACKEND_API_URL%

# Run config sync
python edge_config_sync.py
```

**Method C: Windows System Environment Variable (Permanent)**
1. Press `Win + R`, type `sysdm.cpl`, press Enter
2. Click "Environment Variables" button
3. Under "User variables" or "System variables", click "New"
4. Variable name: `BACKEND_API_URL`
5. Variable value: `https://api.workforce.example.com`
6. Click OK on all dialogs
7. Restart your terminal/IDE for changes to take effect

---

### **Option 2: Bootstrap Config File (One-Time Setup)**

**Step 1: Create the config file**
```bash
# On Jetson device
cd /path/to/jetson
mkdir -p config
```

**Step 2: Create bootstrap_config.json**
```bash
nano config/bootstrap_config.json
```

**Step 3: Add this content:**
```json
{
  "backend_api_url": "https://api.workforce.example.com"
}
```

**Step 4: Save and exit**
- Press `Ctrl+X`
- Press `Y` to confirm
- Press `Enter` to save

**Step 5: Run config sync**
```bash
python edge_config_sync.py
```

**Note:** This file is in `.gitignore` and won't be committed to git.

---

### **Option 3: Command Line Argument (Quick Testing)**

```bash
# Just pass the URL as argument
python edge_config_sync.py https://api.workforce.example.com
```

---

## Finding Your Backend API URL

### **If using local development:**
```
http://localhost:8000
```

### **If using deployed backend:**
```
https://your-backend-domain.com
# OR
https://your-backend.herokuapp.com
# OR
https://api.workforce.example.com
```

### **If using Supabase + custom domain:**
```
https://your-project.supabase.co
```

---

## Verification Steps

**1. Check if environment variable is set:**
```bash
# Linux/Jetson
echo $BACKEND_API_URL

# Windows PowerShell
echo $env:BACKEND_API_URL

# Windows CMD
echo %BACKEND_API_URL%
```

**2. Test the config sync:**
```bash
python edge_config_sync.py
```

**Expected output if successful:**
```
Config synced successfully
```

**Expected output if URL not set:**
```
FATAL: BACKEND_API_URL not set. Provide via:
  1. Environment variable: export BACKEND_API_URL=https://api.example.com
  2. Bootstrap config: config/bootstrap_config.json with 'backend_api_url' key
  3. Command line: python edge_config_sync.py https://api.example.com
```

---

## Recommended Setup for Jetson Device

**For production Jetson devices, use Method B (Shell Profile):**

```bash
# 1. SSH into your Jetson device
ssh jetson@<jetson-ip>

# 2. Navigate to jetson directory
cd /path/to/backend/jetson

# 3. Edit bashrc
nano ~/.bashrc

# 4. Add at the end:
export BACKEND_API_URL=https://api.workforce.example.com

# 5. Save and reload
source ~/.bashrc

# 6. Verify
echo $BACKEND_API_URL

# 7. Run config sync
python edge_config_sync.py
```

This ensures the URL is set every time the device boots or a new terminal session starts.

