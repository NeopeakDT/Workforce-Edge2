"""
jetson/roi_selection/roi_selector.py
1️⃣ How Do You Actually Get Coordinates When Drawing Polygon?

- You need an interactive mouse callback.
- When you click on the image:
- OpenCV gives (x, y) pixel coordinates
- You store them in a list
- After finishing polygon → normalize using width/height

So:
    norm_x = x / width
    norm_y = y / height

You don’t manually calculate anything.
The script prints both pixel and normalized values.
--------------------------------------------------------------------------------------
2️⃣ OpenCV ROI Polygon Tool (Production Ready)

This script:
- Loads frame from RTSP or image
- Lets you click polygon points
- Draws live polygon
- Press:
    c → close polygon
    r → reset
    q → quit
- Prints:
    Pixel coordinates
    ormalized coordinates (0–1)
--------------------------------------------------------------------------------------
3️⃣ How You Use It

1.Run script
2.Click around scrapping zone
3.Press c
4.Press q
5.Copy normalized output
6. Paste into config or DB
---------------------------------------------------------------------------------------
📌 Script: roi_selector.py


rtsp://admin:OMSAI%2312@192.168.31.157:554/Streaming/Channels/502
rtsp://admin:OMSAI%2312@192.168.31.157:554/Streaming/Channels/1902
rtsp://admin:OMSAI%2312@192.168.31.157:554/Streaming/Channels/2301
rtsp://admin:OMSAI%2312@192.168.31.157:554/Streaming/Channels/2201
rtsp://admin:OMSAI%2312@192.168.31.157:554/Streaming/Channels/2401

GRP2-TMR_WAY        → 502
GRP2-FRONT_RIGHT    → 1902
GRP1-FRONT_LEFT     → 2301   
GRP1-FRONT_RIGHT    → 2201
GRP1-FRONT_CENTER   → 2401

"""
import cv2
import json
import numpy as np
import os
import sys
from pathlib import Path
from datetime import datetime

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "jetson"))

from runtime.video_stream import open_stream  # noqa: E402

# -------- CONFIG --------
# SOURCE = "/home/neopeak/Desktop/WF-project/WF/Workforce-Detection/test_data/rahuri_video-7.mp4"
# SOURCE = "jetson/roi_selection/GRP-1-front-right-2.png"
SOURCE = "rtsp://admin:OMSAI%2312@192.168.31.157:554/Streaming/Channels/2301"
DECODE_MODE = "GPU"  # GPU or CPU
# OUTPUT_FILE where coordinates will be saved (absolute, cwd-independent)
OUTPUT_FILE = Path(__file__).resolve().parent / "rtsp_roi_coordinates_new.txt"
# ------------------------

points = []
scale_factor = 1.0  # For downsampling large images
polygon_closed = False

def mouse_callback(event, x, y, flags, param):
    global points, scale_factor
    if event == cv2.EVENT_LBUTTONDOWN:
        # Convert back to original image coordinates if scaled
        orig_x = int(x / scale_factor)
        orig_y = int(y / scale_factor)
        points.append((orig_x, orig_y))
        print(f"Clicked: ({orig_x}, {orig_y}) [Displayed at: ({x}, {y})]")

# ---- Auto-detect and load frame ----
def load_frame(source):
    """Load frame from image, video, or RTSP stream"""
    frame = None
    
    # Check if it's an image file
    if source.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.tiff')):
        print(f"[INFO] Detected image file: {source}")
        frame = cv2.imread(source)
        if frame is None:
            raise Exception(f"Failed to load image: {source}")
    else:
        # Try to load as video or RTSP stream
        print(f"[INFO] Attempting to load as video/RTSP: {source}")
        source_lower = source.lower()
        is_rtsp = source_lower.startswith("rtsp://")

        if is_rtsp:
            print("[INFO] Using production open_stream() (same as edge_detector)")
            stream = open_stream(
                {
                    "code": "roi-selector",
                    "stream_type": "RTSP",
                    "rtsp_url": source,
                    "decode_mode": DECODE_MODE,
                }
            )

            for _ in range(5):
                stream.read()

            ret, frame = stream.read()
            stream.release()
        else:
            cap = cv2.VideoCapture(source)

            if not cap.isOpened():
                raise Exception(f"Failed to open video/stream: {source}")

            for _ in range(5):
                cap.read()

            ret, frame = cap.read()
            cap.release()

        if not ret or frame is None:
            raise Exception(f"Failed to read first frame from: {source}")
    
    return frame

frame = load_frame(SOURCE)
height, width = frame.shape[:2]

print(f"\n{'='*50}")
print(f"Source Resolution: {width} × {height}")
print(f"{'='*50}")

# ---- Screen-aware scaling ----
# Get reasonable display size to fit screen
max_width = 1600  # Adjust based on your monitor (typically 1920-1600)
max_height = 1000

if width > max_width or height > max_height:
    scale_factor = min(max_width / width, max_height / height)
    display_width = int(width * scale_factor)
    display_height = int(height * scale_factor)
    frame = cv2.resize(frame, (display_width, display_height), interpolation=cv2.INTER_AREA)
    print(f"Display Resolution: {display_width} × {display_height} (scaled {scale_factor:.2f}x)")
else:
    print(f"Display Resolution: {width} × {height} (native size)")

cv2.namedWindow("ROI Selector", cv2.WINDOW_AUTOSIZE)
cv2.setMouseCallback("ROI Selector", mouse_callback)

print("\nInstructions:")
print("Click to add polygon points")
print("Press 'c' to close polygon")
print("Press 'r' to reset")
print("Press 'q' to quit\n")

while True:
    display = frame.copy()

    # Draw points (already on scaled frame)
    for p in points:
        display_p = (int(p[0] * scale_factor), int(p[1] * scale_factor))
        cv2.circle(display, display_p, 5, (0, 255, 0), -1)

    # Draw polygon lines (scale points for display)
    if len(points) > 1:
        scaled_points = [(int(p[0] * scale_factor), int(p[1] * scale_factor)) for p in points]
        cv2.polylines(display, [np.array(scaled_points)], False, (255, 0, 0), 2)

    cv2.imshow("ROI Selector", display)
    key = cv2.waitKey(1)

    if key == ord('c'):  # Close polygon
        if len(points) >= 3:
            scaled_points = [(int(p[0] * scale_factor), int(p[1] * scale_factor)) for p in points]
            cv2.polylines(display, [np.array(scaled_points)], True, (0, 0, 255), 2)
            cv2.imshow("ROI Selector", display)
            print("\n✅ Polygon closed.")
            polygon_closed = True
        else:
            print("❌ Need at least 3 points.")

    elif key == ord('r'):
        points = []
        polygon_closed = False
        print("❌ Points reset.")

    elif key == ord('q'):
        break

cv2.destroyAllWindows()

# Remove duplicate closing point if exists
if len(points) >= 3:
    if points[0] == points[-1]:
        points = points[:-1]

    print("\n" + "="*60)
    print("✅ ROI POLYGON SAVED SUCCESSFULLY")
    print("="*60)
    print("\nPixel Coordinates:")
    print(points)

    # Normalize + round
    normalized = [
        {"x": round(x / width, 3), "y": round(y / height, 3)}
        for (x, y) in points
    ]

    print("\nDB Ready ROI JSON:")
    print(json.dumps(normalized, indent=4))

    # ---- Ask for activity and ROI type ----
    print("\n" + "="*60)
    print("\nSelect Activity:")
    print("1. SCRAPPING")
    print("2. FEEDING")
    print("3. MILKING")
    print("4. POSTURE")

    activity_choice = input("Choice: ").strip()

    activity_map = {
        "1": "SCRAPPING",
        "2": "FEEDING",
        "3": "MILKING",
        "4": "POSTURE",
    }

    activity = activity_map.get(activity_choice, "UNKNOWN")
    if activity == "UNKNOWN":
        print("[WARNING] Invalid choice — using UNKNOWN")

    roi_type = ""

    if activity == "POSTURE":
        print("\nSelect ROI Type:")
        print("1. Rest Zone")
        print("2. Cow Feeding Zone")

        roi_choice = input("Choice: ").strip()

        roi_map = {
            "1": "rest_zone",
            "2": "cow_feeding_zone",
        }

        roi_type = roi_map.get(roi_choice, "rest_zone")
        if roi_choice not in roi_map:
            print(f"[WARNING] Invalid ROI type — defaulting to: {roi_type}")

    # ---- Get source filename ----
    source_filename = os.path.basename(SOURCE)

    # ---- Save to file ----
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    output_data = {
        "timestamp": timestamp,
        "source_file": source_filename,
        "full_source_path": SOURCE,
        "activity": activity,
        "roi_type": roi_type if activity == "POSTURE" else None,
        "resolution": f"{width}x{height}",
        "pixel_coordinates": points,
        "normalized_coordinates": normalized,
    }

    try:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_FILE, "a") as f:
            f.write("\n" + "=" * 80 + "\n")
            f.write(f"Timestamp: {timestamp}\n\n")
            f.write(f"Activity: {activity}\n\n")
            if activity == "POSTURE":
                f.write(f"ROI Type: {roi_type}\n\n")
            f.write(f"Source File: {source_filename}\n\n")
            f.write(f"Resolution: {width}x{height}\n\n")
            f.write("Normalized Coordinates:\n")
            f.write(json.dumps(normalized, indent=4) + "\n")
            f.write("=" * 80 + "\n")

        print(f"\n✅ ROI coordinates saved to: {OUTPUT_FILE}")
        print(f"   Activity: {activity}")
        if activity == "POSTURE":
            print(f"   ROI Type: {roi_type}")
        print(f"   Source: {source_filename}")
        print(f"   Points: {len(points)}")
    except Exception as e:
        print(f"\n❌ Error saving to file: {str(e)}")
else:
    print("\n❌ Polygon not defined (need at least 3 points).")
