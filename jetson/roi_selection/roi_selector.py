"""
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

"""
import cv2
import json
import numpy as np

# -------- CONFIG --------
SOURCE = "jetson/roi_selection/GRP-1-front-right-2.png"
# SOURCE = "rtsp://your_rtsp_here"
# ------------------------

points = []
scale_factor = 1.0  # For downsampling large images

def mouse_callback(event, x, y, flags, param):
    global points, scale_factor
    if event == cv2.EVENT_LBUTTONDOWN:
        # Convert back to original image coordinates if scaled
        orig_x = int(x / scale_factor)
        orig_y = int(y / scale_factor)
        points.append((orig_x, orig_y))
        print(f"Clicked: ({orig_x}, {orig_y}) [Displayed at: ({x}, {y})]")

# Load frame
if SOURCE.startswith("rtsp"):
    cap = cv2.VideoCapture(SOURCE)
    ret, frame = cap.read()
    cap.release()
else:
    frame = cv2.imread(SOURCE)

if frame is None:
    raise Exception("Failed to load frame")

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
            print("\nPolygon closed.")
        else:
            print("Need at least 3 points.")

    elif key == ord('r'):
        points = []
        print("Points reset.")

    elif key == ord('q'):
        break

cv2.destroyAllWindows()

# Remove duplicate closing point if exists
if len(points) >= 3:
    if points[0] == points[-1]:
        points = points[:-1]

    print("\nPixel Coordinates:")
    print(points)

    # Normalize + round
    normalized = [
        {"x": round(x / width, 3), "y": round(y / height, 3)}
        for (x, y) in points
    ]

    print("\nDB Ready ROI JSON:")
    print(json.dumps(normalized, indent=4))
else:
    print("Polygon not defined.")
