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
SOURCE = "jetson/roi_selection/grp2_tmr_way.jpeg"
# SOURCE = "rtsp://your_rtsp_here"
# ------------------------

points = []

def mouse_callback(event, x, y, flags, param):
    global points
    if event == cv2.EVENT_LBUTTONDOWN:
        points.append((x, y))
        print(f"Clicked: ({x}, {y})")

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

cv2.namedWindow("ROI Selector")
cv2.setMouseCallback("ROI Selector", mouse_callback)

print("\nInstructions:")
print("Click to add polygon points")
print("Press 'c' to close polygon")
print("Press 'r' to reset")
print("Press 'q' to quit\n")

while True:
    display = frame.copy()

    # Draw points
    for p in points:
        cv2.circle(display, p, 5, (0, 255, 0), -1)

    # Draw polygon lines
    if len(points) > 1:
        cv2.polylines(display, [np.array(points)], False, (255, 0, 0), 2)

    cv2.imshow("ROI Selector", display)
    key = cv2.waitKey(1)

    if key == ord('c'):  # Close polygon
        if len(points) >= 3:
            cv2.polylines(display, [np.array(points)], True, (0, 0, 255), 2)
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
