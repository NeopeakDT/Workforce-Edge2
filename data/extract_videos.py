#!/usr/bin/env python3
"""
===============================================================================
BEST 2 FRAMES PER SECOND SELECTOR (YOLO TRAINING SAFE)
===============================================================================

- Selects the 2 sharpest frames per second
- Uses Laplacian variance (edge clarity)
- Lossless PNG output
- Designed for thin tools (scrapping_tool)
===============================================================================
How this script prepares cleaner frames for pseudo-labeling:

- The video is processed second by second. For each second, all frames are evaluated.
- Each frame gets a sharpness score using Laplacian variance (edge clarity).
- Blurry / low-quality frames (common at night) are discarded using BLUR_THRESHOLD.
- From the remaining frames, only the top 2 sharpest frames per second are saved.
- Output is lossless PNG, which preserves thin tools (e.g., scrapping_tool) for better detection.
"""

import cv2
import os
import numpy as np

# ---------------- CONFIG ----------------
FRAMES_PER_SECOND = 2
BLUR_THRESHOLD = 80.0
USE_FFMPEG = True
# --------------------------------------


def sharpness_score(frame):
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    return cv2.Laplacian(gray, cv2.CV_64F).var()


def extract_best_frames(video_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    backend = cv2.CAP_FFMPEG if USE_FFMPEG else 0
    cap = cv2.VideoCapture(video_path, backend)
    if not cap.isOpened():
        print(f"❌ Failed to open video: {video_path}")
        return

    video_name = os.path.splitext(os.path.basename(video_path))[0]
    prefix = "".join(c for c in video_name if c.isalnum() or c in ('_', '-'))

    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps is None or fps < 1:
        print("⚠ Invalid FPS metadata, defaulting to 25")
        fps = 25.0

    frames_per_sec = int(round(fps))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    total_secs = total_frames // frames_per_sec

    print(f"\n🎬 Video: {video_name}")
    print(f"📐 FPS: {fps:.2f}")
    print(f"⏱ Duration: {total_secs} sec")
    print(f"🎯 Frames/sec target: {FRAMES_PER_SECOND}")

    frame_idx = 0
    saved = 0
    rejected_seconds = 0

    while True:
        candidates = []

        for _ in range(frames_per_sec):
            ret, frame = cap.read()
            if not ret:
                break

            score = sharpness_score(frame)
            if score >= BLUR_THRESHOLD:
                candidates.append((score, frame, frame_idx))

            frame_idx += 1

        if not candidates:
            rejected_seconds += 1
            if frame_idx >= total_frames:
                break
            continue

        # Pick top N sharpest frames
        candidates.sort(key=lambda x: x[0], reverse=True)
        selected = candidates[:FRAMES_PER_SECOND]

        for i, (score, frame, idx) in enumerate(selected):
            fname = f"{prefix}_sec_{saved:05d}_f{i}_sharp_{int(score)}.png"
            cv2.imwrite(
                os.path.join(output_dir, fname),
                frame,
                [cv2.IMWRITE_PNG_COMPRESSION, 0]
            )
            saved += 1

        if frame_idx >= total_frames:
            break

    cap.release()

    print(f"\n✅ Saved frames: {saved}")
    print(f"🚫 Rejected seconds: {rejected_seconds}")
    print(f"📁 Output folder: {output_dir}")


if __name__ == "__main__":
    print("=== Best 2 Frames Per Second Selector ===")

    video_path = input("Enter video path: ").strip().strip('"').strip("'")
    if not os.path.exists(video_path):
        print("❌ Video not found")
        exit(1)

    output_dir = input("Output folder (default: unlabeled): ").strip()
    output_dir = output_dir if output_dir else "unlabeled"

    extract_best_frames(video_path, output_dir)
