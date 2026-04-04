# jetson/rintime/video_stream.py
import cv2
import time
import os
import threading

# Suppress GStreamer warnings in production
os.environ["OPENCV_LOG_LEVEL"] = "ERROR"

# Hardware-aware decode control: limit concurrent GPU-decoded streams.
# NOTE: This is process-wide for the Jetson edge_detector process.
MAX_GPU_STREAMS = 1
gpu_stream_count = 0
gpu_stream_count_lock = threading.Lock()

class VideoStream:
	def __init__(self, cap, on_release=None):
		self.cap = cap
		self._on_release = on_release

	def read(self):
		return self.cap.read()

	def release(self):
		self.cap.release()
		if self._on_release:
			self._on_release()


def _validate_gstreamer():
	"""
	Ensure OpenCV is built with GStreamer support.
	"""
	info = cv2.getBuildInformation()

	for line in info.split("\n"):
		if "GStreamer" in line:
			if "YES" in line:
				return
			else:
				raise RuntimeError("OpenCV built without GStreamer support")

	raise RuntimeError("GStreamer entry not found in OpenCV build info")


def build_pipeline(rtsp_url, codec):
	"""
	Build production-ready GStreamer pipeline for RTSP streams.
	Supports h264 and h265 with hardware acceleration (nvv4l2decoder).
	
	Key features:
	- protocols=tcp: Prevents UDP packet drops on real deployments
	- queue: Buffers between stages for stability
	- nvv4l2decoder: Hardware H.264/H.265 decode
	- drop-on-latency=true: Drops old frames instead of queuing
	- timeout=5000000: Prevents infinite hang on disconnect (5s timeout)
	"""
	if codec == "h264":
		depay = "rtph264depay ! h264parse"
	else:
		depay = "rtph265depay ! h265parse"

	return (
		f"rtspsrc location={rtsp_url} latency=0 protocols=tcp "
		"drop-on-latency=true timeout=5000000 ! "
		"queue ! "
		f"{depay} ! "
		"nvv4l2decoder ! "
		"nvvidconv ! video/x-raw,format=BGRx ! "
		"videoconvert ! video/x-raw,format=BGR ! "
		"appsink drop=true max-buffers=1 sync=false"
	)


def open_file_nvdec(path):
	"""
	Open local video files using Jetson hardware decode.
	Supports MP4, MPEG-PS, H264, H265.
	"""

	pipelines = [

		# MPEG-PS + H265 (DVR export)
		f'filesrc location="{path}" ! mpegpsdemux ! h265parse ! '
		'nvv4l2decoder ! nvvidconv ! video/x-raw,format=BGRx ! '
		'videoconvert ! video/x-raw,format=BGR ! '
		'appsink drop=true max-buffers=1 sync=false',

		# MPEG-PS + H264
		f'filesrc location="{path}" ! mpegpsdemux ! h264parse ! '
		'nvv4l2decoder ! nvvidconv ! video/x-raw,format=BGRx ! '
		'videoconvert ! video/x-raw,format=BGR ! '
		'appsink drop=true max-buffers=1 sync=false',

		# MP4 container
		f'filesrc location="{path}" ! qtdemux ! h265parse ! '
		'nvv4l2decoder ! nvvidconv ! video/x-raw,format=BGRx ! '
		'videoconvert ! video/x-raw,format=BGR ! '
		'appsink drop=true max-buffers=1 sync=false',

		# MP4 container + H264
		f'filesrc location="{path}" ! qtdemux ! h264parse ! '
		'nvv4l2decoder ! nvvidconv ! video/x-raw,format=BGRx ! '
		'videoconvert ! video/x-raw,format=BGR ! '
		'appsink drop=true max-buffers=1 sync=false',

		# Fallback
		f'filesrc location="{path}" ! decodebin ! videoconvert ! '
		'appsink drop=true max-buffers=1 sync=false'
	]

	for pipeline in pipelines:

		cap = cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)

		# Force decoder buffer cleanup
		time.sleep(0.5)

		if cap.isOpened():
			time.sleep(0.3)
			ret, _ = cap.read()
			if ret:
				print("[STREAM] Video file opened successfully")
				return cap

		cap.release()
		time.sleep(1.0)  # Allow NVDEC to free buffers

	raise RuntimeError("Failed to open video file")


def open_rtsp_auto(rtsp_url):
	"""
	Open RTSP stream with hardware acceleration (nvv4l2decoder).
	
	Attempts both H.264 and H.265 codecs to handle varying camera/NVR configurations.
	Includes 500ms warm-up delay for camera initialization.
	"""

	# Try H.264 first, then fall back to H.265 if needed.
	for codec in ("h264", "h265"):
		pipeline = build_pipeline(rtsp_url, codec)

		# Retry loop to handle flaky RTSP connections / camera timeouts.
		for attempt in range(3):
			cap = cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)

			# Best-effort guard against latency buildup on backends that honor this prop.
			try:
				if cap.getBackendName() == "GStreamer":
					cap.set(cv2.CAP_PROP_BUFFERSIZE, 2)
			except Exception:
				pass

			if not cap.isOpened():
				cap.release()
				time.sleep(1)
				continue

			time.sleep(0.5)

			ret, _ = cap.read()
			if ret:
				print(f"[STREAM] RTSP connected ({codec}): {rtsp_url}")
				return cap

			cap.release()
			time.sleep(1)

	raise RuntimeError(f"Failed to open RTSP stream: {rtsp_url}")


def open_stream(camera_cfg):
	"""
	Single entry point for ALL video sources.
	Supported stream_type:
		- FILE
		- RTSP
		- NVR
	
	SMART FALLBACK:
		If rtsp_url exists → use it directly
		Else if nvr_channel exists → construct RTSP from base + channel
	"""
	stream_type = camera_cfg.get("stream_type", "AUTO").upper()
	rtsp_url = None
	determined_stream_type = None

	# --------------------------------------------------
	# Decode selection with GPU stream limiting
	# --------------------------------------------------
	decode_mode = camera_cfg.get("decode_mode", "AUTO").upper()

	# Decide if this stream should use GPU decode.
	# This caps the number of concurrent NVDEC/GStreamer GPU decodes.
	use_gpu = False
	global gpu_stream_count
	with gpu_stream_count_lock:
		if decode_mode == "GPU":
			if gpu_stream_count < MAX_GPU_STREAMS:
				gpu_stream_count += 1
				use_gpu = True
			else:
				print("[STREAM] GPU limit reached → CPU fallback")
		elif decode_mode == "CPU":
			use_gpu = False
		else:  # AUTO
			use_gpu = False

	on_release = None
	if use_gpu:
		def _on_release():
			global gpu_stream_count
			with gpu_stream_count_lock:
				gpu_stream_count = max(0, gpu_stream_count - 1)
		on_release = _on_release

	def rollback_gpu_slot():
		nonlocal on_release
		if on_release:
			on_release()
			on_release = None

	try:
		# --------------------------------------------------
		# FILE (offline / testing)
		# --------------------------------------------------
		if stream_type == "FILE":
			path = camera_cfg.get("video_file_path")
			if not path:
				raise RuntimeError("FILE stream missing video_file_path in local_cache.json")

			if use_gpu:
				print(f"[STREAM] FILE → GPU decode (NVDEC): {path}")
				cap = open_file_nvdec(path)
			else:
				print(f"[STREAM] FILE → CPU decode (FFMPEG): {path}")
				cap = cv2.VideoCapture(path, cv2.CAP_FFMPEG)
				ret, _ = cap.read()
				if not ret:
					cap.release()
					raise RuntimeError(f"FILE stream read failed: {path}")
				try:
					cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
				except Exception:
					pass
			
			determined_stream_type = "FILE"

		# --------------------------------------------------
		# SMART RTSP RESOLUTION (explicit or constructed)ex
		# --------------------------------------------------
		else:
			# Priority 1: Direct RTSP URL
			if camera_cfg.get("rtsp_url"):
				rtsp_url = camera_cfg["rtsp_url"]
				determined_stream_type = "RTSP"
			
			# Priority 2: Construct from NVR channel
			elif camera_cfg.get("nvr_channel") and camera_cfg.get("nvr_rtsp_base"):
				nvr_base_rtsp = camera_cfg["nvr_rtsp_base"]   # e.g. rtsp://user:pass@192.168.1.10:554
				channel = camera_cfg["nvr_channel"]           # e.g. 101 / ch01 / 1

				# Backend MUST provide final format rules
				# Example for Hikvision-style NVR:
				# rtsp://ip:554/Streaming/Channels/101
				rtsp_url = f"{nvr_base_rtsp}/Streaming/Channels/{channel}"
				determined_stream_type = "NVR"
			
			# No valid stream source found
			else:
				raise ValueError(
					f"Camera {camera_cfg.get('camera_id', 'UNKNOWN')}: "
					"Missing rtsp_url AND (nvr_rtsp_base + nvr_channel). "
					"Must provide at least one stream source."
				)

			# Open RTSP stream based on the GPU decode decision above.
			if use_gpu:
				# GPU decode: use GStreamer + nvv4l2decoder
				try:
					cap = open_rtsp_auto(rtsp_url)
					if decode_mode in ("GPU", "AUTO"):
						print(f"[STREAM] Using GPU hardware decoder: {rtsp_url}")
				except Exception as e:
					# If GPU decode fails, fall back to CPU decode for correctness.
					rollback_gpu_slot()
					print(f"[STREAM] GPU decode failed ({decode_mode}) → CPU fallback (FFMPEG): {e}")
					cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
					# Reduce buffering (critical for live streams)
					try:
						cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
					except:
						pass
			else:
				# CPU decode: FFMPEG via OpenCV
				print(f"[STREAM] Using CPU decode (FFMPEG): {rtsp_url}")
				cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
				# Reduce buffering (critical for live streams)
				try:
					cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
				except:
					pass

		if not cap.isOpened():
			raise RuntimeError(
				f"Failed to open stream | type={determined_stream_type} | "
				f"camera_id={camera_cfg.get('camera_id', 'UNKNOWN')}"
			)
	except Exception:
		rollback_gpu_slot()
		raise

	return VideoStream(cap, on_release=on_release)
