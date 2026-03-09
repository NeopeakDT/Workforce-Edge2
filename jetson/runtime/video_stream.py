# jetson/rintime/video_stream.py
import cv2
import time

class VideoStream:
	def __init__(self, cap):
		self.cap = cap

	def read(self):
		return self.cap.read()

	def release(self):
		self.cap.release()


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
		f"rtspsrc location={rtsp_url} latency=50 protocols=tcp "
		"drop-on-latency=true timeout=5000000 ! "
		"queue ! "
		f"{depay} ! "
		"nvv4l2decoder ! "
		"nvvidconv ! video/x-raw,width=1280,height=720,format=BGRx ! "
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
		'nvv4l2decoder ! nvvidconv ! video/x-raw,width=1280,height=720,format=BGRx ! '
		'videoconvert ! video/x-raw,format=BGR ! '
		'appsink drop=true max-buffers=1 sync=false',

		# MPEG-PS + H264
		f'filesrc location="{path}" ! mpegpsdemux ! h264parse ! '
		'nvv4l2decoder ! nvvidconv ! video/x-raw,width=1280,height=720,format=BGRx ! '
		'videoconvert ! video/x-raw,format=BGR ! '
		'appsink drop=true max-buffers=1 sync=false',

		# MP4 container
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
		time.sleep(0.2)

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
	Open RTSP stream with H264 codec and GStreamer pipeline.
	Uses hardware acceleration (nvv4l2decoder).
	
	Includes 500ms warm-up delay for camera initialization.
	"""
	codec = "h264"
	pipeline = build_pipeline(rtsp_url, codec)

	cap = cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)

	if not cap.isOpened():
		raise RuntimeError("Failed to open RTSP stream")

	time.sleep(0.5)

	ret, _ = cap.read()
	if not ret:
		cap.release()
		raise RuntimeError("RTSP stream opened but frame read failed")

	print(f"[STREAM] RTSP connected: {rtsp_url}")

	return cap


def open_stream(camera_cfg):
	"""
	Single entry point for ALL video sources.
	Supported stream_type:
		- FILE
		- RTSP
		- NVR_CHANNEL
	
	SMART FALLBACK:
		If rtsp_url exists → use it directly
		Else if nvr_channel exists → construct RTSP from base + channel
	"""
	# Validate GStreamer support before any stream operations
	_validate_gstreamer()

	stream_type = camera_cfg.get("stream_type")
	rtsp_url = None
	determined_stream_type = None

	# --------------------------------------------------
	# FILE (offline / testing)
	# --------------------------------------------------
	if stream_type == "FILE":
		path = camera_cfg.get("video_file_path")
		if not path:
			raise RuntimeError("FILE stream missing video_file_path in local_cache.json")
		cap = open_file_nvdec(path)  # Use hardware decode for files
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
			determined_stream_type = "NVR_CHANNEL"
		
		# No valid stream source found
		else:
			raise ValueError(
				f"Camera {camera_cfg.get('camera_id', 'UNKNOWN')}: "
				"Missing rtsp_url AND (nvr_rtsp_base + nvr_channel). "
				"Must provide at least one stream source."
			)

		# Open RTSP stream with auto-detected codec
		cap = open_rtsp_auto(rtsp_url)

	if not cap.isOpened():
		raise RuntimeError(
			f"Failed to open stream | type={determined_stream_type} | "
			f"camera_id={camera_cfg.get('camera_id', 'UNKNOWN')}"
		)

	return VideoStream(cap)


