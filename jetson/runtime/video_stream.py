# jetson/rintime/video_stream.py
import cv2

class VideoStream:
	def __init__(self, cap):
		self.cap = cap

	def read(self):
		return self.cap.read()

	def release(self):
		self.cap.release()


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
		cap = cv2.VideoCapture(path)
		determined_stream_type = "FILE"

	# --------------------------------------------------
	# SMART RTSP RESOLUTION (explicit or constructed)
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

		# Open RTSP stream
		cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
		cap.set(cv2.CAP_PROP_BUFFERSIZE, 2)

	if not cap.isOpened():
		raise RuntimeError(
			f"Failed to open stream | type={determined_stream_type} | "
			f"camera_id={camera_cfg.get('camera_id', 'UNKNOWN')}"
		)

	return VideoStream(cap)


