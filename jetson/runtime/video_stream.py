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
	"""

	stream_type = camera_cfg.get("stream_type")

	# --------------------------------------------------
	# FILE (offline / testing)
	# --------------------------------------------------
	if stream_type == "FILE":
		path = camera_cfg.get("video_file_path")
		if not path:
			raise RuntimeError("FILE stream missing video_file_path in local_cache.json")
		cap = cv2.VideoCapture(path)

	# --------------------------------------------------
	# DIRECT RTSP (camera or NVR substream)
	# --------------------------------------------------
	elif stream_type == "RTSP":
		rtsp_url = camera_cfg["rtsp_url"]

		cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
		cap.set(cv2.CAP_PROP_BUFFERSIZE, 2)

	# --------------------------------------------------
	# NVR CHANNEL (RTSP constructed from base + channel)
	# --------------------------------------------------
	elif stream_type == "NVR_CHANNEL":
		nvr_base_rtsp = camera_cfg["nvr_rtsp_base"]   # e.g. rtsp://user:pass@192.168.1.10:554
		channel = camera_cfg["nvr_channel"]           # e.g. 101 / ch01 / 1

		# Backend MUST provide final format rules
		# Example for Hikvision-style NVR:
		# rtsp://ip:554/Streaming/Channels/101
		rtsp_url = f"{nvr_base_rtsp}/Streaming/Channels/{channel}"

		cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
		cap.set(cv2.CAP_PROP_BUFFERSIZE, 2)

	# --------------------------------------------------
	else:
		raise ValueError(f"Unknown stream_type={stream_type}")

	if not cap.isOpened():
		raise RuntimeError(
			f"Failed to open stream | type={stream_type}"
		)

	return VideoStream(cap)


