"""
Model Loader

YOLO/TensorRT load + infer abstraction for Jetson device.
Loads Ultralytics YOLO models and runs inference on frames.

Key Features:
    - Loads YOLO models (supports .pt, .onnx, .engine formats)
    - Runs inference on frames
    - Returns structured detection results
    - Supports CPU/GPU device selection

Usage:
    from runtime.model_loader import ModelRunner
    
    runner = ModelRunner("models/best.pt", device="cuda")
    detections = runner.infer(frame)
"""

from ultralytics import YOLO


class ModelRunner:
    """
    YOLO model runner for inference on Jetson device.
    
    Handles model loading and inference, returning structured detection results.
    """
    
    def __init__(self, model_path: str, device: str = "cuda"):
        """
        Initialize model runner.
        
        Args:
            model_path: Path to YOLO model file (.pt, .onnx, or .engine)
            device: Device to run inference on ("cpu", "cuda", "0", etc.)
        """
        self.model = YOLO(model_path)
        self.device = device
    
    def infer(self, frame):
        """
        Run inference on a single frame.
        
        Args:
            frame: Input frame (numpy array or image)
            
        Returns:
            List of detection dictionaries, each containing:
                - class: Detected class name (e.g., "milking", "scraping")
                - confidence: Detection confidence score (0.0 to 1.0)
                - bbox: Bounding box coordinates [x1, y1, x2, y2]
        """
        results = self.model(frame, device=self.device, verbose=False)[0]
        
        detections = []
        
        for b in results.boxes:
            detections.append({
                "class": self.model.names[int(b.cls)],
                "confidence": float(b.conf),
                "bbox": list(map(float, b.xyxy[0])),
            })
        
        return detections
