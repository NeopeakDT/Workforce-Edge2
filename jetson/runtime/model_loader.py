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

import os
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

        # TEMPORARY: Force CPU for testing (uncomment to test on CPU)
        self.device = "cpu"

        # Check if this is a TensorRT engine file
        self.is_engine = model_path.endswith('.engine')

        # For CPU testing, use .pt model instead of .engine (TensorRT needs GPU)
        if self.device == "cpu" and self.is_engine:
            print("🔧 ModelRunner: CPU detected, switching to PyTorch model for compatibility")
            # Replace .engine with .pt in the path
            pt_path = model_path.replace('.engine', '.pt')
            print(f"🔧 ModelRunner: Checking for PyTorch model at: {pt_path}")
            print(f"🔧 ModelRunner: PyTorch model exists: {os.path.exists(pt_path)}")
            if os.path.exists(pt_path):
                print("🔧 ModelRunner: Loading PyTorch model...")
                self.model = YOLO(pt_path)
                self.is_engine = False
                print(f"✅ ModelRunner: Successfully loaded PyTorch model: {pt_path}")
            else:
                print(f"⚠️ ModelRunner: PyTorch model not found at {pt_path}, using TensorRT anyway")

        print(f"🔧 ModelRunner: Loading model on device '{self.device}' (format: {'TensorRT' if self.is_engine else 'PyTorch'})")

        # Handle different model formats
        if self.is_engine:
            # TensorRT engine files are already optimized, don't apply PyTorch operations
            print("🔧 ModelRunner: TensorRT engine loaded (no device transfer needed)")
        elif self.device == "cuda" or (isinstance(self.device, str) and "cuda" in self.device):
            print("🔧 ModelRunner: Applying CUDA optimizations (FP16, fused)")
            self.model.fuse()
            self.model.to(self.device)
            self.model.model.half()  # Convert to FP16
            print("✅ ModelRunner: CUDA model loaded successfully")
        else:
            print("🔧 ModelRunner: Loading on CPU (no CUDA optimizations)")
            self.model.to(self.device)
    
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
        # Prepare inference parameters based on model type
        inference_kwargs = {
            "imgsz": 320,  # Further reduced for Jetson memory constraints
            "conf": 0.25,  # Default confidence threshold
            "verbose": False
        }

        # Handle different model formats
        if self.is_engine:
            # TensorRT engine - device is already configured, don't specify half
            inference_kwargs["device"] = self.device
        elif self.device == "cuda" or (isinstance(self.device, str) and "cuda" in self.device):
            # PyTorch CUDA model - use FP16 optimizations
            inference_kwargs["device"] = self.device
            inference_kwargs["half"] = True

            # Convert frame to FP16 for CUDA
            import numpy as np
            frame = frame.astype(np.float16)
        else:
            # CPU model
            inference_kwargs["device"] = self.device

        results = self.model(frame, **inference_kwargs)[0]
        
        detections = []
        
        for b in results.boxes:
            detections.append({
                "class": self.model.names[int(b.cls)],
                "confidence": float(b.conf),
                "bbox": list(map(float, b.xyxy[0])),
            })
        
        return detections
