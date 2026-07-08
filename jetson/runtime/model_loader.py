"""
jetson/runtime/model_loader.py
Model Loader

YOLO/TensorRT load + infer abstraction for Jetson device.
Loads Ultralytics YOLO models and runs inference on frames.

Key Features:
    - Loads YOLO models (supports .pt, .onnx, .engine formats)
    - Runs inference on frames
    - Returns structured detection results
    - Supports CPU/GPU device selection

Usage:
    from runtime.model_loader import ModelRunner, create_edge_runners, batch_size_for

    runners = create_edge_runners(
        "models/best.pt",
        optional_models={"MILKING": "models/milking.pt"},
    )
    detections = runners["WORKFORCE"].infer(frame)
"""

import os
from ultralytics import YOLO

# Static TensorRT batch profiles per pipeline runner key.
MODEL_BATCH_SIZE = {
    "WORKFORCE": 2,
    "MILKING": 1,
    "POSTURE": 1,
}


def batch_size_for(model_type: str) -> int:
    """Return static TensorRT batch size for a pipeline runner key."""
    return MODEL_BATCH_SIZE.get(model_type, 1)


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
        
        # Check if this is a TensorRT engine file
        self.is_engine = model_path.endswith('.engine')
        
        # Auto-detect device: Use CUDA for TensorRT engines, respect device parameter otherwise
        if self.is_engine:
            # TensorRT engines require GPU/CUDA
            self.device = "cuda"  # Force CUDA for TensorRT
            print("🔧 ModelRunner: TensorRT engine detected, using CUDA (required)")
        else:
            # For .pt/.onnx models, use the provided device or default to cuda
            self.device = device if device else "cuda"
            print(f"🔧 ModelRunner: Using device: {self.device}")
        
        # Normalize CUDA detection (supports "cuda", "cuda:0", etc.)
        self.is_cuda = self.device == "cuda" or (
            isinstance(self.device, str) and "cuda" in self.device
        )

        # Validate CUDA availability if using CUDA
        if self.is_cuda:
            try:
                import torch
                if not torch.cuda.is_available():
                    raise RuntimeError("CUDA requested but not available. Check GPU drivers.")
                print(f"✅ ModelRunner: CUDA available - Device: {torch.cuda.get_device_name(0)}")
            except ImportError:
                print("⚠️ ModelRunner: PyTorch not available, CUDA check skipped")
            except Exception as e:
                raise RuntimeError(f"CUDA setup failed: {e}")

        print(f"🔧 ModelRunner: Loading model on device '{self.device}' (format: {'TensorRT' if self.is_engine else 'PyTorch'})")

        # Handle different model formats
        if self.is_engine:
            # TensorRT engine files are already optimized for GPU
            # No PyTorch operations needed - TensorRT handles device placement
            print("✅ ModelRunner: TensorRT engine loaded (GPU-optimized)")
        elif (not self.is_engine) and self.is_cuda:
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
        Run inference on a single frame or batch of frames.
        
        Args:
            frame: Single frame (HWC numpy array) or list/tuple of frames
            
        Returns:
            - If single frame: List[Dict] of detections
            - If batch: List[List[Dict]] (one list per input frame)
        """
        is_batch = isinstance(frame, (list, tuple))
        inputs = frame if is_batch else [frame]
        # Prepare inference parameters based on model type
        inference_kwargs = {
            "imgsz": 512,  # Slightly larger for better accuracy, still Jetson-friendly
            "conf": 0.65,  # Default confidence threshold
            "max_det": 100,  # Limit max detections per frame to stabilize latency
        }

        # Handle different model formats
        if self.is_engine:
            # TensorRT engine - device is already configured, don't specify half
            inference_kwargs["device"] = self.device
        elif (not self.is_engine) and self.is_cuda:
            # PyTorch CUDA model - use FP16 optimizations (model is already FP16, frame stays uint8)
            inference_kwargs["device"] = self.device
            inference_kwargs["half"] = True
        else:
            # CPU model
            inference_kwargs["device"] = self.device

        # CRITICAL: stream=False to prevent GPU/DMA aliasing issues
        # Explicitly set stream=False and verbose=False (not in kwargs to ensure they're not overridden)
        results = self.model(
            inputs,
            stream=False,
            verbose=False,
            **inference_kwargs
        )

        # Ultralytics returns a list of results when given a list of inputs
        if not isinstance(results, list):
            results = [results]

        batch_detections = []
        for res in results:
            dets = []
            for b in res.boxes:
                dets.append({
                    "class": self.model.names[int(b.cls)],
                    "confidence": float(b.conf),
                    "bbox": list(map(float, b.xyxy[0])),
                })
            batch_detections.append(dets)
        
        return batch_detections if is_batch else batch_detections[0]


def create_edge_runners(
    workforce_model_path: str,
    *,
    device: str = "cuda",
    optional_models: dict[str, str] | None = None,
) -> dict[str, ModelRunner]:
    """
    Load workforce + optional edge ModelRunner instances.

    Returns a registry keyed by pipeline name, e.g.:
        {"WORKFORCE": ModelRunner, "MILKING": ModelRunner, ...}

    Example:
        runners = create_edge_runners(
            wf_model_path,
            optional_models={
                "MILKING": milking_path,
                "POSTURE": posture_path,
            },
        )
    """
    runners: dict[str, ModelRunner] = {}
    runners["WORKFORCE"] = ModelRunner(workforce_model_path, device=device)

    for model_name, model_path in (optional_models or {}).items():
        if model_path and os.path.exists(model_path):
            runners[model_name] = ModelRunner(model_path, device=device)

    return runners
