from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = PACKAGE_ROOT / "models"


@dataclass(frozen=True)
class Profile:
    name: str
    method: str
    model: str
    rotate: str
    line_ratio: tuple[float, float, float, float]
    imgsz: int
    conf: float
    stride: int
    tracker: str | None = None
    max_distance: float = 42.0
    min_iou: float = 0.02
    max_missed: int = 8
    note: str = ""


DEFAULT_PROFILES: dict[str, Profile] = {
    "horizontal_mainline": Profile(
        name="horizontal_mainline",
        method="bytetrack",
        model=str(MODELS_DIR / "yolo11s.pt"),
        rotate="none",
        line_ratio=(650 / 1920, 820 / 1080, 980 / 1920, 300 / 1080),
        imgsz=1280,
        conf=0.16,
        stride=2,
        tracker=None,
        note="Landscape/high-rise view profile calibrated on test.MOV.",
    ),
    "horizontal_inbound_left": Profile(
        name="horizontal_inbound_left",
        method="bytetrack",
        model=str(MODELS_DIR / "yolo11s.pt"),
        rotate="none",
        line_ratio=(650 / 1920, 820 / 1080, 980 / 1920, 300 / 1080),
        imgsz=1280,
        conf=0.16,
        stride=2,
        tracker=None,
        note="Landscape Caitian Road left carriageway profile.",
    ),
    "horizontal_outbound_right": Profile(
        name="horizontal_outbound_right",
        method="bytetrack",
        model=str(MODELS_DIR / "yolo11s.pt"),
        rotate="none",
        line_ratio=(1035 / 1920, 635 / 1080, 1620 / 1920, 790 / 1080),
        imgsz=1280,
        conf=0.16,
        stride=2,
        tracker=None,
        note="Landscape Caitian Road right carriageway profile for outbound videos.",
    ),
    "vertical_rotated_dense": Profile(
        name="vertical_rotated_dense",
        method="simpletrack",
        model=str(MODELS_DIR / "yolo11m.pt"),
        rotate="cw90",
        line_ratio=(545 / 984, 70 / 552, 628 / 984, 520 / 552),
        imgsz=1600,
        conf=0.05,
        stride=2,
        max_distance=45.0,
        min_iou=0.02,
        max_missed=8,
        note="Portrait video is rotated clockwise before counting; use as visual QA unless manually calibrated.",
    ),
}


def profile_from_mapping(name: str, values: dict[str, Any]) -> Profile:
    base = DEFAULT_PROFILES.get(name)
    merged: dict[str, Any] = dict(base.__dict__) if base else {"name": name}
    merged.update(values)
    if "line_ratio" in merged:
        merged["line_ratio"] = tuple(float(v) for v in merged["line_ratio"])
    return Profile(**merged)


def load_profiles(config_path: Path | None = None) -> dict[str, Profile]:
    profiles = dict(DEFAULT_PROFILES)
    if not config_path:
        return profiles
    data = yaml.safe_load(config_path.read_text()) or {}
    for name, values in (data.get("profiles") or {}).items():
        profiles[name] = profile_from_mapping(name, values)
    return profiles


def resolve_model(model: str) -> str:
    path = Path(model)
    if path.exists():
        return str(path)
    return path.name
