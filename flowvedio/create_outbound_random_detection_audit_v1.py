#!/usr/bin/env python3
"""Create deterministic random detection-audit screenshots for outbound-camera video."""

from pathlib import Path
import json
import random

import cv2
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "batch_outputs/20260723_24_state_dependent_recognition_v5"
CONFIG = ROOT / "batch_outputs/20260723_24_state_dependent_manual_review_v5/state_dependent_config_v5.json"
VIDEOS = ROOT / "2026072324彩田路"
OUTPUT = SOURCE / "analysis_basic_v1/outbound_random_detection_audit_v1"
SEED = 20260825

CLASS_COLORS = {
    "car": (60, 190, 70),
    "bus": (210, 80, 190),
    "truck": (30, 150, 240),
    "motorcycle": (220, 190, 40),
}


def label_box(img: np.ndarray, text: str, x: int, y: int, color: tuple[int, int, int]) -> None:
    scale, thick = 0.46, 1
    (w, h), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, scale, thick)
    top = max(0, y - h - 8)
    cv2.rectangle(img, (x, top), (min(img.shape[1] - 1, x + w + 7), y), color, -1)
    cv2.putText(img, text, (x + 3, y - 4), cv2.FONT_HERSHEY_SIMPLEX,
                scale, (255, 255, 255), thick, cv2.LINE_AA)


def read_frame(path: Path, frame_index: int) -> np.ndarray:
    cap = cv2.VideoCapture(str(path))
    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
    ok, frame = cap.read()
    cap.release()
    if not ok:
        raise RuntimeError(f"Could not read frame {frame_index} from {path}")
    return frame


def annotate(frame: np.ndarray, detections: pd.DataFrame, header: str) -> np.ndarray:
    out = frame.copy()
    for row in detections.itertuples(index=False):
        cls = str(row.current_frame_class)
        color = CLASS_COLORS.get(cls, (100, 220, 220))
        x1, y1, x2, y2 = map(lambda z: int(round(float(z))),
                             [row.bbox_x1, row.bbox_y1, row.bbox_x2, row.bbox_y2])
        cv2.rectangle(out, (x1, y1), (x2, y2), color, 2, cv2.LINE_AA)
        label_box(out, f"{cls} {row.confidence:.2f}  ID {int(row.track_id)}", x1, y1, color)

    overlay = out.copy()
    cv2.rectangle(overlay, (0, 0), (out.shape[1], 54), (0, 0, 0), -1)
    cv2.addWeighted(overlay, 0.72, out, 0.28, 0, out)
    cv2.putText(out, header, (16, 34), cv2.FONT_HERSHEY_SIMPLEX,
                0.72, (255, 255, 255), 2, cv2.LINE_AA)
    return out


def contact_sheet(images: list[np.ndarray], labels: list[str]) -> np.ndarray:
    thumb_w, thumb_h = 800, 450
    tiles = []
    for img, label in zip(images, labels):
        thumb = cv2.resize(img, (thumb_w, thumb_h), interpolation=cv2.INTER_AREA)
        cv2.putText(thumb, label, (14, thumb_h - 14), cv2.FONT_HERSHEY_SIMPLEX,
                    0.55, (255, 255, 255), 2, cv2.LINE_AA)
        tiles.append(thumb)
    rows = [np.hstack(tiles[i:i + 2]) for i in range(0, len(tiles), 2)]
    return np.vstack(rows)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    plan = pd.read_csv(SOURCE / "run_plan.csv")
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    states = {str(x["阶段编号"]): x for x in config["states"]}
    plan["date"] = plan["stage_id"].map(lambda x: str(states[str(x)]["日期"]))
    plan["camera"] = plan["stage_id"].map(lambda x: str(states[str(x)]["采集方向"]))
    plan = plan[plan["camera"].eq("出城")].copy()
    plan = plan[(plan["end_s"] - plan["start_s"]) >= 300].copy()

    selected = []
    for date in ["2026-07-23", "2026-07-24"]:
        for state in ["向南开放", "向北开放"]:
            candidates = plan[(plan["date"] == date) & (plan["tidal_state"] == state)]
            choices = rng.sample(list(candidates.index), k=min(2, len(candidates)))
            selected.extend(choices)

    images, labels, manifest = [], [], []
    for number, idx in enumerate(selected, start=1):
        row = plan.loc[idx]
        tracks_path = Path(row["measurement_dir"]) / "trajectories.csv"
        tracks = pd.read_csv(tracks_path)
        lo = float(row["start_s"]) + 0.15 * (float(row["end_s"]) - float(row["start_s"]))
        hi = float(row["start_s"]) + 0.85 * (float(row["end_s"]) - float(row["start_s"]))
        target = rng.uniform(lo, hi)
        eligible = tracks[(tracks["timestamp_s"] >= lo) & (tracks["timestamp_s"] <= hi)]
        timestamps = eligible["timestamp_s"].drop_duplicates().to_numpy()
        chosen_t = float(timestamps[np.abs(timestamps - target).argmin()])
        chosen = tracks[np.isclose(tracks["timestamp_s"], chosen_t)].copy()
        frame_index = int(chosen["frame_index"].iloc[0])
        state = states[str(row["stage_id"])]
        actual = pd.Timestamp(f'{state["日期"]} {state["实际开始"]}') + pd.to_timedelta(
            chosen_t - float(row["start_s"]), unit="s"
        )
        frame = read_frame(VIDEOS / str(row["source_file"]), frame_index)
        header = (f"Sample {number:02d} | {actual.strftime('%Y-%m-%d %H:%M:%S')} | "
                  f"outbound camera | {row['tidal_state']} | {len(chosen)} vehicles")
        marked = annotate(frame, chosen, header)
        filename = f"sample_{number:02d}_{actual.strftime('%Y%m%d_%H%M%S')}_{row['stage_id']}.jpg"
        cv2.imwrite(str(OUTPUT / filename), marked, [cv2.IMWRITE_JPEG_QUALITY, 95])
        images.append(marked)
        labels.append(f"{number:02d}  {actual.strftime('%m-%d %H:%M:%S')}  {row['tidal_state']}")
        manifest.append({
            "sample": number, "stage_id": row["stage_id"], "source_file": row["source_file"],
            "timestamp_s": chosen_t, "frame_index": frame_index,
            "actual_time": actual.isoformat(), "tidal_state": row["tidal_state"],
            "detections": len(chosen), "image_file": filename, "random_seed": SEED,
        })

    sheet = contact_sheet(images, labels)
    cv2.imwrite(str(OUTPUT / "outbound_random_detection_contact_sheet_v1.jpg"), sheet,
                [cv2.IMWRITE_JPEG_QUALITY, 95])
    pd.DataFrame(manifest).to_csv(OUTPUT / "outbound_random_detection_manifest_v1.csv", index=False)


if __name__ == "__main__":
    main()
