from __future__ import annotations

import importlib.util
import json
import math
import os
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "outputs"
AK_LOCAL_PATH = Path(__file__).resolve().parent / "ak_local.py"


@dataclass(frozen=True)
class BAAKSelection:
    token: str
    source: str
    note: str = ""


def load_config(config_path: str | Path) -> dict:
    path = Path(config_path)
    if not path.is_absolute():
        cwd_path = Path.cwd() / path
        path = cwd_path if cwd_path.exists() else ROOT / path
    with path.open("r", encoding="utf-8") as fh:
        config = json.load(fh)
    config["_config_path"] = str(path.resolve())
    return config


def _load_ak_local_token() -> str:
    if not AK_LOCAL_PATH.exists():
        return ""
    spec = importlib.util.spec_from_file_location("ntc_ak_local", AK_LOCAL_PATH)
    if not spec or not spec.loader:
        return ""
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return str(getattr(mod, "BAIDU_MAP_AUTH_TOKEN", "") or "").strip()


def _looks_like_baidu_ak(token: str) -> bool:
    token = (token or "").strip()
    if not token:
        return False
    if token.startswith("sk-"):
        return False
    if any(ch.isspace() for ch in token):
        return False
    return True


def load_baidu_ak(*, env_var: str = "BAIDU_MAP_AUTH_TOKEN", ak_source: str = "auto") -> BAAKSelection:
    choice = (ak_source or "auto").strip().lower()
    if choice not in {"auto", "local", "env"}:
        raise ValueError(f"Unknown ak_source: {ak_source!r}")

    env_token = os.environ.get(env_var, "").strip()
    local_token = _load_ak_local_token()

    if choice == "local":
        if local_token:
            return BAAKSelection(token=local_token, source="ak_local.py")
        raise RuntimeError(
            f"未在 {AK_LOCAL_PATH} 找到可用 BAIDU_MAP_AUTH_TOKEN。"
            " 请复制 ak_local.py.example 为同目录 ak_local.py 并填写你的百度 AK。"
        )

    if choice == "env":
        if env_token:
            return BAAKSelection(token=env_token, source=f"env:{env_var}")
        raise RuntimeError(f"环境变量 {env_var} 为空；若想改用本地文件，请传 --ak-source local 或 --ak-source auto。")

    if local_token:
        note = ""
        if env_token and env_token != local_token:
            if _looks_like_baidu_ak(env_token):
                note = f"检测到环境变量 {env_var} 与 ak_local.py 同时存在，默认优先使用 ak_local.py；如需强制环境变量，请传 --ak-source env。"
            else:
                note = f"检测到环境变量 {env_var} 存在但看起来不像百度 AK，已忽略并改用 ak_local.py。"
        return BAAKSelection(token=local_token, source="ak_local.py", note=note)

    if env_token:
        return BAAKSelection(token=env_token, source=f"env:{env_var}")

    raise RuntimeError(
        "未找到 AK：请复制 ak_local.py.example 为同目录 ak_local.py 并填写 BAIDU_MAP_AUTH_TOKEN，"
        f"或设置环境变量 {env_var}=你的百度 AK。"
    )


def resolve_path(path_text: str | Path) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    # Config paths like `data_work/outputs` are relative to the repo; ROOT is the `data_work/` tree.
    if path.parts and path.parts[0] == "data_work" and len(path.parts) > 1:
        return (ROOT / Path(*path.parts[1:])).resolve()
    cwd_path = Path.cwd() / path
    if cwd_path.exists():
        return cwd_path
    return (ROOT / path).resolve()


def get_version_root(config: dict, version_id: str | None = None, output_dir: str | None = None) -> Path:
    actual_version = version_id or config.get("version_id")
    if not actual_version:
        raise ValueError("version_id must be provided either in config or CLI.")
    base = resolve_path(output_dir) if output_dir else resolve_path(config.get("output_dir", str(DEFAULT_OUTPUT_DIR)))
    return base / actual_version


def ensure_version_dirs(version_root: Path) -> dict[str, Path]:
    paths = {
        "version_root": version_root,
        "data": version_root / "data",
        "api": version_root / "api",
        "figures": version_root / "figures",
        "metrics": version_root / "metrics",
        "logs": version_root / "logs",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def normalize_id_series(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
        .astype("string")
    )


def normalize_coord_text(value: float) -> str:
    return format(float(value), ".6f")


def haversine_km(lon1, lat1, lon2, lat2):
    lon1 = np.asarray(lon1, dtype=float)
    lat1 = np.asarray(lat1, dtype=float)
    lon2 = np.asarray(lon2, dtype=float)
    lat2 = np.asarray(lat2, dtype=float)
    rad = np.pi / 180.0
    dlon = (lon2 - lon1) * rad
    dlat = (lat2 - lat1) * rad
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1 * rad) * np.cos(lat2 * rad) * np.sin(dlon / 2.0) ** 2
    return 6371.0088 * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    if len(values) == 0 or np.sum(weights) <= 0:
        return float("nan")
    return float(np.sum(values * weights) / np.sum(weights))


def weighted_quantile(values: Iterable[float], weights: Iterable[float], q: float) -> float:
    values = np.asarray(list(values), dtype=float)
    weights = np.asarray(list(weights), dtype=float)
    mask = np.isfinite(values) & np.isfinite(weights) & (weights > 0)
    if mask.sum() == 0:
        return float("nan")
    values = values[mask]
    weights = weights[mask]
    order = np.argsort(values)
    values = values[order]
    weights = weights[order]
    cum_w = np.cumsum(weights)
    cutoff = q * cum_w[-1]
    idx = int(np.searchsorted(cum_w, cutoff, side="left"))
    idx = min(max(idx, 0), len(values) - 1)
    return float(values[idx])


def weighted_median(values: Iterable[float], weights: Iterable[float]) -> float:
    return weighted_quantile(values, weights, 0.5)


def window_target_clock(window_name: str) -> tuple[int, int]:
    """Nominal local-clock targets for *traffic* panels (AM/PM/NIGHT). Not used for FREE."""
    mapping = {"AM": (8, 0), "PM": (18, 0), "NIGHT": (22, 30)}
    if window_name not in mapping:
        raise ValueError(f"Unknown query window: {window_name}")
    return mapping[window_name]


def window_label(window_name: str) -> str:
    if window_name.upper() == "FREE":
        return "—"
    hour, minute = window_target_clock(window_name)
    return f"{hour:02d}:{minute:02d}"


def baidu_driving_routematrix_panel(query_window: str) -> dict:
    """
    routematrix/v2/driving: map query_window to Baidu `tactics` and documentation strings.

    Official-style semantics (simplified, see BAIDU_ROUTE_API_NOTES.md):
    - 11/12: duration reflects traffic; use near AM/PM real time for "peak" readings.
    - 13: shorter path, duration **without** traffic (畅通基准); clock validation not required.

    NIGHT: legacy off-peak real-traffic run (~22:30); prefer FREE (tactics=13) for baseline.
    """
    w = (query_window or "").upper()
    if w == "FREE":
        return {
            "tactics": 13,
            "notional_departure_hhmm": "—",
            "metric_note_zh": "routematrix 驾车 tactics=13：按路况不介入的耗时时长（作畅通/对照基准，可在任意时刻请求）",
            "validate_local_clock": False,
        }
    if w == "AM":
        return {
            "tactics": 12,
            "notional_departure_hhmm": "08:00",
            "metric_note_zh": "routematrix 驾车 tactics=12：短距离+路况；宜在早高峰附近发请求以贴近实时堵情",
            "validate_local_clock": True,
        }
    if w == "PM":
        return {
            "tactics": 12,
            "notional_departure_hhmm": "18:00",
            "metric_note_zh": "routematrix 驾车 tactics=12：短距离+路况；宜在晚高峰附近发请求以贴近实时堵情",
            "validate_local_clock": True,
        }
    if w == "NIGHT":
        return {
            "tactics": 12,
            "notional_departure_hhmm": "22:30",
            "metric_note_zh": "routematrix 驾车 tactics=12：非高峰时段的实时路况（与 FREE 的 tactics=13 不同）",
            "validate_local_clock": True,
        }
    raise ValueError(f"Unknown query window: {query_window!r}")


def baidu_driving_directionlite_panel(query_window: str) -> dict:
    """
    directionlite/v1/driving: map query_window to tactics and documentation strings.

    Official docs support tactics:
    - 0: 常规路线(时间最短)
    - 2: 躲避拥堵
    - 3: 最短距离

    directionlite/v1 does not expose routematrix/v2-style "ignore traffic" semantics,
    so FREE here is only a lightweight baseline, not a true free-flow counterfactual.
    """
    w = (query_window or "").upper()
    if w == "FREE":
        return {
            "tactics": 0,
            "notional_departure_hhmm": "—",
            "metric_note_zh": "directionlite/v1/driving tactics=0：常规路线（时间最短）的轻量单条算路；仅作可跑通基线，不等同于 routematrix/v2 的无路况基准",
            "validate_local_clock": False,
        }
    if w == "AM":
        return {
            "tactics": 2,
            "notional_departure_hhmm": "08:00",
            "metric_note_zh": "directionlite/v1/driving tactics=2：躲避拥堵；宜在早高峰附近发请求以贴近实时路况",
            "validate_local_clock": True,
        }
    if w == "PM":
        return {
            "tactics": 2,
            "notional_departure_hhmm": "18:00",
            "metric_note_zh": "directionlite/v1/driving tactics=2：躲避拥堵；宜在晚高峰附近发请求以贴近实时路况",
            "validate_local_clock": True,
        }
    if w == "NIGHT":
        return {
            "tactics": 0,
            "notional_departure_hhmm": "22:30",
            "metric_note_zh": "directionlite/v1/driving tactics=0：夜间常规路线轻量算路",
            "validate_local_clock": True,
        }
    raise ValueError(f"Unknown query window: {query_window!r}")


SH_TZ = ZoneInfo("Asia/Shanghai")


def _minutes_hhmm(h: int, mm: int) -> int:
    return h * 60 + mm


def local_time_in_suggested_peak_band(window_name: str, now: datetime) -> bool:
    """
    较宽的「建议运行」时段（北京时间，用于 *可选* 的 --strict-clock-window）。
    不表示百度 API 只在这些时刻有效；长任务可从 7 点起跑穿整个早高峰窗。
    """
    m = now.hour * 60 + now.minute
    w = (window_name or "").upper()
    if w == "AM":
        return _minutes_hhmm(5, 0) <= m <= _minutes_hhmm(10, 30)
    if w == "PM":
        return _minutes_hhmm(16, 0) <= m <= _minutes_hhmm(21, 30)
    if w == "NIGHT":
        return m >= _minutes_hhmm(20, 0) or m <= _minutes_hhmm(1, 30)
    return True


def local_time_in_strict_live_band(window_name: str, now: datetime) -> bool:
    """
    更严格的真实运行时段（北京时间）。
    适合把 AM / PM / NIGHT 分三次分别跑，尽量贴近论文口径。
    """
    m = now.hour * 60 + now.minute
    w = (window_name or "").upper()
    if w == "AM":
        return _minutes_hhmm(7, 0) <= m <= _minutes_hhmm(9, 0)
    if w == "PM":
        return _minutes_hhmm(17, 0) <= m <= _minutes_hhmm(19, 30)
    if w == "NIGHT":
        return m >= _minutes_hhmm(22, 0) or m <= _minutes_hhmm(1, 0)
    return False


def validate_query_window_local_clock(
    window_name: str,
    *,
    strict: bool,
    allow_off_window: bool,
    strict_live: bool = False,
) -> None:
    """
    默认 *不* 因本地时间拒绝运行。
    - strict=True: 宽建议窗
    - strict_live=True: 更严格的真实运行窗，适合把不同 panel 分开在真实时段运行
    """
    if allow_off_window:
        return

    if strict_live:
        now = datetime.now(SH_TZ)
        wn = (window_name or "").upper()
        if wn == "FREE":
            raise RuntimeError("已启用 --strict-live-window：FREE 不是严格实时时段口径。请改用 AM / PM / NIGHT 分时运行。")
        if local_time_in_strict_live_band(window_name, now):
            return
        if wn == "AM":
            band = "07:00–09:00"
        elif wn == "PM":
            band = "17:00–19:30"
        else:
            band = "22:00–次日01:00"
        raise RuntimeError(
            f"已启用 --strict-live-window：当前北京时间 {now.strftime('%H:%M')} 不在 {window_name} 的严格时段 {band} 内。"
            " 若只是测试连通性，请去掉该参数；若要保持严格口径，请在对应时段单独运行。"
        )

    if not strict:
        return
    panel = baidu_driving_routematrix_panel(window_name)
    if not panel.get("validate_local_clock"):
        return
    now = datetime.now(SH_TZ)
    if local_time_in_suggested_peak_band(window_name, now):
        return
    wn = (window_name or "").upper()
    if wn == "AM":
        band = "05:00–10:30"
    elif wn == "PM":
        band = "16:00–21:30"
    else:
        band = "20:00 以后或 00:00–01:30"
    raise RuntimeError(
        f"已启用 --strict-clock-window：当前北京时间 {now.strftime('%H:%M')} 不在 {window_name} 的建议窗 {band} 内。"
        f" 若长任务会跨时，可去掉 --strict-clock-window，或使用 --allow-off-window 覆盖。"
    )


def format_latlon(lat: float, lon: float) -> str:
    return f"{float(lat):.6f},{float(lon):.6f}"


def safe_divide(num, den):
    if pd.isna(num) or num is None:
        return np.nan
    if den in (0, 0.0) or pd.isna(den) or den is None:
        return np.nan
    return num / den


def abs_log_ratio(a, b):
    if pd.isna(a) or pd.isna(b) or a <= 0 or b <= 0:
        return np.nan
    return abs(math.log(a / b))
