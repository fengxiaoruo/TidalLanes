from __future__ import annotations

import json
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


def fetch_direction(
    *,
    api_url: str,
    ak: str,
    origin_text: str,
    destination_text: str,
    waypoints_text: str = "",
    tactics: int,
    coord_type: str,
    ret_coordtype: str | None = None,
    gps_direction: int | None = None,
    steps_info: int = 0,
    retry_count: int = 2,
    retry_sleep_sec: float = 1.0,
) -> tuple[dict, dict]:
    params = {
        "origin": origin_text,
        "destination": destination_text,
        "coord_type": coord_type,
        "tactics": str(tactics),
        "ak": ak,
        "steps_info": str(int(steps_info)),
    }
    if waypoints_text:
        params["waypoints"] = waypoints_text
    if ret_coordtype:
        params["ret_coordtype"] = ret_coordtype
    if gps_direction is not None:
        params["gps_direction"] = str(int(gps_direction))
    request_log = {
        "origin": origin_text,
        "destination": destination_text,
        "waypoints": waypoints_text,
        "waypoints_count": 0 if not waypoints_text else len(waypoints_text.split("|")),
        "tactics": tactics,
        "coord_type": coord_type,
        "ret_coordtype": ret_coordtype,
        "gps_direction": gps_direction,
        "steps_info": int(steps_info),
    }
    last_exc = None
    for attempt in range(retry_count + 1):
        try:
            with urlopen(f"{api_url}?{urlencode(params)}", timeout=45) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return payload, request_log
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt >= retry_count:
                break
            time.sleep(retry_sleep_sec)
    return {"status": -1, "message": f"{type(last_exc).__name__}: {last_exc}", "result": {}}, request_log


def parse_baidu_path_text(path_text: str) -> list[tuple[float, float]]:
    """Parse Baidu step path: semicolon-separated `lng,lat` pairs."""
    if not path_text or not str(path_text).strip():
        return []
    coords: list[tuple[float, float]] = []
    for part in str(path_text).split(";"):
        part = part.strip()
        if not part or "," not in part:
            continue
        lng_s, lat_s = part.split(",", 1)
        coords.append((float(lng_s), float(lat_s)))
    return coords


def merge_path_coords(step_paths: list[list[tuple[float, float]]]) -> list[tuple[float, float]]:
    merged: list[tuple[float, float]] = []
    for pts in step_paths:
        for pt in pts:
            if not merged or pt != merged[-1]:
                merged.append(pt)
    return merged


def extract_route_path(payload: dict) -> dict:
    """Build route polyline from `steps[].path` when `steps_info=1` was requested."""
    result = payload.get("result", {}) if isinstance(payload.get("result"), dict) else {}
    routes = result.get("routes", []) if isinstance(result.get("routes", []), list) else []
    route0 = routes[0] if routes else {}
    step_paths: list[list[tuple[float, float]]] = []
    n_steps_with_path = 0
    if isinstance(route0, dict) and isinstance(route0.get("steps"), list):
        for step in route0["steps"]:
            if not isinstance(step, dict):
                continue
            pts = parse_baidu_path_text(str(step.get("path") or ""))
            if pts:
                step_paths.append(pts)
                n_steps_with_path += 1
    coords_lonlat = merge_path_coords(step_paths)
    route_path_wkt = ""
    route_path_lonlat = ""
    if len(coords_lonlat) >= 2:
        route_path_wkt = "LINESTRING(" + ", ".join(f"{lon} {lat}" for lon, lat in coords_lonlat) + ")"
        route_path_lonlat = "|".join(f"{lat:.6f},{lon:.6f}" for lon, lat in coords_lonlat)
    return {
        "n_path_points": len(coords_lonlat),
        "n_steps_with_path": n_steps_with_path,
        "route_path_wkt": route_path_wkt,
        "route_path_lonlat": route_path_lonlat,
        "has_route_path": len(coords_lonlat) >= 2,
    }


def extract_route(payload: dict) -> dict:
    status = int(payload.get("status", -1))
    message = str(payload.get("message", ""))
    result = payload.get("result", {}) if isinstance(payload.get("result"), dict) else {}
    routes = result.get("routes", []) if isinstance(result.get("routes", []), list) else []
    route0 = routes[0] if routes else {}
    duration_sec = float(route0.get("duration")) if isinstance(route0, dict) and route0.get("duration") not in (None, "") else float("nan")
    distance_m = float(route0.get("distance")) if isinstance(route0, dict) and route0.get("distance") not in (None, "") else float("nan")
    n_steps = 0
    steps_distance_m = float("nan")
    if isinstance(route0, dict) and isinstance(route0.get("steps"), list):
        steps = route0["steps"]
        n_steps = len(steps)
        parts = []
        for step in steps:
            if isinstance(step, dict) and step.get("distance") not in (None, ""):
                parts.append(float(step["distance"]))
        if parts:
            steps_distance_m = float(sum(parts))
    path_info = extract_route_path(payload)
    return {
        "api_status": status,
        "api_message": message,
        "duration_sec": duration_sec,
        "distance_m": distance_m,
        "n_steps": n_steps,
        "steps_distance_m": steps_distance_m,
        **path_info,
    }
