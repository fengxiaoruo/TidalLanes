import json
import re
from pathlib import Path

import pandas as pd


# Coordinate tuples: (x y) or (x y z) inside WKT
COORD_TUPLE_RE = re.compile(
    r"([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)(?:\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?))?"
)


def parse_linestring_coords(wkt: str) -> list[tuple[float, float]]:
    """
    Return list of (lat, lon) from LINESTRING WKT.
    Keeps the full geometry for visualization.
    """
    if not isinstance(wkt, str) or not wkt:
        return []
    tuples = COORD_TUPLE_RE.findall(wkt)
    coords = []
    for t in tuples:
        x = float(t[0])
        y = float(t[1])
        coords.append((y, x))  # lat, lon
    return coords


def downsample_coords(coords: list[tuple[float, float]], max_points: int) -> list[tuple[float, float]]:
    """
    Keep endpoints and uniformly sample middle points to cap geometry size.
    """
    if len(coords) <= max_points or max_points < 2:
        return coords
    if max_points == 2:
        return [coords[0], coords[-1]]
    keep_mid = max_points - 2
    step = (len(coords) - 2) / keep_mid
    mid = [coords[1 + int(i * step)] for i in range(keep_mid)]
    return [coords[0], *mid, coords[-1]]


def main():
    base_dir = Path(__file__).resolve().parent
    directed_edges_path = base_dir / "Processed_Data" / "directed_edges.xlsx"
    nodes_path = base_dir / "Processed_Data" / "nodes.xlsx"
    out_html_path = base_dir / "Processed_Data" / "network.html"

    out_html_path.parent.mkdir(parents=True, exist_ok=True)

    # Main urban area filter (WGS84). Format: (min_lon, min_lat, max_lon, max_lat)
    main_urban_bbox = (116.15, 39.85, 116.60, 40.10)
    # Performance controls to reduce HTML size and browser memory.
    max_points_per_edge = 16
    draw_arrows = False
    show_nodes_min_zoom = 13
    edge_batch_size = 800
    node_batch_size = 2000

    edges = pd.read_excel(directed_edges_path)
    nodes_df = pd.read_excel(nodes_path)

    required_edges_cols = [
        "edge_id",
        "from_node_id",
        "to_node_id",
        "road_id",
        "roadname",
        "semantic",
        "location",
        "start_lon",
        "start_lat",
        "end_lon",
        "end_lat",
        "geometry",
    ]
    for c in required_edges_cols:
        if c not in edges.columns:
            raise KeyError(f"Missing column in directed_edges.xlsx: {c}")

    if not {"node_id", "lon", "lat"}.issubset(set(nodes_df.columns)):
        raise KeyError("nodes.xlsx must contain columns: node_id, lon, lat")

    edges = edges.copy()
    edges["from_node_id"] = edges["from_node_id"].astype(int)
    edges["to_node_id"] = edges["to_node_id"].astype(int)

    # Filter nodes to bbox first, then keep edges whose both endpoints are within.
    min_lon, min_lat, max_lon, max_lat = main_urban_bbox
    nodes_df = nodes_df.copy()
    nodes_df["node_id"] = nodes_df["node_id"].astype(int)

    in_bbox = (
        (nodes_df["lon"] >= min_lon)
        & (nodes_df["lon"] <= max_lon)
        & (nodes_df["lat"] >= min_lat)
        & (nodes_df["lat"] <= max_lat)
    )
    node_id_set = set(nodes_df.loc[in_bbox, "node_id"].tolist())

    edges = edges[
        edges["from_node_id"].isin(node_id_set) & edges["to_node_id"].isin(node_id_set)
    ].copy()

    used_node_ids = set(edges["from_node_id"].tolist()) | set(edges["to_node_id"].tolist())
    nodes_df = nodes_df[nodes_df["node_id"].isin(used_node_ids)].copy()

    if len(edges) == 0:
        raise ValueError("No edges found. Please check directed_edges.xlsx / directed_edges_main_urban.xlsx inputs.")

    # Node coordinate lookup: node_id -> (lat, lon)
    node_map = {
        int(r["node_id"]): (float(r["lat"]), float(r["lon"]))
        for _, r in nodes_df.iterrows()
    }

    # Determine map center from endpoints
    lats = pd.concat([edges["start_lat"], edges["end_lat"]], ignore_index=True).astype(float)
    lons = pd.concat([edges["start_lon"], edges["end_lon"]], ignore_index=True).astype(float)
    center_lat = float(lats.mean())
    center_lon = float(lons.mean())

    # Build JS data
    edge_js_items = []
    for _, row in edges.iterrows():
        from_lat = float(row["start_lat"])
        from_lon = float(row["start_lon"])
        to_lat = float(row["end_lat"])
        to_lon = float(row["end_lon"])
        from_node = int(row["from_node_id"])
        to_node = int(row["to_node_id"])

        # Raw segment (full LINESTRING)
        coords_latlon_full = parse_linestring_coords(row["geometry"])
        if len(coords_latlon_full) < 2:
            coords_latlon_full = [(from_lat, from_lon), (to_lat, to_lon)]
        coords_latlon_full = downsample_coords(coords_latlon_full, max_points_per_edge)

        poly_full = [[c[0], c[1]] for c in coords_latlon_full]
        # Topological connection: connect node centroids (so it visually touches the red node markers)
        from_node_lat, from_node_lon = node_map[from_node]
        to_node_lat, to_node_lon = node_map[to_node]
        poly_conn = [[from_node_lat, from_node_lon], [to_node_lat, to_node_lon]]

        edge_id = row["edge_id"]
        road_id = row["road_id"]
        roadname = str(row.get("roadname", ""))
        semantic = str(row.get("semantic", ""))
        location = str(row.get("location", ""))

        edge_js_items.append(
            {
                "eid": str(edge_id),
                "rid": str(road_id),
                "rname": roadname,
                "sem": semantic,
                "loc": location,
                "pf": poly_full,
                "pc": poly_conn,
                "fn": from_node,
                "tn": to_node,
            }
        )

    edges_json = json.dumps(edge_js_items, ensure_ascii=False)
    nodes_json = json.dumps(
        [
            {"node_id": int(r["node_id"]), "lon": float(r["lon"]), "lat": float(r["lat"])}
            for _, r in nodes_df[["node_id", "lon", "lat"]].iterrows()
        ],
        ensure_ascii=False,
    )

    # Single-file HTML using CDN assets
    # Style requirements:
    # - red circle nodes on top layer
    # - yellow solid raw segment
    # - blue dashed connection + arrow
    raw_width = 5
    conn_width = 3
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Road Network (Directed)</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    html, body {{ height: 100%; margin: 0; padding: 0; }}
    #map {{ height: 100%; width: 100%; }}
    .legend {{
      position: fixed; bottom: 10px; left: 10px;
      background: rgba(0,0,0,0.55); color: #fff;
      padding: 10px; border-radius: 6px; font-size: 12px;
      z-index: 9999;
    }}
    .arrow {{
      width: 0; height: 0;
      border-top: 6px solid transparent;
      border-bottom: 6px solid transparent;
      border-left: 12px solid #4da3ff;
      transform-origin: 0px 0px;
    }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="legend">
    <div><b>edges</b>: {len(edge_js_items)}</div>
    <div><b>raw seg</b>: yellow solid (width {raw_width})</div>
    <div><b>connection</b>: blue dashed{' + arrow' if draw_arrows else ''} (width {conn_width})</div>
    <div><b>nodes</b>: red circles (show at zoom >= {show_nodes_min_zoom})</div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const edges = {edges_json};
    const nodes = {nodes_json};

    const map = L.map('map', {{ preferCanvas: true }}).setView([{center_lat:.6f}, {center_lon:.6f}], 11);
    const SHOW_NODES_MIN_ZOOM = {show_nodes_min_zoom};
    const DRAW_ARROWS = {str(draw_arrows).lower()};
    const EDGE_BATCH_SIZE = {edge_batch_size};
    const NODE_BATCH_SIZE = {node_batch_size};
    const edgeRenderer = L.canvas({{ padding: 0.2 }});
    const nodeRenderer = L.canvas({{ padding: 0.2 }});
    const edgeLayer = L.layerGroup().addTo(map);
    const nodeLayer = L.layerGroup();

    // Dark background tiles for better contrast
    L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
      subdomains: ['a','b','c','d'],
      maxZoom: 20,
      attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
    }}).addTo(map);

    function placeArrowAtEnd(poly_conn) {{
      if (!DRAW_ARROWS) return null;
      if (!poly_conn || poly_conn.length < 2) return null;
      const p0 = poly_conn[0]; // [lat, lon]
      const p1 = poly_conn[poly_conn.length - 1];

      const fromLat = p0[0], fromLon = p0[1];
      const toLat = p1[0], toLon = p1[1];

      // Angle in degrees, 0 means pointing to the East (in lon-x direction)
      const dx = (toLon - fromLon);
      const dy = (toLat - fromLat);
      const angleDeg = Math.atan2(dy, dx) * 180 / Math.PI;

      // Place arrow slightly offset so it doesn't exactly overlap with node marker
      const arrowOffsetLat = (toLat - fromLat) * 0.03;
      const arrowOffsetLon = (toLon - fromLon) * 0.03;
      const arrowLat = toLat - arrowOffsetLat;
      const arrowLon = toLon - arrowOffsetLon;

      const divIcon = L.divIcon({{
        className: '',
        iconSize: [18, 18],
        iconAnchor: [0, 6],
        html: '<div class="arrow" style="transform: rotate(' + angleDeg.toFixed(2) + 'deg);"></div>'
      }});

      return L.marker([arrowLat, arrowLon], {{ icon: divIcon, interactive: false }}).addTo(edgeLayer);
    }}

    function edgePopupHtml(e) {{
      return (
        'edge_id: ' + e.eid + '<br>' +
        'road_id: ' + e.rid + '<br>' +
        'nodes: ' + e.fn + ' -> ' + e.tn + '<br>' +
        'roadname: ' + (e.rname || '') + '<br>' +
        'semantic: ' + (e.sem || '') + '<br>' +
        'location: ' + (e.loc || '')
      );
    }}

    function drawEdgesBatched(startIdx = 0) {{
      const endIdx = Math.min(startIdx + EDGE_BATCH_SIZE, edges.length);
      for (let i = startIdx; i < endIdx; i++) {{
        const e = edges[i];
        L.polyline(e.pf, {{
          color: '#ffd400',
          weight: {raw_width},
          opacity: 0.85,
          renderer: edgeRenderer,
          smoothFactor: 1.5
        }}).addTo(edgeLayer);

        const conn = L.polyline(e.pc, {{
          color: '#4da3ff',
          weight: {conn_width},
          opacity: 0.95,
          dashArray: '8,6',
          renderer: edgeRenderer,
          smoothFactor: 1.0
        }}).addTo(edgeLayer);
        conn.bindPopup(edgePopupHtml(e), {{ autoPan: false }});
        placeArrowAtEnd(e.pc);
      }}
      if (endIdx < edges.length) {{
        requestAnimationFrame(() => drawEdgesBatched(endIdx));
      }}
    }}

    let nodesDrawn = false;
    function drawNodesBatched(startIdx = 0) {{
      const endIdx = Math.min(startIdx + NODE_BATCH_SIZE, nodes.length);
      for (let i = startIdx; i < endIdx; i++) {{
        const n = nodes[i];
        L.circleMarker([n.lat, n.lon], {{
          radius: 3.2,
          color: '#ff0000',
          fillColor: '#ff0000',
          fillOpacity: 0.78,
          weight: 1.2,
          renderer: nodeRenderer
        }}).addTo(nodeLayer).bindPopup('node_id: ' + n.node_id, {{ autoPan: false }});
      }}
      if (endIdx < nodes.length) {{
        requestAnimationFrame(() => drawNodesBatched(endIdx));
      }} else {{
        nodesDrawn = true;
      }}
    }}

    function updateNodeLayerByZoom() {{
      const zoom = map.getZoom();
      if (zoom >= SHOW_NODES_MIN_ZOOM) {{
        if (!map.hasLayer(nodeLayer)) {{
          nodeLayer.addTo(map);
        }}
        if (!nodesDrawn) {{
          drawNodesBatched(0);
        }}
      }} else if (map.hasLayer(nodeLayer)) {{
        map.removeLayer(nodeLayer);
      }}
    }}

    // 1) Draw edges first (non-blocking batches), then conditionally draw nodes.
    drawEdgesBatched(0);
    updateNodeLayerByZoom();
    map.on('zoomend', updateNodeLayerByZoom);
  </script>
</body>
</html>"""

    out_html_path.write_text(html, encoding="utf-8")
    print(f"HTML written: {out_html_path}")


if __name__ == "__main__":
    main()

