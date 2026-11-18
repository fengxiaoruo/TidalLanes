from __future__ import annotations

from pathlib import Path

import tidal_lane_specC as spec

ROOT = Path("/Users/fxr/Dropbox/Projects/TidalLanes")
REPL_ROOT = ROOT / "Replication_apply"

spec.SOURCE_DIR = ROOT / "data_work/outputs/model_input_edgeproj_v2_lcc_v1/data"
spec.EDGEPROJ_CSV = ROOT / (
    "data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/"
    "raw_connected_vs_old_adjacent_adjacent_square_AM.csv"
)
spec.V2_INPUT = REPL_ROOT / "aa_input_square_edgeproj_v2_lcc"
spec.MAPPING_CSV = (
    REPL_ROOT / "prepare_from_data_work_square_edgeproj_v2_lcc/node_index_mapping_square.csv"
)
spec.OUT_DIR = REPL_ROOT / "results/tidal_lane_edgeproj_v2_lcc"


if __name__ == "__main__":
    spec.main()
