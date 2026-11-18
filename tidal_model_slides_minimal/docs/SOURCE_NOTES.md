# Source Notes

- Aggregate OD input: `outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_od_square.parquet`, built from Baidu 2022 commuting flow data aggregated to model grid pairs.
- Aggregate speed/edge input: `outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_edges_square.parquet`, built from Baidu 2025 one-week speed data aggregated to directed grid links with AM observed times and night free-flow times.
- Aggregate node input: `outputs/model_input_edgeproj_v2_lcc_gate5_v1/data/qsm_input_nodes_square.parquet`, model grid nodes with residence and workplace marginals.
- Reference results: `outputs/reference_results/`, copied from the current main-project output folders used by `tidal.tex` and checked by `make validate-full`.
- Figures: `figs/`, copied from the current slide figure directory or the speed example figure directory. Map figures are static assets; this package does not rerun figure-generation pipelines.
- Main-project link: `MAIN_REPO` relative path file and `docs/FILE_MAP.csv`.

See `docs/MANIFEST.md` for the minimal code manifest and `docs/FILE_MAP.csv` for file-level paths back to the main project.
