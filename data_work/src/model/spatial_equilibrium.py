from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import shortest_path


EPS = 1e-12


@dataclass
class ModelInputs:
    version_id: str
    grid_type: str
    node_ids: np.ndarray
    total_pop: float
    residents_obs: np.ndarray
    jobs_obs: np.ndarray
    od_origin: np.ndarray
    od_dest: np.ndarray
    od_pop_obs: np.ndarray
    edge_i: np.ndarray
    edge_j: np.ndarray
    edge_t_obs_min: np.ndarray
    edge_t_ff_min: np.ndarray
    edge_len_km: np.ndarray
    edge_lane_obs: np.ndarray
    edge_keys: list[tuple[int, int]]
    edge_grid_o: np.ndarray
    edge_grid_d: np.ndarray

    @property
    def n_nodes(self) -> int:
        return len(self.node_ids)

    @property
    def n_edges(self) -> int:
        return len(self.edge_i)


@dataclass
class CalibratedParameters:
    theta: float
    alpha: float
    beta: float
    lambda_congestion: float
    theta_source: str
    lambda_source: str


@dataclass
class Fundamentals:
    ubar_theta: np.ndarray
    abar_theta: np.ndarray


@dataclass
class EquilibriumResult:
    travel_time_min: np.ndarray
    tau_min_support: np.ndarray
    tau_invtheta_support: np.ndarray
    od_flow: np.ndarray
    residents: np.ndarray
    jobs: np.ndarray
    edge_flow: np.ndarray
    welfare: float
    n_iter: int
    converged: bool


def clone_model_with_od_subset(model: ModelInputs, keep_mask: np.ndarray) -> ModelInputs:
    keep = np.asarray(keep_mask, dtype=bool)
    od_origin = model.od_origin[keep]
    od_dest = model.od_dest[keep]
    od_pop_obs = model.od_pop_obs[keep]
    residents_obs = np.bincount(od_origin, weights=od_pop_obs, minlength=model.n_nodes).astype(float)
    jobs_obs = np.bincount(od_dest, weights=od_pop_obs, minlength=model.n_nodes).astype(float)
    return ModelInputs(
        version_id=model.version_id,
        grid_type=model.grid_type,
        node_ids=model.node_ids.copy(),
        total_pop=float(od_pop_obs.sum()),
        residents_obs=residents_obs,
        jobs_obs=jobs_obs,
        od_origin=od_origin.copy(),
        od_dest=od_dest.copy(),
        od_pop_obs=od_pop_obs.copy(),
        edge_i=model.edge_i.copy(),
        edge_j=model.edge_j.copy(),
        edge_t_obs_min=model.edge_t_obs_min.copy(),
        edge_t_ff_min=model.edge_t_ff_min.copy(),
        edge_len_km=model.edge_len_km.copy(),
        edge_lane_obs=model.edge_lane_obs.copy(),
        edge_keys=model.edge_keys.copy(),
        edge_grid_o=model.edge_grid_o.copy(),
        edge_grid_d=model.edge_grid_d.copy(),
    )


def _normalize_shares(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    total = np.nansum(arr)
    if total <= 0:
        raise ValueError("Population total must be positive.")
    return np.clip(arr / total, EPS, None)


def _geom_normalize(arr: np.ndarray) -> np.ndarray:
    out = np.clip(np.asarray(arr, dtype=float), EPS, None)
    gmean = float(np.exp(np.mean(np.log(out))))
    if not np.isfinite(gmean) or gmean <= 0:
        return out
    return out / gmean


def load_model_inputs(version_root: Path, grid_type: str = "square") -> ModelInputs:
    data_dir = version_root / "data"
    nodes = pd.read_parquet(data_dir / f"qsm_input_nodes_{grid_type}.parquet")
    od = pd.read_parquet(data_dir / f"qsm_input_od_{grid_type}.parquet")
    edges = pd.read_parquet(data_dir / f"qsm_input_edges_{grid_type}.parquet")
    links_long = pd.read_csv(data_dir / f"grid_links_{grid_type}_long.csv")
    lane = pd.read_parquet(data_dir / "centerline_lane_master.parquet")[
        ["cline_id", "dir", "lane_est_length_weighted"]
    ].copy()

    nodes["node_i"] = pd.to_numeric(nodes["node_i"], errors="coerce").astype(int)
    nodes = nodes.sort_values("node_i").reset_index(drop=True)
    old_to_new = {int(old): new for new, old in enumerate(nodes["node_i"].tolist())}
    node_ids = nodes["grid_id"].astype(str).to_numpy()
    residents_obs = pd.to_numeric(nodes["residents"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    jobs_obs = pd.to_numeric(nodes["jobs"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    total_pop = float(residents_obs.sum())

    od = od.copy()
    od["home_i"] = pd.to_numeric(od["home_i"], errors="coerce").astype(int)
    od["work_i"] = pd.to_numeric(od["work_i"], errors="coerce").astype(int)
    od["pop"] = pd.to_numeric(od["pop"], errors="coerce").fillna(0.0)
    od = od[od["pop"] > 0].copy()
    od = od[od["home_i"].isin(old_to_new) & od["work_i"].isin(old_to_new)].copy()
    od["home_i_new"] = od["home_i"].map(old_to_new).astype(int)
    od["work_i_new"] = od["work_i"].map(old_to_new).astype(int)

    links_long = links_long[links_long["period"] == "AM"].copy()
    links_long["grid_o"] = links_long["grid_o"].astype(str)
    links_long["grid_d"] = links_long["grid_d"].astype(str)
    links_long["lane_est_length_weighted"] = pd.to_numeric(
        links_long.merge(lane, on=["cline_id", "dir"], how="left")["lane_est_length_weighted"],
        errors="coerce",
    )
    lane_mean = float(pd.to_numeric(lane["lane_est_length_weighted"], errors="coerce").dropna().mean())
    if not np.isfinite(lane_mean) or lane_mean <= 0:
        lane_mean = 2.0
    links_long["lane_est_length_weighted"] = links_long["lane_est_length_weighted"].fillna(lane_mean)
    links_long["len_m"] = pd.to_numeric(links_long["len_m"], errors="coerce").fillna(0.0)
    links_long["tt_s"] = pd.to_numeric(links_long["tt_s"], errors="coerce").fillna(0.0)
    links_long = links_long[(links_long["len_m"] > 0) & (links_long["tt_s"] > 0)].copy()

    edge_len = (
        links_long.groupby(["grid_o", "grid_d"], as_index=False)["len_m"]
        .sum()
        .rename(columns={"len_m": "edge_len_m"})
    )
    edge_lane = (
        links_long.assign(w_lane=links_long["len_m"] * links_long["lane_est_length_weighted"])
        .groupby(["grid_o", "grid_d"], as_index=False)
        .agg(edge_len_m=("len_m", "sum"), w_lane=("w_lane", "sum"))
    )
    edge_lane["edge_lane_obs"] = edge_lane["w_lane"] / np.clip(edge_lane["edge_len_m"], EPS, None)
    edge_lane = edge_lane[["grid_o", "grid_d", "edge_lane_obs"]]

    edges = edges.copy()
    edges["grid_o"] = edges["grid_o"].astype(str)
    edges["grid_d"] = edges["grid_d"].astype(str)
    edges["i"] = pd.to_numeric(edges["i"], errors="coerce").astype(int)
    edges["j"] = pd.to_numeric(edges["j"], errors="coerce").astype(int)
    edges["t_min"] = pd.to_numeric(edges["t_min"], errors="coerce")
    edges["t_ff_min"] = pd.to_numeric(edges.get("t_ff_min"), errors="coerce")
    edges = edges[np.isfinite(edges["t_min"]) & (edges["t_min"] > 0)].copy()
    edges = edges[edges["i"].isin(old_to_new) & edges["j"].isin(old_to_new)].copy()
    edges["i_new"] = edges["i"].map(old_to_new).astype(int)
    edges["j_new"] = edges["j"].map(old_to_new).astype(int)
    edges = edges.merge(edge_len, on=["grid_o", "grid_d"], how="left")
    edges = edges.merge(edge_lane, on=["grid_o", "grid_d"], how="left")
    edges["edge_len_m"] = edges["edge_len_m"].fillna(1000.0)
    edges["edge_lane_obs"] = edges["edge_lane_obs"].fillna(lane_mean)
    edges["t_ff_min"] = edges["t_ff_min"].fillna(edges["t_min"])
    edges.loc[~np.isfinite(edges["t_ff_min"]) | (edges["t_ff_min"] <= 0), "t_ff_min"] = edges["t_min"]

    return ModelInputs(
        version_id=version_root.name,
        grid_type=grid_type,
        node_ids=node_ids,
        total_pop=total_pop,
        residents_obs=residents_obs,
        jobs_obs=jobs_obs,
        od_origin=od["home_i_new"].to_numpy(dtype=int),
        od_dest=od["work_i_new"].to_numpy(dtype=int),
        od_pop_obs=od["pop"].to_numpy(dtype=float),
        edge_i=edges["i_new"].to_numpy(dtype=int),
        edge_j=edges["j_new"].to_numpy(dtype=int),
        edge_t_obs_min=edges["t_min"].to_numpy(dtype=float),
        edge_t_ff_min=edges["t_ff_min"].to_numpy(dtype=float),
        edge_len_km=(edges["edge_len_m"].to_numpy(dtype=float) / 1000.0),
        edge_lane_obs=edges["edge_lane_obs"].to_numpy(dtype=float),
        edge_keys=list(zip(edges["i_new"].to_numpy(dtype=int), edges["j_new"].to_numpy(dtype=int))),
        edge_grid_o=edges["grid_o"].astype(str).to_numpy(),
        edge_grid_d=edges["grid_d"].astype(str).to_numpy(),
    )


def build_edge_lookup(edge_i: np.ndarray, edge_j: np.ndarray) -> dict[tuple[int, int], int]:
    return {(int(i), int(j)): idx for idx, (i, j) in enumerate(zip(edge_i, edge_j))}


def compute_shortest_paths(
    n_nodes: int,
    edge_i: np.ndarray,
    edge_j: np.ndarray,
    edge_cost: np.ndarray,
    od_origin: np.ndarray,
    od_dest: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    graph = csr_matrix((edge_cost, (edge_i, edge_j)), shape=(n_nodes, n_nodes))
    origin_unique, origin_inv = np.unique(od_origin, return_inverse=True)
    dist, predecessors = shortest_path(
        graph,
        directed=True,
        indices=origin_unique,
        return_predecessors=True,
        method="D",
    )
    tau_support = dist[origin_inv, od_dest]
    positive_edge = edge_cost[np.isfinite(edge_cost) & (edge_cost > 0)]
    intrazonal_cost = float(np.median(positive_edge) * 0.5) if len(positive_edge) else 1.0
    same_node = od_origin == od_dest
    tau_support[same_node] = intrazonal_cost
    return tau_support, predecessors


def assign_edge_flows_from_paths(
    predecessors: np.ndarray,
    origin_unique: np.ndarray,
    od_origin: np.ndarray,
    od_dest: np.ndarray,
    od_flow: np.ndarray,
    edge_lookup: dict[tuple[int, int], int],
    n_edges: int,
) -> np.ndarray:
    origin_pos = {int(o): idx for idx, o in enumerate(origin_unique)}
    edge_flow = np.zeros(n_edges, dtype=float)
    for o, d, mass in zip(od_origin, od_dest, od_flow):
        if mass <= 0:
            continue
        row = origin_pos.get(int(o))
        if row is None:
            continue
        cur = int(d)
        while cur != int(o):
            prev = int(predecessors[row, cur])
            if prev < 0:
                break
            edge_idx = edge_lookup.get((prev, cur))
            if edge_idx is not None:
                edge_flow[edge_idx] += float(mass)
            cur = prev
    return edge_flow


def compute_soft_shortest_path_assignment(
    n_nodes: int,
    edge_i: np.ndarray,
    edge_j: np.ndarray,
    edge_cost: np.ndarray,
    od_origin: np.ndarray,
    od_dest: np.ndarray,
    theta_route: float,
    od_flow: np.ndarray | None = None,
    max_iter: int = 2000,
    tol: float = 1e-10,
) -> tuple[np.ndarray, np.ndarray, np.ndarray | None]:
    edge_cost = np.asarray(edge_cost, dtype=float)
    od_origin = np.asarray(od_origin, dtype=int)
    od_dest = np.asarray(od_dest, dtype=int)
    same_node = od_origin == od_dest
    positive_edge = edge_cost[np.isfinite(edge_cost) & (edge_cost > 0)]
    intrazonal_cost = float(np.median(positive_edge) * 0.5) if len(positive_edge) else 1.0
    intrazonal_tau_invtheta = float(np.power(np.clip(intrazonal_cost, EPS, None), -theta_route))

    weight = np.power(np.clip(edge_cost, 1.001, None), -theta_route)
    tau_support = np.full(len(od_origin), np.inf, dtype=float)
    tau_invtheta_support = np.zeros(len(od_origin), dtype=float)
    edge_flow = np.zeros(len(edge_i), dtype=float) if od_flow is not None else None

    for dest in np.unique(od_dest):
        Z = np.zeros(n_nodes, dtype=float)
        Z[int(dest)] = 1.0
        converged = False

        for _ in range(max_iter):
            Z_new = np.zeros(n_nodes, dtype=float)
            np.add.at(Z_new, edge_i, weight * Z[edge_j])
            Z_new[int(dest)] = 1.0
            delta = float(np.max(np.abs(np.log(np.clip(Z_new, EPS, None)) - np.log(np.clip(Z, EPS, None)))))
            Z = Z_new
            if delta < tol:
                converged = True
                break

        if not converged:
            # Use the latest iterate even if the fixed point is not fully converged.
            pass

        dest_mask = od_dest == int(dest)
        origins_d = od_origin[dest_mask]
        z_origin = Z[origins_d]
        tau_invtheta_support[dest_mask] = np.where(origins_d == int(dest), intrazonal_tau_invtheta, z_origin)
        tau_support[dest_mask] = np.where(
            origins_d == int(dest),
            intrazonal_cost,
            np.where(z_origin > EPS, np.power(z_origin, -1.0 / theta_route), np.inf),
        )

        if edge_flow is None:
            continue

        p_edge = np.zeros(len(edge_i), dtype=float)
        valid_edge = Z[edge_i] > EPS
        p_edge[valid_edge] = (weight[valid_edge] * Z[edge_j[valid_edge]]) / np.clip(Z[edge_i[valid_edge]], EPS, None)
        p_edge[edge_i == int(dest)] = 0.0
        p_edge = np.clip(p_edge, 0.0, 1.0)

        src = np.zeros(n_nodes, dtype=float)
        od_flow_d = np.asarray(od_flow[dest_mask], dtype=float)
        origin_flow_mask = origins_d != int(dest)
        np.add.at(src, origins_d[origin_flow_mask], od_flow_d[origin_flow_mask])

        visit = src.copy()
        for _ in range(max_iter):
            visit_new = src.copy()
            np.add.at(visit_new, edge_j, visit[edge_i] * p_edge)
            delta_visit = float(np.max(np.abs(visit_new - visit)))
            visit = visit_new
            if delta_visit < tol:
                break

        edge_flow += visit[edge_i] * p_edge

    return tau_support, tau_invtheta_support, edge_flow


def estimate_theta_two_way_fe(
    od_origin: np.ndarray,
    od_dest: np.ndarray,
    od_pop: np.ndarray,
    tau_min: np.ndarray,
    max_iter: int = 200,
    tol: float = 1e-10,
) -> dict[str, float]:
    keep = (od_pop > 0) & np.isfinite(tau_min) & (tau_min > 0)
    y = np.log(np.asarray(od_pop[keep], dtype=float))
    x = np.log(np.asarray(tau_min[keep], dtype=float))
    o = np.asarray(od_origin[keep], dtype=int)
    d = np.asarray(od_dest[keep], dtype=int)

    x_t = x.copy()
    y_t = y.copy()
    for _ in range(max_iter):
        x_prev = x_t.copy()
        y_prev = y_t.copy()
        x_t -= pd.Series(x_t).groupby(o).transform("mean").to_numpy()
        x_t -= pd.Series(x_t).groupby(d).transform("mean").to_numpy()
        y_t -= pd.Series(y_t).groupby(o).transform("mean").to_numpy()
        y_t -= pd.Series(y_t).groupby(d).transform("mean").to_numpy()
        delta = max(float(np.max(np.abs(x_t - x_prev))), float(np.max(np.abs(y_t - y_prev))))
        if delta < tol:
            break

    denom = float(np.dot(x_t, x_t))
    beta_hat = float(np.dot(x_t, y_t) / denom) if denom > 0 else np.nan
    theta_hat = float(max(0.1, -beta_hat)) if np.isfinite(beta_hat) else np.nan
    return {
        "theta_hat": theta_hat,
        "beta_hat": beta_hat,
        "n_obs": int(keep.sum()),
    }


def estimate_lambda_cross_section(
    edge_t_obs_min: np.ndarray,
    edge_t_ff_min: np.ndarray,
    edge_lane_obs: np.ndarray,
    edge_flow_obs: np.ndarray,
) -> dict[str, float]:
    density = np.clip(edge_flow_obs / np.clip(edge_lane_obs, 0.5, None), EPS, None)
    congestion_ratio = np.clip(edge_t_obs_min / np.clip(edge_t_ff_min, EPS, None), EPS, None)
    y = np.log(congestion_ratio)
    x = np.log(density)
    lane_bin = np.clip(np.rint(edge_lane_obs).astype(int), 1, None)
    x_t = x - pd.Series(x).groupby(lane_bin).transform("mean").to_numpy()
    y_t = y - pd.Series(y).groupby(lane_bin).transform("mean").to_numpy()
    denom = float(np.dot(x_t, x_t))
    lambda_hat = float(np.dot(x_t, y_t) / denom) if denom > 0 else np.nan
    lambda_hat = float(np.clip(lambda_hat, 0.01, 1.5)) if np.isfinite(lambda_hat) else np.nan
    return {
        "lambda_hat": lambda_hat,
        "n_edges": int(len(edge_t_obs_min)),
        "note": "Cross-sectional lane-bin FE proxy using log(t_obs / t_ff). Use as a weak calibration only.",
    }


def invert_fundamentals(
    tau_invtheta_support: np.ndarray,
    od_origin: np.ndarray,
    od_dest: np.ndarray,
    residents_obs: np.ndarray,
    jobs_obs: np.ndarray,
    theta: float,
    alpha: float,
    beta: float,
    max_iter: int = 500,
    tol: float = 1e-9,
) -> Fundamentals:
    l_r = _normalize_shares(residents_obs)
    l_f = _normalize_shares(jobs_obs)
    n_nodes = len(l_r)
    ubar_theta = np.ones(n_nodes, dtype=float)
    abar_theta = np.ones(n_nodes, dtype=float)

    lf_term = np.power(l_f, alpha * theta)
    lr_term = np.power(l_r, beta * theta)

    target_r = np.power(l_r, 1.0 - theta * beta)
    target_f = np.power(l_f, 1.0 - theta * alpha)

    for _ in range(max_iter):
        u_prev = ubar_theta.copy()
        a_prev = abar_theta.copy()

        denom_r = np.bincount(
            od_origin,
            weights=tau_invtheta_support * abar_theta[od_dest] * lf_term[od_dest],
            minlength=n_nodes,
        )
        ubar_theta = target_r / np.clip(denom_r, EPS, None)
        ubar_theta = _geom_normalize(ubar_theta)

        denom_f = np.bincount(
            od_dest,
            weights=tau_invtheta_support * ubar_theta[od_origin] * lr_term[od_origin],
            minlength=n_nodes,
        )
        abar_theta = target_f / np.clip(denom_f, EPS, None)
        abar_theta = _geom_normalize(abar_theta)

        delta = max(
            float(np.max(np.abs(np.log(np.clip(ubar_theta, EPS, None)) - np.log(np.clip(u_prev, EPS, None))))),
            float(np.max(np.abs(np.log(np.clip(abar_theta, EPS, None)) - np.log(np.clip(a_prev, EPS, None))))),
        )
        if delta < tol:
            break

    return Fundamentals(ubar_theta=ubar_theta, abar_theta=abar_theta)


def solve_population_fixed_point(
    tau_invtheta_support: np.ndarray,
    od_origin: np.ndarray,
    od_dest: np.ndarray,
    total_pop: float,
    fundamentals: Fundamentals,
    theta: float,
    alpha: float,
    beta: float,
    residents_init: np.ndarray,
    jobs_init: np.ndarray,
    max_iter: int = 1000,
    tol: float = 1e-9,
    damping: float = 0.5,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, int, bool]:
    n_nodes = len(residents_init)
    l_r = _normalize_shares(residents_init)
    l_f = _normalize_shares(jobs_init)
    converged = False
    n_iter = 0

    for n_iter in range(1, max_iter + 1):
        utility_theta = fundamentals.ubar_theta * np.power(np.clip(l_r, EPS, None), beta * theta)
        wage_theta = fundamentals.abar_theta * np.power(np.clip(l_f, EPS, None), alpha * theta)

        mass = tau_invtheta_support * utility_theta[od_origin] * wage_theta[od_dest]
        total_mass = float(np.sum(mass))
        if total_mass <= 0:
            raise RuntimeError("Equilibrium mass collapsed to zero.")
        od_flow = total_pop * mass / total_mass

        l_r_new = np.bincount(od_origin, weights=od_flow, minlength=n_nodes) / total_pop
        l_f_new = np.bincount(od_dest, weights=od_flow, minlength=n_nodes) / total_pop
        l_r_upd = damping * l_r_new + (1.0 - damping) * l_r
        l_f_upd = damping * l_f_new + (1.0 - damping) * l_f

        delta = max(
            float(np.max(np.abs(np.log(np.clip(l_r_upd, EPS, None)) - np.log(np.clip(l_r, EPS, None))))),
            float(np.max(np.abs(np.log(np.clip(l_f_upd, EPS, None)) - np.log(np.clip(l_f, EPS, None))))),
        )
        l_r = _normalize_shares(l_r_upd)
        l_f = _normalize_shares(l_f_upd)
        if delta < tol:
            converged = True
            break

    utility_theta = fundamentals.ubar_theta * np.power(np.clip(l_r, EPS, None), beta * theta)
    wage_theta = fundamentals.abar_theta * np.power(np.clip(l_f, EPS, None), alpha * theta)
    mass = tau_invtheta_support * utility_theta[od_origin] * wage_theta[od_dest]
    od_flow = total_pop * mass / np.clip(np.sum(mass), EPS, None)
    welfare = float(np.power(np.sum(mass), 1.0 / theta))
    return l_r * total_pop, l_f * total_pop, od_flow, welfare, n_iter, converged


def solve_congested_equilibrium(
    model: ModelInputs,
    params: CalibratedParameters,
    fundamentals: Fundamentals,
    edge_lane_cf: np.ndarray | None = None,
    max_iter: int = 100,
    tol: float = 1e-6,
    damping: float = 0.35,
) -> EquilibriumResult:
    edge_lane_cf = (
        np.asarray(edge_lane_cf, dtype=float).copy()
        if edge_lane_cf is not None
        else np.asarray(model.edge_lane_obs, dtype=float).copy()
    )
    edge_lane_cf = np.clip(edge_lane_cf, 0.25, None)

    _, _, edge_flow_obs = compute_soft_shortest_path_assignment(
        model.n_nodes,
        model.edge_i,
        model.edge_j,
        model.edge_t_obs_min,
        model.od_origin,
        model.od_dest,
        theta_route=params.theta,
        od_flow=model.od_pop_obs,
    )
    if edge_flow_obs is None:
        edge_flow_obs = np.zeros(model.n_edges, dtype=float)

    edge_t = model.edge_t_obs_min.copy()
    residents_init = model.residents_obs.copy()
    jobs_init = model.jobs_obs.copy()
    converged = False
    out = None

    for outer_iter in range(1, max_iter + 1):
        tau_min, tau_invtheta, _ = compute_soft_shortest_path_assignment(
            model.n_nodes,
            model.edge_i,
            model.edge_j,
            edge_t,
            model.od_origin,
            model.od_dest,
            theta_route=params.theta,
        )

        residents, jobs, od_flow, welfare, inner_iter, inner_conv = solve_population_fixed_point(
            tau_invtheta_support=tau_invtheta,
            od_origin=model.od_origin,
            od_dest=model.od_dest,
            total_pop=model.total_pop,
            fundamentals=fundamentals,
            theta=params.theta,
            alpha=params.alpha,
            beta=params.beta,
            residents_init=residents_init,
            jobs_init=jobs_init,
        )

        _, _, edge_flow = compute_soft_shortest_path_assignment(
            model.n_nodes,
            model.edge_i,
            model.edge_j,
            edge_t,
            model.od_origin,
            model.od_dest,
            theta_route=params.theta,
            od_flow=od_flow,
        )
        if edge_flow is None:
            edge_flow = np.zeros(model.n_edges, dtype=float)
        density_cf = np.clip(edge_flow / np.clip(edge_lane_cf, 0.25, None), EPS, None)
        edge_t_new = model.edge_t_ff_min * np.power(density_cf, params.lambda_congestion)
        edge_t_upd = damping * edge_t_new + (1.0 - damping) * edge_t
        delta = float(np.max(np.abs(np.log(np.clip(edge_t_upd, EPS, None)) - np.log(np.clip(edge_t, EPS, None)))))
        edge_t = edge_t_upd
        residents_init = residents
        jobs_init = jobs
        out = EquilibriumResult(
            travel_time_min=edge_t.copy(),
            tau_min_support=tau_min,
            tau_invtheta_support=tau_invtheta,
            od_flow=od_flow,
            residents=residents,
            jobs=jobs,
            edge_flow=edge_flow,
            welfare=welfare,
            n_iter=outer_iter,
            converged=bool(inner_conv),
        )
        if delta < tol:
            converged = True
            break

    if out is None:
        raise RuntimeError("Equilibrium solver did not produce an output.")
    out.converged = converged and out.converged
    out.n_iter = outer_iter
    return out


def pick_top_congested_edges(
    model: ModelInputs,
    baseline_eq: EquilibriumResult,
    top_n: int,
) -> np.ndarray:
    score = baseline_eq.edge_flow * model.edge_t_obs_min / np.clip(model.edge_lane_obs, 0.5, None)
    order = np.argsort(-score)
    return order[:top_n]


def build_symmetric_edge_times(model: ModelInputs) -> np.ndarray:
    pair_to_idx = build_edge_lookup(model.edge_i, model.edge_j)
    t_sym = model.edge_t_obs_min.copy()
    visited: set[int] = set()
    for idx, (i, j) in enumerate(zip(model.edge_i, model.edge_j)):
        if idx in visited:
            continue
        rev = pair_to_idx.get((int(j), int(i)))
        if rev is None:
            continue
        avg = 0.5 * (model.edge_t_obs_min[idx] + model.edge_t_obs_min[rev])
        t_sym[idx] = avg
        t_sym[rev] = avg
        visited.add(idx)
        visited.add(rev)
    return t_sym


def pick_top_tidal_edges(
    model: ModelInputs,
    version_root: Path,
    top_n: int,
) -> np.ndarray:
    data_dir = version_root / "data"
    links = pd.read_csv(data_dir / f"grid_links_{model.grid_type}_long.csv")
    asym = pd.read_parquet(data_dir / "centerline_asymmetry_table.parquet")
    asym = asym[asym["peak"] == "AM"].copy()
    asym["ratio"] = pd.to_numeric(asym["ratio"], errors="coerce")
    asym["asym_strength"] = 1.0 - np.clip(asym["ratio"], 0.0, 1.0)
    asym["slow_dir"] = pd.Series(pd.NA, index=asym.index, dtype="string")
    asym.loc[asym["faster_dir"] == "AB", "slow_dir"] = "BA"
    asym.loc[asym["faster_dir"] == "BA", "slow_dir"] = "AB"
    links = links[links["period"] == "AM"].copy()
    links = links.merge(
        asym[["cline_id", "slow_dir", "asym_strength"]],
        left_on=["cline_id", "dir"],
        right_on=["cline_id", "slow_dir"],
        how="left",
    )
    links["asym_strength"] = pd.to_numeric(links["asym_strength"], errors="coerce").fillna(0.0)
    links["cong_score"] = links["tt_s"] / np.clip(links["len_m"], EPS, None)
    links["score"] = links["len_m"] * links["asym_strength"] * links["cong_score"]
    score = (
        links.groupby(["grid_o", "grid_d"], as_index=False)["score"]
        .sum()
        .rename(columns={"score": "edge_score"})
    )
    score["key"] = list(zip(score["grid_o"].astype(str), score["grid_d"].astype(str)))
    edge_key_to_idx = {(go, gd): idx for idx, (go, gd) in enumerate(zip(model.edge_grid_o, model.edge_grid_d))}
    score["edge_idx"] = score["key"].map(edge_key_to_idx)
    score = score[score["edge_idx"].notna()].copy()
    score["edge_idx"] = score["edge_idx"].astype(int)
    score = score.sort_values(["edge_score", "edge_idx"], ascending=[False, True])
    return score["edge_idx"].drop_duplicates().head(top_n).to_numpy(dtype=int)


def reallocate_tidal_lanes(
    model: ModelInputs,
    treated_idx: np.ndarray,
    add_lane: float = 1.0,
) -> np.ndarray:
    edge_lane_cf = model.edge_lane_obs.copy()
    reverse_lookup = build_edge_lookup(model.edge_j, model.edge_i)
    for idx in treated_idx:
        i = int(model.edge_i[idx])
        j = int(model.edge_j[idx])
        edge_lane_cf[idx] += add_lane
        rev_idx = reverse_lookup.get((i, j))
        if rev_idx is not None:
            edge_lane_cf[rev_idx] = max(0.25, edge_lane_cf[rev_idx] - add_lane)
    return edge_lane_cf


def summarise_equilibrium(
    model: ModelInputs,
    eq: EquilibriumResult,
    label: str,
) -> pd.DataFrame:
    finite = np.isfinite(eq.tau_min_support) & np.isfinite(eq.od_flow)
    weighted_avg_commute = (
        float(np.average(eq.tau_min_support[finite], weights=eq.od_flow[finite]))
        if finite.any() and float(eq.od_flow[finite].sum()) > 0
        else np.nan
    )
    summary = pd.DataFrame(
        [
            {
                "scenario": label,
                "welfare": eq.welfare,
                "avg_residents": float(np.mean(eq.residents)),
                "avg_jobs": float(np.mean(eq.jobs)),
                "avg_edge_time_min": float(np.mean(eq.travel_time_min)),
                "weighted_avg_commute_time_min": weighted_avg_commute,
                "weighted_avg_edge_flow": float(np.mean(eq.edge_flow)),
                "solver_iterations": int(eq.n_iter),
                "converged": bool(eq.converged),
            }
        ]
    )
    return summary


def save_calibration_bundle(
    output_dir: Path,
    params: CalibratedParameters,
    theta_fit: dict[str, float],
    lambda_fit: dict[str, float],
) -> None:
    payload = {
        "parameters": {
            "theta": params.theta,
            "alpha": params.alpha,
            "beta": params.beta,
            "lambda_congestion": params.lambda_congestion,
            "theta_source": params.theta_source,
            "lambda_source": params.lambda_source,
        },
        "theta_fit": theta_fit,
        "lambda_fit": lambda_fit,
    }
    (output_dir / "calibration_summary.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
