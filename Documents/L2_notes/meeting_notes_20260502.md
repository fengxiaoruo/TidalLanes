# Meeting Notes, 2026-05-02

## Synced Documents

- `Documents/L1_manuscripts/TidalLanes_0429.tex`
- `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`
- `Documents/L1_manuscripts/TidalLanes_Model_v4.tex`

## Main Points

- For figures where a directed segment has no data, or only one direction is observed, the corresponding line should be drawn in light gray to mark missing directional information.
- The concrete construction of grid-level travel cost is still unsettled. Keep this as an open design choice rather than locking the current version as final.
- For the national version, add a city-level figure with congestion index on the x-axis and asymmetry on the y-axis. More city data can be crawled if the current sample is not enough.
- Parameter choices should refer to Professor You Wei's related work before the next model draft locks calibration values.

## Agreed Next Steps

1. Do not focus on parameter estimation for now. Build the full estimation framework first, excluding exact-hat, and make the counterfactual workflow explicit.
2. Finish the pattern evidence, within city first, and possibly add Shenzhen.
3. Decide whether to add a cross-city subsection. If added, use demand-side differences, especially jobs-housing separation, and supply-side differences, including transport-mode shares and road-network structure, to explain cross-city variation.
4. For the Shenzhen case, check whether a reduced-form analysis is feasible.
5. Refine parameter choices and data details after the framework and core pattern evidence are in place.

## Immediate Implications For Work Planning

- Treat grid travel cost as an open modeling choice.
- Keep missing-data visual treatment separate from the substantive asymmetry measure.
- Move the model line toward a full-estimation scaffold and counterfactual design before spending effort on parameter estimation.
- Keep national evidence as a city-level pattern exercise, with a congestion-index versus asymmetry scatter as the next figure.
