# Slide Snapshot Manifest

Manual snapshots taken while `Tidal0715.tex` was rewritten between 2026-07-15 and 2026-07-18. Each file records the state of the deck immediately **before** the edit named in its filename.

The `.tex` snapshots are retained in full because they are small and carry the actual content. Compiled PDFs were retained only for three milestones; the rest were discarded on 2026-07-27 because `latexmk` regenerates them from the corresponding source.

## Tidal0715 line

| Snapshot suffix | Date | Edit performed after the snapshot | PDF retained |
|---|---|---|---|
| `25min` | 07-15 | Restructured the deck for a 25-minute delivery | yes |
| `model_notation_restore` | 07-15 | Restored the model notation system | no |
| `physical_time` | 07-16 | Introduced physical minutes $m_{kl}$ | yes |
| `data_route_layout` | 07-16 | Reworked the Data section and route layout | no |
| `layout_formula_review` | 07-16 | Formula typesetting review | no |
| `definition_sources` | 07-16 | Added the definitions and sources page | no |
| `appendix27_layout` | 07-16 | Appendix page 27 layout | no |
| `actual_path_figure` | 07-16 | Added the realised shortest-path figure | no |
| `kappa_t_anchor` | 07-17 | Anchored $\kappa$ and $t$ | no |
| `slide19_table_indent` | 07-17 | Table indentation on slide 19 | no |
| `appendix_script_rebuild` | 07-17 | Rebuilt the appendix speaker script | no |
| `layout_cost_audit` | 07-17 | Layout audit of the cost section | no |
| `result_merge` | 07-17 | Merged the results pages | yes |
| `two_outcome_decomp` | 07-17 | Two-outcome decomposition | no |
| `indented_LT_totals` | 07-17 | Indented the long-run totals | no |
| `remove_cumulative` | 07-17 | Removed cumulative phrasing | no |
| `decomp_rename_and_nattrip_split` | 07-18 | Renamed the decomposition and split NatTrip | never compiled |

## Other lines

| File | Origin |
|---|---|
| `tidal_backup_20260630.tex` | State of `tidal.tex` before the 2026-06-30 revision |
| `tidal_advisor_revision_20260703.tex` | First advisor revision pass |
| `tidal_advisor_revision_round2_20260703.tex` | Second advisor revision pass, the version the Dropbox replication package still maps to |
| `tidal_advisor_revision_segmentonly_20260703.tex` | Segment-only variant of the same pass |
| `tidal_speaker_script_*_backup_*.md` | Speaker-script states matching the deck snapshots above |

## Going forward

Snapshots of this kind duplicate what version control already does, and the filename carries only a short phrase where a commit message would carry the reasoning. Commit the deck instead; if a snapshot is genuinely needed, place it here rather than in the working directory.
