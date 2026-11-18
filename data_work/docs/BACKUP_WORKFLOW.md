# Backup Workflow

Operational procedure for protecting TidalLanes against disk loss. The script that implements it is `data_work/scripts/backup_to_drive.sh`.

## What is copied, and why the rest is not

Backup coverage is decided by one question: can the artefact be regenerated from code plus inputs? Everything answering yes is excluded, because copying it consumes space that the irreplaceable material needs.

Three categories are irreplaceable and are copied to the external drive:

| Set name | Path | Approximate size |
|---|---|---|
| `rawdata` | `data_work/raw_data/` | 24 GB |
| `video` | `flowvedio/` (source video and code) | 250 GB |
| `fieldvisit` | `fieldvisit/` | 17 GB |
| `presentation` | `Documents/PreSlides/` | 1 GB |

`data_work/outputs/` is the largest single directory in the repository and is deliberately absent from that list. Every run under it is reproducible from `data_work/src/` and the locked input bundle, and it holds roughly 148,000 files, a shape that behaves badly on the current drive (see the exFAT section). `flowvedio/split_outputs/` is excluded for the same reason. `flowvedio/batch_outputs/` and `flowvedio/traffic_count_outputs/` are retained despite being regenerable, because rebuilding them costs substantial GPU time.

Text sources are also absent, because they belong in Git rather than on a drive. Code, `.tex` sources, notes, and configuration are small, benefit from diffs and history, and are the class of asset most often lost to an unlucky edit rather than to hardware failure.

## Division of labour across the three locations

Each location protects against a different failure, and none substitutes for another.

**GitHub** holds every text artefact: `data_work/src/`, `Documents/L1_manuscripts/`, `Documents/L2_notes/`, `data_work/config/`, and the coordination files at the repository root. It answers the question "what did this look like last week, and why did it change".

**The external drive** holds the four sets above. It answers "the laptop is gone". It carries no history by design; a second dated copy of the same video is waste, not safety.

**Dropbox** (`Projects/Projects_FengGu/DirectionalCongestion`) holds what coauthors need to open without a Python environment: the current deck, current drafts, and the self-contained replication package. It is a distribution channel rather than a backup, and its contents are generated from the main repository through the package's `make sync-from-main`, not copied by hand.

## Running a backup

The script defaults to planning and changes nothing until `--run` is passed. No mode of the script deletes anything at the destination.

```bash
./data_work/scripts/backup_to_drive.sh                 # plan, print what would move
./data_work/scripts/backup_to_drive.sh --run           # perform all four sets
./data_work/scripts/backup_to_drive.sh --run --set video   # one set only
```

The destination is `/Volumes/冯筱箬/TidalLanes_archive`. A different drive is selected through the environment:

```bash
TIDAL_BACKUP_DRIVE="MyDisk" ./data_work/scripts/backup_to_drive.sh --run
```

Each completed run appends a dated entry to `TidalLanes_archive/MANIFEST.md` recording which sets were synced and the Git commit the working tree stood at.

Cadence follows the rate at which each set changes. Video is synced after every field collection session, which is the only moment new irreplaceable bytes enter the project. The other three sets are synced monthly, or before any cleanup that touches them. Git is pushed at the end of each working day, which is a separate habit and the one that matters most, since text is what changes hourly.

## The exFAT constraint

The current drive is formatted exFAT with a one-megabyte allocation unit. Every file occupies at least that much regardless of its true size, so a directory of many small files inflates severely. Measured on this project, a 4 MB output directory of 284 files occupied 339 MB on the drive, a factor of 85.

Two consequences follow. Directories of small files are kept off this volume entirely, which the exclusion list already enforces. And the drive should be reformatted to APFS once its contents have been consolidated, after which the inflation disappears and the exclusion list becomes a space optimisation rather than a necessity.

exFAT also stores modification times at two-second granularity and cannot represent POSIX permissions. The script passes `--modify-window=2` and disables permission preservation for this reason; without them every file appears modified on each run and the whole 290 GB is recopied.

The script uses only flags available in `openrsync`, the rsync implementation shipped with macOS 15, which reports itself as compatible with rsync 2.6.9 and rejects newer options such as `--info=stats2`.

## Restoring

The archive is a plain directory tree, not an image, so recovery is a copy in the opposite direction. To restore one set:

```bash
rsync -rlt --modify-window=2 --no-perms --no-owner --no-group \
  /Volumes/冯筱箬/TidalLanes_archive/raw_data/ \
  ~/Dropbox/Projects/TidalLanes/data_work/raw_data/
```

A full rebuild from scratch takes three steps: clone the repository from GitHub, restore the four sets from the drive, then regenerate `data_work/outputs/` by rerunning the pipeline. The third step is why outputs are not backed up.

## Audit state, 2026-07-27

An audit of the drive on this date established the following, recorded here so the reasoning is not repeated.

The drive held nine dated snapshots of the project under `project备份/`, totalling roughly 472 GB nominal. The most recent, `Tidal0723`, proved to be an interrupted copy: `Documents/`, `Replication_apply/`, `sample_data/`, both replication packages, and `data_work/src/` were empty or absent, and the snapshot contained no video, field photographs, or archive. Its two populated directories, `data_work/outputs` and `data_work/raw_data`, were verified to be strict subsets of the local copies, with zero files present on the drive and missing locally once AppleDouble metadata was discounted.

The operational conclusions were that no snapshot needed to be preserved for its content, that the video had never in fact been backed up, and that dated full snapshots should be replaced by the incremental scheme described above.
