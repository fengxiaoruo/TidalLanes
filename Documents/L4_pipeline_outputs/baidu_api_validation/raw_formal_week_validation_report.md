# Formal week 数据验证报告

Generated: 2026-06-03T13:37:22+08:00

## Sessions (best per day×panel)

| Day | Panel | Session | n | api_ok | complete |
|-----|-------|---------|---|--------|----------|
| d01 | AM | `AM_20260528T154730_formal_week_20260528_d01` | 23,799 | 100.0% | True |
| d01 | FREE | `FREE_20260528T110340_formal_week_20260528_d01` | 23,799 | 100.0% | True |
| d01 | PM | `PM_20260528T202114_formal_week_20260528_d01` | 23,799 | 100.0% | True |
| d02 | AM | `AM_20260529T073000_formal_week_20260528_d02` | 23,799 | 100.0% | True |
| d02 | FREE | `FREE_20260529T023000_formal_week_20260528_d02` | 23,799 | 100.0% | True |
| d02 | PM | `PM_20260529T173001_formal_week_20260528_d02` | 23,799 | 100.0% | True |
| d03 | AM | `AM_20260530T073000_formal_week_20260528_d03` | 23,799 | 100.0% | True |
| d03 | FREE | `FREE_20260530T023000_formal_week_20260528_d03` | 23,799 | 100.0% | True |
| d03 | PM | `PM_20260530T173000_formal_week_20260528_d03` | 23,799 | 100.0% | True |
| d04 | AM | `AM_20260531T073000_formal_week_20260528_d04` | 23,799 | 100.0% | True |
| d04 | FREE | `FREE_20260531T023000_formal_week_20260528_d04` | 23,799 | 100.0% | True |
| d04 | PM | `PM_20260531T173000_formal_week_20260528_d04` | 23,799 | 29.6% | False |
| d05 | AM | `AM_20260601T073000_formal_week_20260528_d05` | 23,799 | 86.4% | False |
| d05 | FREE | `FREE_20260601T023000_formal_week_20260528_d05` | 23,799 | 100.0% | True |
| d05 | PM | `PM_20260601T173001_formal_week_20260528_d05` | 11,090 | 0.0% | False |


## Panel summary (bind_ok ∩ direct speed)

| Panel | n bind∩direct | median ratio | share∈[0.8,1.25] | MAE (min) | r |
|-------|---------------|--------------|----------------|-----------|---|
| FREE | 109805 | 1.34 | 29.3% | 0.30 | 0.89 |
| AM | 106981 | 1.47 | 25.6% | 0.45 | 0.84 |
| PM | 72528 | 1.37 | 28.2% | 0.39 | 0.85 |

## By crawl day

| Day | Panel | api_ok | bind_ok | median ratio | r |
|-----|-------|--------|---------|--------------|---|
| d01 | AM | 100% | 92.8% | 1.56 | 0.82 |
| d01 | FREE | 100% | 92.8% | 1.59 | 0.89 |
| d01 | PM | 100% | 92.8% | 1.23 | 0.86 |
| d02 | AM | 100% | 92.8% | 1.49 | 0.84 |
| d02 | FREE | 100% | 92.8% | 1.30 | 0.90 |
| d02 | PM | 100% | 92.8% | 1.47 | 0.83 |
| d03 | AM | 100% | 92.8% | 1.42 | 0.86 |
| d03 | FREE | 100% | 92.7% | 1.28 | 0.89 |
| d03 | PM | 100% | 92.8% | 1.37 | 0.87 |
| d04 | AM | 100% | 92.8% | 1.38 | 0.86 |
| d04 | FREE | 100% | 92.8% | 1.29 | 0.89 |
| d04 | PM | 30% | 27.8% | 1.47 | 0.84 |
| d05 | AM | 86% | 80.5% | 1.51 | 0.83 |
| d05 | FREE | 100% | 92.8% | 1.27 | 0.90 |
| d05 | PM | 0% | 0.0% | nan | nan |

## Cross-day stability

- **AM**: n=21,989, median CV=0.140, share CV≤0.15=53.5%
- **FREE**: n=21,989, median CV=0.156, share CV≤0.15=48.1%
- **PM**: n=21,986, median CV=0.127, share CV≤0.15=57.7%

## Spatial fit (raw segment geometry)

- sample n=2500
- spatial PASS: **27.8%**
- overlap median: **0.00**
- mean_dist median: **105.5 m**
