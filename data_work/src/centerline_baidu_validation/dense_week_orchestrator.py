"""
Dense formal-week crawl: anchored FREE/AM/PM panels plus gap-filler rounds.

- Day 2+: no route geometry (--no-steps-info).
- Anchored panels run at configured HH:MM (Asia/Shanghai); missed anchors run immediately on --resume.
- Between anchors, run FREE→AM→PM cycles while time remains before the next buffer.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SH_TZ = ZoneInfo("Asia/Shanghai")
PANELS = ("FREE", "AM", "PM")
REQUIRED_ROWS = 11890
SESSIONS_ROOT = ROOT / "outputs/centerline_baidu_validation_beijing_v1/api/crawl_sessions"


def parse_hhmm(s: str) -> tuple[int, int]:
    h, m = s.strip().split(":")
    return int(h), int(m)


def now_sh() -> datetime:
    return datetime.now(SH_TZ)


def day_anchors(
    ref: datetime,
    free_hhmm: str,
    am_hhmm: str,
    pm_hhmm: str,
) -> tuple[datetime, datetime, datetime]:
    hf, mf = parse_hhmm(free_hhmm)
    ha, ma = parse_hhmm(am_hhmm)
    hp, mp = parse_hhmm(pm_hhmm)
    base = ref.replace(hour=0, minute=0, second=0, microsecond=0)
    return (
        base.replace(hour=hf, minute=mf),
        base.replace(hour=ha, minute=ma),
        base.replace(hour=hp, minute=mp),
    )


def log(msg: str, master_log: Path) -> None:
    line = f"[dense_week] {msg} {now_sh().isoformat(timespec='seconds')}"
    print(line, flush=True)
    with master_log.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def stop_requested(stop_file: Path) -> bool:
    return stop_file.exists()


def wait_until(deadline: datetime, master_log: Path) -> None:
    while True:
        remaining = (deadline - now_sh()).total_seconds()
        if remaining <= 0:
            return
        log(f"sleep until {deadline.isoformat(timespec='seconds')} ({remaining:.0f}s)", master_log)
        time.sleep(min(remaining, 300))


def wait_for_anchor(anchor: datetime, master_log: Path, *, resume: bool) -> None:
    if resume and now_sh() >= anchor:
        log(f"anchor passed — run now (was {anchor.isoformat(timespec='seconds')})", master_log)
        return
    wait_until(anchor, master_log)


def count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as fh:
        return sum(1 for _ in fh)


def panel_complete(panel: str, label: str) -> bool:
    for sd in SESSIONS_ROOT.glob(f"{panel}_*_{label}"):
        if count_jsonl_lines(sd / "responses.jsonl") >= REQUIRED_ROWS:
            mf = sd / "run_manifest.json"
            if mf.exists():
                try:
                    meta = json.loads(mf.read_text(encoding="utf-8"))
                    if int(meta.get("n_success", 0)) >= REQUIRED_ROWS:
                        return True
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass
            return True
    return False


def next_extra_seq(day_label: str) -> int:
    mx = 0
    pat = re.compile(rf"_{re.escape(day_label)}_e(\d+)$")
    for sd in SESSIONS_ROOT.iterdir():
        if not sd.is_dir():
            continue
        m = pat.search(sd.name)
        if not m:
            continue
        if count_jsonl_lines(sd / "responses.jsonl") >= REQUIRED_ROWS:
            mx = max(mx, int(m.group(1)))
    return mx + 1


def run_panel(
    *,
    python: str,
    config: str,
    panel: str,
    label: str,
    sleep_sec: float,
    steps_info: bool,
    log_dir: Path,
    master_log: Path,
    resume: bool,
) -> int:
    if resume and panel_complete(panel, label):
        log(f"SKIP complete panel={panel} label={label}", master_log)
        return 0

    steps_flag = "--steps-info" if steps_info else "--no-steps-info"
    panel_log = log_dir / f"{label}_{panel}.log"
    cmd = [
        python,
        "-m",
        "src.centerline_baidu_validation.query_baidu_direction",
        "--config",
        config,
        "--panel",
        panel,
        "--label",
        label,
        "--sleep-sec",
        str(sleep_sec),
        steps_flag,
    ]
    log(f"RUN panel={panel} label={label} steps_info={int(steps_info)}", master_log)
    with panel_log.open("a", encoding="utf-8") as plog, master_log.open("a", encoding="utf-8") as mlog:
        plog.write(f"===== begin {now_sh().isoformat(timespec='seconds')} =====\n")
        mlog.write(f"===== {label} {panel} begin {now_sh().isoformat(timespec='seconds')} =====\n")
        proc = subprocess.run(cmd, cwd=str(ROOT), stdout=plog, stderr=subprocess.STDOUT)
        plog.write(f"===== end {now_sh().isoformat(timespec='seconds')} exit={proc.returncode} =====\n")
        mlog.write(f"===== {label} {panel} end {now_sh().isoformat(timespec='seconds')} exit={proc.returncode} =====\n")
    return proc.returncode


def gap_fill(
    *,
    deadline: datetime,
    day_label: str,
    seq_start: int,
    panel_estimate_sec: float,
    python: str,
    config: str,
    sleep_sec: float,
    steps_info: bool,
    log_dir: Path,
    master_log: Path,
    stop_file: Path,
    resume: bool,
) -> int:
    seq = seq_start
    panel_idx = 0
    while True:
        if stop_requested(stop_file):
            log("stop file found during gap fill", master_log)
            return seq
        remaining = (deadline - now_sh()).total_seconds()
        if remaining <= panel_estimate_sec:
            log(
                f"gap fill stop day={day_label} remaining={remaining:.0f}s <= estimate={panel_estimate_sec:.0f}s",
                master_log,
            )
            return seq
        panel = PANELS[panel_idx % 3]
        label = f"{day_label}_e{seq:03d}"
        if resume and panel_complete(panel, label):
            log(f"SKIP complete gap panel={panel} label={label}", master_log)
            seq += 1
            panel_idx += 1
            continue
        rc = run_panel(
            python=python,
            config=config,
            panel=panel,
            label=label,
            sleep_sec=sleep_sec,
            steps_info=steps_info,
            log_dir=log_dir,
            master_log=master_log,
            resume=False,
        )
        if rc != 0:
            log(f"gap fill aborted: panel rc={rc} label={label}", master_log)
            return seq
        seq += 1
        panel_idx += 1
    return seq


def parse_args():
    p = argparse.ArgumentParser(description="Dense anchored + gap-fill centerline Baidu week crawl.")
    p.add_argument("--config", default="config/centerline_baidu_validation_beijing_v1.json")
    p.add_argument("--week-label", required=True)
    p.add_argument("--start-day", type=int, default=2)
    p.add_argument("--days", type=int, default=4, help="Number of calendar days to run (from start-day).")
    p.add_argument("--start-immediately", action="store_true", help="Gap-fill now until first sched FREE buffer.")
    p.add_argument("--resume", action="store_true", help="Skip completed panels; run missed anchors immediately.")
    p.add_argument("--start-free-hhmm", default="02:30")
    p.add_argument("--start-am-hhmm", default="07:30")
    p.add_argument("--start-pm-hhmm", default="17:30")
    p.add_argument("--buffer-before-anchor-min", type=int, default=30)
    p.add_argument("--overnight-buffer-free-hhmm", default="02:00")
    p.add_argument("--panel-estimate-sec", type=float, default=8000.0)
    p.add_argument("--sleep-sec", type=float, default=0.25)
    p.add_argument("--python", default=os.environ.get("PYTHON", "python3.11"))
    return p.parse_args()


def main():
    args = parse_args()
    log_dir = ROOT / "outputs/centerline_baidu_validation_beijing_v1/logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    master_log = log_dir / f"{args.week_label}_dense_master.log"
    stop_file = log_dir / f"{args.week_label}.stop"
    if stop_file.exists():
        stop_file.unlink()
        log(f"removed stale stop file {stop_file}", master_log)

    buf = timedelta(minutes=int(args.buffer_before_anchor_min))
    end_day = args.start_day + int(args.days) - 1

    log(
        f"start week={args.week_label} days={args.start_day}-{end_day} "
        f"anchors FREE={args.start_free_hhmm} AM={args.start_am_hhmm} PM={args.start_pm_hhmm} "
        f"panel_est={args.panel_estimate_sec}s immediate={args.start_immediately} resume={args.resume}",
        master_log,
    )
    log(f"stop file: {stop_file}", master_log)

    for day_num in range(args.start_day, end_day + 1):
        if stop_requested(stop_file):
            log("stop file found — exiting", master_log)
            return

        day_label = f"{args.week_label}_d{day_num:02d}"
        steps_info = False
        extra_seq = next_extra_seq(day_label)
        log(f"=== d{day_num:02d} steps_info=0 extra_seq_start={extra_seq:03d} ===", master_log)

        ref = now_sh()
        sched_free, sched_am, sched_pm = day_anchors(
            ref, args.start_free_hhmm, args.start_am_hhmm, args.start_pm_hhmm
        )
        log(
            f"anchors today: FREE={sched_free.isoformat(timespec='seconds')} "
            f"AM={sched_am.isoformat(timespec='seconds')} PM={sched_pm.isoformat(timespec='seconds')}",
            master_log,
        )

        if args.start_immediately and day_num == args.start_day:
            overnight_h, overnight_m = parse_hhmm(args.overnight_buffer_free_hhmm)
            overnight_stop = sched_free.replace(hour=overnight_h, minute=overnight_m)
            if overnight_stop > sched_free:
                overnight_stop -= timedelta(days=1)
            if overnight_stop <= now_sh():
                overnight_stop = sched_free - buf
            log(f"immediate gap fill until {overnight_stop.isoformat(timespec='seconds')}", master_log)
            extra_seq = gap_fill(
                deadline=overnight_stop,
                day_label=day_label,
                seq_start=extra_seq,
                panel_estimate_sec=args.panel_estimate_sec,
                python=args.python,
                config=args.config,
                sleep_sec=args.sleep_sec,
                steps_info=steps_info,
                log_dir=log_dir,
                master_log=master_log,
                stop_file=stop_file,
                resume=args.resume,
            )
            args.start_immediately = False

        # --- sched FREE ---
        if stop_requested(stop_file):
            return
        wait_for_anchor(sched_free, master_log, resume=args.resume)
        rc = run_panel(
            python=args.python,
            config=args.config,
            panel="FREE",
            label=f"{day_label}_sched",
            sleep_sec=args.sleep_sec,
            steps_info=steps_info,
            log_dir=log_dir,
            master_log=master_log,
            resume=args.resume,
        )
        if rc != 0:
            log(f"fatal: sched FREE failed rc={rc}", master_log)
            return

        extra_seq = gap_fill(
            deadline=sched_am - buf,
            day_label=day_label,
            seq_start=extra_seq,
            panel_estimate_sec=args.panel_estimate_sec,
            python=args.python,
            config=args.config,
            sleep_sec=args.sleep_sec,
            steps_info=steps_info,
            log_dir=log_dir,
            master_log=master_log,
            stop_file=stop_file,
            resume=args.resume,
        )

        # --- sched AM ---
        if stop_requested(stop_file):
            return
        wait_for_anchor(sched_am, master_log, resume=args.resume)
        rc = run_panel(
            python=args.python,
            config=args.config,
            panel="AM",
            label=f"{day_label}_sched",
            sleep_sec=args.sleep_sec,
            steps_info=steps_info,
            log_dir=log_dir,
            master_log=master_log,
            resume=args.resume,
        )
        if rc != 0:
            log(f"fatal: sched AM failed rc={rc}", master_log)
            return

        extra_seq = gap_fill(
            deadline=sched_pm - buf,
            day_label=day_label,
            seq_start=extra_seq,
            panel_estimate_sec=args.panel_estimate_sec,
            python=args.python,
            config=args.config,
            sleep_sec=args.sleep_sec,
            steps_info=steps_info,
            log_dir=log_dir,
            master_log=master_log,
            stop_file=stop_file,
            resume=args.resume,
        )

        # --- sched PM ---
        if stop_requested(stop_file):
            return
        wait_for_anchor(sched_pm, master_log, resume=args.resume)
        rc = run_panel(
            python=args.python,
            config=args.config,
            panel="PM",
            label=f"{day_label}_sched",
            sleep_sec=args.sleep_sec,
            steps_info=steps_info,
            log_dir=log_dir,
            master_log=master_log,
            resume=args.resume,
        )
        if rc != 0:
            log(f"fatal: sched PM failed rc={rc}", master_log)
            return

        next_ref = now_sh() + timedelta(days=1)
        next_free, _, _ = day_anchors(next_ref, args.start_free_hhmm, args.start_am_hhmm, args.start_pm_hhmm)
        overnight_h, overnight_m = parse_hhmm(args.overnight_buffer_free_hhmm)
        overnight_stop = next_free.replace(hour=overnight_h, minute=overnight_m)
        if overnight_stop >= next_free:
            overnight_stop -= timedelta(days=1)
        if overnight_stop <= now_sh():
            overnight_stop = next_free - buf

        extra_seq = gap_fill(
            deadline=overnight_stop,
            day_label=day_label,
            seq_start=extra_seq,
            panel_estimate_sec=args.panel_estimate_sec,
            python=args.python,
            config=args.config,
            sleep_sec=args.sleep_sec,
            steps_info=steps_info,
            log_dir=log_dir,
            master_log=master_log,
            stop_file=stop_file,
            resume=args.resume,
        )

        log(f"=== d{day_num:02d} complete ===", master_log)

    log(f"dense week complete {args.week_label}", master_log)


if __name__ == "__main__":
    main()
