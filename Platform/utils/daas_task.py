#!/usr/bin/env python3
"""Submit, poll and fetch TidalLanes DaaS tasks through the already logged-in Chrome tab.

No credentials are read or stored here: requests run inside the open daas.smartsteps.com tab
with its own session. Same mechanism as SubwayMobility Local/Code/00_setup/daas_task_20261018.js.

  python3 Platform/utils/daas_task.py lint <sql>
  python3 Platform/utils/daas_task.py folders
  python3 Platform/utils/daas_task.py recent [prefix]
  python3 Platform/utils/daas_task.py submit <queue_id>
  python3 Platform/utils/daas_task.py status <taskid>
  python3 Platform/utils/daas_task.py fetch <taskid> <csv_stem>
  python3 Platform/utils/daas_task.py chain <queue_id>...
"""
import csv
import hashlib
import io
import json
import re
import secrets
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BATCH = ROOT / "202505_v01"
RUN = ROOT / "runs" / "202505_v01"
QUEUE = BATCH / "queue.json"
SUBMISSIONS = RUN / "submissions" / "submissions.jsonl"
QR = RUN / "query-results"
PROJECT_FOLDER = "TidalLanes"
PARENT_FOLDER = "TL25M05_V01"
TASK_PAGES = 40


def lint(sql):
    errs = []
    rules = [
        (r"^\s*WITH\b", "top-level WITH"),
        (r"SELECT\s+\*", "SELECT *"),
        (r"\buid\s+IS\s+(NOT\s+)?NULL", "uid IS [NOT] NULL"),
        (r"(^|\n)\s*--", "dash comment"),
        (r"(^|\n)\s*#", "hash comment"),
        (r"stay_poi\.zone_id", "stay_poi.zone_id"),
        (r"\bDESCRIBE\b", "DESCRIBE"),
        (r"sum\s*\(\s*CASE\s+WHEN", "sum(CASE WHEN) dropped by firewall, use sum(if(...))"),
        (r"\b1\s*=\s*1\b", "1=1 tautology"),
        (r"UNION[ \t]+ALL[ \t]+SELECT", "UNION ALL SELECT on one line"),
        (r"\{\{", "unbound template variable"),
    ]
    for pat, msg in rules:
        if re.search(pat, sql, re.I):
            errs.append(msg)
    if not re.search(r"V0110000", sql) and re.search(r"\bFROM\s+(move_month|move_rn|stay_poi|user_attribute)\b", sql, re.I):
        errs.append("fact table without city filter")
    return errs


def chrome_eval(js):
    with tempfile.TemporaryDirectory(prefix="daas-chrome-") as d:
        jsf = Path(d) / "eval.js"
        jsf.write_text(js, encoding="utf-8")
        script = "\n".join([
            f'set js to do shell script "cat " & quoted form of "{jsf}"',
            'tell application "Google Chrome"',
            "  set targetTab to missing value",
            "  repeat with w in windows",
            "    repeat with t in tabs of w",
            "      try",
            '        if (URL of t) contains "daas.smartsteps.com" then',
            "          set targetTab to t",
            "          exit repeat",
            "        end if",
            "      end try",
            "    end repeat",
            "    if targetTab is not missing value then exit repeat",
            "  end repeat",
            '  if targetTab is missing value then error "no daas tab"',
            "  return execute targetTab javascript js",
            "end tell",
        ])
        out = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
        if out.returncode:
            raise RuntimeError(out.stderr.strip())
        return out.stdout.strip()


def post_once(url, body, wait_s):
    slot = "tlSlot" + secrets.token_hex(8)
    chrome_eval(f"""(() => {{
      window.{slot} = '';
      const token = localStorage.getItem('daas.smartsteps.com/-userToken');
      fetch({json.dumps(url)}, {{
        method: 'POST', credentials: 'include',
        headers: {{ Authorization: token, 'Content-Type': 'application/x-www-form-urlencoded' }},
        body: new URLSearchParams({json.dumps(body)}).toString()
      }}).then(async r => {{ window.{slot} = JSON.stringify({{status: r.status, text: await r.text()}}); }})
        .catch(e => {{ window.{slot} = JSON.stringify({{status: 0, text: String(e)}}); }});
      return 'started';
    }})()""")
    for _ in range(wait_s * 2):
        time.sleep(0.5)
        raw = chrome_eval(f"window.{slot} || ''")
        if raw and raw != "missing value":
            chrome_eval(f"delete window.{slot}; 'cleared'")
            out = json.loads(raw)
            text = out["text"]
            if '"code":9' in text:
                raise RuntimeError("LOGIN_EXPIRED")
            try:
                return json.loads(text)
            except ValueError:
                raise TransientResponse(f"non-json response {out['status']} {url}")
    raise RuntimeError("chrome fetch timed out " + url)


class TransientResponse(RuntimeError):
    pass


def post(url, body, wait_s=300, retries=4):
    """Read endpoints retry empty/HTML replies; writes pass retries=1 so a task is never sent twice."""
    for i in range(retries):
        try:
            return post_once(url, body, wait_s)
        except TransientResponse:
            if i == retries - 1:
                raise
            time.sleep(3 * (i + 1))


def folder_list():
    acc = []

    def walk(nodes):
        for n in nodes if isinstance(nodes, list) else [nodes]:
            if not n:
                continue
            acc.append({"title": n.get("title") or n.get("name"), "key": n.get("key"), "parentid": n.get("parentid")})
            walk(n.get("pd") or n.get("children") or [])

    walk(post("/publicdomain/folder/selectFolderAll", {"belongto": "1"}).get("result") or [])
    return acc


def tasks(page=1, size=80):
    r = post("/qsql/gettasks", {"tasktype": "0", "time": str(int(time.time() * 1000)), "pageNum": str(page), "pageSize": str(size)})
    return (r.get("result") or {}).get("constDataSource") or []


def slim(t):
    return {"filename": t.get("filename"), "taskid": t.get("taskid"), "st": t.get("taskstatus"),
            "progress": t.get("progress"), "start": t.get("runstarttime") or t.get("submittime"),
            "end": t.get("runendtime"), "err": re.sub(r"\s+", " ", t.get("errmsg") or "")[-400:]}


def recent(prefix):
    out = []
    for page in range(1, TASK_PAGES + 1):
        rows = tasks(page)
        out += [slim(t) for t in rows if (t.get("filename") or "").startswith(prefix)]
        if len(rows) < 80:
            break
    return out


def status(taskid):
    for page in range(1, TASK_PAGES + 1):
        rows = tasks(page)
        for t in rows:
            if t.get("taskid") == taskid:
                return slim(t)
        if len(rows) < 80:
            break
    return {"taskid": taskid, "found": False}


def ensure_dir(title, parentid):
    def hit():
        return [f for f in folder_list() if f["title"] == title and str(f["parentid"]) == str(parentid)]
    if not hit():
        post("/publicdomain/folder/insertFolders", {"name": title, "belongto": "1", "type": "1", "parentid": str(parentid), "excutetype": "0"}, retries=1)
        time.sleep(1.2)
    found = hit()
    if not found:
        raise RuntimeError("directory not created " + title)
    return found[-1]["key"]


def ensure_parent():
    return ensure_dir(PARENT_FOLDER, ensure_dir(PROJECT_FOLDER, 0))


def submit(qid):
    queue = json.loads(QUEUE.read_text(encoding="utf-8"))
    item = next(q for q in queue if q["id"] == qid)
    if item["status"] not in ("not_run", "failed"):
        raise RuntimeError(f"{qid} status is {item['status']}; check the task before resubmitting")
    for dep in item["depends_on"]:
        d = next(q for q in queue if q["id"] == dep)
        if d["status"] not in ("passed", "satisfied_by_reference"):
            raise RuntimeError(f"dependency {dep} is {d['status']}")
    path = ROOT / item["file"]
    sql = path.read_text(encoding="utf-8")
    sha = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    if sha != item["sha256"]:
        raise RuntimeError(f"{qid} sha256 differs from queue.json")
    errs = lint(sql)
    if errs:
        raise RuntimeError(f"{qid} lint: {', '.join(errs)}")
    name = path.name.split(".")[0]
    if recent(name):
        raise RuntimeError(f"{name} already exists on the platform task list")
    parent = ensure_parent()
    post("/publicdomain/folder/insertFolders", {"name": name, "belongto": "1", "type": "2", "parentid": str(parent), "excutetype": "0"}, retries=1)
    time.sleep(1.2)
    hits = [f for f in folder_list() if f["title"] == name and str(f["parentid"]) == str(parent)]
    if not hits:
        raise RuntimeError("folder not found " + name)
    post("/qsql/insertSqlScript", {"folder_id": str(hits[-1]["key"]), "operation_val": sql, "excutetype": "0"}, retries=1)
    taskid = secrets.token_hex(16)
    run = post("/qsql/tasks", {"excutetype": "hive", "taskid": taskid, "filename": name, "sqls": sql}, retries=1)
    if run.get("code") != 1:
        raise RuntimeError(run.get("message"))
    rec = {"queue_id": qid, "name": name, "file": item["file"], "sha256": sha, "taskid": taskid,
           "parent_folder": PARENT_FOLDER, "submitted_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "platform_submittime": (run.get("result") or {}).get("submittime")}
    with SUBMISSIONS.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    record(qid, status="submitted", taskid=taskid)
    return rec


def fetch(taskid, stem):
    out = QR / f"{stem}.csv"
    if out.exists() or out.is_symlink():
        raise RuntimeError(f"target exists, not overwriting {out}")
    rows = []
    for page in range(1, 201):
        raw = post("/qsql/getresult", {"taskid": taskid, "pageNum": str(page), "pageSize": "50000"}, wait_s=600)
        if raw.get("code") != 1:
            raise RuntimeError("getresult " + str(raw.get("message")))
        got = (raw.get("result") or {}).get("constDataSource") or []
        rows += got
        if len(got) < 50000:
            break
    fields = []
    for r in rows:
        for k in r:
            if k != "ctid" and k not in fields:
                fields.append(k)
    if any(re.fullmatch(r"uid", f, re.I) for f in fields):
        raise RuntimeError("uid column in result, not writing")
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(fields)
    for r in rows:
        w.writerow(["" if r.get(f) is None else r.get(f) for f in fields])
    data = buf.getvalue().encode("utf-8")
    QR.mkdir(parents=True, exist_ok=True)
    out.write_bytes(data)
    return {"file": str(out.relative_to(ROOT.parent)), "n_rows": len(rows), "n_fields": len(fields),
            "sha256": hashlib.sha256(data).hexdigest()}


def wait(taskid, poll_s=60, max_polls=240):
    for _ in range(max_polls):
        try:
            s = status(taskid)
        except TransientResponse:
            s = {}
        if s.get("st") == 1:
            return s
        if s.get("st") not in (0, 2, 3, -1, None):
            return s
        time.sleep(poll_s)
    raise RuntimeError(f"{taskid} still pending after {poll_s * max_polls}s")


def record(qid, **fields):
    queue = json.loads(QUEUE.read_text(encoding="utf-8"))
    next(q for q in queue if q["id"] == qid).update(fields)
    QUEUE.write_text(json.dumps(queue, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def chain(qids):
    """Submit in order; after each pass fetch the final SELECT and mark passed. Stop at the first failure."""
    done = []
    for qid in qids:
        item = next(q for q in json.loads(QUEUE.read_text(encoding="utf-8")) if q["id"] == qid)
        if item["status"] == "submitted" and item.get("taskid"):
            rec = {"taskid": item["taskid"], "name": Path(item["file"]).stem}
        else:
            rec = submit(qid)
        s = wait(rec["taskid"])
        if s.get("st") != 1:
            record(qid, status="failed", error=s.get("err"))
            done.append({"queue_id": qid, "status": "failed", "taskid": rec["taskid"], "err": s.get("err")})
            break
        out = fetch(rec["taskid"], rec["name"])
        record(qid, status="passed", output={"path": str(Path(out["file"]).relative_to("Platform")), "n_rows": out["n_rows"],
                                             "sha256": out["sha256"]}, platform_end=s.get("end"))
        done.append({"queue_id": qid, "status": "passed", "taskid": rec["taskid"], "end": s.get("end"), **out})
        print(json.dumps(done[-1], ensure_ascii=False), flush=True)
    return done


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    cmd = argv[1]
    if cmd == "lint":
        errs = lint(Path(argv[2]).read_text(encoding="utf-8"))
        print(json.dumps({"file": argv[2], "errors": errs}, ensure_ascii=False))
        return 1 if errs else 0
    if cmd == "folders":
        res = [f for f in folder_list() if f["title"] == PARENT_FOLDER or str(f["title"]).startswith("TL25M05")]
    elif cmd == "recent":
        res = recent(argv[2] if len(argv) > 2 else "TL25M05")
    elif cmd == "status":
        res = status(argv[2])
    elif cmd == "submit":
        res = submit(argv[2])
    elif cmd == "fetch":
        res = fetch(argv[2], argv[3])
    elif cmd == "chain":
        res = chain(argv[2:])
    else:
        print(__doc__)
        return 2
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
