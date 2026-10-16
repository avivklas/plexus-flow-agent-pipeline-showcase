#!/usr/bin/env python3
"""How does scale affect latency? A benchmark for the agent pipeline on plexus-flow.

Three sweeps, each point measured on a freshly started engine and worker fleet:

  load      workflows submitted at once (bursts), real 10-step agent pipeline
  fleet     number of worker processes at a fixed load, real agent pipeline
  pipeline  steps per workflow: synthetic fork-join DAGs of no-op steps (+ the real pipeline)
  history   identical bursts back to back on one engine: does latency drift as work accumulates?

The mock LLM answers instantly and per-step logging is off, so the numbers are
the cost of orchestration itself: Raft commits, dispatch to gRPC workers,
dependency resolution, the approval signal.

Latency is taken from the engine's own timestamps (workflow created -> completed),
and completion is streamed from the engine's SSE watch endpoint rather than polled.

    uv run python bench/bench.py                       # all sweeps
    uv run python bench/bench.py --sweep load --quick  # one quick sweep
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "bench"
BIN = ROOT / "bin"
sys.path.insert(0, str(ROOT / "python"))
from launcher import load_definition  # noqa: E402

DEDUP_MEMORY = False  # set by --dedup-memory
TERMINAL = {"COMPLETED", "FAILED", "COMPENSATED", "CANCELLED", "COMPENSATION_FAILED"}
HTTP, GRPC, RAFT, ACT_RAFT = "127.0.0.1:28080", "127.0.0.1:29090", "127.0.0.1:29000", "127.0.0.1:29001"
CYAN, GREEN, YELLOW, RED, BOLD, DIM, RESET = "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[1m", "\033[2m", "\033[0m"


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

@dataclass
class Case:
    sweep: str
    x: int                      # the swept value
    workload: str               # "agent" (real pipeline) or "synthetic" (no-op fork-join DAG)
    steps: int
    critical_path: int          # longest dependency chain, in steps
    concurrency: int            # workflows submitted at once (one burst)
    py_workers: int = 0
    go_workers: int = 0
    noop_workers: int = 0
    worker_concurrency: int = 32
    rounds: int = 0             # bursts to run; 0 = enough for at least 30 workflows


@dataclass
class Result:
    case: Case
    completed: int = 0
    failed: int = 0
    timed_out: bool = False
    stuck: int = 0
    round_p50_ms: list[float] = field(default_factory=list)  # p50 of each burst, in order
    p50_ms: float = 0
    p95_ms: float = 0
    p99_ms: float = 0
    mean_ms: float = 0
    max_ms: float = 0
    per_step_ms: float = 0      # p50 latency / critical path
    throughput_wps: float = 0   # workflows per second
    steps_per_s: float = 0
    wall_s: float = 0
    latencies_ms: list[float] = field(default_factory=list, repr=False)


def sweeps(quick: bool) -> dict[str, list[Case]]:
    agent_steps, agent_cp = 10, 8  # triage→kb→plan→refund→draft→guardrail→approval→send
    load_c = [1, 10, 50, 100, 250] if quick else [1, 10, 50, 100, 250, 500, 1000]
    fleet_n = [1, 2, 4] if quick else [1, 2, 4, 8]
    sizes = [12, 24, 48] if quick else [12, 24, 48, 100]
    fleet_c = 100 if quick else 200
    return {
        "load": [Case("load", c, "agent", agent_steps, agent_cp, c, py_workers=4, go_workers=2)
                 for c in load_c],
        "fleet": [Case("fleet", n, "agent", agent_steps, agent_cp, fleet_c,
                       py_workers=n, go_workers=n, worker_concurrency=8) for n in fleet_n],
        "pipeline": [Case("pipeline", agent_steps, "agent", agent_steps, agent_cp, 10, py_workers=1, go_workers=1,
                          worker_concurrency=64)]
                    + [Case("pipeline", n, "synthetic", n, 2 * (n // 4), 10, noop_workers=1, worker_concurrency=64)
                       for n in sizes],
        "history": [Case("history", 50 * (10 if quick else 20), "agent", agent_steps, agent_cp, 50,
                         py_workers=4, go_workers=2, rounds=10 if quick else 20)],
    }


def synthetic_definition(steps: int) -> dict[str, Any]:
    """Repeating agent-shaped units: one step fans out to three parallel steps, which join into the next unit."""
    defs, prev = [], []
    for u in range(steps // 4):
        head = f"u{u}-plan"
        defs.append({"name": head, "activity": "noop", "depends_on": prev})
        prev = []
        for b in range(3):
            name = f"u{u}-branch{b}"
            defs.append({"name": name, "activity": "noop", "depends_on": [head]})
            prev.append(name)
    return {"name": f"synthetic-{steps}", "steps": defs}


# ---------------------------------------------------------------------------
# The stack under test: engine + worker fleet, restarted for every case
# ---------------------------------------------------------------------------

class Stack:
    def __init__(self, case: Case, run_dir: Path) -> None:
        self.case, self.dir = case, run_dir
        self.procs: list[subprocess.Popen[bytes]] = []

    def __enter__(self) -> "Stack":
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        if DEDUP_MEMORY:
            # Experiment: a directory where dedup.db should be makes the engine fall back to its in-memory dedup store.
            (self.dir / "flow" / "dedup.db").mkdir(parents=True)
        log = open(self.dir / "engine.log", "wb")
        self.procs.append(subprocess.Popen(
            [str(BIN / "plexus-flow"), "--http-addr", HTTP, "--grpc-addr", GRPC, "--raft-addr", RAFT,
             "--act-raft-addr", ACT_RAFT, "--data-dir", str(self.dir / "flow")], stdout=log, stderr=log))
        _wait_http(f"http://{HTTP}/api/v1/cluster/status")

        env = {**os.environ, "SHOWCASE_QUIET": "1", "LLM_LEDGER": "0", "PLEXUS_LLM": "mock", "KB_URL": ""}
        wlog = open(self.dir / "workers.log", "wb")
        c = str(self.case.worker_concurrency)
        for i in range(self.case.py_workers):
            self._spawn([sys.executable, str(ROOT / "python" / "agents.py"), f"--grpc-addr={GRPC}",
                         f"--worker-id=py-{i}", f"--max-concurrency={c}", f"--pidfile={self.dir}/py-{i}.pid"], env, wlog)
        for i in range(self.case.go_workers):
            self._spawn([str(BIN / "tool-worker"), f"--grpc-addr={GRPC}", f"--worker-id=go-{i}",
                         f"--max-concurrency={c}"], env, wlog)
        for i in range(self.case.noop_workers):
            self._spawn([sys.executable, str(BENCH / "noop_worker.py"), f"--grpc-addr={GRPC}",
                         f"--worker-id=noop-{i}", f"--max-concurrency={c}"], env, wlog)
        return self

    def _spawn(self, cmd: list[str], env: dict[str, str], log: Any) -> None:
        self.procs.append(subprocess.Popen(cmd, env=env, stdout=log, stderr=log))

    def __exit__(self, *exc: Any) -> None:
        for p in reversed(self.procs):
            p.terminate()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def _wait_http(url: str, timeout: float = 15) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {url}")


# ---------------------------------------------------------------------------
# Closed-loop driver
# ---------------------------------------------------------------------------

def _ts(v: str) -> float:
    """RFC 3339 with nanoseconds -> epoch seconds (keeps microsecond precision)."""
    head, _, frac = v.replace("Z", "").partition(".")
    frac = (frac + "000000")[:6]
    return datetime.fromisoformat(f"{head}.{frac}+00:00").timestamp()


async def drive(case: Case, budget_s: float, rounds: int | None = None) -> Result:
    if case.workload == "agent":
        definition = load_definition()
        ticket = json.loads((ROOT / "fixtures" / "tickets" / "damaged-espresso.json").read_text())
    else:
        definition, ticket = synthetic_definition(case.steps), {}
    flow = definition["name"]
    approval = {"signal_name": "approval", "payload": {"approved": True, "reviewer": "bench"}}

    limits = httpx.Limits(max_connections=256, max_keepalive_connections=256)
    async with httpx.AsyncClient(base_url=f"http://{HTTP}", limits=limits, timeout=60) as client:

        async def start(wf_id: str) -> None:
            r = await client.post("/api/v1/workflows/start",
                                  json={"workflow_id": wf_id, "definition": definition, "input": ticket})
            r.raise_for_status()
            if case.workload == "agent":  # signals sent early are buffered until await-approval runs
                (await client.post(f"/api/v1/workflows/{wf_id}/signal", json=approval)).raise_for_status()

        summaries: dict[str, dict[str, Any]] = {}
        spans: list[float] = []
        round_p50: list[float] = []
        stuck: list[str] = []

        # Stream terminal workflow summaries as server-sent events instead of polling /traces.
        async with client.stream("GET", f"/api/v1/flows/{flow}/watch?terminal=1", timeout=None) as watch:
            watch.raise_for_status()
            lines = watch.aiter_lines()

            async def collect(pending: set[str], deadline: float) -> None:
                while pending:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return
                    try:
                        line = await asyncio.wait_for(anext(lines), timeout=remaining)
                    except (TimeoutError, StopAsyncIteration):
                        return
                    if not line.startswith("data:"):
                        continue
                    s = json.loads(line[5:])
                    if s["workflow_id"] in pending and s["status"] in TERMINAL:
                        summaries[s["workflow_id"]] = s
                        pending.discard(s["workflow_id"])

            # Warm-up: one workflow end to end, so every worker is connected and every code path is hot.
            warm = {"warmup"}
            await start("warmup")
            await collect(warm, time.time() + 30)
            if warm:
                raise RuntimeError("warm-up workflow did not finish; check bench/data/*/workers.log")
            summaries.clear()

            # Bursts: submit `concurrency` workflows at once, wait for all of them on the watch stream, repeat.
            rounds = rounds or case.rounds or max(3, math.ceil(30 / case.concurrency))
            t0 = time.time()
            for rnd in range(rounds):
                ids = [f"bench-{rnd}-{i}" for i in range(case.concurrency)]
                pending = set(ids)
                await asyncio.gather(*(start(i) for i in ids))
                await collect(pending, t0 + budget_s)
                done = [summaries[i] for i in ids if i in summaries]
                if done:
                    round_lat = sorted((_ts(d["ended_at"]) - _ts(d["started_at"])) * 1000 for d in done)
                    round_p50.append(round(statistics.median(round_lat), 1))
                    spans.append(max(_ts(d["ended_at"]) for d in done) - min(_ts(d["started_at"]) for d in done))
                if pending:
                    stuck = sorted(pending)
                    wf = (await client.get(f"/api/v1/workflows/{stuck[0]}")).json()
                    states = {n: (st["status"], st.get("attempt"), st.get("error", "")) for n, st in wf["steps"].items()
                              if st["status"] != "COMPLETED"}
                    print(f"{RED}  {len(stuck)} workflow(s) unfinished when the time budget ran out, "
                          f"e.g. {stuck[0]}: {wf.get('status')} {states}{RESET}")
                    break

    res = Result(case=case, wall_s=time.time() - t0, round_p50_ms=round_p50)
    rows = list(summaries.values())
    res.timed_out = bool(stuck)
    res.stuck = len(stuck)
    res.completed = sum(1 for s in rows if s["status"] == "COMPLETED")
    res.failed = len(rows) - res.completed
    lat = sorted((_ts(s["ended_at"]) - _ts(s["started_at"])) * 1000 for s in rows if s.get("ended_at"))
    if lat:
        q = statistics.quantiles(lat, n=100, method="inclusive") if len(lat) > 1 else [lat[0]] * 99
        res.p50_ms, res.p95_ms, res.p99_ms = statistics.median(lat), q[94], q[98]
        res.mean_ms, res.max_ms = statistics.fmean(lat), lat[-1]
        res.per_step_ms = res.p50_ms / case.critical_path
        res.throughput_wps = len(lat) / sum(spans) if sum(spans) > 0 else 0
        res.steps_per_s = res.throughput_wps * case.steps
        res.latencies_ms = lat
    return res


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

SWEEP_LABEL = {"load": "in flight", "fleet": "workers/kind", "pipeline": "steps", "history": "workflows"}


def print_header(sweep: str) -> None:
    print(f"\n{BOLD}{CYAN}▶ sweep: {sweep}{RESET}")
    print(f"{DIM}{SWEEP_LABEL[sweep]:>12} {'workload':>9} {'runs':>6} {'p50':>9} {'p95':>9} {'p99':>9} "
          f"{'per step':>9} {'wf/s':>8} {'steps/s':>8}{RESET}")


def print_row(r: Result) -> None:
    c = r.case
    warn = f" {RED}({r.failed} failed){RESET}" if r.failed else ""
    warn += f" {YELLOW}({r.stuck} unfinished at budget){RESET}" if r.stuck else ""
    print(f"{c.x:>12} {c.workload:>9} {r.completed + r.failed:>6} {r.p50_ms:>7.1f}ms {r.p95_ms:>7.1f}ms "
          f"{r.p99_ms:>7.1f}ms {r.per_step_ms:>7.2f}ms {r.throughput_wps:>8.1f} {r.steps_per_s:>8.0f}{warn}")


def _engine_version(sh: Any) -> str:
    repo = str(ROOT.parent / "plexus-flow")
    commit = sh("git", "-C", repo, "rev-parse", "--short", "HEAD")
    branch = sh("git", "-C", repo, "rev-parse", "--abbrev-ref", "HEAD")
    dirty = " + uncommitted changes" if sh("git", "-C", repo, "status", "--porcelain", "--untracked-files=no") else ""
    return f"{commit} ({branch}){dirty}"


def machine_info() -> dict[str, str]:
    def sh(*cmd: str) -> str:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=5).stdout.strip()
        except Exception:  # noqa: BLE001
            return ""
    cpu = sh("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor()
    return {"cpu": cpu, "cores": str(os.cpu_count()), "os": f"{platform.system()} {platform.release()}",
            "go": sh("go", "version").replace("go version ", ""), "python": platform.python_version(),
            "plexus_flow_commit": _engine_version(sh),
            "date": datetime.now().isoformat(timespec="seconds")}


def save(results: list[Result], info: dict[str, str]) -> Path:
    out_dir = BENCH / "results"
    out_dir.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S") + ("-dedup-memory" if DEDUP_MEMORY else "")
    payload = {"machine": info, "results": [{**asdict(r), "latencies_ms": None} for r in results]}
    path = out_dir / f"{stamp}.json"
    path.write_text(json.dumps(payload, indent=2))
    with open(out_dir / f"{stamp}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sweep", "x", "workload", "steps", "concurrency", "py_workers", "go_workers", "runs", "failed",
                    "p50_ms", "p95_ms", "p99_ms", "mean_ms", "max_ms", "per_step_ms", "throughput_wps", "steps_per_s"])
        for r in results:
            c = r.case
            w.writerow([c.sweep, c.x, c.workload, c.steps, c.concurrency, c.py_workers, c.go_workers,
                        r.completed + r.failed, r.failed, *(round(v, 2) for v in (
                            r.p50_ms, r.p95_ms, r.p99_ms, r.mean_ms, r.max_ms, r.per_step_ms,
                            r.throughput_wps, r.steps_per_s))])
    return path


def ensure_binaries() -> None:
    BIN.mkdir(exist_ok=True)
    print(f"{DIM}building plexus-flow and the Go tool worker...{RESET}")
    # Built through this module, whose go.mod replaces plexus-flow and plexus with the sibling checkouts.
    subprocess.run(["go", "build", "-o", str(BIN / "plexus-flow"), "github.com/avivklas/plexus-flow/cmd/plexus-flow"],
                   cwd=ROOT, check=True)
    subprocess.run(["go", "build", "-o", str(BIN / "tool-worker"), "./go"], cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sweep", default="history,load,fleet,pipeline",
                        help="comma-separated: history,load,fleet,pipeline")
    parser.add_argument("--dedup-memory", action="store_true",
                        help="experiment: run the engine with its in-memory dedup store instead of dedup.db")
    parser.add_argument("--quick", action="store_true", help="smaller sweeps (~1 minute)")
    parser.add_argument("--budget", type=float, default=120, help="max seconds per data point")
    parser.add_argument("--rounds", type=int, default=None, help="override bursts per data point")
    parser.add_argument("--only", default="", help="comma-separated x values to run, e.g. --only 250")
    parser.add_argument("--no-report", action="store_true", help="skip writing bench/RESULTS.md")
    args = parser.parse_args()

    global DEDUP_MEMORY
    DEDUP_MEMORY = args.dedup_memory
    ensure_binaries()
    info = machine_info()
    info["dedup_store"] = "memory (experiment)" if DEDUP_MEMORY else "file (default)"
    print(f"{BOLD}plexus-flow scale benchmark{RESET} {DIM}{info['cpu']} · {info['cores']} cores · {info['os']} · "
          f"dedup store: {info['dedup_store']}{RESET}")

    plan = sweeps(args.quick)
    results: list[Result] = []
    for sweep in [s.strip() for s in args.sweep.split(",")]:
        print_header(sweep)
        for case in plan[sweep]:
            if args.only and str(case.x) not in args.only.split(","):
                continue
            with Stack(case, BENCH / "data" / f"{sweep}-{case.workload}-{case.x}"):
                r = asyncio.run(drive(case, args.budget, args.rounds))
            print_row(r)
            if args.rounds or sweep == "history":
                print(f"{DIM}{'':>12} p50 per burst: {r.round_p50_ms}{RESET}")
            results.append(r)

    path = save(results, info)
    print(f"\n{GREEN}results:{RESET} {path.relative_to(ROOT)} (+ .csv)")
    if not args.no_report:
        from report import write_report  # noqa: PLC0415
        print(f"{GREEN}report:{RESET}  {write_report(path).relative_to(ROOT)}")


if __name__ == "__main__":
    main()
