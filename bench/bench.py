#!/usr/bin/env python3
"""How does scale affect latency? A benchmark for the agent pipeline on plexus-flow.

Sweeps:
  scaling   2D matrix of (worker fleet size W) x (parallel workflows in flight C)
            with realistic per-step I/O duration (default 20 ms/step, 4 slots/worker).
            Answers: how many parallel flows can I run per worker without hurting
            latency, and where does adding more workers hit the engine ceiling?
  engine    pure orchestration ceiling (0 ms steps): how fast can one plexus-flow
            coordinator dispatch and commit steps when workers are infinitely fast?
  history   back-to-back bursts of 50 workflows comparing the default file-backed
            dedup store (dedup.db) against the in-memory dedup store.
  pipeline  steps per workflow (10-step real agent pipeline + 12/24/48/100-step
            synthetic fork-join DAGs): how DAG size affects per-step overhead.

Completion is streamed via Server-Sent Events (GET /api/v1/flows/{flow}/watch?terminal=1),
and latency is computed from the engine's own started_at -> ended_at timestamps.

    uv run python bench/bench.py                        # all sweeps + charts
    uv run python bench/bench.py --sweep scaling        # only the worker scaling matrix
    uv run python bench/bench.py --quick                # faster subset (~45s)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
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

TERMINAL = {"COMPLETED", "FAILED", "COMPENSATED", "CANCELLED", "COMPENSATION_FAILED"}
HTTP, GRPC, RAFT, ACT_RAFT = "127.0.0.1:28080", "127.0.0.1:29090", "127.0.0.1:29000", "127.0.0.1:29001"
CYAN, GREEN, YELLOW, RED, BOLD, DIM, RESET = "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[1m", "\033[2m", "\033[0m"


# ---------------------------------------------------------------------------
# Cases & Results
# ---------------------------------------------------------------------------

@dataclass
class Case:
    sweep: str
    x: int                      # primary x-axis value (parallel flows, total workflows, or steps)
    series: str                 # series label for multi-line plots (e.g. "4 workers (16 slots)")
    workload: str               # "agent" (real 10-step pipeline) or "synthetic" (no-op fork-join DAG)
    steps: int
    critical_path: int          # longest dependency chain, in steps
    concurrency: int            # parallel workflows in flight per burst
    py_workers: int = 0
    go_workers: int = 0
    noop_workers: int = 0
    worker_concurrency: int = 4
    step_ms: float = 0.0        # simulated I/O duration per activity step (ms)
    dedup_memory: bool = True   # True avoids dedup.db full-file rewrite drift unless testing history
    rounds: int = 2             # bursts to run for this point


@dataclass
class Result:
    case: Case
    completed: int = 0
    failed: int = 0
    timed_out: bool = False
    stuck: int = 0
    round_p50_ms: list[float] = field(default_factory=list)
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


def sweeps(quick: bool, step_ms: float, slots_per_worker: int) -> dict[str, list[Case]]:
    # In the 10-step agent pipeline:
    # - 8 steps are on the critical path (7 activity stages + 1 signal stage; stage 2 fans out to 3 parallel agents)
    agent_steps, agent_cp = 10, 7 if step_ms > 0 else 8
    worker_counts = [1, 2, 4, 8] if quick else [1, 2, 4, 8, 16]
    parallel_flows = [1, 4, 8, 16, 32, 64, 128] if quick else [1, 4, 8, 16, 32, 64, 128, 256]
    engine_loads = [1, 10, 50, 100, 250] if quick else [1, 10, 50, 100, 250, 500]
    dag_sizes = [12, 24, 48] if quick else [12, 24, 48, 100]
    hist_rounds = 10 if quick else 16

    scaling_cases: list[Case] = []
    for w in worker_counts:
        slots = w * slots_per_worker
        label = f"{w} worker{'s' if w > 1 else ''} ({slots} slots)"
        for c in parallel_flows:
            rnds = 4 if c == 1 else (3 if c <= 8 else 2)
            scaling_cases.append(Case(
                sweep="scaling", x=c, series=label, workload="agent",
                steps=agent_steps, critical_path=agent_cp, concurrency=c,
                py_workers=w, go_workers=w, worker_concurrency=slots_per_worker,
                step_ms=step_ms, dedup_memory=True, rounds=rnds,
            ))

    engine_cases = [
        Case(
            sweep="engine", x=c, series="pure engine (0 ms steps)", workload="agent",
            steps=agent_steps, critical_path=8, concurrency=c,
            py_workers=4, go_workers=2, worker_concurrency=32,
            step_ms=0.0, dedup_memory=True, rounds=max(2, 30 // c),
        )
        for c in engine_loads
    ]

    history_cases = [
        Case(
            sweep="history", x=50 * hist_rounds, series="in-memory dedup", workload="agent",
            steps=agent_steps, critical_path=8, concurrency=50,
            py_workers=4, go_workers=2, worker_concurrency=32,
            step_ms=0.0, dedup_memory=True, rounds=hist_rounds,
        ),
        Case(
            sweep="history", x=50 * hist_rounds, series="dedup.db (file store)", workload="agent",
            steps=agent_steps, critical_path=8, concurrency=50,
            py_workers=4, go_workers=2, worker_concurrency=32,
            step_ms=0.0, dedup_memory=False, rounds=hist_rounds,
        ),
    ]

    pipeline_cases = [
        Case(
            sweep="pipeline", x=agent_steps, series="real agent pipeline", workload="agent",
            steps=agent_steps, critical_path=8, concurrency=10,
            py_workers=2, go_workers=2, worker_concurrency=32,
            step_ms=0.0, dedup_memory=True, rounds=3,
        ),
        *[
            Case(
                sweep="pipeline", x=n, series="synthetic fork-join DAG", workload="synthetic",
                steps=n, critical_path=2 * (n // 4), concurrency=10,
                noop_workers=2, worker_concurrency=32,
                step_ms=0.0, dedup_memory=True, rounds=3,
            )
            for n in dag_sizes
        ],
    ]

    return {
        "scaling": scaling_cases,
        "engine": engine_cases,
        "history": history_cases,
        "pipeline": pipeline_cases,
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
# Stack under test: engine + worker fleet
# ---------------------------------------------------------------------------

class Stack:
    def __init__(self, case: Case, run_dir: Path) -> None:
        self.case, self.dir = case, run_dir
        self.procs: list[subprocess.Popen[bytes]] = []
        self.warmed = False

    def __enter__(self) -> "Stack":
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        if self.case.dedup_memory:
            # A directory at dedup.db causes the engine to use its in-memory dedup store.
            (self.dir / "flow" / "dedup.db").mkdir(parents=True)
        log = open(self.dir / "engine.log", "wb")
        self.procs.append(subprocess.Popen(
            [str(BIN / "plexus-flow"), "--http-addr", HTTP, "--grpc-addr", GRPC, "--raft-addr", RAFT,
             "--act-raft-addr", ACT_RAFT, "--data-dir", str(self.dir / "flow")], stdout=log, stderr=log))
        _wait_http(f"http://{HTTP}/api/v1/cluster/status")

        env = {
            **os.environ,
            "SHOWCASE_QUIET": "1",
            "LLM_LEDGER": "0",
            "PLEXUS_LLM": "mock",
            "KB_URL": "",
            "BENCH_STEP_MS": str(self.case.step_ms),
        }
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


def _stack_key(case: Case) -> tuple[Any, ...]:
    """Small scaling points (c <= 32) for the same worker fleet share a Stack; large bursts get a fresh Stack."""
    if case.sweep != "scaling" or case.concurrency >= 64 or not case.dedup_memory:
        return ("unique", id(case))
    return (case.sweep, case.workload, case.py_workers, case.go_workers, case.worker_concurrency, case.step_ms)


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
# Closed-loop SSE driver
# ---------------------------------------------------------------------------

def _ts(v: str) -> float:
    """RFC 3339 with nanoseconds -> epoch seconds (keeps microsecond precision)."""
    head, _, frac = v.replace("Z", "").partition(".")
    frac = (frac + "000000")[:6]
    return datetime.fromisoformat(f"{head}.{frac}+00:00").timestamp()


async def drive(stack: Stack, case: Case, budget_s: float, rounds_override: int | None = None) -> Result:
    if case.workload == "agent":
        definition = load_definition()
        ticket = json.loads((ROOT / "fixtures" / "tickets" / "damaged-espresso.json").read_text())
    else:
        definition, ticket = synthetic_definition(case.steps), {}
    flow = definition["name"]
    approval = {"signal_name": "approval", "payload": {"approved": True, "reviewer": "bench"}}
    prefix = f"{case.sweep}-w{case.py_workers}-c{case.concurrency}-s{case.steps}"

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

            if not stack.warmed:
                warm = {f"{prefix}-warmup"}
                await start(f"{prefix}-warmup")
                await collect(warm, time.time() + 30)
                if warm:
                    raise RuntimeError("warm-up workflow did not finish; check bench/data/*/workers.log")
                summaries.clear()
                stack.warmed = True

            # Pre-warm the HTTP keep-alive connection pool before starting the timed burst.
            await asyncio.gather(*(client.get("/api/v1/cluster/status") for _ in range(min(case.concurrency, 64))))

            rounds = rounds_override or case.rounds
            t0 = time.time()
            for rnd in range(rounds):
                ids = [f"{prefix}-r{rnd}-{i}" for i in range(case.concurrency)]
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
# Output & Persistence
# ---------------------------------------------------------------------------

SWEEP_LABEL = {
    "scaling": "parallel wf",
    "engine": "parallel wf",
    "history": "total wf",
    "pipeline": "steps/wf",
}


def print_header(sweep: str) -> None:
    print(f"\n{BOLD}{CYAN}▶ sweep: {sweep}{RESET}")
    print(f"{DIM}{'series':<22} {SWEEP_LABEL[sweep]:>11} {'runs':>6} {'p50':>9} {'p95':>9} {'p99':>9} "
          f"{'per step':>9} {'wf/s':>8} {'steps/s':>8}{RESET}")


def print_row(r: Result) -> None:
    c = r.case
    warn = f" {RED}({r.failed} failed){RESET}" if r.failed else ""
    warn += f" {YELLOW}({r.stuck} unfinished at budget){RESET}" if r.stuck else ""
    print(f"{c.series:<22} {c.x:>11} {r.completed + r.failed:>6} {r.p50_ms:>7.1f}ms {r.p95_ms:>7.1f}ms "
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
    return {
        "cpu": cpu,
        "cores": str(os.cpu_count()),
        "os": f"{platform.system()} {platform.release()}",
        "go": sh("go", "version").replace("go version ", ""),
        "python": platform.python_version(),
        "plexus_flow_commit": _engine_version(sh),
        "date": datetime.now().isoformat(timespec="seconds"),
    }


def save(results: list[Result], info: dict[str, str]) -> Path:
    out_dir = BENCH / "results"
    out_dir.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    payload = {"machine": info, "results": [{**asdict(r), "latencies_ms": None} for r in results]}
    path = out_dir / f"{stamp}.json"
    path.write_text(json.dumps(payload, indent=2))
    with open(out_dir / f"{stamp}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sweep", "series", "x", "workload", "steps", "concurrency", "py_workers", "go_workers",
                    "worker_concurrency", "step_ms", "dedup_memory", "runs", "failed",
                    "p50_ms", "p95_ms", "p99_ms", "mean_ms", "max_ms", "per_step_ms", "throughput_wps", "steps_per_s"])
        for r in results:
            c = r.case
            w.writerow([c.sweep, c.series, c.x, c.workload, c.steps, c.concurrency, c.py_workers, c.go_workers,
                        c.worker_concurrency, c.step_ms, c.dedup_memory, r.completed + r.failed, r.failed,
                        *(round(v, 2) for v in (
                            r.p50_ms, r.p95_ms, r.p99_ms, r.mean_ms, r.max_ms, r.per_step_ms,
                            r.throughput_wps, r.steps_per_s))])
    return path


def ensure_binaries() -> None:
    BIN.mkdir(exist_ok=True)
    print(f"{DIM}building plexus-flow and the Go tool worker...{RESET}")
    subprocess.run(["go", "build", "-o", str(BIN / "plexus-flow"), "github.com/avivklas/plexus-flow/cmd/plexus-flow"],
                   cwd=ROOT, check=True)
    subprocess.run(["go", "build", "-o", str(BIN / "tool-worker"), "./go"], cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sweep", default="scaling,engine,history,pipeline",
                        help="comma-separated: scaling,engine,history,pipeline")
    parser.add_argument("--step-ms", type=float, default=20.0,
                        help="simulated step I/O time in ms for the worker scaling sweep (default: 20)")
    parser.add_argument("--worker-concurrency", type=int, default=4,
                        help="max concurrent tasks per worker process in the scaling sweep (default: 4)")
    parser.add_argument("--quick", action="store_true", help="smaller sweeps (~45s)")
    parser.add_argument("--budget", type=float, default=120, help="max seconds per data point")
    parser.add_argument("--rounds", type=int, default=None, help="override bursts per data point")
    parser.add_argument("--only", default="", help="comma-separated x values to run, e.g. --only 1,16")
    parser.add_argument("--no-report", action="store_true", help="skip writing bench/RESULTS.md and SVG charts")
    args = parser.parse_args()

    ensure_binaries()
    info = machine_info()
    info["scaling_step_ms"] = str(args.step_ms)
    info["scaling_worker_concurrency"] = str(args.worker_concurrency)
    print(f"{BOLD}plexus-flow scale benchmark{RESET} {DIM}{info['cpu']} · {info['cores']} cores · {info['os']} · "
          f"scaling step={args.step_ms:g}ms, {args.worker_concurrency} slots/worker{RESET}")

    plan = sweeps(args.quick, args.step_ms, args.worker_concurrency)
    results: list[Result] = []
    for sweep in [s.strip() for s in args.sweep.split(",") if s.strip()]:
        print_header(sweep)
        cases = [c for c in plan[sweep] if not args.only or str(c.x) in args.only.split(",")]
        # Group consecutive cases that share the same Stack configuration (e.g. same worker fleet in `scaling`).
        idx = 0
        while idx < len(cases):
            first = cases[idx]
            key = _stack_key(first)
            group = [first]
            idx += 1
            while idx < len(cases) and _stack_key(cases[idx]) == key:
                group.append(cases[idx])
                idx += 1
            slug = f"{sweep}-{first.workload}-w{first.py_workers}-m{int(first.dedup_memory)}-{first.x}"
            with Stack(first, BENCH / "data" / slug) as stack:
                for case in group:
                    r = asyncio.run(drive(stack, case, args.budget, args.rounds))
                    print_row(r)
                    if args.rounds or sweep == "history":
                        print(f"{DIM}{'':>22} p50 per burst: {r.round_p50_ms}{RESET}")
                    results.append(r)

    path = save(results, info)
    print(f"\n{GREEN}results:{RESET} {path.relative_to(ROOT)} (+ .csv)")
    if not args.no_report:
        from report import write_report  # noqa: PLC0415
        print(f"{GREEN}report:{RESET}  {write_report(path).relative_to(ROOT)}")


if __name__ == "__main__":
    main()
