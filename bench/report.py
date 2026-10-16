"""Render a benchmark results JSON file as bench/RESULTS.md (tables + Mermaid charts)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

BENCH = Path(__file__).resolve().parent

TITLES = {
    "load": ("Load: workflows in flight", "workflows submitted at once",
             "Real 10-step agent pipeline (incl. approval signal), 4 Python + 2 Go workers × 32 concurrent tasks."),
    "fleet": ("Worker fleet size", "worker processes per kind (Python agents + Go tools)",
              "Real agent pipeline at a fixed load, each worker running at most 8 tasks at a time."),
    "history": ("History: identical bursts back to back", "workflows completed so far",
                "Bursts of 50 real agent-pipeline workflows run one after another on the same engine. Nothing about "
                "the load changes between bursts, so a flat line is the expected result."),
    "pipeline": ("Pipeline size", "steps per workflow",
                 "10 workflows in flight. `10*` is the real agent pipeline; the rest are synthetic fork-join DAGs of "
                 "no-op steps (one step fans out to three, which join into the next)."),
}


def _label(r: dict[str, Any]) -> str:
    c = r["case"]
    return f"{c['x']}*" if c["sweep"] == "pipeline" and c["workload"] == "agent" else str(c["x"])


def _chart(title: str, x_title: str, labels: list[str], series: list[list[float]], y_title: str, bar: bool = False) -> str:
    top = max((v for s in series for v in s), default=1) * 1.15 or 1
    xs = ", ".join(f'"{l}"' for l in labels)
    kind = "bar" if bar else "line"
    lines = "\n".join(f"    {kind} [{', '.join(f'{v:.1f}' for v in s)}]" for s in series)
    return (f"```mermaid\nxychart-beta\n    title \"{title}\"\n    x-axis \"{x_title}\" [{xs}]\n"
            f"    y-axis \"{y_title}\" 0 --> {top:.0f}\n{lines}\n```")


def _history(rows: list[dict[str, Any]], compare: list[dict[str, Any]] | None) -> list[str]:
    r = rows[0]
    burst = r["case"]["concurrency"]
    series = [r["round_p50_ms"]]
    labels = [str(burst * (i + 1)) for i in range(len(r["round_p50_ms"]))]
    out = []
    if compare:
        c = compare[0]["round_p50_ms"][: len(labels)]
        series.append(c)
        out.append("Lines: dedup store as shipped (`dedup.db`, rising) and the in-memory dedup store (flat).")
    out += ["", _chart("p50 latency of each burst (ms)", "workflows completed so far", labels, series, "ms"), "",
            "| workflows so far | " + " | ".join(labels) + " |", "|---|" + "---:|" * len(labels),
            "| p50, dedup.db (ms) | " + " | ".join(f"{v:.0f}" for v in series[0]) + " |"]
    if compare:
        out.append("| p50, in-memory dedup (ms) | " + " | ".join(f"{v:.0f}" for v in series[1]) + " |")
    return out


def write_report(results_path: Path, compare_path: Path | None = None) -> Path:
    data = json.loads(results_path.read_text())
    cmp_rows: dict[str, list[dict[str, Any]]] = {}
    if compare_path:
        for r in json.loads(compare_path.read_text())["results"]:
            cmp_rows.setdefault(r["case"]["sweep"], []).append(r)
    m = data["machine"]
    out = [
        "# Benchmark: how scale affects latency",
        "",
        f"Generated from [`{results_path.name}`](results/{results_path.name}) by `bench/report.py`.",
        "",
        f"**Machine:** {m['cpu']} · {m['cores']} cores · {m['os']} · Go {m['go']} · Python {m['python']} · "
        f"plexus-flow `{m['plexus_flow_commit']}` · dedup store: {m.get('dedup_store', 'file (default)')} · {m['date']}",
        "",
        "**What is measured.** End-to-end workflow latency from the engine's own timestamps (created → completed), "
        "with the mock LLM answering instantly and per-step logging off, so the numbers are pure orchestration cost: "
        "Raft commits on two consensus groups, dispatch to gRPC workers, dependency resolution and the approval "
        "signal. Every data point runs on a freshly started single-node engine and worker fleet, after one warm-up "
        "workflow. Each point submits N workflows at once (a burst), waits for all of them, and repeats until at least "
        "30 workflows have run; throughput is runs / time spent inside bursts.",
        "",
        "Charts: lines are p50 (lowest), p95 and p99 (highest).",
    ]
    by_sweep: dict[str, list[dict[str, Any]]] = {}
    for r in data["results"]:
        by_sweep.setdefault(r["case"]["sweep"], []).append(r)

    for sweep, rows in by_sweep.items():
        title, x_title, desc = TITLES[sweep]
        if sweep == "history":
            out += ["", f"## {title}", "", desc, ""] + _history(rows, cmp_rows.get("history"))
            continue
        labels = [_label(r) for r in rows]
        cmp_by_x = {(c["case"]["x"], c["case"]["workload"]): c for c in cmp_rows.get(sweep, [])}
        cmp = [cmp_by_x.get((r["case"]["x"], r["case"]["workload"])) for r in rows]
        has_cmp = all(c is not None for c in cmp) and bool(cmp)
        out += ["", f"## {title}", "", desc, "",
                _chart(f"{title}: latency (ms)", x_title, labels,
                       [[r["p50_ms"] for r in rows], [r["p95_ms"] for r in rows], [r["p99_ms"] for r in rows]], "ms"),
                "",
                _chart(f"{title}: throughput (workflows/s)", x_title, labels,
                       [[r["throughput_wps"] for r in rows]], "workflows/s", bar=True),
                ""]
        if has_cmp:
            out += ["p50 as shipped (higher line) vs. with the in-memory dedup store (lower line):", "",
                    _chart(f"{title}: p50, dedup.db vs in-memory (ms)", x_title, labels,
                           [[r["p50_ms"] for r in rows], [c["p50_ms"] for c in cmp]], "ms"), ""]
        cmp_head = " p50 (mem dedup) | p99 (mem dedup) | wf/s (mem dedup) |" if has_cmp else ""
        out += [f"| {x_title} | runs | p50 | p95 | p99 | max | per step (p50) | workflows/s | steps/s |{cmp_head}",
                "|---:|---:|---:|---:|---:|---:|---:|---:|---:|" + ("---:|" * 3 if has_cmp else "")]
        for r, label, c in zip(rows, labels, cmp):
            note = " ⚠" if r["failed"] or r["timed_out"] else ""
            out.append(f"| {label}{note} | {r['completed'] + r['failed']} | {r['p50_ms']:.1f} ms | {r['p95_ms']:.1f} ms | "
                       f"{r['p99_ms']:.1f} ms | {r['max_ms']:.1f} ms | {r['per_step_ms']:.2f} ms | "
                       f"{r['throughput_wps']:.1f} | {r['steps_per_s']:.0f} |"
                       + (f" {c['p50_ms']:.1f} ms | {c['p99_ms']:.1f} ms | {c['throughput_wps']:.1f} |" if has_cmp else ""))
    out += ["", "⚠ = some runs failed or were unfinished when the per-point time budget (120 s) ran out; "
            "their latency is not included, so the real numbers for that point are worse.", ""]
    path = BENCH / "RESULTS.md"
    path.write_text("\n".join(out))
    return path


if __name__ == "__main__":
    # report.py [results.json] [--compare results-dedup-memory.json]
    args = sys.argv[1:]
    cmp_path = Path(args[args.index("--compare") + 1]) if "--compare" in args else None
    rest = [a for a in args if a != "--compare" and (cmp_path is None or a != str(cmp_path))]
    src = Path(rest[0]) if rest else max(p for p in (BENCH / "results").glob("*.json") if "dedup-memory" not in p.name)
    print(write_report(src, cmp_path))
