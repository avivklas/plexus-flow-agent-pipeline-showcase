"""Render benchmark results as bench/RESULTS.md with labeled SVG charts in bench/charts/*.svg."""

from __future__ import annotations

import html
import json
import math
import sys
from pathlib import Path
from typing import Any

BENCH = Path(__file__).resolve().parent
CHARTS = BENCH / "charts"

# Distinct, accessible color palette
PALETTE = [
    "#dc2626",  # red
    "#ea580c",  # orange
    "#ca8a04",  # amber
    "#2563eb",  # blue
    "#16a34a",  # green
    "#7c3aed",  # purple
]


def _nice_max(val: float) -> float:
    if val <= 0:
        return 10.0
    exp = math.floor(math.log10(val))
    f = val / (10 ** exp)
    for step in (1.0, 1.2, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0):
        if f <= step:
            return step * (10 ** exp)
    return 10.0 * (10 ** exp)


def _fmt_num(v: float, unit: str = "") -> str:
    if v >= 1000:
        s = f"{v:,.0f}"
    elif v >= 100:
        s = f"{v:.0f}"
    elif v >= 10:
        s = f"{v:.1f}"
    else:
        s = f"{v:.2f}"
    return f"{s}{unit}"


def render_line_svg(
    path: Path,
    *,
    title: str,
    subtitle: str,
    x_title: str,
    y_title: str,
    x_labels: list[str],
    series: list[dict[str, Any]],  # [{"name": str, "short": str, "values": list[float], "color": str, "dashed": bool}]
    ref_lines: list[dict[str, Any]] | None = None,  # [{"y": float, "label": str, "color": str}]
    y_unit: str = "",
    y_log: bool = False,
) -> None:
    """Write a self-contained SVG line chart with a top legend AND direct end-of-line labels."""
    CHARTS.mkdir(exist_ok=True)
    width, height = 920, 460
    pad_l, pad_r, pad_t, pad_b = 80, 185, 102, 62
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    all_vals = [v for s in series for v in s["values"] if v is not None and v > 0]
    if ref_lines:
        all_vals.extend(r["y"] for r in ref_lines if r["y"] > 0)

    if y_log and all_vals:
        min_v = min(all_vals)
        max_v = max(all_vals)
        exp_min = math.floor(math.log2(min_v * 0.85))
        exp_max = math.ceil(math.log2(max_v * 1.15))
        if exp_max <= exp_min:
            exp_max = exp_min + 1
        y_min = 2.0 ** exp_min
        y_max = 2.0 ** exp_max
        tick_vals = [2.0 ** e for e in range(exp_min, exp_max + 1)]

        def y_pos(v: float) -> float:
            vc = min(max(v, y_min), y_max)
            frac = (math.log2(vc) - exp_min) / (exp_max - exp_min)
            return pad_t + plot_h - frac * plot_h
    else:
        y_min = 0.0
        y_max = _nice_max(max(all_vals, default=1.0) * 1.08)
        ticks = 5
        tick_vals = [(y_max / ticks) * t for t in range(ticks + 1)]

        def y_pos(v: float) -> float:
            return pad_t + plot_h - (min(max(v, 0.0), y_max) / y_max) * plot_h

    def x_pos(i: int) -> float:
        if len(x_labels) <= 1:
            return pad_l + plot_w / 2
        return pad_l + (i / (len(x_labels) - 1)) * plot_w

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" '
        f'style="font-family: -apple-system, BlinkMacSystemFont, \'Segoe UI\', Roboto, Helvetica, Arial, sans-serif; background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px;">',
        f'<text x="{pad_l}" y="30" font-size="16" font-weight="700" fill="#111827">{html.escape(title)}</text>',
        f'<text x="{pad_l}" y="50" font-size="12" fill="#4b5563">{html.escape(subtitle)}</text>',
    ]

    # Top Legend Box
    leg_x = float(pad_l)
    leg_y = 74
    for s in series:
        name = s["name"]
        color = s["color"]
        dash = ' stroke-dasharray="5,3"' if s.get("dashed") else ""
        svg.append(f'<line x1="{leg_x:.1f}" y1="{leg_y - 4}" x2="{leg_x + 20:.1f}" y2="{leg_y - 4}" stroke="{color}" stroke-width="2.5"{dash}/>')
        svg.append(f'<circle cx="{leg_x + 10:.1f}" cy="{leg_y - 4}" r="3.5" fill="{color}"/>')
        svg.append(f'<text x="{leg_x + 26:.1f}" y="{leg_y}" font-size="12" font-weight="600" fill="#1f2937">{html.escape(name)}</text>')
        leg_x += 30 + len(name) * 6.4

    # Horizontal gridlines & Y ticks
    for idx_t, val in enumerate(tick_vals):
        yp = y_pos(val)
        stroke = "#d1d5db" if idx_t == 0 else "#f3f4f6"
        svg.append(f'<line x1="{pad_l}" y1="{yp:.1f}" x2="{pad_l + plot_w}" y2="{yp:.1f}" stroke="{stroke}" stroke-width="1"/>')
        svg.append(f'<text x="{pad_l - 10}" y="{yp + 4:.1f}" font-size="11" fill="#6b7280" text-anchor="end">{html.escape(_fmt_num(val, y_unit))}</text>')

    # Vertical gridlines & X ticks
    for i, xl in enumerate(x_labels):
        xp = x_pos(i)
        svg.append(f'<line x1="{xp:.1f}" y1="{pad_t}" x2="{xp:.1f}" y2="{pad_t + plot_h}" stroke="#f9fafb" stroke-width="1"/>')
        svg.append(f'<line x1="{xp:.1f}" y1="{pad_t + plot_h}" x2="{xp:.1f}" y2="{pad_t + plot_h + 5}" stroke="#9ca3af" stroke-width="1"/>')
        svg.append(f'<text x="{xp:.1f}" y="{pad_t + plot_h + 20}" font-size="11" font-weight="500" fill="#374151" text-anchor="middle">{html.escape(str(xl))}</text>')

    # Axis titles
    svg.append(f'<text x="{pad_l + plot_w / 2:.1f}" y="{height - 14}" font-size="12" font-weight="600" fill="#374151" text-anchor="middle">{html.escape(x_title)}</text>')
    svg.append(f'<text x="18" y="{pad_t + plot_h / 2:.1f}" font-size="12" font-weight="600" fill="#374151" text-anchor="middle" transform="rotate(-90 18 {pad_t + plot_h / 2:.1f})">{html.escape(y_title)}</text>')

    # Reference lines (e.g. SLA thresholds or ceilings)
    for r_idx, r in enumerate(ref_lines or []):
        if y_min <= r["y"] <= y_max:
            ry = y_pos(r["y"])
            rc = r.get("color", "#059669")
            lx_off = pad_l + 8 + (r_idx * 195)
            svg.append(f'<line x1="{pad_l}" y1="{ry:.1f}" x2="{pad_l + plot_w}" y2="{ry:.1f}" stroke="{rc}" stroke-width="1.5" stroke-dasharray="6,4"/>')
            svg.append(f'<text x="{lx_off}" y="{ry - 5:.1f}" font-size="11" font-weight="600" fill="{rc}">{html.escape(r["label"])}</text>')

    # Series polylines & markers + collect right-edge labels
    end_labels: list[dict[str, Any]] = []
    for s in series:
        pts: list[tuple[float, float, float]] = []
        for i, v in enumerate(s["values"]):
            if v is not None:
                pts.append((x_pos(i), y_pos(v), v))
        if not pts:
            continue
        color = s["color"]
        dash = ' stroke-dasharray="6,4"' if s.get("dashed") else ""
        poly = " ".join(f"{px:.1f},{py:.1f}" for px, py, _ in pts)
        svg.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.5"{dash} points="{poly}"/>')
        for px, py, _ in pts:
            svg.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="4" fill="#ffffff" stroke="{color}" stroke-width="2.2"/>')
        last_x, last_y, last_v = pts[-1]
        short = s.get("short") or s["name"]
        end_labels.append({
            "y": last_y,
            "orig_y": last_y,
            "x": last_x,
            "color": color,
            "text": f"{short}: {_fmt_num(last_v, y_unit)}",
        })

    # De-overlap right-edge direct labels so every line is readable
    end_labels.sort(key=lambda d: d["y"])
    min_gap = 16.0
    for i in range(1, len(end_labels)):
        if end_labels[i]["y"] - end_labels[i - 1]["y"] < min_gap:
            end_labels[i]["y"] = end_labels[i - 1]["y"] + min_gap
    if end_labels and end_labels[-1]["y"] > pad_t + plot_h:
        shift = end_labels[-1]["y"] - (pad_t + plot_h)
        for d in end_labels:
            d["y"] -= shift
    for d in end_labels:
        lx = pad_l + plot_w + 10
        svg.append(f'<line x1="{d["x"] + 5:.1f}" y1="{d["orig_y"]:.1f}" x2="{lx - 3:.1f}" y2="{d["y"]:.1f}" stroke="{d["color"]}" stroke-width="1" stroke-opacity="0.6"/>')
        svg.append(f'<text x="{lx:.1f}" y="{d["y"] + 4:.1f}" font-size="11.5" font-weight="600" fill="{d["color"]}">{html.escape(d["text"])}</text>')

    svg.append("</svg>")
    path.write_text("\n".join(svg))


def render_grouped_bar_svg(
    path: Path,
    *,
    title: str,
    subtitle: str,
    x_title: str,
    y_title: str,
    x_labels: list[str],
    groups: list[dict[str, Any]],  # [{"name": str, "values": list[float], "color": str}]
    y_unit: str = "",
) -> None:
    """Write a grouped bar chart SVG with value labels on top of every bar and a top legend."""
    CHARTS.mkdir(exist_ok=True)
    width, height = 920, 420
    pad_l, pad_r, pad_t, pad_b = 80, 40, 100, 62
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    all_vals = [v for g in groups for v in g["values"]]
    y_max = _nice_max(max(all_vals, default=1.0) * 1.15)

    def y_pos(v: float) -> float:
        return pad_t + plot_h - (min(max(v, 0.0), y_max) / y_max) * plot_h

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" '
        f'style="font-family: -apple-system, BlinkMacSystemFont, \'Segoe UI\', Roboto, Helvetica, Arial, sans-serif; background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px;">',
        f'<text x="{pad_l}" y="30" font-size="16" font-weight="700" fill="#111827">{html.escape(title)}</text>',
        f'<text x="{pad_l}" y="50" font-size="12" fill="#4b5563">{html.escape(subtitle)}</text>',
    ]

    # Top Legend
    leg_x = pad_l
    leg_y = 74
    for g in groups:
        svg.append(f'<rect x="{leg_x}" y="{leg_y - 11}" width="14" height="14" rx="3" fill="{g["color"]}"/>')
        svg.append(f'<text x="{leg_x + 20}" y="{leg_y}" font-size="12" font-weight="600" fill="#1f2937">{html.escape(g["name"])}</text>')
        leg_x += 34 + len(g["name"]) * 7.0

    # Gridlines
    ticks = 5
    for t in range(ticks + 1):
        val = (y_max / ticks) * t
        yp = y_pos(val)
        stroke = "#d1d5db" if t == 0 else "#f3f4f6"
        svg.append(f'<line x1="{pad_l}" y1="{yp:.1f}" x2="{pad_l + plot_w}" y2="{yp:.1f}" stroke="{stroke}" stroke-width="1"/>')
        svg.append(f'<text x="{pad_l - 10}" y="{yp + 4:.1f}" font-size="11" fill="#6b7280" text-anchor="end">{html.escape(_fmt_num(val, y_unit))}</text>')

    n_cats = max(len(x_labels), 1)
    n_grp = max(len(groups), 1)
    band_w = plot_w / n_cats
    bar_w = min(46.0, (band_w * 0.68) / n_grp)

    for i, xl in enumerate(x_labels):
        cx = pad_l + (i + 0.5) * band_w
        start_x = cx - (n_grp * bar_w) / 2
        svg.append(f'<text x="{cx:.1f}" y="{pad_t + plot_h + 22}" font-size="12" font-weight="600" fill="#374151" text-anchor="middle">{html.escape(xl)}</text>')
        for g_idx, g in enumerate(groups):
            v = g["values"][i]
            bx = start_x + g_idx * bar_w + 2
            by = y_pos(v)
            bh = max(2.0, pad_t + plot_h - by)
            svg.append(f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w - 4:.1f}" height="{bh:.1f}" rx="3" fill="{g["color"]}"/>')
            svg.append(f'<text x="{bx + (bar_w - 4) / 2:.1f}" y="{by - 6:.1f}" font-size="11" font-weight="700" fill="{g["color"]}" text-anchor="middle">{html.escape(_fmt_num(v, y_unit))}</text>')

    svg.append(f'<text x="{pad_l + plot_w / 2:.1f}" y="{height - 14}" font-size="12" font-weight="600" fill="#374151" text-anchor="middle">{html.escape(x_title)}</text>')
    svg.append(f'<text x="18" y="{pad_t + plot_h / 2:.1f}" font-size="12" font-weight="600" fill="#374151" text-anchor="middle" transform="rotate(-90 18 {pad_t + plot_h / 2:.1f})">{html.escape(y_title)}</text>')
    svg.append("</svg>")
    path.write_text("\n".join(svg))


def _interp_max_flows(xs: list[int], ys: list[float], limit_ms: float) -> float:
    """Interpolate the maximum parallel flows X where latency Y stays <= limit_ms."""
    if not xs or ys[0] > limit_ms:
        return 1.0
    best = float(xs[0])
    for i in range(len(xs) - 1):
        x0, x1 = float(xs[i]), float(xs[i + 1])
        y0, y1 = ys[i], ys[i + 1]
        if y1 <= limit_ms:
            best = x1
        elif y0 <= limit_ms < y1 and y1 > y0:
            frac = (limit_ms - y0) / (y1 - y0)
            return round(x0 + frac * (x1 - x0), 1)
    return round(best, 1)


def write_report(results_path: Path) -> Path:
    data = json.loads(results_path.read_text())
    m = data["machine"]
    step_ms = float(m.get("scaling_step_ms", 50))
    slots_per_w = int(m.get("scaling_worker_concurrency", 128))

    by_sweep: dict[str, list[dict[str, Any]]] = {}
    for r in data["results"]:
        by_sweep.setdefault(r["case"]["sweep"], []).append(r)

    out: list[str] = [
        "# Benchmark: How Worker Slots, Fleet Size & Load Affect Workflow Latency",
        "",
        f"Generated from [`{results_path.name}`](results/{results_path.name}) by `bench/report.py`.",
        "",
        f"**Machine:** {m['cpu']} · {m['cores']} cores · {m['os']} · Go {m['go']} · Python {m['python']} · "
        f"plexus-flow `{m['plexus_flow_commit']}` · {m['date']}",
        "",
        "## What is a \"Slot\" (`--max-concurrency`)?",
        "",
        "When a worker connects to `plexus-flow` over gRPC (`WorkerService.Connect`), it registers with a `max_concurrency` limit—its **slots**:",
        "- **In the Go worker (`pkg/workerclient`):** Each slot is a concurrent **goroutine** executing an activity handler (`MaxConcurrency`).",
        "- **In the Python worker (`plexus_flow.Worker`):** Each slot is a concurrent **`asyncio.Task`** on the event loop (`max_concurrency`).",
        "- **In the `plexus-flow` Broker (`pkg/remote/broker.go`):** The broker tracks `len(session.inflight) < session.max` per connected worker. As long as a worker has free slots, the broker streams tasks to it immediately; once all slots across all workers are full, new tasks wait in `broker.pending[activity]`.",
        "",
    ]

    # -----------------------------------------------------------------------
    # 1. Scaling Slots Within a Single Worker (8 .. 128 slots/worker)
    # -----------------------------------------------------------------------
    if "slots" in by_sweep:
        rows = by_sweep["slots"]
        concurrencies = sorted({r["case"]["concurrency"] for r in rows})
        slot_counts = sorted({r["case"]["worker_concurrency"] for r in rows})
        by_kc: dict[tuple[int, int], dict[str, Any]] = {
            (r["case"]["worker_concurrency"], r["case"]["concurrency"]): r for r in rows
        }

        c1_p95 = [by_kc[(k, concurrencies[0])]["p95_ms"] for k in slot_counts if (k, concurrencies[0]) in by_kc]
        base_p95 = sorted(c1_p95)[len(c1_p95) // 2] if c1_p95 else (7 * step_ms + 15.0)
        sla_15x = round(base_p95 * 1.5, 1)
        sla_20x = round(base_p95 * 2.0, 1)

        slot_p95_series: list[dict[str, Any]] = []
        slot_tput_series: list[dict[str, Any]] = []
        slot_cap_15x: list[float] = []
        slot_cap_20x: list[float] = []

        for idx, k in enumerate(slot_counts):
            color = PALETTE[idx % len(PALETTE)]
            cs = [c for c in concurrencies if (k, c) in by_kc]
            p95s = [by_kc[(k, c)]["p95_ms"] for c in cs]
            tputs = [by_kc[(k, c)]["throughput_wps"] for c in cs]
            slot_p95_series.append({"name": f"{k} slots/worker (1w)", "short": f"{k} slots", "values": p95s, "color": color})
            slot_tput_series.append({"name": f"{k} slots/worker (1w)", "short": f"{k} slots", "values": tputs, "color": color})
            slot_cap_15x.append(_interp_max_flows(cs, p95s, sla_15x))
            slot_cap_20x.append(_interp_max_flows(cs, p95s, sla_20x))

        render_line_svg(
            CHARTS / "slots_latency_p95.svg",
            title=f"Single Worker: Slots per Worker (8 → 128) vs. p95 Latency ({step_ms:g} ms/step)",
            subtitle="1 Python + 1 Go worker process. Increasing max_concurrency (goroutines / asyncio tasks) up to 128 slots.",
            x_title="Parallel workflows in flight",
            y_title="Workflow latency p95 (ms, log scale)",
            x_labels=[str(c) for c in concurrencies],
            series=slot_p95_series,
            ref_lines=[
                {"y": sla_15x, "label": f"1.5× baseline SLA ({sla_15x:.0f} ms)", "color": "#059669"},
                {"y": sla_20x, "label": f"2.0× baseline SLA ({sla_20x:.0f} ms)", "color": "#d97706"},
            ],
            y_unit=" ms",
            y_log=True,
        )

        out += [
            "## 1. Scaling Slots Within a Single Worker (`8 → 32 → 64 → 128` Slots/Worker)",
            "",
            f"Because LLM and tool steps are I/O-bound (`{step_ms:g} ms` per step), a single worker process can hold many concurrent "
            "tasks in flight at once (goroutines in Go, `asyncio.Task`s in Python). Increasing `--max-concurrency` from `8` to `128` "
            "slots on **just 1 worker process per kind** dramatically increases how many parallel workflows it can handle before queueing:",
            "",
            "![Single Worker: Slots per Worker vs p95 Latency](charts/slots_latency_p95.svg)",
            "",
            f"| Slots per worker (1 process/kind) | Max parallel flows (**p95 ≤ 1.5× / {sla_15x:.0f} ms**) | Max parallel flows (**p95 ≤ 2.0× / {sla_20x:.0f} ms**) | Peak throughput | Peak step rate |",
            "|---|---:|---:|---:|---:|",
        ]
        for idx, k in enumerate(slot_counts):
            peak_wf = max(by_kc[(k, c)]["throughput_wps"] for c in concurrencies if (k, c) in by_kc)
            peak_st = max(by_kc[(k, c)]["steps_per_s"] for c in concurrencies if (k, c) in by_kc)
            out.append(
                f"| **{k} slots/worker** | **{slot_cap_15x[idx]:.0f} parallel flows** | "
                f"**{slot_cap_20x[idx]:.0f} parallel flows** | **{peak_wf:.1f} wf/s** | {peak_st:,.0f} steps/s |"
            )
        out.append("")

    # -----------------------------------------------------------------------
    # 2. Worker Fleet Scaling Matrix at 128 Slots/Worker (1..8 workers = 128..1,024 slots)
    # -----------------------------------------------------------------------
    if "scaling" in by_sweep:
        rows = by_sweep["scaling"]
        concurrencies = sorted({r["case"]["concurrency"] for r in rows})
        worker_counts = sorted({r["case"]["py_workers"] for r in rows})
        by_wc: dict[tuple[int, int], dict[str, Any]] = {
            (r["case"]["py_workers"], r["case"]["concurrency"]): r for r in rows
        }

        c1_p50 = [by_wc[(w, concurrencies[0])]["p50_ms"] for w in worker_counts if (w, concurrencies[0]) in by_wc]
        c1_p95 = [by_wc[(w, concurrencies[0])]["p95_ms"] for w in worker_counts if (w, concurrencies[0]) in by_wc]
        baseline_ms = sorted(c1_p50)[len(c1_p50) // 2] if c1_p50 else (7 * step_ms + 8.0)
        baseline_p95 = sorted(c1_p95)[len(c1_p95) // 2] if c1_p95 else baseline_ms
        sla_15x = round(baseline_p95 * 1.5, 1)
        sla_20x = round(baseline_p95 * 2.0, 1)

        p50_series: list[dict[str, Any]] = []
        p95_series: list[dict[str, Any]] = []
        tput_series: list[dict[str, Any]] = []
        cap_15x: list[float] = []
        cap_20x: list[float] = []

        for idx, w in enumerate(worker_counts):
            slots = w * slots_per_w
            color = PALETTE[idx % len(PALETTE)]
            cs = [c for c in concurrencies if (w, c) in by_wc]
            p50s = [by_wc[(w, c)]["p50_ms"] for c in cs]
            p95s = [by_wc[(w, c)]["p95_ms"] for c in cs]
            tputs = [by_wc[(w, c)]["throughput_wps"] for c in cs]
            label = f"{w} worker{'s' if w > 1 else ''} ({slots} slots)"
            short = f"{w}w ({slots}s)"
            p50_series.append({"name": label, "short": short, "values": p50s, "color": color})
            p95_series.append({"name": label, "short": short, "values": p95s, "color": color})
            tput_series.append({"name": label, "short": short, "values": tputs, "color": color})
            cap_15x.append(_interp_max_flows(cs, p95s, sla_15x))
            cap_20x.append(_interp_max_flows(cs, p95s, sla_20x))

        render_line_svg(
            CHARTS / "scaling_latency_p95.svg",
            title=f"Parallel Flows (1 → {concurrencies[-1]}) vs. p95 Latency at {slots_per_w} Slots/Worker ({step_ms:g} ms/step)",
            subtitle=f"Log2 latency scale · Each worker process runs up to {slots_per_w} concurrent tasks (128 → {worker_counts[-1] * slots_per_w} total slots).",
            x_title="Parallel workflows in flight (submitted concurrently)",
            y_title="Workflow latency p95 (ms, log scale)",
            x_labels=[str(c) for c in concurrencies],
            series=p95_series,
            ref_lines=[
                {"y": sla_15x, "label": f"1.5× baseline SLA ({sla_15x:.0f} ms)", "color": "#059669"},
                {"y": sla_20x, "label": f"2.0× baseline SLA ({sla_20x:.0f} ms)", "color": "#d97706"},
            ],
            y_unit=" ms",
            y_log=True,
        )

        render_line_svg(
            CHARTS / "scaling_latency_p50.svg",
            title=f"Parallel Flows (1 → {concurrencies[-1]}) vs. Median Latency (p50) at {slots_per_w} Slots/Worker ({step_ms:g} ms/step)",
            subtitle=f"Log2 latency scale · Each line is a worker fleet size ({slots_per_w} slots/worker).",
            x_title="Parallel workflows in flight (submitted concurrently)",
            y_title="Workflow latency p50 (ms, log scale)",
            x_labels=[str(c) for c in concurrencies],
            series=p50_series,
            ref_lines=[
                {"y": sla_15x, "label": f"1.5× baseline SLA ({sla_15x:.0f} ms)", "color": "#059669"},
                {"y": sla_20x, "label": f"2.0× baseline SLA ({sla_20x:.0f} ms)", "color": "#d97706"},
            ],
            y_unit=" ms",
            y_log=True,
        )

        render_grouped_bar_svg(
            CHARTS / "iso_latency_capacity.svg",
            title=f"How Many Parallel Flows Can You Run at {slots_per_w} Slots/Worker Without Hurting Latency?",
            subtitle=f"Max parallel workflows supported while keeping p95 latency ≤ 1.5× ({sla_15x:.0f} ms) or ≤ 2.0× ({sla_20x:.0f} ms) of single-flow baseline ({baseline_p95:.0f} ms).",
            x_title=f"Worker fleet size ({slots_per_w} slots per worker process)",
            y_title="Max parallel workflows supported",
            x_labels=[f"{w} worker{'s' if w > 1 else ''} ({w * slots_per_w} slots)" for w in worker_counts],
            groups=[
                {"name": f"Strict SLA: p95 ≤ 1.5× baseline ({sla_15x:.0f} ms)", "values": cap_15x, "color": "#16a34a"},
                {"name": f"Relaxed SLA: p95 ≤ 2.0× baseline ({sla_20x:.0f} ms)", "values": cap_20x, "color": "#2563eb"},
            ],
            y_unit=" wf",
        )

        render_line_svg(
            CHARTS / "scaling_throughput.svg",
            title=f"Parallel Flows vs. Throughput (workflows/s) at {slots_per_w} Slots/Worker ({step_ms:g} ms/step)",
            subtitle="Throughput rises with parallel flows until hitting either the worker fleet slot cap or the coordinator ceiling.",
            x_title="Parallel workflows in flight",
            y_title="Throughput (completed workflows / sec)",
            x_labels=[str(c) for c in concurrencies],
            series=tput_series,
            y_unit=" wf/s",
        )

        base_peak_wf = max(by_wc[(worker_counts[0], c)]["throughput_wps"] for c in concurrencies if (worker_counts[0], c) in by_wc)

        out += [
            f"## 2. Scaling Worker Fleet at `{slots_per_w}` Slots/Worker (Up to `{concurrencies[-1]:,}` Parallel Workflows)",
            "",
            f"**Workload:** Real 10-step agent pipeline (`{step_ms:g} ms` simulated I/O per step, **{slots_per_w} concurrent slots per worker process**). "
            f"Single-workflow baseline latency is **{baseline_ms:.0f} ms p50 / {baseline_p95:.0f} ms p95** (`7 × {step_ms:g} ms = {7 * step_ms:.0f} ms` of step work).",
            "",
            "![Max Parallel Flows Without Degrading Latency](charts/iso_latency_capacity.svg)",
            "",
            "![Parallel Flows vs. p95 Latency by Worker Count](charts/scaling_latency_p95.svg)",
            "",
            "![Parallel Flows vs. p50 Latency by Worker Count](charts/scaling_latency_p50.svg)",
            "",
            "![Parallel Flows vs. Throughput by Worker Count](charts/scaling_throughput.svg)",
            "",
            f"### Capacity Sizing Table (`{slots_per_w}` Slots/Worker, `{step_ms:g} ms`/Step)",
            "",
            f"| Worker fleet (`py` + `go`) | Total slots/kind | Max parallel flows (**p95 ≤ 1.5× / {sla_15x:.0f} ms**) | Max parallel flows (**p95 ≤ 2.0× / {sla_20x:.0f} ms**) | Peak throughput | Peak step rate | Fleet speedup |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
        for idx, w in enumerate(worker_counts):
            slots = w * slots_per_w
            peak_wf = max(by_wc[(w, c)]["throughput_wps"] for c in concurrencies if (w, c) in by_wc)
            peak_st = max(by_wc[(w, c)]["steps_per_s"] for c in concurrencies if (w, c) in by_wc)
            speedup = peak_wf / base_peak_wf if base_peak_wf > 0 else 1.0
            out.append(
                f"| **{w} worker{'s' if w > 1 else ''}** | {slots:,} slots | **{cap_15x[idx]:.0f} parallel flows** | "
                f"**{cap_20x[idx]:.0f} parallel flows** | **{peak_wf:.1f} wf/s** | {peak_st:,.0f} steps/s | **{speedup:.1f}×** |"
            )

        out += [
            "",
            f"### Full p95 Latency Matrix (ms) — Parallel Flows × Worker Fleet Size (`{slots_per_w}` Slots/Worker)",
            "",
            "| Parallel workflows in flight | " + " | ".join(f"**{w} worker{'s' if w > 1 else ''}** ({w * slots_per_w:,} slots)" for w in worker_counts) + " |",
            "|---:|" + "---:|" * len(worker_counts),
        ]
        for c in concurrencies:
            cells = []
            for w in worker_counts:
                r = by_wc.get((w, c))
                if not r:
                    cells.append("—")
                else:
                    v = r["p95_ms"]
                    mark = "✅" if v <= sla_15x else ("⚠️" if v <= sla_20x else "⏳")
                    cells.append(f"{v:.0f} ms {mark}")
            out.append(f"| **{c:,}** | " + " | ".join(cells) + " |")
        out.append("")
        out.append(f"*Legend: ✅ within 1.5× baseline p95 (≤ {sla_15x:.0f} ms) · ⚠️ within 2.0× baseline p95 (≤ {sla_20x:.0f} ms) · ⏳ queueing regime.*")
        out.append("")

    # -----------------------------------------------------------------------
    # 3. Pure Engine Ceiling (0 ms steps)
    # -----------------------------------------------------------------------
    if "engine" in by_sweep:
        rows = by_sweep["engine"]
        xs = [str(r["case"]["x"]) for r in rows]
        render_line_svg(
            CHARTS / "engine_ceiling.svg",
            title="Pure Engine Orchestration Latency vs. Parallel Flows (0 ms step work, 128 slots/worker)",
            subtitle="Workers return immediately (0 ms), isolating the plexus-flow coordinator's Raft commit + gRPC dispatch cost.",
            x_title="Parallel workflows in flight",
            y_title="Orchestration latency (ms)",
            x_labels=xs,
            series=[
                {"name": "p50 (median latency)", "short": "p50", "values": [r["p50_ms"] for r in rows], "color": "#16a34a"},
                {"name": "p95 latency", "short": "p95", "values": [r["p95_ms"] for r in rows], "color": "#ea580c"},
                {"name": "p99 (tail latency)", "short": "p99", "values": [r["p99_ms"] for r in rows], "color": "#dc2626"},
            ],
            y_unit=" ms",
        )
        peak_wps = max(r["throughput_wps"] for r in rows)
        peak_sps = max(r["steps_per_s"] for r in rows)
        out += [
            "## 3. Single-Node Engine Ceiling (0 ms Steps)",
            "",
            "When workers execute in **0 ms**, all measured latency is pure `plexus-flow` orchestration overhead "
            "(two Raft consensus groups, `flowstore` state transitions, gRPC task dispatch, and SSE completion notifications). "
            f"A single coordinator node peaks at **{peak_wps:.0f} workflows/s ({peak_sps:,.0f} steps/s)**. "
            f"Once your worker fleet produces more than ~{int(round(peak_sps, -3)):,} step completions per second, the coordinator itself becomes the bottleneck.",
            "",
            "![Pure Engine Ceiling](charts/engine_ceiling.svg)",
            "",
            "| Parallel workflows in flight | Runs | p50 (median) | p95 | p99 (tail) | Max | Per step (p50) | Throughput (wf/s) | Step rate (steps/s) |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for r in rows:
            c = r["case"]
            out.append(
                f"| **{c['x']:,}** | {r['completed']} | {r['p50_ms']:.1f} ms | {r['p95_ms']:.1f} ms | "
                f"{r['p99_ms']:.1f} ms | {r['max_ms']:.1f} ms | {r['per_step_ms']:.2f} ms | "
                f"**{r['throughput_wps']:.1f} wf/s** | **{r['steps_per_s']:,.0f} steps/s** |"
            )
        out.append("")

    # -----------------------------------------------------------------------
    # 4. Sustained History: in-memory dedup vs. file-backed dedup.db
    # -----------------------------------------------------------------------
    if "history" in by_sweep:
        rows = by_sweep["history"]
        n_rounds = max(len(r["round_p50_ms"]) for r in rows)
        burst = rows[0]["case"]["concurrency"]
        x_labels = [str(burst * (i + 1)) for i in range(n_rounds)]
        hist_series = []
        for r in rows:
            is_mem = r["case"]["dedup_memory"]
            hist_series.append({
                "name": r["case"]["series"],
                "short": "in-memory" if is_mem else "dedup.db",
                "values": r["round_p50_ms"],
                "color": "#16a34a" if is_mem else "#dc2626",
            })
        render_line_svg(
            CHARTS / "history_dedup.svg",
            title="Sustained Bursts Over Time: In-Memory Dedup vs. File-Backed dedup.db",
            subtitle=f"Back-to-back bursts of {burst} workflows on the same engine. File-backed dedup.db rewrites the whole file on every step.",
            x_title="Cumulative workflows completed on the engine",
            y_title="Burst p50 latency (ms)",
            x_labels=x_labels,
            series=hist_series,
            y_unit=" ms",
        )
        out += [
            "## 4. Sustained Throughput Over Time (`dedup.db` vs. In-Memory Dedup)",
            "",
            "Running identical bursts of 50 workflows back-to-back on a single engine isolates how state accumulation affects latency:",
            "- **`in-memory dedup` (green line):** p50 latency stays completely flat (~65–75 ms at `50` concurrent workflows) across 800 cumulative workflows.",
            "- **`dedup.db (file store)` (red line):** `plexus`'s default `dedup.FileStore` serializes and rewrites its entire JSON snapshot on every dispatched step, causing latency to grow linearly with total historical steps executed.",
            "",
            "![Sustained History: In-Memory vs dedup.db](charts/history_dedup.svg)",
            "",
            "| Dedup store | Cumulative workflows | First burst p50 | Final burst p50 | Overall p50 | Overall p99 | Avg throughput |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
        for r in rows:
            rp = r["round_p50_ms"]
            first_p50 = rp[0] if rp else 0.0
            last_p50 = rp[-1] if rp else 0.0
            out.append(
                f"| **{r['case']['series']}** | {r['completed']} | {first_p50:.1f} ms | {last_p50:.1f} ms | "
                f"{r['p50_ms']:.1f} ms | {r['p99_ms']:.1f} ms | {r['throughput_wps']:.1f} wf/s |"
            )
        out.append("")

    # -----------------------------------------------------------------------
    # 5. Pipeline DAG Size (steps per workflow)
    # -----------------------------------------------------------------------
    if "pipeline" in by_sweep:
        rows = by_sweep["pipeline"]
        xs = [f"{r['case']['x']}*" if r["case"]["workload"] == "agent" else str(r["case"]["x"]) for r in rows]
        render_line_svg(
            CHARTS / "pipeline_dag.svg",
            title="Workflow DAG Size vs. Latency (10 Parallel Workflows, 0 ms Steps)",
            subtitle="10* is the real 10-step agent pipeline; 12–100 are synthetic fork-join DAGs (1 step fans out to 3, repeated).",
            x_title="Total steps per workflow",
            y_title="Workflow latency (ms)",
            x_labels=xs,
            series=[
                {"name": "p50 (median latency)", "short": "p50", "values": [r["p50_ms"] for r in rows], "color": "#2563eb"},
                {"name": "p95 latency", "short": "p95", "values": [r["p95_ms"] for r in rows], "color": "#ea580c"},
                {"name": "p99 (tail latency)", "short": "p99", "values": [r["p99_ms"] for r in rows], "color": "#dc2626"},
            ],
            y_unit=" ms",
        )
        out += [
            "## 5. Pipeline DAG Size (Steps per Workflow)",
            "",
            "How workflow complexity affects orchestration cost at 10 concurrent workflows in flight (`10*` is the real agent pipeline; `12`–`100` are synthetic 1→3 fork-join DAGs):",
            "",
            "![Pipeline DAG Size vs Latency](charts/pipeline_dag.svg)",
            "",
            "| Steps per workflow | Critical path | Runs | p50 | p95 | p99 | Per step (p50) | Throughput (wf/s) | Step rate (steps/s) |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for r, label in zip(rows, xs):
            c = r["case"]
            out.append(
                f"| **{label}** | {c['critical_path']} stages | {r['completed']} | {r['p50_ms']:.1f} ms | "
                f"{r['p95_ms']:.1f} ms | {r['p99_ms']:.1f} ms | {r['per_step_ms']:.2f} ms | "
                f"{r['throughput_wps']:.1f} wf/s | {r['steps_per_s']:,.0f} steps/s |"
            )
        out.append("")

    path = BENCH / "RESULTS.md"
    path.write_text("\n".join(out))
    return path


if __name__ == "__main__":
    args = sys.argv[1:]
    src = Path(args[0]) if args else max((BENCH / "results").glob("*.json"))
    print(write_report(src))
