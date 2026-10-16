# Scale Benchmark: How Worker Scaling & Load Affect Latency

How many parallel workflows can `plexus-flow` support as you scale out workers without hurting latency, and where does the single-node coordinator become the bottleneck?

```bash
uv run python bench/bench.py                    # full suite (~90s): writes bench/results/*.json|csv, bench/charts/*.svg, bench/RESULTS.md
uv run python bench/bench.py --sweep scaling    # only the 2D (workers × parallel flows) scaling matrix
uv run python bench/bench.py --quick            # quick run (~45s)
uv run python bench/report.py                   # re-render bench/RESULTS.md and bench/charts/*.svg from the latest JSON
```

**→ Full report & charts: [RESULTS.md](RESULTS.md)**

---

## Key Takeaways (Apple M5 Pro, 15 cores, single coordinator node)

### 1. Horizontal Worker Scaling: How Many Parallel Flows Can You Run per Worker?

In the **10-step agent pipeline** (7 Python LLM agent steps with a 3-way parallel fan-out, 2 Go tool steps, 1 approval signal step), each activity step executes **20 ms** of simulated external I/O (`--step-ms 20`) and each worker process runs up to **4 concurrent tasks** (`--worker-concurrency 4`).

- **Single-workflow baseline (1 flow in flight):** **161 ms p50 / 163 ms p95** (`7` sequential step stages × `20 ms` = `140 ms` of step work + `~21 ms` of `plexus-flow` orchestration).
- **Doubling workers linearly doubles parallel flow capacity (`1 → 2 → 4 → 8` workers):**
  Because each workflow executes 9 activity steps across 7 stages (including a 3-way fan-out in stage 2), active tasks begin queueing in `plexus-flow`'s broker (`b.pending`) once concurrent step demand exceeds total worker slots (`workers × 4`). Adding workers shifts the latency wall proportionally to the right:

| Worker Fleet (`py` + `go`) | Concurrent Slots / Kind | Max Parallel Flows at **p95 ≤ 1.5× Baseline (244 ms)** | Max Parallel Flows at **p95 ≤ 2.0× Baseline (325 ms)** | Peak Throughput (`wf/s`) | Peak Step Rate (`steps/s`) | Fleet Speedup |
|---|---:|---:|---:|---:|---:|---:|
| **1 worker** | 4 slots | **5 parallel flows** | **8 parallel flows** | **26.3 wf/s** | 263 steps/s | **1.0×** |
| **2 workers** | 8 slots | **10 parallel flows** | **17 parallel flows** | **51.7 wf/s** | 517 steps/s | **2.0×** |
| **4 workers** | 16 slots | **26 parallel flows** | **40 parallel flows** | **104.5 wf/s** | 1,045 steps/s | **4.0×** |
| **8 workers** | 32 slots | **65 parallel flows** | **98 parallel flows** | **202.6 wf/s** | 2,026 steps/s | **7.7×** |
| **16 workers** | 64 slots | **105 parallel flows** | **138 parallel flows** | **240.3 wf/s** | 2,403 steps/s | **9.1×** |

![Max Parallel Flows Without Degrading Latency](charts/iso_latency_capacity.svg)

![Parallel Flows vs. p95 Latency by Worker Count](charts/scaling_latency_p95.svg)

### 2. Where Adding Workers Stops Helping: The Single-Node Coordinator Ceiling

- In the **pure engine sweep (`0 ms` step duration)**, a single `plexus-flow` coordinator node peaks at **~320–417 workflows/s (~3,200–4,170 steps/s)** (with `3.3 ms` single-workflow orchestration latency, or `0.41 ms/step`).
- In the `20 ms`/step scaling matrix above, **8 workers** reach `2,026 steps/s` (`7.7×` speedup) and **16 workers** reach `2,403 steps/s` (`9.1×` speedup). Beyond ~16 workers (at `20 ms`/step), total step completions approach the coordinator's single-node Raft commit ceiling (~3,200 steps/s), so the bottleneck shifts from the workers to the hub.
- **Sizing rule of thumb for real LLM workloads:**
  If a real LLM step averages `500 ms` (instead of `20 ms`), a 10-step workflow takes `~3.5 s` and generates `~2.85 steps/s`. Because the single-node coordinator can sustain `~3,200 steps/s`, **one `plexus-flow` node can orchestrate ~1,000 concurrent LLM workflows in flight** before the coordinator itself becomes the bottleneck—provided your worker fleet supplies `~1,500` concurrent async task slots.

### 3. Sustained Throughput Over Time: `in-memory dedup` vs. `dedup.db`

- With the **in-memory dedup store**, burst p50 latency stays flat at **~16–29 ms** (`352 workflows/s`) across 800+ back-to-back workflows.
- With the default file-backed **`dedup.db`** (`dedup.FileStore` in `plexus`), every step dispatch rewrites the entire JSON file under a mutex, causing burst p50 latency to drift from **28 ms → 1,006 ms** over 800 workflows.

---

## Sweeps Included

| Sweep | What varies | What is measured |
|---|---|---|
| `scaling` | **2D matrix:** Worker fleet size (`1, 2, 4, 8, 16` workers × 4 slots) × Parallel flows (`1, 4, 8, 16, 32, 64, 128, 256`) at `20 ms`/step | Iso-latency parallel flow capacity, p50/p95 latency curves, and per-fleet throughput plateaus |
| `engine` | Parallel flows (`1 → 500`) with `0 ms` steps | Pure `plexus-flow` orchestration latency (`p50`, `p95`, `p99`) and single-node coordinator ceiling (`steps/s`) |
| `history` | 16 consecutive bursts of 50 workflows (`800` workflows) | Long-running latency stability: `in-memory dedup` vs. file-backed `dedup.db` |
| `pipeline` | Steps per workflow (`10` real agent pipeline, `12, 24, 48, 100` synthetic fork-join DAGs) | How DAG size affects per-step transition overhead |
