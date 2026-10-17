# Scale Benchmark: How Worker Slots, Fleet Size & Load Affect Latency

How many parallel workflows can `plexus-flow` support as you scale out **slots per worker** (`--max-concurrency`) and **worker processes** without hurting latency, and where does the single-node coordinator become the bottleneck?

```bash
uv run python bench/bench.py                          # full 5-sweep suite: writes bench/results/*.json|csv, bench/charts/*.svg, bench/RESULTS.md
uv run python bench/bench.py --sweep slots,scaling    # slot scaling (8..128) & worker fleet scaling (128..1,024 slots)
uv run python bench/bench.py --quick                  # quick run (~60s)
uv run python bench/report.py                         # re-render bench/RESULTS.md and bench/charts/*.svg from the latest JSON
```

**→ Full report & charts: [RESULTS.md](RESULTS.md)**

---

## What is a "Slot" (`--max-concurrency`)?

When a worker connects to `plexus-flow` over its persistent gRPC stream (`WorkerService.Connect`), it declares `max_concurrency`—its **slots**:
- **In the Go worker (`pkg/workerclient`):** Each slot is a concurrent **goroutine** (`go func(a *pb.WorkAssignment) { ... }`) spawned per incoming task.
- **In the Python worker (`plexus_flow.Worker`):** Each slot is a concurrent **`asyncio.Task`** running on the worker's event loop.
- **In the `plexus-flow` Broker (`pkg/remote/broker.go`):** `pickLocked` selects the connected worker session with the lowest `len(session.inflight)` that still satisfies `len(session.inflight) < session.max`. Once all slots across all workers are busy, ready steps wait in `broker.pending[activity]`.

---

## Key Takeaways (Apple M5 Pro, 15 cores, single coordinator node)

### 1. Scaling Slots Within a Single Worker (`8 → 32 → 64 → 128` Slots/Worker)

Because LLM and external tool steps are I/O-bound (`150 ms` simulated I/O per step, `1,071 ms` baseline workflow duration across 7 serial stages), increasing `--max-concurrency` from `8` to `128` slots on **a single worker process** (1 Python + 1 Go process) scales parallel workflow capacity by **20×** before you even add a second worker process:

![Single Worker: Slots per Worker vs p95 Latency](charts/slots_latency_p95.svg)

| Slots per Worker (`1` process/kind) | Max Parallel Flows at **p95 ≤ 1.5× Baseline (1,609 ms)** | Max Parallel Flows at **p95 ≤ 2.0× Baseline (2,145 ms)** | Peak Throughput (`wf/s`) | Peak Step Rate (`steps/s`) |
|---|---:|---:|---:|---:|
| **8 slots/worker** | **7 parallel flows** | **13 parallel flows** | **7.5 wf/s** | 75 steps/s |
| **32 slots/worker** | **35 parallel flows** | **54 parallel flows** | **29.7 wf/s** | 297 steps/s |
| **64 slots/worker** | **70 parallel flows** | **108 parallel flows** | **58.9 wf/s** | 589 steps/s |
| **128 slots/worker** | **140 parallel flows** | **206 parallel flows** | **108.7 wf/s** | 1,087 steps/s |

### 2. Horizontal Worker Scaling at `128` Slots/Worker (Up to `1,024` Parallel Workflows)

Once a single 128-slot worker saturates (~141 parallel workflows at `150 ms`/step), scaling horizontally from **1 → 2 → 4 → 8 workers per kind** (`128 → 256 → 512 → 1,024` total slots) scales parallel workflow capacity all the way to **1,024 parallel workflows within the strict 1.5× p95 SLA (`1,474 ms` p95 at 1,024 flows)**:

| Worker Fleet (`py` + `go`) | Total Slots / Kind | Max Parallel Flows at **p95 ≤ 1.5× Baseline (1,612 ms)** | Max Parallel Flows at **p95 ≤ 2.0× Baseline (2,150 ms)** | Peak Throughput (`wf/s`) | Peak Step Rate (`steps/s`) | Fleet Speedup |
|---|---:|---:|---:|---:|---:|---:|
| **1 worker** | 128 slots | **141 parallel flows** | **218 parallel flows** | **117.4 wf/s** | 1,174 steps/s | **1.0×** |
| **2 workers** | 256 slots | **281 parallel flows** | **412 parallel flows** | **221.7 wf/s** | 2,217 steps/s | **1.9×** |
| **4 workers** | 512 slots | **578 parallel flows** | **874 parallel flows** | **391.5 wf/s** | 3,915 steps/s | **3.3×** |
| **8 workers** | 1,024 slots | **1,024 parallel flows** | **1,024 parallel flows** | **646.4 wf/s** | 6,464 steps/s | **5.5×** |

![Max Parallel Flows Without Degrading Latency](charts/iso_latency_capacity.svg)

![Parallel Flows vs. p95 Latency by Worker Count](charts/scaling_latency_p95.svg)

### 3. Where Adding Workers Stops Helping: The Single-Node Coordinator Ceiling

- With **single-commit atomic step advancement** (`SetAutoAdvance`), **version-gated fork-join dispatch**, and **automatic `ActivityStore` pruning**, a single `plexus-flow` coordinator node peaks at **1,058.5 workflows/s (10,585 steps/s)** in the pure engine sweep (`0 ms` step duration, `128` slots/worker), with a **2.7 ms** single-workflow orchestration latency (`0.33 ms/step`).
- At `8 workers × 128 slots` (`1,024` slots/kind), the `150 ms`/step workload reaches **646.4 wf/s (`6,464 steps/s`)** and completes `1,024` concurrent workflows in `1,474 ms` p95 (✅ within 1.5× of the single-workflow baseline). Beyond ~11,000 steps/s, horizontal consensus scaling via `graphflow.ShardedCoordinator` (Multi-Raft) partitions workflows across $N$ independent Raft Graph shards.

### 4. Sustained Throughput Over Time: `in-memory dedup` vs. `dedup.db`

- With the **in-memory dedup store** and automatic `ActivityStore` pruning, burst p50 latency stays flat at **~40–47 ms** (`41.1 ms` overall p50, **`1,084.9 workflows/s`**, **`10,849 steps/s`**) across 800+ back-to-back workflows.
- With the default file-backed **`dedup.db`** (`dedup.FileStore` in `plexus`), every step dispatch rewrites the entire JSON file under a mutex, causing burst p50 latency to drift from **155 ms → 1,233 ms** over 800 workflows.

---

## Sweeps Included

| Sweep | What varies | What is measured |
|---|---|---|
| `slots` | **Single worker (`1 py + 1 go`):** Slots per worker (`8, 32, 64, 128`) × Parallel flows (`1..512`) at `150 ms`/step | How increasing goroutines / `asyncio.Task` slots per worker scales single-worker capacity |
| `scaling` | **2D matrix (`128` slots/worker):** Worker fleet size (`1, 2, 4, 8` workers = `128..1,024` slots) × Parallel flows (`1..1,024`) at `150 ms`/step | Iso-latency parallel flow capacity, p50/p95 latency curves, and per-fleet throughput plateaus |
| `engine` | Parallel flows (`1 → 1,000`) with `0 ms` steps (`128` slots/worker) | Pure `plexus-flow` orchestration latency (`p50`, `p95`, `p99`) and single-node coordinator ceiling (`10,585 steps/s`) |
| `history` | 16 consecutive bursts of 50 workflows (`800` workflows) | Long-running latency stability: `in-memory dedup` vs. file-backed `dedup.db` |
| `pipeline` | Steps per workflow (`10` real agent pipeline, `12, 24, 48, 100` synthetic fork-join DAGs) | How DAG size affects per-step transition overhead (`12.2 ms` p50 for 10 steps; `284.5 ms` p50 for 100 steps) |
