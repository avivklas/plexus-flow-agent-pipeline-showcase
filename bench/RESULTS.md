# Benchmark: How Worker Slots, Fleet Size & Load Affect Workflow Latency

Generated from [`20261017-135612.json`](results/20261017-135612.json) by `bench/report.py`.

**Machine:** Apple M5 Pro · 15 cores · Darwin 25.6.0 · Go go1.27.0 darwin/arm64 · Python 3.13.13 · plexus-flow `aa8cc48 (main)` · 2026-10-17T13:49:26

## What is a "Slot" (`--max-concurrency`)?

When a worker connects to `plexus-flow` over gRPC (`WorkerService.Connect`), it registers with a `max_concurrency` limit—its **slots**:
- **In the Go worker (`pkg/workerclient`):** Each slot is a concurrent **goroutine** executing an activity handler (`MaxConcurrency`).
- **In the Python worker (`plexus_flow.Worker`):** Each slot is a concurrent **`asyncio.Task`** on the event loop (`max_concurrency`).
- **In the `plexus-flow` Broker (`pkg/remote/broker.go`):** The broker tracks `len(session.inflight) < session.max` per connected worker. As long as a worker has free slots, the broker streams tasks to it immediately; once all slots across all workers are full, new tasks wait in `broker.pending[activity]`.

## 1. Scaling Slots Within a Single Worker (`8 → 32 → 64 → 128` Slots/Worker)

Because LLM and tool steps are I/O-bound (`150 ms` per step), a single worker process can hold many concurrent tasks in flight at once (goroutines in Go, `asyncio.Task`s in Python). Increasing `--max-concurrency` from `8` to `128` slots on **just 1 worker process per kind** dramatically increases how many parallel workflows it can handle before queueing:

![Single Worker: Slots per Worker vs p95 Latency](charts/slots_latency_p95.svg)

| Slots per worker (1 process/kind) | Max parallel flows (**p95 ≤ 1.5× / 1609 ms**) | Max parallel flows (**p95 ≤ 2.0× / 2145 ms**) | Peak throughput | Peak step rate |
|---|---:|---:|---:|---:|
| **8 slots/worker** | **7 parallel flows** | **13 parallel flows** | **7.5 wf/s** | 75 steps/s |
| **32 slots/worker** | **35 parallel flows** | **54 parallel flows** | **29.7 wf/s** | 297 steps/s |
| **64 slots/worker** | **70 parallel flows** | **108 parallel flows** | **58.9 wf/s** | 589 steps/s |
| **128 slots/worker** | **140 parallel flows** | **206 parallel flows** | **108.7 wf/s** | 1,087 steps/s |

## 2. Scaling Worker Fleet at `128` Slots/Worker (Up to `1,024` Parallel Workflows)

**Workload:** Real 10-step agent pipeline (`150 ms` simulated I/O per step, **128 concurrent slots per worker process**). Single-workflow baseline latency is **1074 ms p50 / 1075 ms p95** (`7 × 150 ms = 1050 ms` of step work).

![Max Parallel Flows Without Degrading Latency](charts/iso_latency_capacity.svg)

![Parallel Flows vs. p95 Latency by Worker Count](charts/scaling_latency_p95.svg)

![Parallel Flows vs. p50 Latency by Worker Count](charts/scaling_latency_p50.svg)

![Parallel Flows vs. Throughput by Worker Count](charts/scaling_throughput.svg)

### Capacity Sizing Table (`128` Slots/Worker, `150 ms`/Step)

| Worker fleet (`py` + `go`) | Total slots/kind | Max parallel flows (**p95 ≤ 1.5× / 1612 ms**) | Max parallel flows (**p95 ≤ 2.0× / 2150 ms**) | Peak throughput | Peak step rate | Fleet speedup |
|---|---:|---:|---:|---:|---:|---:|
| **1 worker** | 128 slots | **141 parallel flows** | **218 parallel flows** | **117.4 wf/s** | 1,174 steps/s | **1.0×** |
| **2 workers** | 256 slots | **281 parallel flows** | **412 parallel flows** | **221.7 wf/s** | 2,217 steps/s | **1.9×** |
| **4 workers** | 512 slots | **578 parallel flows** | **874 parallel flows** | **391.5 wf/s** | 3,915 steps/s | **3.3×** |
| **8 workers** | 1,024 slots | **1024 parallel flows** | **1024 parallel flows** | **646.4 wf/s** | 6,464 steps/s | **5.5×** |

### Full p95 Latency Matrix (ms) — Parallel Flows × Worker Fleet Size (`128` Slots/Worker)

| Parallel workflows in flight | **1 worker** (128 slots) | **2 workers** (256 slots) | **4 workers** (512 slots) | **8 workers** (1,024 slots) |
|---:|---:|---:|---:|---:|
| **1** | 1070 ms ✅ | 1075 ms ✅ | 1074 ms ✅ | 1075 ms ✅ |
| **16** | 1075 ms ✅ | 1081 ms ✅ | 1081 ms ✅ | 1084 ms ✅ |
| **64** | 1228 ms ✅ | 1082 ms ✅ | 1085 ms ✅ | 1087 ms ✅ |
| **128** | 1519 ms ✅ | 1224 ms ✅ | 1086 ms ✅ | 1089 ms ✅ |
| **256** | 2419 ms ⏳ | 1508 ms ✅ | 1210 ms ✅ | 1086 ms ✅ |
| **512** | 4506 ms ⏳ | 2563 ms ⏳ | 1493 ms ✅ | 1218 ms ✅ |
| **1,024** | 8617 ms ⏳ | 4392 ms ⏳ | 2423 ms ⏳ | 1474 ms ✅ |

*Legend: ✅ within 1.5× baseline p95 (≤ 1612 ms) · ⚠️ within 2.0× baseline p95 (≤ 2150 ms) · ⏳ queueing regime.*

## 3. Single-Node Engine Ceiling (0 ms Steps)

When workers execute in **0 ms**, all measured latency is pure `plexus-flow` orchestration overhead (two Raft consensus groups, `flowstore` state transitions, gRPC task dispatch, and SSE completion notifications). A single coordinator node peaks at **1058 workflows/s (10,585 steps/s)**. Once your worker fleet produces more than ~11,000 step completions per second, the coordinator itself becomes the bottleneck.

![Pure Engine Ceiling](charts/engine_ceiling.svg)

| Parallel workflows in flight | Runs | p50 (median) | p95 | p99 (tail) | Max | Per step (p50) | Throughput (wf/s) | Step rate (steps/s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **1** | 30 | 2.7 ms | 3.5 ms | 6.4 ms | 7.5 ms | 0.33 ms | **346.9 wf/s** | **3,469 steps/s** |
| **10** | 30 | 10.8 ms | 13.9 ms | 13.9 ms | 13.9 ms | 1.34 ms | **778.7 wf/s** | **7,787 steps/s** |
| **50** | 100 | 47.1 ms | 52.0 ms | 52.8 ms | 53.1 ms | 5.88 ms | **921.3 wf/s** | **9,213 steps/s** |
| **100** | 200 | 89.5 ms | 96.0 ms | 96.8 ms | 98.4 ms | 11.18 ms | **981.5 wf/s** | **9,815 steps/s** |
| **250** | 500 | 201.4 ms | 220.4 ms | 222.3 ms | 225.2 ms | 25.17 ms | **1056.0 wf/s** | **10,560 steps/s** |
| **500** | 1000 | 407.5 ms | 437.4 ms | 438.7 ms | 441.7 ms | 50.94 ms | **1058.5 wf/s** | **10,585 steps/s** |
| **1,000** | 2000 | 808.8 ms | 848.5 ms | 856.8 ms | 865.5 ms | 101.10 ms | **1052.0 wf/s** | **10,520 steps/s** |

## 4. Sustained Throughput Over Time (`dedup.db` vs. In-Memory Dedup)

Running identical bursts of 50 workflows back-to-back on a single engine isolates how state accumulation affects latency:
- **`in-memory dedup` (green line):** p50 latency stays completely flat (~40–47 ms at `50` concurrent workflows, **1,084.9 wf/s** / **10,849 steps/s**) across 800 cumulative workflows (with automatic `ActivityStore` pruning of terminal workflows).
- **`dedup.db (file store)` (red line):** `plexus`'s default `dedup.FileStore` serializes and rewrites its entire JSON snapshot on every dispatched step, causing latency to grow linearly with total historical steps executed.

![Sustained History: In-Memory vs dedup.db](charts/history_dedup.svg)

| Dedup store | Cumulative workflows | First burst p50 | Final burst p50 | Overall p50 | Overall p99 | Avg throughput |
|---|---:|---:|---:|---:|---:|---:|
| **in-memory dedup** | 800 | 46.9 ms | 40.7 ms | 41.1 ms | 48.3 ms | 1084.9 wf/s |
| **dedup.db (file store)** | 800 | 155.4 ms | 1233.4 ms | 679.2 ms | 1275.5 ms | 67.0 wf/s |

## 5. Pipeline DAG Size (Steps per Workflow)

How workflow complexity affects orchestration cost at 10 concurrent workflows in flight (`10*` is the real agent pipeline; `12`–`100` are synthetic 1→3 fork-join DAGs):

![Pipeline DAG Size vs Latency](charts/pipeline_dag.svg)

| Steps per workflow | Critical path | Runs | p50 | p95 | p99 | Per step (p50) | Throughput (wf/s) | Step rate (steps/s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **10*** | 8 stages | 30 | 12.2 ms | 14.3 ms | 14.5 ms | 1.53 ms | 733.4 wf/s | 7,334 steps/s |
| **12** | 6 stages | 30 | 11.7 ms | 13.7 ms | 13.8 ms | 1.95 ms | 742.2 wf/s | 8,906 steps/s |
| **24** | 12 stages | 30 | 29.0 ms | 33.7 ms | 33.7 ms | 2.41 ms | 313.7 wf/s | 7,529 steps/s |
| **48** | 24 stages | 30 | 80.0 ms | 87.5 ms | 88.0 ms | 3.33 ms | 117.9 wf/s | 5,657 steps/s |
| **100** | 50 stages | 30 | 284.5 ms | 303.2 ms | 304.0 ms | 5.69 ms | 33.9 wf/s | 3,393 steps/s |
