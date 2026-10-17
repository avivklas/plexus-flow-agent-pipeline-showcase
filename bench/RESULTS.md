# Benchmark: How Worker Slots, Fleet Size & Load Affect Workflow Latency

Generated from [`20261017-130204.json`](results/20261017-130204.json) by `bench/report.py`.

**Machine:** Apple M5 Pro · 15 cores · Darwin 25.6.0 · Go go1.27.0 darwin/arm64 · Python 3.13.13 · plexus-flow `0f949cc (feat/scalability-and-execution-primitives) + uncommitted changes` · 2026-10-17T12:55:18

## What is a "Slot" (`--max-concurrency`)?

When a worker connects to `plexus-flow` over gRPC (`WorkerService.Connect`), it registers with a `max_concurrency` limit—its **slots**:
- **In the Go worker (`pkg/workerclient`):** Each slot is a concurrent **goroutine** executing an activity handler (`MaxConcurrency`).
- **In the Python worker (`plexus_flow.Worker`):** Each slot is a concurrent **`asyncio.Task`** on the event loop (`max_concurrency`).
- **In the `plexus-flow` Broker (`pkg/remote/broker.go`):** The broker tracks `len(session.inflight) < session.max` per connected worker. As long as a worker has free slots, the broker streams tasks to it immediately; once all slots across all workers are full, new tasks wait in `broker.pending[activity]`.

## 1. Scaling Slots Within a Single Worker (`8 → 32 → 64 → 128` Slots/Worker)

Because LLM and tool steps are I/O-bound (`150 ms` per step), a single worker process can hold many concurrent tasks in flight at once (goroutines in Go, `asyncio.Task`s in Python). Increasing `--max-concurrency` from `8` to `128` slots on **just 1 worker process per kind** dramatically increases how many parallel workflows it can handle before queueing:

![Single Worker: Slots per Worker vs p95 Latency](charts/slots_latency_p95.svg)

| Slots per worker (1 process/kind) | Max parallel flows (**p95 ≤ 1.5× / 1607 ms**) | Max parallel flows (**p95 ≤ 2.0× / 2142 ms**) | Peak throughput | Peak step rate |
|---|---:|---:|---:|---:|
| **8 slots/worker** | **7 parallel flows** | **13 parallel flows** | **7.5 wf/s** | 75 steps/s |
| **32 slots/worker** | **35 parallel flows** | **54 parallel flows** | **29.7 wf/s** | 297 steps/s |
| **64 slots/worker** | **69 parallel flows** | **102 parallel flows** | **58.8 wf/s** | 588 steps/s |
| **128 slots/worker** | **141 parallel flows** | **217 parallel flows** | **112.3 wf/s** | 1,123 steps/s |

## 2. Scaling Worker Fleet at `128` Slots/Worker (Up to `1,024` Parallel Workflows)

**Workload:** Real 10-step agent pipeline (`150 ms` simulated I/O per step, **128 concurrent slots per worker process**). Single-workflow baseline latency is **1074 ms p50 / 1075 ms p95** (`7 × 150 ms = 1050 ms` of step work).

![Max Parallel Flows Without Degrading Latency](charts/iso_latency_capacity.svg)

![Parallel Flows vs. p95 Latency by Worker Count](charts/scaling_latency_p95.svg)

![Parallel Flows vs. p50 Latency by Worker Count](charts/scaling_latency_p50.svg)

![Parallel Flows vs. Throughput by Worker Count](charts/scaling_throughput.svg)

### Capacity Sizing Table (`128` Slots/Worker, `150 ms`/Step)

| Worker fleet (`py` + `go`) | Total slots/kind | Max parallel flows (**p95 ≤ 1.5× / 1613 ms**) | Max parallel flows (**p95 ≤ 2.0× / 2150 ms**) | Peak throughput | Peak step rate | Fleet speedup |
|---|---:|---:|---:|---:|---:|---:|
| **1 worker** | 128 slots | **141 parallel flows** | **217 parallel flows** | **114.2 wf/s** | 1,142 steps/s | **1.0×** |
| **2 workers** | 256 slots | **285 parallel flows** | **436 parallel flows** | **208.8 wf/s** | 2,088 steps/s | **1.8×** |
| **4 workers** | 512 slots | **574 parallel flows** | **844 parallel flows** | **390.3 wf/s** | 3,903 steps/s | **3.4×** |
| **8 workers** | 1,024 slots | **1024 parallel flows** | **1024 parallel flows** | **651.7 wf/s** | 6,517 steps/s | **5.7×** |

### Full p95 Latency Matrix (ms) — Parallel Flows × Worker Fleet Size (`128` Slots/Worker)

| Parallel workflows in flight | **1 worker** (128 slots) | **2 workers** (256 slots) | **4 workers** (512 slots) | **8 workers** (1,024 slots) |
|---:|---:|---:|---:|---:|
| **1** | 1073 ms ✅ | 1077 ms ✅ | 1075 ms ✅ | 1071 ms ✅ |
| **16** | 1075 ms ✅ | 1079 ms ✅ | 1081 ms ✅ | 1082 ms ✅ |
| **64** | 1224 ms ✅ | 1086 ms ✅ | 1083 ms ✅ | 1079 ms ✅ |
| **128** | 1518 ms ✅ | 1225 ms ✅ | 1085 ms ✅ | 1080 ms ✅ |
| **256** | 2428 ms ⏳ | 1510 ms ✅ | 1214 ms ✅ | 1092 ms ✅ |
| **512** | 4517 ms ⏳ | 2420 ms ⏳ | 1489 ms ✅ | 1210 ms ✅ |
| **1,024** | 8592 ms ⏳ | 4505 ms ⏳ | 2511 ms ⏳ | 1465 ms ✅ |

*Legend: ✅ within 1.5× baseline p95 (≤ 1613 ms) · ⚠️ within 2.0× baseline p95 (≤ 2150 ms) · ⏳ queueing regime.*

## 3. Single-Node Engine Ceiling (0 ms Steps)

When workers execute in **0 ms**, all measured latency is pure `plexus-flow` orchestration overhead (two Raft consensus groups, `flowstore` state transitions, gRPC task dispatch, and SSE completion notifications). A single coordinator node peaks at **1074 workflows/s (10,736 steps/s)**. Once your worker fleet produces more than ~11,000 step completions per second, the coordinator itself becomes the bottleneck.

![Pure Engine Ceiling](charts/engine_ceiling.svg)

| Parallel workflows in flight | Runs | p50 (median) | p95 | p99 (tail) | Max | Per step (p50) | Throughput (wf/s) | Step rate (steps/s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **1** | 30 | 2.6 ms | 3.5 ms | 3.9 ms | 4.1 ms | 0.33 ms | **366.4 wf/s** | **3,664 steps/s** |
| **10** | 30 | 12.2 ms | 12.8 ms | 12.9 ms | 12.9 ms | 1.52 ms | **773.4 wf/s** | **7,734 steps/s** |
| **50** | 100 | 46.9 ms | 51.6 ms | 52.6 ms | 53.2 ms | 5.87 ms | **932.5 wf/s** | **9,325 steps/s** |
| **100** | 200 | 87.8 ms | 92.3 ms | 93.8 ms | 96.4 ms | 10.97 ms | **991.0 wf/s** | **9,910 steps/s** |
| **250** | 500 | 207.0 ms | 211.5 ms | 211.9 ms | 213.2 ms | 25.88 ms | **1065.0 wf/s** | **10,650 steps/s** |
| **500** | 1000 | 402.2 ms | 427.4 ms | 428.8 ms | 430.7 ms | 50.27 ms | **1073.6 wf/s** | **10,736 steps/s** |
| **1,000** | 2000 | 782.8 ms | 858.3 ms | 867.6 ms | 877.9 ms | 97.85 ms | **1073.3 wf/s** | **10,733 steps/s** |

## 4. Sustained Throughput Over Time (`dedup.db` vs. In-Memory Dedup)

Running identical bursts of 50 workflows back-to-back on a single engine isolates how state accumulation affects latency:
- **`in-memory dedup` (green line):** p50 latency stays completely flat (~40–48 ms at `50` concurrent workflows, **1,064.4 wf/s** / **10,644 steps/s**) across 800 cumulative workflows (with automatic `ActivityStore` pruning of terminal workflows).
- **`dedup.db (file store)` (red line):** `plexus`'s default `dedup.FileStore` serializes and rewrites its entire JSON snapshot on every dispatched step, causing latency to grow linearly with total historical steps executed.

![Sustained History: In-Memory vs dedup.db](charts/history_dedup.svg)

| Dedup store | Cumulative workflows | First burst p50 | Final burst p50 | Overall p50 | Overall p99 | Avg throughput |
|---|---:|---:|---:|---:|---:|---:|
| **in-memory dedup** | 800 | 47.5 ms | 40.7 ms | 41.5 ms | 49.3 ms | 1064.4 wf/s |
| **dedup.db (file store)** | 800 | 159.9 ms | 1275.5 ms | 703.0 ms | 1319.0 ms | 66.2 wf/s |

## 5. Pipeline DAG Size (Steps per Workflow)

How workflow complexity affects orchestration cost at 10 concurrent workflows in flight (`10*` is the real agent pipeline; `12`–`100` are synthetic 1→3 fork-join DAGs):

![Pipeline DAG Size vs Latency](charts/pipeline_dag.svg)

| Steps per workflow | Critical path | Runs | p50 | p95 | p99 | Per step (p50) | Throughput (wf/s) | Step rate (steps/s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **10*** | 8 stages | 30 | 11.9 ms | 14.1 ms | 14.6 ms | 1.49 ms | 741.5 wf/s | 7,415 steps/s |
| **12** | 6 stages | 30 | 12.4 ms | 15.1 ms | 16.4 ms | 2.07 ms | 664.0 wf/s | 7,968 steps/s |
| **24** | 12 stages | 30 | 28.2 ms | 31.8 ms | 32.0 ms | 2.35 ms | 321.7 wf/s | 7,722 steps/s |
| **48** | 24 stages | 30 | 82.7 ms | 90.1 ms | 90.5 ms | 3.45 ms | 114.1 wf/s | 5,478 steps/s |
| **100** | 50 stages | 30 | 284.1 ms | 305.7 ms | 306.8 ms | 5.68 ms | 34.0 wf/s | 3,404 steps/s |
