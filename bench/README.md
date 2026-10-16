# Scale benchmark

How does scale affect latency when plexus-flow orchestrates the agent pipeline?

```bash
uv run python bench/bench.py              # all sweeps (~10 min), writes bench/results/*.json|csv + bench/RESULTS.md
uv run python bench/bench.py --quick      # smaller sweeps (~2 min)
uv run python bench/bench.py --sweep load --only 1,100
uv run python bench/bench.py --dedup-memory   # experiment: engine with its in-memory dedup store
uv run python bench/report.py <results.json> --compare <results-dedup-memory.json>
```

**→ Latest results: [RESULTS.md](RESULTS.md)**

## Findings (2026-10-16, Apple M5 Pro, single node)

**1. The engine adds little overhead at low scale.** One ticket at a time takes **5.7 ms p50** through all 10 steps, including two worker languages, a refund, a guardrail and the approval signal round trip. That is about 0.7 ms per step.

**2. As shipped, latency drifts upward with every workflow the engine has ever run.** In the `history` sweep, identical bursts of 50 slow from **51 ms to 1,412 ms p50** over 1,000 workflows, and nothing about the load changes.
   - **Cause:** the exactly-once dedup store ([`FileStore`](https://github.com/avivklas/plexus/blob/main/pkg/dedup/dedup.go) in plexus) rewrites its *entire* file on every recorded dispatch, under a mutex, and never shrinks while running. Each step costs more than the one before.
   - **Proof:** with the engine's in-memory dedup store (`--dedup-memory`), the same 1,000 workflows stay **flat at 11–33 ms p50**.
   - **Effect on the other sweeps:** this drift dominates them. At 1,000 in flight, the shipped engine manages 17 workflows/s with a 24 s p50, and 949 workflows were still unfinished when the 120 s budget ran out. With the in-memory store, the same point does **261 workflows/s at a 164 ms p50**.

**3. Without that, throughput plateaus at ≈290 workflows/s (≈2,900 steps/s) and extra load becomes queueing.** These are the in-memory numbers. From 50 in flight upward, throughput stays flat. p50 rises gently (25 → 164 ms from 50 to 1,000 in flight), but the tail grows linearly: p99 goes from 163 ms to 3.1 s. That is the shape of a single serialised commit path.

**4. Adding workers doesn't help, because the workers aren't the bottleneck.** Going from 1 to 8 worker processes per kind changes nothing (≈300 workflows/s in-memory, ≈97 as shipped). A single worker running 8 tasks at a time keeps up with the engine.

**5. Bigger DAGs cost more per step.** Per-step latency rises from 2.1 ms to 11.8 ms between 12- and 100-step workflows (in-memory), and steps/s drops from 5,136 to 1,630. This points at work proportional to workflow size on every transition. A CPU sample shows JSON encode/decode on top: every commit deep-copies the whole workflow, history included, through a JSON round trip. This is likely, but not yet confirmed by a fix.

**6. It found a correctness bug.** About 1 in 100 workflows hung forever on `await-approval` once 10 or more were in flight. Two concurrent commits could both dispatch the same step, and the second reopened a signal wait that had already consumed its signal. It is fixed in plexus-flow branch `fix/idempotent-step-dispatch` (`1fe2e44`, with regression tests), and all numbers above include the fix.

### What would move the numbers (in order)

1. **Dedup store:** an append-only log or embedded KV instead of rewriting the file, and prune records once their Raft index is covered by a snapshot. This removes the drift in (2), which is the difference between 17 and 261 workflows/s at 1,000 in flight.
2. **No full JSON deep copy per commit:** copy-on-write or targeted reads, and cap or offload history. This should help (3) and (5).
3. **Multi-node join**, so the 1-node vs 3-node replication cost can be measured.


## Sweeps

| Sweep | What changes | What stays fixed |
|---|---|---|
| `history` | workflows completed so far (identical bursts of 50, back to back) | everything |
| `load` | workflows submitted at once: 1 → 1000 | real 10-step pipeline, 4 Python + 2 Go workers × 32 tasks |
| `fleet` | worker processes per kind: 1, 2, 4, 8 | 200 workflows at once, 8 tasks per worker |
| `pipeline` | steps per workflow: 10 (real) and 12/24/48/100 (synthetic fork-join) | 10 workflows at once |

## Method

- **Pure orchestration cost.** The mock LLM answers instantly, per-step logging is off, and the KB is in-memory. What is left is the engine: Raft commits on two consensus groups, dispatch over gRPC, dependency resolution, the approval signal.
- **Fresh stack per point.** Every data point starts a new single-node engine and a new worker fleet on an empty data directory, then runs one warm-up workflow.
- **Bursts.** A point submits N workflows at once, waits for all of them, and repeats until at least 30 workflows have run. The approval signal is sent right after each start; the engine buffers it until `await-approval` runs.
- **Engine timestamps.** Latency is `completed_at − created_at` as recorded by the engine, so polling for completion does not change the numbers. Polling is also infrequent at high load, because the endpoint copies every workflow under the store's read lock. Measured: 0.25s vs 2s polling at 250 in flight changed p50 by 2%.
- **Throughput** is runs divided by the time spent inside bursts.
- **Per step** is p50 latency divided by the critical path: 8 steps for the agent pipeline, 2 per unit for the synthetic DAGs.

## Limits

- **Needs the dispatch fix.** Until plexus-flow branch `fix/idempotent-step-dispatch` is merged, run against it. On older engines about 1% of workflows hang at 10+ in flight, and they show up as "unfinished at budget".

- **Single node only.** plexus-flow cannot currently form a multi-node cluster: a joining node's join fails (`EOF`), and `server.Start` waits for the node to lead both Raft groups. So the planned 1-node vs 3-node comparison is not included.
- **One machine.** Engine, workers and the benchmark share the same CPU. On macOS, `fsync` is cheaper than on Linux, so expect slower Raft commits on Linux servers.
- **Mock LLM.** With a real model, step latency is dominated by the model; these numbers are the overhead plexus-flow adds on top.
