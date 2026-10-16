#!/usr/bin/env python3
"""A worker that serves one activity, ``noop``, which returns immediately.

Used by the pipeline-size sweep: with zero work per step, the measured
latency is purely the cost of orchestrating the DAG.
"""

import argparse
import asyncio

from plexus_flow import TaskContext, Worker


async def noop(ctx: TaskContext, payload: object) -> dict:
    return {"ok": True}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--grpc-addr", default="127.0.0.1:9090")
    parser.add_argument("--worker-id", default="noop-worker")
    parser.add_argument("--max-concurrency", type=int, default=64)
    args = parser.parse_args()

    worker = Worker(args.grpc_addr, worker_id=args.worker_id, max_concurrency=args.max_concurrency)
    worker.register("noop", noop)
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
