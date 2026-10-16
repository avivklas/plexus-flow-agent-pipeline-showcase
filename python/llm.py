"""The one function every agent calls: ``complete()``.

Backends (``PLEXUS_LLM``):
  * ``mock`` (default): deterministic, instant, offline, zero API keys. Each agent passes
    a ``mock`` callable that computes what a well-behaved model would answer.
  * ``openai``: any OpenAI-compatible ``/chat/completions`` endpoint (OpenAI,
    Ollama, vLLM, LM Studio, ...). Configure ``OPENAI_BASE_URL``,
    ``OPENAI_API_KEY`` and ``OPENAI_MODEL``.

Note what is *not* here: no retry loops, no backoff, no circuit breakers.
Provider hiccups are raised as retryable ``ActivityFailure`` and plexus-flow
retries the step according to ``retries`` in flow.json.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Callable

import httpx
from plexus_flow import ActivityFailure, PermanentFailure, TaskContext

BACKEND = os.environ.get("PLEXUS_LLM", "mock").lower()
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
QUIET = os.environ.get("SHOWCASE_QUIET") == "1"
LEDGER_ENABLED = os.environ.get("LLM_LEDGER", "1") != "0"  # the benchmark turns it off
LEDGER_DIR = Path(os.environ.get("LLM_LEDGER_DIR", Path(__file__).resolve().parent.parent / "data" / "llm_calls"))

DIM, PURPLE, RED, RESET = "\033[2m", "\033[35m", "\033[31m", "\033[0m"


def describe_backend() -> str:
    if BACKEND == "mock":
        return "mock (deterministic, offline)"
    return f"openai-compatible {OPENAI_BASE_URL} model={OPENAI_MODEL}"


async def complete(
    ctx: TaskContext,
    *,
    system: str,
    prompt: str,
    mock: Callable[[], dict[str, Any]],
    chaos: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Ask the model for a JSON object and return it as a dict."""
    _record_call(ctx)
    await _inject_chaos(ctx, chaos or {})

    started = time.monotonic()
    if BACKEND == "mock":
        out = mock()
    elif BACKEND == "openai":
        try:
            out = await _openai(ctx, system, prompt)
        except ActivityFailure as e:
            print(f"{RED}    ⚠ {ctx.step_name}: {e.code}: {e} (attempt {ctx.attempt}){RESET}", flush=True)
            raise
    else:
        raise PermanentFailure(f"unknown PLEXUS_LLM backend {BACKEND!r}", code="LLM_CONFIG")

    if not QUIET:
        print(f"{DIM}    🧠 {ctx.step_name}: llm call ({BACKEND}, {(time.monotonic() - started) * 1000:.0f}ms, attempt {ctx.attempt}){RESET}", flush=True)
    return out


def require(out: dict[str, Any], *keys: str) -> None:
    """Validate a model answer. A malformed answer is retryable: ask again."""
    missing = [k for k in keys if k not in out]
    if missing:
        raise ActivityFailure(f"model answer is missing {missing}", code="LLM_SCHEMA")


# --------------------------------------------------------------------------
# Call ledger: proves the crash-and-resume demo does not re-run finished steps
# --------------------------------------------------------------------------

def _record_call(ctx: TaskContext) -> None:
    if not LEDGER_ENABLED:
        return
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    entry = {"step": ctx.step_name, "attempt": ctx.attempt, "task_id": ctx.task_id, "pid": os.getpid(), "ts": time.time()}
    with open(LEDGER_DIR / f"{ctx.workflow_id}.jsonl", "a") as f:
        f.write(json.dumps(entry) + "\n")


def read_ledger(workflow_id: str) -> list[dict[str, Any]]:
    path = LEDGER_DIR / f"{workflow_id}.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


# --------------------------------------------------------------------------
# Chaos: fault injection driven by the ticket payload, identical in all backends
# --------------------------------------------------------------------------

async def _inject_chaos(ctx: TaskContext, chaos: dict[str, Any]) -> None:
    failures = chaos.get("rate_limit", {}).get(ctx.step_name, 0)
    if ctx.attempt <= failures:
        print(f"{RED}    ⚠ {ctx.step_name}: provider returned 429 Too Many Requests (attempt {ctx.attempt}){RESET}", flush=True)
        raise ActivityFailure("429 Too Many Requests: rate limit exceeded", code="RATE_LIMITED")
    # A slow model response, used to keep a step in flight long enough to crash the worker.
    # Only the first delivery is slow; the redelivered task after the crash runs at full speed.
    slow_ms = chaos.get("slow_ms", {}).get(ctx.step_name, 0)
    first_delivery = sum(1 for c in read_ledger(ctx.workflow_id) if c["step"] == ctx.step_name) == 1
    if slow_ms and first_delivery:
        print(f"{PURPLE}    ⏳ {ctx.step_name}: slow model response ({slow_ms} ms){RESET}", flush=True)
        await asyncio.sleep(slow_ms / 1000)


# --------------------------------------------------------------------------
# OpenAI-compatible backend
# --------------------------------------------------------------------------

async def _openai(ctx: TaskContext, system: str, prompt: str) -> dict[str, Any]:
    body = {
        "model": OPENAI_MODEL,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system + "\nRespond with a single JSON object only."},
            {"role": "user", "content": prompt},
        ],
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"} if OPENAI_API_KEY else {}
    timeout = ctx.timeout_s or 60.0
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(f"{OPENAI_BASE_URL}/chat/completions", json=body, headers=headers)
    except httpx.TransportError as e:
        raise ActivityFailure(f"LLM provider unreachable: {e}", code="PROVIDER_UNAVAILABLE") from e

    if resp.status_code == 429:
        raise ActivityFailure("429 Too Many Requests from provider", code="RATE_LIMITED")
    if resp.status_code >= 500:
        raise ActivityFailure(f"provider error {resp.status_code}", code="PROVIDER_ERROR")
    if resp.status_code >= 400:
        raise PermanentFailure(f"provider rejected request {resp.status_code}: {resp.text[:200]}", code="LLM_BAD_REQUEST")

    content = resp.json()["choices"][0]["message"]["content"] or ""
    content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        out = json.loads(content)
    except json.JSONDecodeError as e:
        raise ActivityFailure(f"model returned invalid JSON: {content[:120]!r}", code="LLM_BAD_JSON") from e
    if not isinstance(out, dict):
        raise ActivityFailure("model returned JSON that is not an object", code="LLM_BAD_JSON")
    return out
