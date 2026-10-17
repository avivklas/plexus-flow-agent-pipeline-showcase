#!/usr/bin/env python3
"""Run the showcase scenarios against a running plexus-flow server.

    uv run python python/launcher.py --scenario happy
    uv run python python/launcher.py --scenario all --interactive

The launcher plays three roles, all through the public REST API:
  * the helpdesk, which submits a ticket as a workflow (flow.json + ticket),
  * the human reviewer, who answers the ``approval`` signal,
  * the chaos monkey, which kills the agent worker mid-run (crash-resume).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
from plexus_flow import watch_workflow

sys.path.insert(0, str(Path(__file__).resolve().parent))
import llm  # noqa: E402  (for the call ledger)

ROOT = Path(__file__).resolve().parent.parent
CYAN, GREEN, YELLOW, RED, PURPLE, BOLD, DIM, RESET = (
    "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[35m", "\033[1m", "\033[2m", "\033[0m")
TERMINAL = {"COMPLETED", "FAILED", "COMPENSATED", "CANCELLED", "COMPENSATION_FAILED"}
STATUS_COLOR = {"RUNNING": CYAN, "COMPLETED": GREEN, "FAILED": RED, "COMPENSATING": PURPLE, "COMPENSATED": YELLOW}

SCENARIOS: dict[str, dict[str, Any]] = {
    "happy": {
        "title": "HAPPY PATH: damaged espresso machine",
        "ticket": "damaged-espresso.json",
        "decision": {"approved": True, "reviewer": "dana@support", "note": "Looks good"},
        "story": "Parallel agents research the ticket, a refund is issued, a human approves, the reply is sent.",
    },
    "flaky-llm": {
        "title": "FLAKY LLM PROVIDER: late delivery, kb-research gets 429s",
        "ticket": "late-shoes.json",
        "chaos": {"rate_limit": {"kb-research": 2}},
        "decision": {"approved": True, "reviewer": "dana@support", "note": "Approved"},
        "story": "The model provider rate-limits kb-research twice. `retries: 5` in flow.json absorbs it. No retry code anywhere.",
    },
    "guardrail": {
        "title": "GUARDRAIL VETO: double charge, the draft over-promises",
        "ticket": "double-charge.json",
        "chaos": {"off_policy_draft": True},
        "story": "The draft promises a free month and an extra $50. The guardrail vetoes it, and the already-issued refund is reversed automatically.",
    },
    "human-reject": {
        "title": "HUMAN REJECTS: repeat refunder",
        "ticket": "damaged-headphones.json",
        "decision": {"approved": False, "reviewer": "lead@support", "note": "4 refunds in 90 days, send to fraud review"},
        "story": "Agents follow policy, but the reviewer spots a refund-abuse pattern. One signal rolls back the refund.",
    },
    "crash-resume": {
        "title": "CRASH & RESUME: agent worker killed mid-run",
        "ticket": "damaged-espresso.json",
        "chaos": {"slow_ms": {"draft-reply": 2000}},
        "decision": {"approved": True, "reviewer": "dana@support", "note": "Approved"},
        "crash_on": "draft-reply",
        "story": "The Python worker is SIGKILLed while drafting. On restart only the in-flight step runs again. Finished LLM calls are never repeated.",
    },
    "timeout": {
        "title": "NOBODY ANSWERS: approval times out",
        "ticket": "damaged-espresso.json",
        "approval_timeout": "3s",
        "story": "No reviewer responds within the approval window (shortened to 3s for the demo). SIGNAL_TIMEOUT rolls the refund back.",
    },
}
ALL = ["happy", "flaky-llm", "guardrail", "human-reject", "crash-resume", "timeout"]


# ---------------------------------------------------------------------------
# flow.json -> engine definition
# ---------------------------------------------------------------------------

_UNITS = {"ms": 10**6, "s": 10**9, "m": 60 * 10**9, "h": 3600 * 10**9}


def to_ns(value: Any) -> int:
    """flow.json uses readable durations ("60s"); the engine wants nanoseconds."""
    if isinstance(value, (int, float)):
        return int(value)
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)", str(value).strip())
    if not m:
        raise ValueError(f"bad duration {value!r}")
    return int(float(m.group(1)) * _UNITS[m.group(2)])


def load_definition(approval_timeout: str | None = None) -> dict[str, Any]:
    definition = json.loads((ROOT / "flow.json").read_text())
    for step in definition["steps"]:
        if step["name"] == "await-approval" and approval_timeout:
            step["timeout"] = approval_timeout
        for key in ("timeout", "delay"):
            if key in step:
                step[key] = to_ns(step[key])
        if rp := step.get("retry_policy"):
            for key in ("initial_interval", "max_interval"):
                if key in rp:
                    rp[key] = to_ns(rp[key])
    return definition


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

class Runner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.api = args.http_addr.rstrip("/")
        self.grpc_addr = args.grpc_addr
        self.interactive = args.interactive
        self.pidfile = Path(args.pidfile)
        self.http = httpx.Client(timeout=10.0)

    def run(self, name: str) -> str:
        sc = SCENARIOS[name]
        ticket = json.loads((ROOT / "fixtures" / "tickets" / sc["ticket"]).read_text())
        if sc.get("chaos"):
            ticket["chaos"] = sc["chaos"]
        wf_id = f"{ticket['ticket_id']}-{name}-{int(time.time() * 1000) % 100000}"

        print(f"\n{CYAN}{'═' * 78}{RESET}")
        print(f"{BOLD}▶ {sc['title']}{RESET}")
        print(f"{DIM}  {sc['story']}{RESET}")
        print(f"{CYAN}{'═' * 78}{RESET}")

        req = {"workflow_id": wf_id, "definition": load_definition(sc.get("approval_timeout")), "input": ticket,
               "metadata": {"scenario": name, "channel": ticket.get("channel", "")}}
        r = self.http.post(f"{self.api}/api/v1/workflows/start", json=req)
        r.raise_for_status()
        print(f"{CYAN}[helpdesk]{RESET} ticket {ticket['ticket_id']} submitted as workflow {BOLD}{wf_id}{RESET}")

        seen: dict[str, tuple[str, int]] = {}
        signaled = killed = False
        wf: dict[str, Any] = {}

        # No polling: plexus-flow pushes the workflow's state on every change
        # (server-sent events), the same way it pushes tasks to workers.
        for wf in watch_workflow(self.api, wf_id):
            steps = wf.get("steps", {})
            for step_name in wf.get("step_order", []):
                st = steps[step_name]
                key = (st["status"], st.get("attempt", 0))
                if seen.get(step_name) != key and st["status"] != "PENDING":
                    seen[step_name] = key
                    self._print_step(step_name, st)

            # Chaos monkey: kill the agent worker while the target step is in flight.
            target = sc.get("crash_on")
            if target and not killed and steps.get(target, {}).get("status") == "RUNNING":
                time.sleep(0.3)
                killed = True
                self._kill_agent_worker()
                time.sleep(0.5)  # nothing changes while no worker serves the step
                self._restart_agent_worker()

            # Human reviewer: answer the approval signal.
            if not signaled and steps.get("await-approval", {}).get("status") == "RUNNING":
                signaled = True
                self._review(wf_id, wf, sc.get("decision"))

        return self._summary(name, wf_id, wf)

    # -- reviewer ------------------------------------------------------------

    def _review(self, wf_id: str, wf: dict[str, Any], decision: dict[str, Any] | None) -> None:
        steps = wf["steps"]
        plan = json.loads(json.dumps(steps["resolution-plan"].get("output") or {}))
        draft = steps["draft-reply"].get("output") or {}
        print(f"\n{YELLOW}┌─ 🙋 APPROVAL NEEDED ── workflow is parked on a wait_signal step (zero compute) ──{RESET}")
        print(f"{YELLOW}│{RESET} action: {BOLD}{plan.get('action')}{RESET}  refund: ${plan.get('refund_amount', 0):.2f}  "
              f"flags: {plan.get('risk_flags')}")
        print(f"{YELLOW}│{RESET} subject: {draft.get('subject')}")
        for line in (draft.get("body") or "").splitlines():
            print(f"{YELLOW}│{RESET}   {DIM}{line}{RESET}")
        print(f"{YELLOW}│{RESET} {DIM}curl -X POST {self.api}/api/v1/workflows/{wf_id}/signal "
              f"-d '{{\"signal_name\":\"approval\",\"payload\":{{\"approved\":true,\"reviewer\":\"you\"}}}}'{RESET}")

        if self.interactive and decision is not None:
            answer = input(f"{YELLOW}└─ approve? [y/n] (default {'y' if decision['approved'] else 'n'}): {RESET}").strip().lower()
            if answer in ("y", "n"):
                decision = {**decision, "approved": answer == "y", "reviewer": "you@terminal",
                            "note": "approved from terminal" if answer == "y" else "rejected from terminal"}
        elif decision is None:
            print(f"{YELLOW}└─ (no reviewer is going to answer this one…){RESET}\n")
            return
        else:
            print(f"{YELLOW}└─{RESET}")

        verdict = f"{GREEN}APPROVED{RESET}" if decision["approved"] else f"{RED}REJECTED{RESET}"
        print(f"{YELLOW}[reviewer]{RESET} {decision['reviewer']} {verdict}: \"{decision['note']}\"\n")
        r = self.http.post(f"{self.api}/api/v1/workflows/{wf_id}/signal",
                           json={"signal_name": "approval", "payload": decision})
        r.raise_for_status()

    # -- chaos ---------------------------------------------------------------

    def _kill_agent_worker(self) -> None:
        pid = int(self.pidfile.read_text())
        os.kill(pid, signal.SIGKILL)
        print(f"\n{RED}{BOLD}💥 [chaos] SIGKILL python agent worker (pid {pid}) while draft-reply is in flight{RESET}\n")

    def _restart_agent_worker(self) -> None:
        cmd = [sys.executable, str(ROOT / "python" / "agents.py"), f"--grpc-addr={self.grpc_addr}", f"--pidfile={self.pidfile}"]
        subprocess.Popen(cmd, start_new_session=True)
        print(f"{GREEN}{BOLD}♻️  [chaos] restarting python agent worker: plexus-flow redelivers the lost task{RESET}\n")

    # -- output --------------------------------------------------------------

    @staticmethod
    def _print_step(name: str, st: dict[str, Any]) -> None:
        status = st["status"]
        color = STATUS_COLOR.get(status, RESET)
        attempt = st.get("attempt", 0)
        extra = f" {DIM}(attempt {attempt}){RESET}" if attempt > 1 else ""
        if st.get("kind") == "wait_signal" and status == "RUNNING":
            extra = f" {DIM}(waiting for signal '{st.get('signal')}'){RESET}"
        print(f"  {DIM}•{RESET} {name:<16} {color}{status}{RESET}{extra}")
        if st.get("error") and status in ("FAILED", "RUNNING"):
            print(f"    {RED}└─ {st.get('error_code') or 'ERROR'}: {st['error']}{RESET}")

    @staticmethod
    def _elapsed(wf: dict[str, Any]) -> float:
        from datetime import datetime
        def ts(v: str) -> float:
            return datetime.fromisoformat(re.sub(r"(\.\d{6})\d*", r"\1", v.replace("Z", "+00:00"))).timestamp()
        end = wf.get("completed_at") or wf.get("updated_at")
        return ts(end) - ts(wf["created_at"]) if end and wf.get("created_at") else 0.0

    def _summary(self, name: str, wf_id: str, wf: dict[str, Any]) -> str:
        status = wf.get("status", "UNKNOWN")
        steps = wf.get("steps", {})
        refund = steps.get("issue-refund", {})
        print()
        if status == "COMPLETED":
            print(f"{GREEN}{BOLD}✔ {status}{RESET}: ticket resolved, refund kept, reply sent.")
        elif status == "COMPENSATED":
            print(f"{YELLOW}{BOLD}↩ {status}{RESET} ({RED}{wf.get('error_code')}{RESET}): {wf.get('error')}")
            if refund.get("status") == "COMPENSATED":
                out = refund.get("output") or {}
                print(f"  refund {out.get('refund_id')} of ${out.get('amount', 0):.2f} was reversed automatically. No money leaked.")
        else:
            print(f"{RED}{BOLD}✖ {status}{RESET}: {wf.get('error')}")

        if target := SCENARIOS[name].get("crash_on"):
            if prog := steps.get(target, {}).get("progress"):
                print(f"  {CYAN}♻️  Heartbeat checkpoint preserved across worker crash ({target}):{RESET} {prog}")
            calls = llm.read_ledger(wf_id)
            print(f"\n  {BOLD}LLM call ledger{RESET} (one line per model call; pids show the worker restart):")
            for step_name in wf.get("step_order", []):
                rows = [c for c in calls if c["step"] == step_name]
                if not rows:
                    continue
                pids = sorted({c["pid"] for c in rows})
                mark = f"{YELLOW}← in flight during the crash, re-run{RESET}" if len(rows) > 1 else f"{GREEN}✓ once{RESET}"
                print(f"    {step_name:<16} {len(rows)} call(s)  pids={pids}  {mark}")
        print(f"  {DIM}took {self._elapsed(wf):.2f}s end to end · trace: {self.api}/  (workflow {wf_id}){RESET}")
        return status


def main() -> None:
    parser = argparse.ArgumentParser(description="Agent pipeline showcase launcher")
    parser.add_argument("--scenario", default="happy", choices=[*SCENARIOS, "all"])
    parser.add_argument("--http-addr", default="http://127.0.0.1:8080")
    parser.add_argument("--grpc-addr", default="127.0.0.1:9090")
    parser.add_argument("--pidfile", default=str(ROOT / "data" / "agents.pid"))
    parser.add_argument("--interactive", action="store_true", help="you are the human reviewer")
    args = parser.parse_args()

    runner = Runner(args)
    names = ALL if args.scenario == "all" else [args.scenario]
    results = [(n, runner.run(n)) for n in names]

    if len(results) > 1:
        print(f"\n{CYAN}{'═' * 78}{RESET}\n{BOLD}Summary{RESET}")
        for n, status in results:
            color = GREEN if status == "COMPLETED" else YELLOW if status == "COMPENSATED" else RED
            print(f"  {n:<14} {color}{status}{RESET}")


if __name__ == "__main__":
    main()
