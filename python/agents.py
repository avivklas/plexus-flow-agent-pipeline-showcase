#!/usr/bin/env python3
"""The Python agent worker: seven LLM agents, each a plain async function.

There is no agent framework here. Each agent:
  1. reads the ticket (its input) and upstream results (``ctx.dependency_outputs``),
  2. asks the model one focused question via ``llm.complete()``,
  3. returns a JSON-serialisable dict, which plexus-flow commits through Raft
     and hands to downstream steps.

Ordering, parallelism, retries, timeouts, the human approval gate and undoing
side effects are all declared in flow.json. None of it lives in this file.

    uv run python python/agents.py --grpc-addr=127.0.0.1:9090
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any

from plexus_flow import PermanentFailure, TaskContext, Worker

import llm
from kb import get_kb

ROOT = Path(__file__).resolve().parent.parent
CUSTOMERS: dict[str, Any] = json.loads((ROOT / "fixtures" / "customers.json").read_text())
KB = get_kb()

CYAN, GREEN, YELLOW, RED, BOLD, RESET = "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[1m", "\033[0m"
CATEGORIES = ["damaged_item", "late_delivery", "billing_error", "returns", "warranty", "other"]

AGENTS: dict[str, Any] = {}


def agent(name: str):
    """Mark a function as the plexus-flow activity `name`."""
    def register(fn):
        AGENTS[name] = fn
        return fn
    return register


QUIET = os.environ.get("SHOWCASE_QUIET") == "1"  # the benchmark silences per-step logs


def say(agent: str, ticket: dict[str, Any], msg: str, color: str = GREEN) -> None:
    if QUIET:
        return
    print(f"{color}[{agent}]{RESET} {ticket.get('ticket_id', '?')}: {msg}", flush=True)


def ticket_text(t: dict[str, Any]) -> str:
    return f"Subject: {t['subject']}\n\n{t['body']}"


# ---------------------------------------------------------------------------
# 1. Triage: what is this ticket about?
# ---------------------------------------------------------------------------

@agent("triage-agent")
async def triage(ctx: TaskContext, t: dict[str, Any]) -> dict[str, Any]:
    def mock() -> dict[str, Any]:
        text = ticket_text(t).lower()
        if re.search(r"warranty|serial|stop.*working|stopped working|faulty", text):
            category = "warranty"
        elif re.search(r"crack|broken|damag|leak|dent", text):
            category = "damaged_item"
        elif re.search(r"charged twice|double|billing|charge", text):
            category = "billing_error"
        elif re.search(r"late|tracking|transit|arrive", text):
            category = "late_delivery"
        else:
            category = "other"
        urgency = "high" if re.search(r"immediately|unacceptable|!!|cancel", text) else "normal"
        return {"category": category, "urgency": urgency,
                "summary": t["subject"], "search_query": f"{category.replace('_', ' ')} {t['subject']}"}

    out = await llm.complete(
        ctx,
        system=f"You triage customer support tickets. category must be one of {CATEGORIES}; urgency is low|normal|high.",
        prompt=f"{ticket_text(t)}\n\nReturn JSON with keys: category, urgency, summary (one sentence), "
               "search_query (keywords to find the relevant support policy).",
        mock=mock, chaos=t.get("chaos"),
    )
    llm.require(out, "category", "urgency", "search_query")
    say("triage-agent", t, f"category={BOLD}{out['category']}{RESET} urgency={out['urgency']}")
    return out


# ---------------------------------------------------------------------------
# 2a. KB research (RAG over plexus-search): which policy applies?
# ---------------------------------------------------------------------------

@agent("kb-research-agent")
async def kb_research(ctx: TaskContext, t: dict[str, Any]) -> dict[str, Any]:
    tri = ctx.dependency_outputs["triage"]
    hits = await KB.search(tri["search_query"], size=3)
    if not hits:
        raise PermanentFailure("no policy article matches this ticket", code="NO_POLICY")
    by_id = {a["id"]: a for a in hits}

    def mock() -> dict[str, Any]:
        best = next((a for a in hits if a.get("category") == tri["category"]), hits[0])
        return {"policy_id": best["id"], "guidance": best["body"][:240]}

    out = await llm.complete(
        ctx,
        system="You are a support policy researcher. Pick the single policy article that governs this ticket.",
        prompt=f"Ticket category: {tri['category']}\n{ticket_text(t)}\n\nCandidate articles:\n"
               + "\n".join(f"- {a['id']}: {a['title']}: {a['body']}" for a in hits)
               + "\n\nReturn JSON with keys: policy_id (one of the ids above), guidance (what the policy allows, 1-2 sentences).",
        mock=mock, chaos=t.get("chaos"),
    )
    llm.require(out, "policy_id")
    # The model chooses; the numbers come from the article itself, never from the model.
    article = by_id.get(out["policy_id"]) or hits[0]
    policy = {k: article.get(k) for k in ("id", "title", "category", "refund_pct", "refund_shipping",
                                          "refund_duplicate_only", "auto_refund_cap")}
    say("kb-research-agent", t, f"{KB.name} → {BOLD}{article['id']}{RESET} \"{article['title']}\" (top of {len(hits)} hit{'s' if len(hits) != 1 else ''})")
    return {"policy": policy, "guidance": out.get("guidance", ""), "sources": list(by_id), "kb_backend": KB.name}


# ---------------------------------------------------------------------------
# 2b. Account agent: who is the customer and what did they buy?
# ---------------------------------------------------------------------------

@agent("account-agent")
async def account(ctx: TaskContext, t: dict[str, Any]) -> dict[str, Any]:
    customer = CUSTOMERS.get(t["customer_id"])
    if customer is None:
        raise PermanentFailure(f"unknown customer {t['customer_id']}", code="UNKNOWN_CUSTOMER")
    order = customer["orders"].get(t["order_id"])
    if order is None:
        raise PermanentFailure(f"order {t['order_id']} not found", code="UNKNOWN_ORDER")
    facts = {k: v for k, v in customer.items() if k != "orders"}

    def mock() -> dict[str, Any]:
        flags = []
        if customer["refunds_90d"] > 3:
            flags.append("repeat_refunds")
        if customer["tier"] in ("gold", "platinum"):
            flags.append("high_value_customer")
        return {"risk_flags": flags, "summary": f"{customer['tier']} customer, {customer['refunds_90d']} refunds in 90 days"}

    out = await llm.complete(
        ctx,
        system="You are an account analyst. Flag anything a support agent must know before offering compensation.",
        prompt=f"Customer: {json.dumps(facts)}\nOrder: {json.dumps(order)}\n\n"
               "Return JSON with keys: risk_flags (list of short snake_case flags), summary (one sentence).",
        mock=mock, chaos=t.get("chaos"),
    )
    llm.require(out, "risk_flags")
    say("account-agent", t, f"{customer['name']} ({customer['tier']}) flags={out['risk_flags']}")
    return {"customer": facts, "order": order, **out}


# ---------------------------------------------------------------------------
# 2c. Sentiment agent: how upset is the customer?
# ---------------------------------------------------------------------------

@agent("sentiment-agent")
async def sentiment(ctx: TaskContext, t: dict[str, Any]) -> dict[str, Any]:
    def mock() -> dict[str, Any]:
        text = t["body"].lower()
        angry = bool(re.search(r"unacceptable|immediately|!!|cancel", text + t["subject"]))
        return {"sentiment": "angry" if angry else "frustrated", "churn_risk": "high" if "cancel" in text else "medium"}

    out = await llm.complete(
        ctx,
        system="You assess customer sentiment for support tickets.",
        prompt=f"{ticket_text(t)}\n\nReturn JSON with keys: sentiment (calm|frustrated|angry), churn_risk (low|medium|high).",
        mock=mock, chaos=t.get("chaos"),
    )
    llm.require(out, "sentiment", "churn_risk")
    say("sentiment-agent", t, f"sentiment={out['sentiment']} churn_risk={out['churn_risk']}")
    return out


# ---------------------------------------------------------------------------
# 3. Planner: join the three parallel branches into one decision
# ---------------------------------------------------------------------------

@agent("planner-agent")
async def planner(ctx: TaskContext, t: dict[str, Any]) -> dict[str, Any]:
    deps = ctx.dependency_outputs
    tri, kb, acct, mood = deps["triage"], deps["kb-research"], deps["account-lookup"], deps["sentiment"]
    policy, order = kb["policy"], acct["order"]
    cap = float(policy.get("auto_refund_cap") or 0)

    def mock() -> dict[str, Any]:
        if policy.get("refund_duplicate_only") and order.get("duplicate_charge"):
            return {"action": "refund_duplicate_charge", "refund_amount": order["duplicate_charge"],
                    "rationale": "Verified duplicate charge; policy refunds the duplicate in full."}
        if policy.get("refund_shipping") and order.get("delivered_days_late", 0) > 5:
            return {"action": "refund_shipping_fee", "refund_amount": order["shipping_fee"],
                    "rationale": f"Delivery is {order['delivered_days_late']} days late; policy refunds shipping."}
        if policy.get("refund_pct"):
            amount = round(order["price"] * policy["refund_pct"] / 100, 2)
            return {"action": "goodwill_partial_refund", "refund_amount": amount,
                    "rationale": f"{policy['refund_pct']}% goodwill refund for an item damaged on arrival."}
        return {"action": "escalate", "refund_amount": 0, "rationale": "No automatic remedy under policy."}

    out = await llm.complete(
        ctx,
        system="You are a senior support lead. Decide the resolution strictly within policy.",
        prompt=f"Ticket: {ticket_text(t)}\nTriage: {json.dumps(tri)}\nPolicy: {json.dumps(policy)}\n"
               f"Guidance: {kb['guidance']}\nAccount: {json.dumps(acct)}\nSentiment: {json.dumps(mood)}\n\n"
               "Return JSON with keys: action (snake_case), refund_amount (number, USD, 0 if none), rationale.",
        mock=mock, chaos=t.get("chaos"),
    )
    llm.require(out, "action", "refund_amount")
    requested = float(out["refund_amount"])
    # Agents propose, code bounds: never let a model exceed the policy's automatic cap.
    amount = round(min(requested, cap), 2)
    plan = {**out, "refund_amount": amount, "requested_amount": requested, "currency": "USD",
            "policy_id": policy["id"], "auto_refund_cap": cap, "clamped": amount < requested,
            "customer_name": acct["customer"]["name"], "item": order["item"], "sentiment": mood["sentiment"],
            "risk_flags": acct.get("risk_flags", [])}
    say("planner-agent", t, f"{BOLD}{plan['action']}{RESET} ${amount:.2f} (policy {policy['id']}, cap ${cap:.0f})", YELLOW)
    return plan


# ---------------------------------------------------------------------------
# 4. Draft reply: write to the customer about what was actually done
# ---------------------------------------------------------------------------

@agent("draft-reply-agent")
async def draft_reply(ctx: TaskContext, t: dict[str, Any]) -> dict[str, Any]:
    plan = ctx.dependency_outputs["resolution-plan"]
    # When issue-refund is skipped by its declarative `when` guard (refund_amount == 0),
    # it has no output in dependency_outputs.
    refund = ctx.dependency_outputs.get("issue-refund") or {"skipped": True, "amount": 0}
    first = plan["customer_name"].split()[0]
    chaos = t.get("chaos") or {}

    if not QUIET:
        if ctx.last_progress:
            say("draft-reply-agent", t, f"♻️  resumed from heartbeat checkpoint {ctx.last_progress}", CYAN)
        else:
            await ctx.report_progress({
                "stage": "context_prepared",
                "action": plan["action"],
                "refund_id": refund.get("refund_id"),
                "amount": refund.get("amount", 0),
            })

    def mock() -> dict[str, Any]:
        if refund.get("skipped"):
            done = "I've escalated your case to our specialist team, who will contact you within one business day."
        else:
            done = (f"I've issued a refund of ${refund['amount']:.2f} to your original payment method "
                    f"(reference {refund['refund_id']}). It should appear within 3-5 business days.")
        body = f"Hi {first},\n\nI'm really sorry about the trouble with your {plan['item']}. That's not the experience we want you to have.\n{done}"
        if chaos.get("off_policy_draft"):
            body += "\nTo make it right, we'll also give you a free month of Pro and an extra $50 credit on top of that!"
        body += "\n\nBest regards,\nThe Support Team"
        return {"subject": f"Re: {t['subject']}", "body": body}

    out = await llm.complete(
        ctx,
        system="You write short, warm, precise customer support replies (under 120 words). "
               "Only describe actions that were actually taken. Never promise anything else.",
        prompt=f"Customer first name: {first}\nTicket: {ticket_text(t)}\nSentiment: {plan['sentiment']}\n"
               f"Action taken: {plan['action']}\nRefund: {json.dumps(refund)}\n\nReturn JSON with keys: subject, body.",
        mock=mock, chaos=chaos,
    )
    llm.require(out, "subject", "body")
    say("draft-reply-agent", t, f"drafted \"{out['subject']}\" ({len(out['body'].split())} words)")
    return out


# ---------------------------------------------------------------------------
# 5. Guardrail: deterministic checks + an LLM judge. Veto = saga rollback.
# ---------------------------------------------------------------------------

FORBIDDEN = [r"free month", r"free replacement", r"on top of", r"guarantee", r"extra \$?\d+"]


@agent("guardrail-agent")
async def guardrail(ctx: TaskContext, t: dict[str, Any]) -> dict[str, Any]:
    plan, draft = ctx.dependency_outputs["resolution-plan"], ctx.dependency_outputs["draft-reply"]
    body = draft["body"]

    violations = [f"forbidden promise: '{m.group(0)}'" for p in FORBIDDEN if (m := re.search(p, body, re.I))]
    for amount in re.findall(r"\$(\d+(?:\.\d{2})?)", body):
        if abs(float(amount) - plan["refund_amount"]) > 0.01:
            violations.append(f"mentions ${amount} but the approved refund is ${plan['refund_amount']:.2f}")

    def mock() -> dict[str, Any]:
        return {"verdict": "fail" if violations else "pass", "reasons": violations}

    judge = await llm.complete(
        ctx,
        system="You are a compliance reviewer. A reply fails if it promises anything beyond the approved action.",
        prompt=f"Approved action: {plan['action']} refund ${plan['refund_amount']:.2f}\nReply:\n{body}\n\n"
               "Return JSON with keys: verdict (pass|fail), reasons (list of strings).",
        mock=mock, chaos=t.get("chaos"),
    )
    judged = judge.get("reasons", []) if judge.get("verdict") == "fail" else []
    reasons = list(dict.fromkeys(violations + judged))  # deterministic checks first, then the judge's own
    if reasons:
        say("guardrail-agent", t, f"🛑 VETO: {reasons[0]}", RED)
        raise PermanentFailure("; ".join(reasons), code="POLICY_VIOLATION")
    say("guardrail-agent", t, "✅ reply is within policy")
    return {"verdict": "pass", "checks": ["forbidden_promises", "amount_consistency", "llm_judge"]}


# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Agent pipeline: Python agent worker")
    parser.add_argument("--grpc-addr", default="127.0.0.1:9090")
    parser.add_argument("--pidfile", default=str(ROOT / "data" / "agents.pid"))
    parser.add_argument("--worker-id", default="py-agent-worker", help="unique per worker process")
    parser.add_argument("--max-concurrency", type=int, default=8, help="tasks run in parallel by this process")
    args = parser.parse_args()

    Path(args.pidfile).parent.mkdir(parents=True, exist_ok=True)
    Path(args.pidfile).write_text(str(os.getpid()))
    worker = Worker(args.grpc_addr, worker_id=args.worker_id, max_concurrency=args.max_concurrency)
    for name, fn in AGENTS.items():
        worker.register(name, fn)

    print(f"{CYAN}[Python Agent Worker]{RESET} pid={os.getpid()} llm={llm.describe_backend()} kb={KB.name} "
          f"target={args.grpc_addr} agents={list(AGENTS)}", flush=True)
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        worker.stop()


if __name__ == "__main__":
    main()
