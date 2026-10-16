"""Knowledge base for the kb-research agent.

Uses plexus-search (a Raft-replicated document store with full-text search)
when ``KB_URL`` points at a running node, and an in-memory keyword index
otherwise. Both expose the same ``search(query, size)`` coroutine.

Seed plexus-search from the markdown articles in ``kb/``:

    uv run python python/kb.py seed --url http://127.0.0.1:8090
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Any

import httpx

KB_DIR = Path(__file__).resolve().parent.parent / "kb"
INDEX = "kb"


def load_articles() -> list[dict[str, Any]]:
    """Parse kb/*.md: a small `key: value` front matter block, then the body."""
    articles = []
    for path in sorted(KB_DIR.glob("*.md")):
        text = path.read_text()
        m = re.match(r"^---\n(.*?)\n---\n(.*)$", text, re.S)
        if not m:
            continue
        meta: dict[str, Any] = {}
        for line in m.group(1).splitlines():
            key, _, value = line.partition(":")
            value = value.strip()
            if value.lower() in ("true", "false"):
                meta[key.strip()] = value.lower() == "true"
            elif re.fullmatch(r"-?\d+(\.\d+)?", value):
                meta[key.strip()] = float(value) if "." in value else int(value)
            else:
                meta[key.strip()] = value
        body = " ".join(m.group(2).split())
        meta["body"] = body
        meta["text"] = f"{meta.get('title', '')} {meta.get('category', '').replace('_', ' ')} {body}"
        articles.append(meta)
    return articles


def _terms(s: str) -> set[str]:
    return {t for t in re.findall(r"[a-z]+", s.lower()) if len(t) > 2}


class InMemoryKB:
    name = "in-memory"

    def __init__(self) -> None:
        self.articles = load_articles()

    async def search(self, query: str, size: int = 3) -> list[dict[str, Any]]:
        q = _terms(query)
        scored = [(len(q & _terms(a["text"])), a) for a in self.articles]
        scored = [s for s in scored if s[0] > 0]
        scored.sort(key=lambda s: -s[0])
        return [a for _, a in scored[:size]]


class PlexusSearchKB:
    name = "plexus-search"

    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")

    async def search(self, query: str, size: int = 3) -> list[dict[str, Any]]:
        body = {"query": {"type": "match", "field": "text", "value": query}, "load_docs": True, "size": size}
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(f"{self.url}/api/v1/indexes/{INDEX}/search", json=body)
            resp.raise_for_status()
        return [h["document"]["data"] for h in resp.json().get("hits", []) if h.get("document")]


def get_kb() -> InMemoryKB | PlexusSearchKB:
    url = os.environ.get("KB_URL", "")
    if url:
        try:
            httpx.get(f"{url.rstrip('/')}/health", timeout=2.0).raise_for_status()
            return PlexusSearchKB(url)
        except httpx.HTTPError:
            print(f"[kb] plexus-search at {url} is not reachable, falling back to the in-memory KB", file=sys.stderr)
    return InMemoryKB()


async def seed(url: str) -> int:
    articles = load_articles()
    batch = {"indexes": [{"id": a["id"], "data": a} for a in articles]}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{url.rstrip('/')}/api/v1/indexes/{INDEX}/batch", json=batch)
        resp.raise_for_status()
    return len(articles)


def main() -> None:
    parser = argparse.ArgumentParser(description="KB tools")
    sub = parser.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed", help="index kb/*.md into plexus-search")
    s.add_argument("--url", default="http://127.0.0.1:8090")
    q = sub.add_parser("search", help="run a query against the configured KB")
    q.add_argument("query")
    args = parser.parse_args()

    if args.cmd == "seed":
        n = asyncio.run(seed(args.url))
        print(f"[kb] indexed {n} articles into plexus-search index '{INDEX}' at {args.url}")
    else:
        kb = get_kb()
        for a in asyncio.run(kb.search(args.query)):
            print(f"[{kb.name}] {a['id']}  {a['title']}")


if __name__ == "__main__":
    main()
