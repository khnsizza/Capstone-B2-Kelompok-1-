#!/usr/bin/env python3
"""
load_test.py - Simulate 10 concurrent requests for the same QR code.

Usage:
    pip install httpx
    python load_test.py [--url http://localhost:8000] [--runs 1]

Outputs per-request latency, source (cache/database/cache_after_lock),
and a summary table with p50/p95/p99 latencies plus the DB query count
fetched from /metrics.
"""

import asyncio
import time
import argparse
import json
import sys
from typing import List, Dict, Any

# Force UTF-8 output on Windows terminals
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    import httpx
except ImportError:
    print("Please install httpx:  pip install httpx")
    raise


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

BASE_URL = "http://localhost:8000"
QR_CODE  = "QR_MERCHANT_001"
CONCURRENT_REQUESTS = 10
TIMEOUT  = 10.0  # seconds


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

async def send_inquiry(
    client: httpx.AsyncClient,
    request_id: int,
    results: List[Dict[str, Any]],
) -> None:
    start = time.perf_counter()
    try:
        resp = await client.post(
            f"{BASE_URL}/inquiry",
            json={"qr_code": QR_CODE},
            timeout=TIMEOUT,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        body = resp.json()
        results.append(
            {
                "id": request_id,
                "status_code": resp.status_code,
                "latency_ms": round(elapsed_ms, 1),
                "source": body.get("source", "unknown"),
                "merchant": body.get("merchant_name", "?"),
                "qris_status": body.get("status", "?"),
            }
        )
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        results.append(
            {
                "id": request_id,
                "status_code": 0,
                "latency_ms": round(elapsed_ms, 1),
                "source": "error",
                "error": str(exc),
            }
        )


async def reset_metrics(client: httpx.AsyncClient) -> None:
    await client.post(f"{BASE_URL}/metrics/reset", timeout=5.0)


async def flush_redis_cache(client: httpx.AsyncClient) -> None:
    """Delete the cached QR code so the stampede scenario actually triggers."""
    # We do this by hitting a dummy endpoint or simply waiting for the server
    # to start fresh. Since we don't have a dedicated cache-clear endpoint,
    # we use redis-cli via docker if available, otherwise we just note it.
    import subprocess
    try:
        subprocess.run(
            ["docker", "exec", "qris_redis", "redis-cli", "FLUSHDB"],
            capture_output=True, timeout=5,
        )
        print("  [*] Redis cache flushed (FLUSHDB)")
    except Exception:
        print("  [!] Could not flush Redis - results may show all cache hits")


async def fetch_metrics(client: httpx.AsyncClient) -> Dict[str, Any]:
    resp = await client.get(f"{BASE_URL}/metrics", timeout=5.0)
    return resp.json()


def percentile(data: List[float], pct: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * pct / 100)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

async def run_scenario(base_url: str, runs: int) -> None:
    global BASE_URL
    BASE_URL = base_url

    async with httpx.AsyncClient() as client:
        for run in range(1, runs + 1):
            print(f"\n{'='*60}")
            print(f"  RUN {run}/{runs}  --  {CONCURRENT_REQUESTS} concurrent requests for {QR_CODE}")
            print(f"{'='*60}")

            # Reset server-side counters
            await reset_metrics(client)

            # Flush Redis cache so the stampede scenario is properly tested
            await flush_redis_cache(client)

            results: List[Dict[str, Any]] = []

            # Fire all requests simultaneously
            await asyncio.gather(
                *[send_inquiry(client, i + 1, results) for i in range(CONCURRENT_REQUESTS)]
            )

            # Sort by request ID for readability
            results.sort(key=lambda r: r["id"])

            # Print individual results
            print(f"\n{'ID':>3}  {'HTTP':>4}  {'Latency(ms)':>11}  {'Source':<18}  Details")
            print("-" * 65)
            for r in results:
                if r["source"] == "error":
                    print(
                        f"{r['id']:>3}  {r['status_code']:>4}  {r['latency_ms']:>11.1f}  "
                        f"{'ERROR':<18}  {r.get('error','')[:30]}"
                    )
                else:
                    print(
                        f"{r['id']:>3}  {r['status_code']:>4}  {r['latency_ms']:>11.1f}  "
                        f"{r['source']:<18}  {r.get('merchant','')} / {r.get('qris_status','')}"
                    )

            # Latency statistics
            latencies = [r["latency_ms"] for r in results]
            p50  = percentile(latencies, 50)
            p95  = percentile(latencies, 95)
            p99  = percentile(latencies, 99)
            _max = max(latencies)
            _min = min(latencies)

            # Source breakdown
            sources = {}
            for r in results:
                sources[r["source"]] = sources.get(r["source"], 0) + 1

            # Fetch server-side metrics
            srv_metrics = await fetch_metrics(client)

            print(f"\n{'-'*60}")
            print(f"  LATENCY STATISTICS")
            print(f"    p50 : {p50:.1f} ms")
            sla_ok = p95 <= 1600
            print(f"    p95 : {p95:.1f} ms  {'[PASS] SLA OK' if sla_ok else '[FAIL] SLA BREACH'}")
            print(f"    p99 : {p99:.1f} ms")
            print(f"    min : {_min:.1f} ms")
            print(f"    max : {_max:.1f} ms")
            print(f"\n  RESPONSE SOURCES")
            for src, count in sorted(sources.items()):
                print(f"    {src:<20}: {count}")
            db_q = srv_metrics.get('db_queries', '?')
            print(f"\n  SERVER METRICS")
            print(f"    DB queries       : {db_q}  "
                  f"{'[PASS] Only 1 query!' if db_q == 1 else '[WARN] Multiple queries!' if isinstance(db_q, int) and db_q > 1 else ''}")
            ratio = srv_metrics.get('cache_hit_ratio', 0)
            print(f"    Cache hit ratio  : {ratio:.2%}  "
                  f"{'[PASS] >= 90%' if ratio >= 0.9 else '[WARN] < 90%'}")
            print(f"    SLA breaches     : {srv_metrics.get('sla_breaches', '?')}")
            print(f"    Lock contentions : {srv_metrics.get('lock_contentions', '?')}")
            print(f"{'-'*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QRIS Cache Stampede Load Test")
    parser.add_argument("--url", default="http://localhost:8000", help="Base URL of the service")
    parser.add_argument("--runs", type=int, default=1, help="Number of test runs")
    args = parser.parse_args()

    asyncio.run(run_scenario(args.url, args.runs))
