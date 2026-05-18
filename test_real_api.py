"""
Real Deribit API test: validate data structure assumptions.

Key questions:
1. Does has_more correctly signal "there are more trades beyond this chunk"?
2. Is next_seq = trade_seq + 1 valid for continuation?
3. Are chunks disjoint (no overlap)?
4. What are the actual trade fields?
5. Does the finalize condition work for last chunks?
"""

import asyncio
import httpx
import json

BASE_URL = "https://history.deribit.com/api/v2/public"
CHUNK_SIZE = 10000


async def fetch_json(client, endpoint, params):
    url = f"{BASE_URL}{endpoint}"
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


# ═══════════════════════════════════════════════════════
# TEST 1: Future chunking behavior
# ═══════════════════════════════════════════════════════
async def test_future_chunking(client, currency="BTC"):
    """Verify has_more behavior and chunk boundary continuity for futures."""
    print("\n═══ FUTURE CHUNKING ═══")

    data = await fetch_json(
        client,
        "/get_instruments",
        {"currency": currency, "kind": "future", "expired": "true"},
    )
    expired = [f for f in data["result"] if not f["is_active"]]

    # Find futures with lots of trades using batch approach
    tasks = []
    for f in expired:
        tasks.append(f["instrument_name"])
    # Just check a reasonable sample: 5 expired + all active
    active = [f for f in data["result"] if f["is_active"]]
    test_list = expired[:10] + active

    candidates = []
    for f in test_list:
        instr = f["instrument_name"] if isinstance(f, dict) else f
        d = await fetch_json(
            client,
            "/get_last_trades_by_instrument",
            {"instrument_name": instr, "count": 1},
        )
        trades = d["result"].get("trades", [])
        if trades:
            candidates.append((instr, trades[0]["trade_seq"]))
        await asyncio.sleep(0.1)

    if not candidates:
        print("  No futures with trades found")
        return

    # Test top candidates with most trades
    candidates.sort(key=lambda x: -x[1])
    for instr, last_seq in candidates[:3]:
        print(f"\n--- {instr} (last_seq={last_seq}) ---")

        # Chunk 1: 1..10000
        c1 = await fetch_json(
            client,
            "/get_last_trades_by_instrument",
            {
                "instrument_name": instr,
                "start_seq": 1,
                "end_seq": 10000,
                "count": 10000,
            },
        )
        r1 = c1["result"]
        print(
            f"  Chunk [1,10000]: {len(r1['trades'])} trades, has_more={r1['has_more']}"
        )

        # Chunk 2: 10001..20000 (only if has_more)
        if r1["has_more"]:
            c2 = await fetch_json(
                client,
                "/get_last_trades_by_instrument",
                {
                    "instrument_name": instr,
                    "start_seq": 10001,
                    "end_seq": 20000,
                    "count": 10000,
                },
            )
            r2 = c2["result"]
            print(
                f"  Chunk [10001,20000]: {len(r2['trades'])} trades, has_more={r2['has_more']}"
            )

            # Check disjoint
            seqs1 = {t["trade_seq"] for t in r1["trades"]}
            seqs2 = {t["trade_seq"] for t in r2["trades"]}
            overlap = seqs1 & seqs2
            print(f"  Overlap: {len(overlap)} | Disjoint: {len(overlap) == 0}")

        # Final chunk (near last_seq)
        for start in range(1, last_seq + 1, CHUNK_SIZE):
            end = min(start + CHUNK_SIZE - 1, last_seq)
            if end == last_seq:
                c_last = await fetch_json(
                    client,
                    "/get_last_trades_by_instrument",
                    {
                        "instrument_name": instr,
                        "start_seq": start,
                        "end_seq": end,
                        "count": CHUNK_SIZE,
                    },
                )
                r_last = c_last["result"]
                cnt = len(r_last["trades"])
                hm = r_last["has_more"]
                finalize = (cnt >= CHUNK_SIZE) or not hm
                print(f"  Last chunk [{start},{end}]: {cnt} trades, has_more={hm}")
                print(f"    → finalize (count≥{CHUNK_SIZE} OR has_more=0): {finalize}")
                break
        await asyncio.sleep(0.3)


# ═══════════════════════════════════════════════════════
# TEST 2: Option streaming and has_more
# ═══════════════════════════════════════════════════════
async def test_option_streaming(client, currency="BTC"):
    """Verify option streaming logic: next_seq and has_more."""
    print("\n═══ OPTION STREAMING ═══")

    # Use ACTIVE options (they're more likely to have trades)
    data = await fetch_json(
        client,
        "/get_instruments",
        {"currency": currency, "kind": "option", "expired": "false"},
    )
    active = data["result"]
    print(f"  Active options: {len(active)}")

    # Also grab a few expired ones with trades
    data2 = await fetch_json(
        client,
        "/get_instruments",
        {"currency": currency, "kind": "option", "expired": "true"},
    )
    all_options = data2["result"]

    # Find options with trades (both active and expired)
    candidates = []
    for opt in active[:20]:  # Check first 20 active first
        d = await fetch_json(
            client,
            "/get_last_trades_by_instrument",
            {"instrument_name": opt["instrument_name"], "count": 1},
        )
        trades = d["result"].get("trades", [])
        if trades:
            candidates.append((opt["instrument_name"], trades[0]["trade_seq"], True))
        await asyncio.sleep(0.1)

    # Check expired options too (stop when we have enough)
    for opt in all_options:
        if len(candidates) >= 5:
            break
        if opt["is_active"]:
            continue  # Already checked active ones
        d = await fetch_json(
            client,
            "/get_last_trades_by_instrument",
            {"instrument_name": opt["instrument_name"], "count": 1},
        )
        trades = d["result"].get("trades", [])
        if trades:
            candidates.append((opt["instrument_name"], trades[0]["trade_seq"], False))
        await asyncio.sleep(0.1)

    if not candidates:
        print("  No options with trades found")
        return

    candidates.sort(key=lambda x: -x[1])
    for instr, last_seq, is_active in candidates[:3]:
        print(f"\n--- {instr} (active={is_active}, last_seq={last_seq}) ---")

        start = 1
        for i in range(min(3, (last_seq // CHUNK_SIZE) + 2)):  # Up to 3 chunks
            end = start + CHUNK_SIZE - 1
            d = await fetch_json(
                client,
                "/get_last_trades_by_instrument",
                {
                    "instrument_name": instr,
                    "start_seq": start,
                    "end_seq": end,
                    "count": CHUNK_SIZE,
                },
            )
            r = d["result"]
            trades = r["trades"]
            hm = r["has_more"]
            recent_seq = trades[0]["trade_seq"] if trades else "N/A"
            oldest_seq = trades[-1]["trade_seq"] if trades else "N/A"
            print(
                f"  Chunk [{start},{end}]: {len(trades)} trades, has_more={hm}, first_seq={recent_seq}"
            )

            # Test: next_seq = first_trade_seq + 1
            if trades:
                computed_next = trades[0]["trade_seq"] + 1
                print(
                    f"    → next_seq = {computed_next} (from first_seq={trades[0]['trade_seq']} + 1)"
                )
                # Verify by fetching
                next_end = computed_next + CHUNK_SIZE - 1
                d2 = await fetch_json(
                    client,
                    "/get_last_trades_by_instrument",
                    {
                        "instrument_name": instr,
                        "start_seq": computed_next,
                        "end_seq": next_end,
                        "count": CHUNK_SIZE,
                    },
                )
                r2 = d2["result"]
                if r2["trades"]:
                    gap = trades[0]["trade_seq"] - r2["trades"][0]["trade_seq"]
                    print(
                        f"    Next chunk: {len(r2['trades'])} trades, first_seq={r2['trades'][0]['trade_seq']}, gap={gap}"
                    )
                else:
                    print(f"    Next chunk: empty (reached end)")

                # Check should_continue: has_more or len(trades) >= CHUNK_SIZE
                should_continue = hm or len(trades) >= CHUNK_SIZE
                finished = not should_continue and not is_active
                print(
                    f"    should_continue={should_continue}, finished(expired)={finished}"
                )

                start = computed_next
                await asyncio.sleep(0.3)
            else:
                break

        await asyncio.sleep(0.3)


# ═══════════════════════════════════════════════════════
# TEST 3: Trade structure deep-dive
# ═══════════════════════════════════════════════════════
async def test_trade_structure(client, currency="BTC"):
    """Examine actual trade fields for both future and option."""
    print("\n═══ TRADE STRUCTURE ═══")
    for kind_name, kind in [("FUTURE", "future"), ("OPTION", "option")]:
        data = await fetch_json(
            client,
            "/get_instruments",
            {"currency": currency, "kind": kind, "expired": "true"},
        )

        # Get batch - just find an instrument with trades quickly
        for instr in data["result"][:50]:  # Check first 50
            d = await fetch_json(
                client,
                "/get_last_trades_by_instrument",
                {"instrument_name": instr["instrument_name"], "count": 5},
            )
            if d["result"]["trades"]:
                trade = d["result"]["trades"][0]
                print(f"\n  {kind_name} (from {instr['instrument_name']}):")
                for k, v in sorted(trade.items()):
                    print(f"    {k}: {repr(v)[:100]}")
                # Check field consistency
                all_trades = d["result"]["trades"]
                common_keys = set(all_trades[0].keys())
                for t in all_trades[1:]:
                    common_keys &= set(t.keys())
                print(f"    Consistent fields: {len(common_keys)}/{len(all_trades[0])}")
                break
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.3)


# ═══════════════════════════════════════════════════════
# TEST 4: has_more semantics - what does it actually mean?
# ═══════════════════════════════════════════════════════
async def test_has_more_semantics(client, currency="BTC"):
    """Check what has_more actually means: more in range, or more beyond?"""
    print("\n═══ HAS_MORE SEMANTICS ═══")

    # Get an ACTIVE future with lots of trades
    data = await fetch_json(
        client,
        "/get_instruments",
        {"currency": currency, "kind": "future", "expired": "false"},
    )
    for fut in data["result"]:
        instr = fut["instrument_name"]
        d = await fetch_json(
            client,
            "/get_last_trades_by_instrument",
            {"instrument_name": instr, "count": 1},
        )
        trades = d["result"].get("trades", [])
        if trades and trades[0]["trade_seq"] > 50000:
            last_seq = trades[0]["trade_seq"]
            print(f"\n  Instrument: {instr} (last_seq={last_seq})\n")

            # 1. Full range query (count = range size)
            r1 = await fetch_json(
                client,
                "/get_last_trades_by_instrument",
                {
                    "instrument_name": instr,
                    "start_seq": 1,
                    "end_seq": 10000,
                    "count": 10000,
                },
            )
            print(
                f"  [1,10000] count=10000: {len(r1['result']['trades'])} trades, has_more={r1['result']['has_more']}"
            )
            print(f"    → If has_more=True: means 'more trades exist beyond seq 10000'")
            print(
                f"    → If has_more=False: means 'all trades in [1,10000] returned, but more may exist beyond'"
            )

            # 2. Partial range query (count < range size)
            r2 = await fetch_json(
                client,
                "/get_last_trades_by_instrument",
                {
                    "instrument_name": instr,
                    "start_seq": 1,
                    "end_seq": 10000,
                    "count": 100,
                },
            )
            print(
                f"  [1,10000] count=100: {len(r2['result']['trades'])} trades, has_more={r2['result']['has_more']}"
            )
            print(
                f"    → If has_more=True here but False in #1: has_more = 'more in current range'"
            )
            print(
                f"    → If has_more=True in both: has_more = 'more beyond this range too'"
            )

            # 3. No range (default "most recent" mode)
            r3 = await fetch_json(
                client,
                "/get_last_trades_by_instrument",
                {"instrument_name": instr, "count": 100},
            )
            r3_trades = r3["result"].get("trades", [])
            print(
                f"  No start/end, count=100: {len(r3_trades)} trades, first_seq={r3_trades[0]['trade_seq'] if r3_trades else 'N/A'}, has_more={r3['result']['has_more']}"
            )

            # 4. Last chunk
            start = max(1, last_seq - CHUNK_SIZE + 1)
            r4 = await fetch_json(
                client,
                "/get_last_trades_by_instrument",
                {
                    "instrument_name": instr,
                    "start_seq": start,
                    "end_seq": last_seq,
                    "count": CHUNK_SIZE,
                },
            )
            print(
                f"  [{start},{last_seq}]: {len(r4['result']['trades'])} trades, has_more={r4['result']['has_more']}"
            )
            break
        await asyncio.sleep(0.15)


async def main():
    limits = httpx.Limits(
        max_connections=5, max_keepalive_connections=5, keepalive_expiry=30.0
    )
    async with httpx.AsyncClient(
        limits=limits, timeout=httpx.Timeout(60.0, connect=10.0)
    ) as client:
        print(f"Testing Deribit API at {BASE_URL}")
        print(f"CHUNK_SIZE = {CHUNK_SIZE}")

        await test_future_chunking(client)
        await test_option_streaming(client)
        await test_trade_structure(client)
        await test_has_more_semantics(client)

    print("\n\n══════════ ALL TESTS COMPLETED ══════════")


if __name__ == "__main__":
    asyncio.run(main())
