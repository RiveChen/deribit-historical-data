"""Final scan: check active instruments for ALL fields comprehensively."""

import asyncio
import httpx

BASE_URL = "https://history.deribit.com/api/v2/public"


async def fetch_json(client, endpoint, params):
    resp = await client.get(f"{BASE_URL}{endpoint}", params=params)
    resp.raise_for_status()
    return resp.json()


async def main():
    limits = httpx.Limits(max_connections=5, max_keepalive_connections=5)
    async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(60.0)) as client:
        seen = set()
        field_types = {}
        sample_values = {}
        total_trades = 0

        for kind in ("future", "option"):
            data = await fetch_json(
                client,
                "/get_instruments",
                {"currency": "BTC", "kind": kind, "expired": "false"},
            )
            active = data["result"]
            print(f"Scanning {len(active)} active {kind}s...")

            for instr_obj in active[:20]:
                instr = instr_obj["instrument_name"]
                start_seq = 1
                while total_trades < 5000:
                    d = await fetch_json(
                        client,
                        "/get_last_trades_by_instrument",
                        {
                            "instrument_name": instr,
                            "start_seq": start_seq,
                            "count": 100,
                            "end_seq": start_seq + 9999,
                        },
                    )
                    chunk = d["result"].get("trades", [])
                    if not chunk:
                        break
                    for t in chunk:
                        total_trades += 1
                        seen.update(t.keys())
                        for k, v in t.items():
                            if k not in field_types:
                                field_types[k] = type(v)
                                sample_values[k] = f"{repr(v)[:100]}"
                    if not d["result"].get("has_more", False):
                        break
                    start_seq = chunk[-1]["trade_seq"] + 1
                    await asyncio.sleep(0.03)
                    if total_trades >= 5000:
                        break

        print(f"\nScanned {total_trades} trades total.")
        print(f"\nALL FIELDS ({len(seen)} total):")
        for k in sorted(seen):
            tname = field_types.get(k, "?").__name__
            val = sample_values.get(k, "N/A")
            print(f"  {k:30s}  {tname:12s}  e.g. {val}")

        print(f"\nPL SCHEMA:")
        for k in sorted(seen):
            tname = field_types.get(k, "?").__name__
            pl_map = {
                int: "pl.Int64",
                float: "pl.Float64",
                str: "pl.String",
                bool: "pl.Boolean",
                type(None): "pl.Float64  # nullable",
            }
            pt = pl_map.get(tname, f"# UNKNOWN: {tname}")
            print(f'    "{k}": {pt},')

        targets = ["mmp", "fee_currency", "block_rfq_id", "block_trade_leg_count"]
        for t in targets:
            status = sample_values.get(t, None)
            print(f"\n  {t}: {'FOUND = ' + repr(status) if status else 'NOT FOUND'}")


asyncio.run(main())
