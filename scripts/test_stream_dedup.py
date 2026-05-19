"""Quick functional test for streaming + max_seqs dedup.

Simulates a single-instrument large file (the actual production case).
"""
import tempfile, json, pathlib
import polars as pl
from scripts.gen_parquet import _stream_batches

# Single instrument, 20 rows, monotonic trade_seq
rows = [
    json.dumps({"trade_seq": i, "instrument_name": "BTC-PERPETUAL", "trade_id": str(i), "timestamp": 1, "tick_direction": 0, "price": 100.0, "mark_price": 100.0, "iv": None, "index_price": 100.0, "direction": "buy", "amount": 1.0, "contracts": None, "block_trade_id": None, "block_rfq_id": None, "block_trade_leg_count": None, "combo_id": None, "combo_trade_id": None, "liquidation": None})
    for i in range(20)
]

with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
    f.write("\n".join(rows))
    f.flush()
    p = pathlib.Path(f.name)

# Simulate Phase 1 dedup logic (exactly as in gen_parquet.py)
max_seqs: dict[str, int] = {}
total_rows = 0
for fname, df, intra, instr, pos in _stream_batches(p, batch_size=5):
    max_seen = max_seqs.get(instr, -1)
    if max_seen >= 0:
        before = len(df)
        df = df.filter(pl.col("trade_seq").cast(pl.Int64) > max_seen)
        deduped = before - len(df)
        if deduped:
            print(f"  {fname}: {deduped} dups for {instr}")
    batch_max = df.select(pl.col("trade_seq").cast(pl.Int64).max()).item()
    if batch_max is not None and batch_max > max_seen:
        max_seqs[instr] = batch_max
    total_rows += len(df)

assert total_rows == 20, f"Expected 20 rows, got {total_rows}"
assert max_seqs == {"BTC-PERPETUAL": 19}, f"Unexpected max_seqs: {max_seqs}"
print(f"PASS: {total_rows} rows, max_seqs={max_seqs}")
import os; os.unlink(p.name)
