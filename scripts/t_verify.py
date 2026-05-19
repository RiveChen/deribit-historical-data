import tempfile, json, pathlib
import polars as pl
from scripts.gen_parquet import _stream_batches

rows = [json.dumps({"trade_seq": i, "instrument_name": "BTC-PERPETUAL", "trade_id": str(i), "timestamp": 1, "tick_direction": 0, "price": 100.0, "mark_price": 100.0, "iv": None, "index_price": 100.0, "direction": "buy", "amount": 1.0, "contracts": None, "block_trade_id": None, "block_rfq_id": None, "block_trade_leg_count": None, "combo_id": None, "combo_trade_id": None, "liquidation": None}) for i in range(20)]

with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
    f.write("\n".join(rows))
    f.flush()
    p = pathlib.Path(f.name)

max_seqs = {}
total = 0
for fname, df, intra, instr, pos in _stream_batches(p, batch_size=5):
    max_seen = max_seqs.get(instr, -1)
    if max_seen >= 0:
        before = len(df)
        df = df.filter(pl.col("trade_seq").cast(pl.Int64) > max_seen)
        deduped = before - len(df)
        if deduped:
            print(f"  dup {deduped} for {instr}")
    batch_max = df.select(pl.col("trade_seq").cast(pl.Int64).max()).item()
    if batch_max is not None and batch_max > max_seen:
        max_seqs[instr] = batch_max
    total += len(df)

assert total == 20, f"fail {total}"
assert max_seqs == {"BTC-PERPETUAL": 19}, f"fail {max_seqs}"
print(f"PASS: {total} rows, max_seqs={max_seqs}")
import os; os.unlink(p.name)
os.unlink(__file__)
