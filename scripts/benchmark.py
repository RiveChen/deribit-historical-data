"""
Benchmark harness for gen_parquet.py — turns README performance claims into
real, reproducible numbers.

It measures, for the ACTUAL code paths in scripts/gen_parquet.py:
  * throughput           — input rows/s and input MB/s
  * thread-pool scaling  — small-file phase at workers = 1, 2, 4, 8, ...
  * dedup cost           — dedup on vs off
  * codec tradeoff       — zstd vs lz4 (--fast)
  * streaming batch size — large-file phase at different --stream-batch-size
  * compression ratio    — JSONL bytes / Parquet bytes
  * peak memory (RSS)    — each case runs in an isolated subprocess so the
                           large-file streaming path's low memory is provable

Data source:
  --data-dir PATH   benchmark your REAL JSONL (best, most credible numbers)
  (default)         generate synthetic Deribit-like JSONL if no --data-dir

Examples
--------
# Quick synthetic smoke run (seconds):
python scripts/benchmark.py --quick

# Realistic synthetic run:
python scripts/benchmark.py --small-files 300 --small-rows 8000 \
                            --large-rows 3000000 --dup-rate 0.02

# Benchmark your real downloaded data:
python scripts/benchmark.py --data-dir data/BTC/option
python scripts/benchmark.py --data-dir data/BTC/future --large-threshold-mb 100

Outputs: prints a Markdown table and writes benchmark_results/{results.json,BENCHMARK.md}.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import platform
import random
import shutil
import sys
import time
from pathlib import Path

import orjson

# --- make the repo's own modules importable regardless of CWD ----------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))
sys.path.insert(0, str(_REPO_ROOT / "scripts"))


# =============================================================================
# Synthetic data generation
# =============================================================================

_INSTR_PREFIX = "BTC"


def _make_row(instr: str, seq: int, ts: int) -> dict:
    """A single trade dict matching gen_parquet's COMPREHENSIVE_SCHEMA fields."""
    return {
        "trade_seq": seq,
        "trade_id": f"{instr}-{seq}",
        "timestamp": ts,
        "tick_direction": seq % 4,
        "price": round(30000 + (seq % 5000) * 0.5, 2),
        "mark_price": round(30000 + (seq % 5000) * 0.5, 2),
        "iv": round((seq % 100) * 0.7, 2),
        "instrument_name": instr,
        "index_price": round(30000 + (seq % 3000) * 0.4, 2),
        "direction": "buy" if seq % 2 else "sell",
        "amount": round((seq % 50 + 1) * 10.0, 1),
        "contracts": float(seq % 50 + 1),
        "block_trade_id": None,
        "block_rfq_id": None,
        "block_trade_leg_count": None,
        "combo_id": None,
        "combo_trade_id": None,
        "liquidation": None,
    }


def _write_jsonl(path: Path, instr: str, n_rows: int, dup_rate: float, rng: random.Random) -> tuple[int, int]:
    """Write one instrument file with monotonic trade_seq.

    Duplicates simulate Deribit chunk-boundary overlap: a fraction of rows are
    re-emitted. Returns (rows_written_including_dups, unique_rows).
    """
    ts0 = 1_600_000_000_000
    written = 0
    with open(path, "wb") as fh:
        buf = []
        for seq in range(1, n_rows + 1):
            row = _make_row(instr, seq, ts0 + seq * 1000)
            buf.append(orjson.dumps(row))
            written += 1
            if dup_rate and rng.random() < dup_rate:
                buf.append(orjson.dumps(row))  # duplicate
                written += 1
            if len(buf) >= 50_000:
                fh.write(b"\n".join(buf) + b"\n")
                buf.clear()
        if buf:
            fh.write(b"\n".join(buf) + b"\n")
    return written, n_rows


def generate_synth(
    out_dir: Path,
    n_small: int,
    small_rows: int,
    large_rows: int,
    dup_rate: float,
    seed: int = 7,
) -> dict:
    """Generate synthetic JSONL. Returns stats dict."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    total_written = total_unique = 0

    for i in range(n_small):
        instr = f"{_INSTR_PREFIX}-{i:04d}-OPT"
        w, u = _write_jsonl(out_dir / f"{instr}.jsonl", instr, small_rows, dup_rate, rng)
        total_written += w
        total_unique += u

    if large_rows > 0:
        instr = f"{_INSTR_PREFIX}-PERPETUAL"
        w, u = _write_jsonl(out_dir / f"{instr}.jsonl", instr, large_rows, dup_rate, rng)
        total_written += w
        total_unique += u

    input_bytes = sum(f.stat().st_size for f in out_dir.glob("*.jsonl"))
    return {
        "input_rows": total_written,
        "unique_rows": total_unique,
        "input_bytes": input_bytes,
        "n_files": len(list(out_dir.glob("*.jsonl"))),
    }


def scan_dir_stats(data_dir: Path) -> dict:
    """Compute stats for a real/pre-existing JSONL directory (rows counted cheaply)."""
    files = list(data_dir.glob("*.jsonl"))
    input_bytes = sum(f.stat().st_size for f in files)
    # Count rows by counting newlines (fast, buffered).
    rows = 0
    for f in files:
        with open(f, "rb") as fh:
            while True:
                block = fh.read(1024 * 1024)
                if not block:
                    break
                rows += block.count(b"\n")
    return {"input_rows": rows, "unique_rows": None, "input_bytes": input_bytes, "n_files": len(files)}


# =============================================================================
# One benchmark case, executed in an isolated subprocess (for clean peak RSS)
# =============================================================================


def _case_worker(params: dict, q: "mp.Queue") -> None:
    # Always put exactly one message so the parent never deadlocks, even on error.
    try:
        import resource  # POSIX only; peak RSS of THIS process
        import pyarrow.parquet as pq
        from gen_parquet import generate_parquet  # noqa: import from scripts/

        data_dir = Path(params["data_dir"])
        out_file = Path(params["out_file"])
        if out_file.exists():
            out_file.unlink()

        t0 = time.perf_counter()
        generate_parquet(
            data_dir=data_dir,
            output_file=out_file,
            dedup=params["dedup"],
            workers=params["workers"],
            fast=params["fast"],
            large_file_threshold=params["large_threshold"],
            stream_batch_size=params["stream_batch_size"],
        )
        elapsed = time.perf_counter() - t0

        out_bytes = out_file.stat().st_size if out_file.exists() else 0
        out_rows = pq.ParquetFile(out_file).metadata.num_rows if out_bytes else 0
        peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports bytes, Linux reports KB.
        peak_mb = peak_kb / (1024 * 1024) if sys.platform == "darwin" else peak_kb / 1024

        q.put({"elapsed": elapsed, "out_bytes": out_bytes, "out_rows": out_rows, "peak_mb": peak_mb})
    except Exception as e:  # noqa: BLE001
        import traceback
        q.put({"error": f"{e!r}", "trace": traceback.format_exc()})


def run_case(label: str, params: dict, input_bytes: int, input_rows: int) -> dict:
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=_case_worker, args=(params, q))
    p.start()
    try:
        res = q.get(timeout=params.get("case_timeout", 3600))
    except Exception:  # queue empty -> child was killed (e.g. OOM)
        res = {"error": f"no result (exit code {p.exitcode})"}
    p.join(timeout=10)
    if p.is_alive():
        p.terminate()

    if "error" in res:
        print(f"   FAILED: {res['error']}")
        if res.get("trace"):
            print(res["trace"])
        return {
            "case": label, "workers": params["workers"], "dedup": params["dedup"],
            "codec": "lz4" if params["fast"] else "zstd",
            "stream_batch_size": params["stream_batch_size"],
            "wall_s": 0, "in_rows_per_s": 0, "in_mb_per_s": 0, "out_rows": 0,
            "in_mb": round(input_bytes / (1024 * 1024), 1), "out_mb": 0,
            "compression_x": 0, "peak_rss_mb": 0, "error": res["error"],
        }

    elapsed = res["elapsed"]
    in_mb = input_bytes / (1024 * 1024)
    out_mb = res["out_bytes"] / (1024 * 1024)
    return {
        "case": label,
        "workers": params["workers"],
        "dedup": params["dedup"],
        "codec": "lz4" if params["fast"] else "zstd",
        "stream_batch_size": params["stream_batch_size"],
        "wall_s": round(elapsed, 3),
        "in_rows_per_s": int(input_rows / elapsed) if elapsed else 0,
        "in_mb_per_s": round(in_mb / elapsed, 1) if elapsed else 0,
        "out_rows": res["out_rows"],
        "in_mb": round(in_mb, 1),
        "out_mb": round(out_mb, 2),
        "compression_x": round(in_mb / out_mb, 2) if out_mb else 0,
        "peak_rss_mb": round(res["peak_mb"], 1),
    }


# =============================================================================
# Orchestration
# =============================================================================


def build_matrix(args, threshold_bytes: int) -> list[tuple[str, dict]]:
    """Define which configurations to benchmark."""
    base = dict(
        data_dir=str(args._data_dir),
        out_file=str(args._work / "bench.parquet"),
        large_threshold=threshold_bytes,
        stream_batch_size=args.stream_batch_size,
    )

    def cfg(**over):
        c = dict(base)
        c.update(dict(dedup=True, workers=args.default_workers, fast=False))
        c.update(over)
        return c

    matrix: list[tuple[str, dict]] = []

    # Thread-pool scaling (small-file phase)
    worker_grid = [1, 2, 4, 8] if not args.quick else [1, 4]
    worker_grid = [w for w in worker_grid if w <= (os.cpu_count() or 4)] or [1]
    for w in worker_grid:
        matrix.append((f"workers={w}, dedup=on, zstd", cfg(workers=w)))

    # Dedup cost
    matrix.append(("dedup=OFF, zstd", cfg(dedup=False)))

    # Codec tradeoff
    matrix.append(("dedup=on, lz4 (--fast)", cfg(fast=True)))

    # Streaming batch size (only meaningful if a large file exists)
    if not args.quick and args.large_rows > 0:
        for bs in (50_000, 200_000):
            matrix.append((f"stream_batch={bs}, dedup=on", cfg(stream_batch_size=bs)))

    return matrix


def render_markdown(env: dict, data_stats: dict, results: list[dict]) -> str:
    lines = []
    lines.append("# gen_parquet Benchmark Results\n")
    lines.append(
        f"_Machine: {env['cpu']} · {env['cores']} cores · {env['platform']} · "
        f"Python {env['python']} · polars {env['polars']} · pyarrow {env['pyarrow']}_\n"
    )
    src = data_stats.get("source", "synthetic")
    lines.append(
        f"**Input:** {data_stats['n_files']:,} JSONL files · "
        f"{data_stats['input_rows']:,} rows · "
        f"{data_stats['input_bytes'] / (1024*1024):.1f} MB ({src})\n"
    )
    lines.append("| Case | Wall (s) | Rows/s | MB/s in | Parquet MB | Compress× | Peak RSS MB |")
    lines.append("|------|---------:|-------:|--------:|-----------:|----------:|------------:|")
    for r in results:
        lines.append(
            f"| {r['case']} | {r['wall_s']:.2f} | {r['in_rows_per_s']:,} | "
            f"{r['in_mb_per_s']} | {r['out_mb']} | {r['compression_x']} | {r['peak_rss_mb']} |"
        )
    lines.append("")
    lines.append(
        "> Notes: `Rows/s` and `MB/s in` are measured against input (pre-dedup) size. "
        "`Compress×` = input JSONL bytes / output Parquet bytes. "
        "`Peak RSS` is the isolated subprocess high-water mark."
    )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", type=str, default=None, help="Benchmark real JSONL here instead of synthetic.")
    ap.add_argument("--quick", action="store_true", help="Tiny synthetic run for a smoke test.")
    ap.add_argument("--small-files", type=int, default=200, help="Synthetic small (option-like) files.")
    ap.add_argument("--small-rows", type=int, default=5000, help="Rows per small file.")
    ap.add_argument("--large-rows", type=int, default=800_000, help="Rows in the one large (perpetual-like) file; 0 to skip.")
    ap.add_argument("--dup-rate", type=float, default=0.01, help="Fraction of duplicated rows (chunk-overlap simulation).")
    ap.add_argument("--large-threshold-mb", type=float, default=100.0, help="Files >= this size use the streaming path.")
    ap.add_argument("--stream-batch-size", type=int, default=200_000, help="Default streaming batch size (rows).")
    ap.add_argument("--default-workers", type=int, default=min(8, os.cpu_count() or 4), help="Worker count for non-scaling cases.")
    ap.add_argument("--out-dir", type=str, default="benchmark_results", help="Where to write results.")
    args = ap.parse_args()

    if args.quick:
        args.small_files, args.small_rows, args.large_rows, args.large_threshold_mb = 20, 500, 40_000, 5.0

    work = Path(args.out_dir)
    work.mkdir(parents=True, exist_ok=True)
    args._work = work
    threshold_bytes = int(args.large_threshold_mb * 1024 * 1024)

    # --- assemble input --------------------------------------------------
    if args.data_dir:
        data_dir = Path(args.data_dir)
        if not data_dir.exists():
            ap.error(f"--data-dir not found: {data_dir}")
        print(f"Scanning real data in {data_dir} ...")
        data_stats = scan_dir_stats(data_dir)
        data_stats["source"] = f"real: {data_dir}"
        args._data_dir = data_dir
    else:
        synth_dir = work / "synth_jsonl"
        print(
            f"Generating synthetic data: {args.small_files} small files x {args.small_rows} rows"
            + (f" + 1 large file x {args.large_rows} rows" if args.large_rows else "")
            + f" (dup-rate {args.dup_rate}) ..."
        )
        data_stats = generate_synth(
            synth_dir, args.small_files, args.small_rows, args.large_rows, args.dup_rate
        )
        data_stats["source"] = "synthetic"
        args._data_dir = synth_dir

    print(
        f"Input ready: {data_stats['n_files']} files, {data_stats['input_rows']:,} rows, "
        f"{data_stats['input_bytes'] / (1024*1024):.1f} MB\n"
    )

    # --- run matrix ------------------------------------------------------
    matrix = build_matrix(args, threshold_bytes)
    results = []
    for label, params in matrix:
        print(f"→ {label} ...", flush=True)
        r = run_case(label, params, data_stats["input_bytes"], data_stats["input_rows"])
        print(
            f"   {r['wall_s']:.2f}s · {r['in_rows_per_s']:,} rows/s · "
            f"{r['in_mb_per_s']} MB/s · {r['compression_x']}x smaller · peak {r['peak_rss_mb']} MB"
        )
        results.append(r)

    # --- environment + write --------------------------------------------
    import polars as pl
    import pyarrow as pa

    env = {
        "cpu": platform.processor() or platform.machine(),
        "cores": os.cpu_count(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "polars": pl.__version__,
        "pyarrow": pa.__version__,
    }

    (work / "results.json").write_bytes(
        orjson.dumps({"env": env, "input": data_stats, "results": results}, option=orjson.OPT_INDENT_2)
    )
    md = render_markdown(env, data_stats, results)
    (work / "BENCHMARK.md").write_text(md)

    print("\n" + md)
    print(f"\nWrote {work/'results.json'} and {work/'BENCHMARK.md'}")


if __name__ == "__main__":
    main()
