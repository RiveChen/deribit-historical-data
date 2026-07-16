"""Tests for parquet.py: dedup functions, gap bucketing, and parallel reading.

All tests operate on in-memory Polars DataFrames — no I/O, no network.
"""

import os
import tempfile
from pathlib import Path

import polars as pl
import pytest

from deribit_fetcher.parquet import (
    _split_blocks,
    dedup_cross_batch,
    dedup_cross_file,
    dedup_intra,
    parallel_read_large_file,
    stream_batches,
)

# =============================================================================
# Helpers
# =============================================================================

N_BUCKETS = 10


def _df(seqs: list[int], instr: str = "BTC-TEST") -> pl.DataFrame:
    """Build a minimal DataFrame with instrument_name and trade_seq columns."""
    return pl.DataFrame({"instrument_name": [instr] * len(seqs), "trade_seq": seqs})


def _compute_bucket(trade_seq: int, seq_min: int, expected_total: int) -> int:
    """Replicate the exact bucket formula from _show_gap_histogram.

    Uses integer arithmetic to avoid floating-point drift:
        bucket = (trade_seq - seq_min) * N_BUCKETS // expected_total
    """
    return (trade_seq - seq_min) * N_BUCKETS // expected_total


def _bucket_counts(df: pl.DataFrame, seq_min: int, expected_total: int) -> dict[int, int]:
    """Apply the same bucket expression as _show_gap_histogram and return per-bucket counts."""
    result = (
        df.lazy()
        .with_columns(
            (pl.col("trade_seq") - seq_min)
            .cast(pl.Int64)
            .mul(N_BUCKETS)
            .truediv(expected_total)
            .floor()
            .cast(pl.UInt32)
            .clip(0, N_BUCKETS - 1)
            .alias("bucket")
        )
        .group_by("bucket")
        .len()
        .collect()
    )
    return dict(result.iter_rows())


def _expected_counts_of_full(expected_total: int, seq_min: int = 1) -> dict[int, int]:
    """Return the ideal per-bucket row count for a full (gapless) sequence."""
    result = (
        _df(list(range(seq_min, seq_min + expected_total)))
        .lazy()
        .with_columns(
            (pl.col("trade_seq") - seq_min)
            .cast(pl.Int64)
            .mul(N_BUCKETS)
            .truediv(expected_total)
            .floor()
            .cast(pl.UInt32)
            .clip(0, N_BUCKETS - 1)
            .alias("bucket")
        )
        .group_by("bucket")
        .len()
        .collect()
    )
    return dict(result.iter_rows())


# =============================================================================
# dedup_intra — file-level / batch-level dedup
# =============================================================================


class TestDedupIntra:
    """Remove duplicate (instrument_name, trade_seq) rows within a single file."""

    def test_removes_exact_duplicates(self):
        """Same instrument + same trade_seq should be deduplicated (keep first)."""
        df = _df([1, 2, 2, 3, 3, 3])
        result, removed = dedup_intra(df)
        assert sorted(result["trade_seq"].to_list()) == [1, 2, 3], (
            "Unique trade_seqs should remain (order not guaranteed by unique())"
        )
        assert removed == 3

    def test_no_duplicates_returns_same(self):
        """All trade_seqs unique -> df unchanged, removed=0."""
        df = _df([1, 2, 3, 4, 5])
        result, removed = dedup_intra(df)
        assert result.shape == df.shape
        assert removed == 0

    def test_multiple_instruments_dedup_separately(self):
        """Dedup is per (instrument_name, trade_seq) — different instrs don't collide."""
        df = pl.DataFrame(
            {
                "instrument_name": ["BTC-A", "BTC-A", "BTC-B", "BTC-B", "BTC-A"],
                "trade_seq": [1, 1, 1, 2, 2],
            }
        )
        result, removed = dedup_intra(df)
        # BTC-A: (1, 2) unique; BTC-B: (1, 2) unique -> 4 unique combos
        assert result.shape == (4, 2), "Each unique (instr, seq) pair should survive"
        assert removed == 1

    def test_empty_dataframe(self):
        """Empty DataFrame -> removed=0."""
        df = pl.DataFrame(
            {"instrument_name": [], "trade_seq": []},
            schema={"instrument_name": pl.Utf8, "trade_seq": pl.Int64},
        )
        result, removed = dedup_intra(df)
        assert result.is_empty()
        assert removed == 0


# =============================================================================
# dedup_cross_batch — stream cross-batch dedup via trade_seq > max_seen
# =============================================================================


class TestDedupCrossBatch:
    """Remove rows where trade_seq <= max_seen (boundary is critical)."""

    def test_removes_leq_max_seen(self):
        """trade_seq <= 5 should be removed; trade_seq > 5 should survive."""
        df = _df([3, 5, 7, 10])
        result, removed = dedup_cross_batch(df, max_seen=5)
        assert result["trade_seq"].to_list() == [7, 10]
        assert removed == 2

    def test_boundary_seq_equals_max_seen(self):
        """trade_seq == max_seen MUST be removed (not retained)."""
        df = _df([5])
        result, removed = dedup_cross_batch(df, max_seen=5)
        assert result.is_empty(), "trade_seq=5 should be removed when max_seen=5"
        assert removed == 1

    def test_all_above_max_seen(self):
        """All trade_seqs > max_seen -> nothing removed."""
        df = _df([10, 20, 30])
        result, removed = dedup_cross_batch(df, max_seen=5)
        assert result.shape == df.shape
        assert removed == 0

    def test_all_below_max_seen(self):
        """All trade_seqs <= max_seen -> everything removed."""
        df = _df([1, 2, 3])
        result, removed = dedup_cross_batch(df, max_seen=5)
        assert result.is_empty()
        assert removed == 3

    def test_empty_dataframe(self):
        """Empty DataFrame -> removed=0."""
        df = _df([])
        result, removed = dedup_cross_batch(df, max_seen=5)
        assert result.is_empty()
        assert removed == 0


# =============================================================================
# dedup_cross_file — cross-file dedup via seen_key set
# =============================================================================


class TestDedupCrossFile:
    """Remove rows whose trade_seq exists in seen_keys."""

    def test_removes_seen_seqs(self):
        """trade_seq in seen_keys should be filtered out."""
        df = _df([1, 2, 3, 4])
        result, removed = dedup_cross_file(df, seen_keys={1, 3})
        assert result["trade_seq"].to_list() == [2, 4]
        assert removed == 2

    def test_seen_keys_none(self):
        """seen_keys=None -> nothing removed."""
        df = _df([1, 2, 3])
        result, removed = dedup_cross_file(df, seen_keys=None)
        assert result.shape == df.shape
        assert removed == 0

    def test_seen_keys_empty_set(self):
        """seen_keys=set() -> nothing removed."""
        df = _df([1, 2, 3])
        result, removed = dedup_cross_file(df, seen_keys=set())
        assert result.shape == df.shape
        assert removed == 0

    def test_all_seqs_seen(self):
        """All trade_seqs already seen -> everything removed."""
        df = _df([1, 2, 3])
        result, removed = dedup_cross_file(df, seen_keys={1, 2, 3})
        assert result.is_empty()
        assert removed == 3

    def test_empty_dataframe(self):
        """Empty DataFrame -> removed=0."""
        df = _df([])
        result, removed = dedup_cross_file(df, seen_keys={1, 2})
        assert result.is_empty()
        assert removed == 0

    def test_no_overlap_with_seen_keys(self):
        """No trade_seq in seen_keys -> nothing removed."""
        df = _df([10, 20, 30])
        result, removed = dedup_cross_file(df, seen_keys={1, 2})
        assert result.shape == df.shape
        assert removed == 0


# =============================================================================
# Combined scenarios — simulate real pipeline
# =============================================================================


class TestCombinedDedup:
    """Real-world scenario: intra-file then cross-file dedup sequencing."""

    def test_intra_then_cross_file(self):
        """Simulate small-file pipeline: intra-dedup first, then cross-file."""
        df1 = _df([1, 2, 2, 3])  # seq 2 duplicated
        df1, _intra = dedup_intra(df1)

        seen: set[int] = set()
        df1, _cross = dedup_cross_file(df1, seen)
        seen.update(df1["trade_seq"].to_list())
        assert sorted(df1["trade_seq"].to_list()) == [1, 2, 3]

        df2 = _df([2, 3, 4, 5])
        df2, _intra = dedup_intra(df2)
        df2, _cross = dedup_cross_file(df2, seen)
        assert sorted(df2["trade_seq"].to_list()) == [4, 5]


# =============================================================================
# Gap histogram bucketing — regression tests for integer arithmetic
# =============================================================================


class TestGapHistogram:
    """Test the per-bucket deficit computation used in gap histograms."""

    def test_gapless_even(self):
        """Expected_total=100, N_BUCKETS=10 — each bucket should have 10 rows, deficit=0."""
        seq_min = 1
        expected_total = 100
        df = _df(list(range(seq_min, seq_min + expected_total)))
        counts = _bucket_counts(df, seq_min, expected_total)
        expected = _expected_counts_of_full(expected_total)
        for b in range(N_BUCKETS):
            deficit = expected[b] - counts.get(b, 0)
            exp = expected[b]
            got = counts.get(b, 0)
            assert deficit == 0, f"Bucket {b}: deficit={deficit}, expected {exp} got {got}"

    def test_gapless_with_remainder(self):
        """Expected_total=103, N_BUCKETS=10 — verify buckets sum to total and no row is lost."""
        seq_min = 1
        expected_total = 103
        df = _df(list(range(seq_min, seq_min + expected_total)))
        counts = _bucket_counts(df, seq_min, expected_total)
        total_in_buckets = sum(counts.values())
        assert total_in_buckets == expected_total, (
            f"Total rows in buckets {total_in_buckets} != {expected_total}"
        )
        assert set(counts.keys()) == set(range(N_BUCKETS)), "All 10 buckets should have data"

    def test_gap_in_middle_bucket(self):
        """Remove seq 21-30 (bucket 2) — only bucket 2 should have deficit."""
        seq_min = 1
        expected_total = 100
        missing = set(range(21, 31))
        present = sorted(set(range(seq_min, seq_min + expected_total)) - missing)
        df = _df(present)
        counts = _bucket_counts(df, seq_min, expected_total)
        expected = _expected_counts_of_full(expected_total)
        for b in range(N_BUCKETS):
            deficit = expected[b] - counts.get(b, 0)
            if b == 2:
                assert deficit == 10, f"Bucket {b}: should have 10 gaps, got {deficit}"
            else:
                exp = expected[b]
                got = counts.get(b, 0)
                assert deficit == 0, f"Bucket {b}: unexpected {deficit}, expected {exp} got {got}"

    def test_gap_spans_two_buckets(self):
        """Remove seq 46-55 — overlaps buckets 4 and 5 (partial)."""
        seq_min = 1
        expected_total = 100
        full = set(range(seq_min, seq_min + expected_total))
        missing = set(range(46, 56))
        present = sorted(full - missing)
        df = _df(present)
        counts = _bucket_counts(df, seq_min, expected_total)
        assert counts.get(4, 0) == 5, "Bucket 4 should have 5 rows (lost 5)"
        assert counts.get(5, 0) == 5, "Bucket 5 should have 5 rows (lost 5)"

    def test_boundary_seq_min_in_bucket_zero(self):
        """seq_min should always map to bucket 0."""
        for seq_min in [1, 0, 1000]:
            for expected_total in [50, 100, 501]:
                bucket = _compute_bucket(seq_min, seq_min, expected_total)
                assert bucket == 0, (
                    f"seq_min={seq_min}, expected_total={expected_total}: "
                    f"expected bucket 0, got {bucket}"
                )

    def test_boundary_seq_max_in_last_bucket(self):
        """seq_max (seq_min + expected_total - 1) should map to bucket 9."""
        for seq_min in [1, 0, 1000]:
            for expected_total in [50, 100, 501]:
                seq_max = seq_min + expected_total - 1
                bucket = _compute_bucket(seq_max, seq_min, expected_total)
                assert bucket == N_BUCKETS - 1, (
                    f"seq_max={seq_max}, seq_min={seq_min}, "
                    f"expected_total={expected_total}: "
                    f"expected bucket {N_BUCKETS - 1}, got {bucket}"
                )

    def test_no_float_drift_at_large_numbers(self):
        """Large expected_total should not cause boundary mis-bucketing."""
        seq_min = 1
        expected_total = 50001
        cases = [
            (1, 0),
            (5000, 0),
            (5001, 0),
            (expected_total, N_BUCKETS - 1),
            (expected_total - 1, N_BUCKETS - 1),
        ]
        for seq, expected_bucket in cases:
            bucket = _compute_bucket(seq, seq_min, expected_total)
            assert bucket == expected_bucket, (
                f"seq={seq}, seq_min={seq_min}, expected_total={expected_total}: "
                f"expected bucket {expected_bucket}, got {bucket} — "
                f"possible float drift!"
            )

    def test_bucket_monotonic_not_decreasing(self):
        """bucket(trade_seq) must be non-decreasing as trade_seq increases."""
        seq_min = 0
        expected_total = 50001
        prev = -1
        for seq in range(seq_min, seq_min + expected_total, 100):
            b = _compute_bucket(seq, seq_min, expected_total)
            assert b >= prev, f"bucket decreased: seq={seq}, bucket={b} < prev={prev}"
            prev = b


# =============================================================================
# Parallel reading — ProcessPoolExecutor block splitting and correctness
# =============================================================================


class TestSplitBlocks:
    r"""Test _split_blocks which divides a file into \\n-aligned byte ranges."""

    def test_single_line(self):
        """A single-line file produces one block."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write('{"trade_seq":1,"instrument_name":"BTC-TEST"}\n')
            tmp = Path(f.name)
        try:
            blocks = _split_blocks(tmp, 10)
            assert len(blocks) == 1
            assert blocks[0][0] == 0, "Should start at offset 0"
            assert blocks[0][1] > 0, "Should cover the entire file"
        finally:
            os.unlink(tmp)

    def test_split_at_exact_newline(self):
        r"""When block_bytes aligns exactly with \\n boundaries, should split cleanly."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            # Each line is about 70 bytes, write 5 lines
            for i in range(1, 6):
                f.write(b'{"trade_seq":%d,"instrument_name":"BTC-TEST","price":50000.0}\n' % i)
            tmp = Path(f.name)
        try:
            # small block_bytes so we get multiple blocks
            blocks = _split_blocks(tmp, 100)
            assert len(blocks) >= 2, "Should split into at least 2 blocks"
            # each block's start is 0 (file start) or follows a \n
            for start, _end in blocks:
                if start > 0:
                    with open(tmp, "rb") as fh:
                        fh.seek(start - 1)
                        prev_byte = fh.read(1)
                        assert prev_byte == b"\n", (
                            f"Block at offset {start} should follow a newline"
                        )
        finally:
            os.unlink(tmp)

    def test_empty_file(self):
        """An empty file produces no blocks."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            tmp = Path(f.name)
        try:
            blocks = _split_blocks(tmp, 10)
            assert blocks == []
        finally:
            os.unlink(tmp)


class TestParallelRead:
    """Parallel reading should produce the same result as serial streaming."""

    @pytest.fixture
    def jsonl_file(self):
        """Create a temp JSONL file suitable for dedup tests."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            # Write trades with monotonic seq; include some intentional intra-file dups
            instr = "BTC-TEST"
            for seq in range(1, 51):
                f.write(
                    f'{{"trade_seq":{seq},"instrument_name":"{instr}","price":50000,"timestamp":1}}\n'  # noqa: UP031
                )
            for seq in (5, 10, 15):
                f.write(
                    f'{{"trade_seq":{seq},"instrument_name":"{instr}","price":50000,"timestamp":1}}\n'  # noqa: UP031
                )
            tmp = Path(f.name)
        yield tmp
        os.unlink(tmp)

    def _collect(self, gen):
        """Helper: iterate a generator and return sorted trade_seqs."""
        all_seqs = []
        for _fname, df, _intra, _instr, _pos in gen:
            if df is not None and not df.is_empty():
                all_seqs.extend(df["trade_seq"].to_list())
        return sorted(all_seqs)

    def test_parallel_matches_serial(self, jsonl_file):
        """Parallel reading must produce the same unique deduped seq set as serial streaming."""
        # stream_batches only does intra-batch dedup; parallel does cross-block too.
        # Compare unique seqs which must match.
        serial_unique = []
        for _fname, df, _intra, _instr, _pos in stream_batches(jsonl_file, batch_size=10):
            if df is not None and not df.is_empty():
                serial_unique.extend(df["trade_seq"].to_list())
        serial_set = set(serial_unique)

        parallel_unique = []
        for _fname, df, _intra, _instr, _pos in parallel_read_large_file(
            jsonl_file, block_bytes=200, workers=2
        ):
            if df is not None and not df.is_empty():
                parallel_unique.extend(df["trade_seq"].to_list())
        parallel_set = set(parallel_unique)

        assert serial_set == parallel_set, "Unique seqs from parallel and serial must match"
        assert len(parallel_unique) == 50, f"Expected 50 unique, got {len(parallel_unique)}"

    def test_parallel_dedup_no_dupes(self, jsonl_file):
        """Parallel reading should deduplicate all intra-file duplicates."""
        all_seqs = []
        for _fname, df, _intra, _instr, _pos in parallel_read_large_file(
            jsonl_file, block_bytes=200, workers=2
        ):
            if df is not None and not df.is_empty():
                all_seqs.extend(df["trade_seq"].to_list())
        # After intra + cross-block dedup, we should have exactly 50 unique seqs
        assert len(all_seqs) == 50, f"Expected 50 unique seqs, got {len(all_seqs)}"
        assert sorted(all_seqs) == list(range(1, 51)), "Should have seq 1..50"
