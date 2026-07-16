"""Tests for parquet.py: dedup_intra, dedup_cross_batch, dedup_cross_file.

All tests operate on in-memory Polars DataFrames — no I/O, no network.
"""

import polars as pl

from deribit_fetcher.parquet import dedup_cross_batch, dedup_cross_file, dedup_intra

# =============================================================================
# Helpers
# =============================================================================


def _df(seqs: list[int], instr: str = "BTC-TEST") -> pl.DataFrame:
    """Build a minimal DataFrame with instrument_name and trade_seq columns."""
    return pl.DataFrame({"instrument_name": [instr] * len(seqs), "trade_seq": seqs})


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
        # File 1 has a duplicate row
        df1 = _df([1, 2, 2, 3])  # seq 2 duplicated
        df1, _intra = dedup_intra(df1)  # -> [1, 2, 3]

        # Cross-file: seen keys from previous files
        seen: set[int] = set()
        df1, _cross = dedup_cross_file(df1, seen)
        seen.update(df1["trade_seq"].to_list())
        # First file: nothing to cross-dedup against
        assert sorted(df1["trade_seq"].to_list()) == [1, 2, 3]

        # File 2: some seqs already seen
        df2 = _df([2, 3, 4, 5])
        df2, _intra = dedup_intra(df2)
        df2, _cross = dedup_cross_file(df2, seen)
        # seq 2 and 3 already seen -> removed; 4 and 5 survive
        assert sorted(df2["trade_seq"].to_list()) == [4, 5]
