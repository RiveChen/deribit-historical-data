"""Tests for checkpoint-aware validation CLI exit semantics."""

import sqlite3
import sys

import polars as pl
import pytest

from deribit_fetcher.config import settings
from scripts.validate_data import main


def _write_fixture(base_dir, *, completed: bool, seqs: list[int]) -> None:
    """Write one option Parquet file and its checkpoint database."""
    pl.DataFrame(
        {
            "instrument_name": ["BTC-A"] * len(seqs),
            "trade_seq": seqs,
            "timestamp": [1_700_000_000_000] * len(seqs),
        }
    ).write_parquet(base_dir / "option.parquet")

    with sqlite3.connect(base_dir / "option.db") as connection:
        connection.execute(
            """
            CREATE TABLE option_meta (
                instrument TEXT PRIMARY KEY,
                last_no INTEGER,
                is_expired INTEGER,
                is_completed INTEGER
            )
            """
        )
        connection.execute(
            "INSERT INTO option_meta VALUES ('BTC-A', 3, 1, ?)",
            (int(completed),),
        )


@pytest.fixture(autouse=True)
def restore_base_dir():
    """Restore the mutable global path override after each CLI test."""
    previous = settings.base_dir_override
    yield
    settings.base_dir_override = previous


def test_cli_returns_normally_for_proven_complete(tmp_path, monkeypatch):
    """A final checkpoint with an exact sequence set exits zero."""
    _write_fixture(tmp_path, completed=True, seqs=[1, 2, 3])
    monkeypatch.setattr(
        sys,
        "argv",
        ["validate_data.py", "--type", "option", "--base-dir", str(tmp_path)],
    )

    main()


def test_cli_exits_one_for_known_missing_data(tmp_path, monkeypatch):
    """A checkpoint-proven gap is a validation failure."""
    _write_fixture(tmp_path, completed=True, seqs=[1, 3])
    monkeypatch.setattr(
        sys,
        "argv",
        ["validate_data.py", "--type", "option", "--base-dir", str(tmp_path)],
    )

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 1


def test_cli_exits_two_when_final_upper_bound_is_unknown(tmp_path, monkeypatch):
    """A continuous active checkpoint must not be reported as proven complete."""
    _write_fixture(tmp_path, completed=False, seqs=[1, 2, 3])
    monkeypatch.setattr(
        sys,
        "argv",
        ["validate_data.py", "--type", "option", "--base-dir", str(tmp_path)],
    )

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2
