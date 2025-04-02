import os
import pandas as pd
import pyarrow
import glob


def bool_to_str(value: bool) -> str:
    return "true" if value else "false"


def str_to_bool(value: str) -> bool:
    return value == "true"


def bool_to_filename(value: bool) -> str:
    return "expired" if value else "active"


def prepare_dir(base_dir: str, currency: str, instrument: str) -> None:
    """Create the directory structure for storing data."""
    if instrument == "all":
        os.makedirs(
            os.path.join(base_dir, currency, "option", "expired"), exist_ok=True
        )
        os.makedirs(os.path.join(base_dir, currency, "option", "active"), exist_ok=True)
        os.makedirs(
            os.path.join(base_dir, currency, "future", "expired"), exist_ok=True
        )
        os.makedirs(os.path.join(base_dir, currency, "future", "active"), exist_ok=True)
    else:
        os.makedirs(
            os.path.join(base_dir, currency, instrument, "expired"),
            exist_ok=True,
        )
        os.makedirs(
            os.path.join(base_dir, currency, instrument, "active"),
            exist_ok=True,
        )


def save_to_parquet(dir_path: str) -> None:
    """Save all CSV files in the directory to a single Parquet file."""
    files = glob.glob(os.path.join(dir_path, "*.csv"))
    if not files:
        return

    # Read first file to get common columns
    first_df = pd.read_csv(files[0])
    common_columns = set(first_df.columns)

    # Find common columns across all files
    for file in files[1:]:
        df = pd.read_csv(file)
        common_columns &= set(df.columns)

    common_columns = list(common_columns)
    parquet_path = os.path.join(dir_path, "data.parquet")
    writer = None

    try:
        for file in files:
            for chunk in pd.read_csv(file, usecols=common_columns, chunksize=100000):
                if writer is None:
                    writer = pd.io.parquet.PyArrowWriter(
                        parquet_path,
                        engine="pyarrow",
                        schema=chunk[common_columns].schema,
                    )
                writer.write(chunk[common_columns])

        if writer is not None:
            writer.close()

    except Exception as e:
        if writer is not None:
            writer.close()
        if os.path.exists(parquet_path):
            os.remove(parquet_path)
        raise
