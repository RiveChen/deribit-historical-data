import os
import asyncio
import orjson
from collections import defaultdict
from pathlib import Path


class JSONLinesSink:
    """
    A generic sink for writing chunks of JSON data to JSONL files.
    Operating asynchronously by offloading disk I/O to a thread pool executor.
    """

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    async def flush(self, data_buffers: dict[str, list[dict]]):
        """
        Flush memory buffers to disk.
        data_buffers is expected to be a dict mapping:
            instrument_name -> list of items
        where each item has a "data" key containing a list of dictionaries (trades/ticks).
        """
        if not data_buffers:
            return

        def sync_io_task(buffers):
            for instrument, items in buffers.items():
                file_path = self.base_dir / f"{instrument}.jsonl"
                lines = []
                for item in items:
                    data = item.get("data")
                    if data:
                        for row in data:
                            lines.append(orjson.dumps(row))
                if lines:
                    with open(file_path, "ab") as f:
                        f.write(b"\n".join(lines) + b"\n")

        # Convert to dict to detach from defaultdict or references that might change
        await asyncio.get_running_loop().run_in_executor(
            None, sync_io_task, dict(data_buffers)
        )
