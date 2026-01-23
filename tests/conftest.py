import pytest
import shutil
from pathlib import Path
from unittest.mock import MagicMock
from deribit_historical_data.client import DeribitClient
from deribit_historical_data.storage import StorageManager


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create a temporary directory for data storage."""
    d = tmp_path / "data"
    d.mkdir()
    yield d
    shutil.rmtree(d)


@pytest.fixture
def temp_db(temp_data_dir):
    """Path to a temporary SQLite database."""
    return temp_data_dir / "sync_status.db"


@pytest.fixture
def mock_client():
    """Mock DeribitClient."""
    client = MagicMock(spec=DeribitClient)
    return client


@pytest.fixture
def mock_storage(temp_data_dir):
    """StorageManager with temporary directory."""
    return StorageManager(base_dir=str(temp_data_dir))
