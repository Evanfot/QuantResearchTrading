import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_logger():
    """Shared mock logger for all execution tests."""
    return MagicMock()

@pytest.fixture
def base_metadata():
    """Shared exchange metadata."""
    return {
        "sz_decimals": {"BTC": 5, "ETH": 3, "SOL": 2, "MEME": 1},
        "ltps": {"BTC": 60000.0, "ETH": 2500.0, "SOL": 100.0, "MEME": 0.000123456}
    }

@pytest.fixture
def run_context(mock_logger, base_metadata):
    """Combines logger and metadata into a single context object."""
    return {
        "logger": mock_logger,
        **base_metadata
    }