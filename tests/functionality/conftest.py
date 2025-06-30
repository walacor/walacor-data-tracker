import pytest

from walacor_data_tracker.adapters import PandasAdapter
from walacor_data_tracker.core.events import global_bus
from walacor_data_tracker.core.tracker import Tracker


@pytest.fixture(autouse=True)
def _reset_global_bus():
    """Isolation → every test gets a pristine EventBus registry."""
    yield
    global_bus.reset()


@pytest.fixture
def tracker():
    tr = Tracker().start()
    yield tr
    tr.stop()


@pytest.fixture
def pandas_adapter(tracker):
    """Monkey-patch DataFrame methods and roll back afterwards."""
    adapter = PandasAdapter().start(tracker)
    yield adapter
    adapter.stop()
