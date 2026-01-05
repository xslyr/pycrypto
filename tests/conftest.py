import pytest
from app.commons.utils import Singleton

@pytest.fixture(autouse=True)
def reset_singletons():
    Singleton._reset_all()
    yield