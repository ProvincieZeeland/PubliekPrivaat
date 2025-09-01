from pathlib import Path
import pytest


# This is used to get code coverage working correctly when running unit tests
def pytest_collection_modifyitems(items):
    no_cov = pytest.mark.no_cover
    for item in items:
        if "integration" in Path(item.fspath).parts:
            item.add_marker(no_cov)
