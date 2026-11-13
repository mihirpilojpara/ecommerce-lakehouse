import os
import tempfile

import pytest

# Set before any lakehouse module is imported: config reads these at import time.
os.environ.setdefault("SPARK_MASTER", "local[2]")
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
os.environ["LAKEHOUSE_ROOT"] = tempfile.mkdtemp(prefix="lakehouse-tests-")


@pytest.fixture(scope="session")
def spark():
    from lakehouse.spark import get_spark

    session = get_spark("tests")
    yield session
    session.stop()
