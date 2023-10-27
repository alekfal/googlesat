import os
import pytest
from googlesat.sentinel import get_metadata
from googlesat.utils import get_cache_dir

get_metadata_params = [("L2A", None, False), ("L1C", None, False)]

@pytest.mark.parametrize("level, store, force_update", get_metadata_params,)
def test_get_metadata(level, store, force_update):
    e_db_name = f"db_{level}.db"
    e_table_name = f"S2{level}"
    if store is None:
        cache = get_cache_dir(subdir = level)
    else:
        cache = store
    e_db_file = os.path.join(os.path.realpath(cache), e_db_name)

    db_file, table_name = get_metadata(level, store, force_update = force_update)

    assert e_db_file == db_file
    assert e_table_name == table_name
