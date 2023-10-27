import os
import pytest
import dateutil
import pandas as pd
from pandas.testing import assert_frame_equal
from googlesat.sentinel import get_metadata, query
from googlesat.utils import get_links

e_data = pd.read_csv("./data/test_1.csv", index_col = False).sort_values(by=['BASE_URL'], ascending=True)
get_metadata_params = [("L2A", None, False, 40, dateutil.parser.isoparse('2021-10-15'),
                        dateutil.parser.isoparse('2021-10-30'), "34SEJ"),]

@pytest.mark.parametrize("level, store, force_update, cc_limit, date_start, date_end, tile", get_metadata_params,)
def test_get_metadata(level, store, force_update, cc_limit, date_start, date_end, tile):
    db_file, table = get_metadata(level, store, force_update = force_update)
    data = query(db_file, table, cc_limit, date_start, date_end, tile)
    data = get_links(data)
    data = data.sort_values(by=['BASE_URL'], ascending=True)
    assert isinstance(data, pd.DataFrame)
    assert_frame_equal(data.reset_index(drop = True), e_data)
    