import dateutil
from .sentinel import get_metadata, query

def run():
    db_file = get_metadata()
    cc_limit = 40
    date_start = dateutil.parser.isoparse('2021-10-15')
    date_end = dateutil.parser.isoparse('2021-10-30')
    tile = "34SEJ"
    level = "L2A"
    query(db_file, f'Sentinel-2_{level}', cc_limit, date_start, date_end, tile)

    