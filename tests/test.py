import dateutil
import time
import os
from googlesat.sentinel import get_metadata, query, geometry_from_file
from googlesat.utils import get_links

#CHECK UNIQUE
import pandas as pd
from googlesat.utils import create_connection, get_cache_dir

class Time:
    """Calculate elapsing time.
    """
    def __init__(self) -> None:
        pass
    
    @property
    def start(self):
        self.start_time = time.time()
        
        return (self.start_time)
    
    @property
    def end(self):
        self.end_time = time.time() - self.start_time
        mins, secs = divmod(self.end_time, 60)
        hours, mins = divmod(mins, 60)
        print ("Elapsed time: {}:{}:{}".format(int(hours), int(mins), round(secs,2)))   
        
        return self.end_time

def test_DB_L2A_no_geometry():
    time = Time()
    time.start
    level = "L2A"
    db_file, table_name = get_metadata(level = level)
    cc_limit = 40
    date_start = dateutil.parser.isoparse('2021-10-15')
    date_end = dateutil.parser.isoparse('2021-10-30')
    tile = "34SEJ"
    level = "L2A"
    result = query(db_file, table_name, cc_limit, date_start, date_end, tile)
    result = get_links(result)
    result.to_csv("test_1.csv")
    time.end

def test_DB_L2A_with_geometry():
    time = Time()
    time.start
    level = "L2A"
    db_file, table_name = get_metadata(level = level)
    cc_limit = 40
    date_start = dateutil.parser.isoparse('2021-10-15')
    date_end = dateutil.parser.isoparse('2021-10-30')
    file = "./data/test.geojson"
    tiles = geometry_from_file(file)
    result = query(db_file, table_name, cc_limit, date_start, date_end, tiles)
    result = get_links(result)
    result.to_csv("test_2.csv")
    time.end

def test_DB_L1c_with_geometry():
    time = Time()
    time.start
    level = "L1C"
    db_file, table_name = get_metadata(level = level)
    cc_limit = 40
    date_start = dateutil.parser.isoparse('2021-10-15')
    date_end = dateutil.parser.isoparse('2021-10-30')
    file = "./data/test.geojson"
    tiles = geometry_from_file(file)
    result = query(db_file, table_name, cc_limit, date_start, date_end, tiles)
    result = get_links(result)
    result.to_csv("test_2.csv")
    time.end

def check_unique(level = "L2A"):
    cache = get_cache_dir(subdir = level)
    db_file = os.path.join(cache, f"db_{level}.db")    
    conn = create_connection(db_file)
    query = "SELECT DISTINCT *, COUNT(*) c FROM S2L2A GROUP BY BASE_URL HAVING c > 1"
    result = pd.read_sql(query, conn)    
    print(result)

    query = "SELECT * FROM S2L2A WHERE BASE_URL = 'gs://gcp-public-data-sentinel-2/L2/tiles/19/L/HL/S2A_MSIL2A_20190608T143751_N0212_R096_T19LHL_20190608T184522.SAFE'" 
    result = pd.read_sql(query, conn)    
    print(result)

def test_download_data(CSVfile):
    time = Time()

def main():
    #test_DB_L2A_no_geometry()
    test_DB_L2A_with_geometry()
    #test_DB_L1C_with_geometry()
    check_unique()

if __name__ == '__main__':
    main() 