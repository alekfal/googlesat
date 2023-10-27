import dateutil
import time
import os
from googlesat.sentinel import get_metadata, query, geometry_from_file
from googlesat.utils import get_links
import pandas as pd
from googlesat.utils import create_connection, get_cache_dir
from googlesat.downloader import get_data

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

def get_DB_L2A_no_geometry():
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
    result.to_csv("results_1.csv")
    time.end

def get_DB_L2A_with_geometry():
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
    result.to_csv("results_2.csv")

def get_DB_L1C_with_geometry():
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
    result.to_csv("results_3.csv")
    time.end

def get_huge_query():
    print ("Count time to update...")
    time = Time()
    time.start
    level = "L1C"
    db_file, table_name = get_metadata(level = level)
    cc_limit = 80
    date_start = dateutil.parser.isoparse('2019-11-01')
    date_end = dateutil.parser.isoparse('2020-12-31')
    tiles = ["34TGL","34TGM","34SEF","34SEG","34SEH","34SEJ","35TLE","35TLF","34SFF","34SFG","34SFH","34SFJ","34SCJ","34TFL","35SPA","35SQA","34TDL","35SLC","35SLD","35SLU","35SLV","35SMA","34TDK","35SMB","35SMC","35SMD","35SMU","35SMV","35SNA","34TEK","34TEL","35SNV","34TFK","35TMF","35SKA","35TMG","35SKB","35SKC","34SGE","35SKD","34SGF","34SGG","34SGH","34SGJ","35SKU","35SKV","35SLA","34TCK","35SLB","34SDG","34SDH","34SDJ","34TGK"]
    time.end
    print ("Count time to query...")
    time = Time()
    time.start
    result = query(db_file, table_name, cc_limit, date_start, date_end, tiles)
    result = get_links(result)
    result.to_csv("results_4.csv")
    time.end

def check_unique(level = "L2A"):
    cache = get_cache_dir(subdir = level)
    db_file = os.path.join(cache, f"db_{level}.db")    
    conn = create_connection(db_file)
    query = "SELECT *, COUNT(*) c FROM S2L2A GROUP BY BASE_URL HAVING c > 1"
    result = pd.read_sql(query, conn)    
    print(result)

    query = "SELECT * FROM S2L2A WHERE BASE_URL = 'gs://gcp-public-data-sentinel-2/L2/tiles/19/L/HL/S2A_MSIL2A_20190608T143751_N0212_R096_T19LHL_20190608T184522.SAFE'" 
    result = pd.read_sql(query, conn)    
    print(result)

def get_download_data(CSVfile, GOOGLE_SAT_DATA):

    result = pd.read_csv(CSVfile)
    # Get data
    scenes = result["URL"].tolist()
    for scene in scenes:
        time = Time()
        time.start
        get_data(scene, GOOGLE_SAT_DATA)
        time.end

def get_complete_L2A(GOOGLE_SAT_DATA):
    time = Time()
    time.start
    level = "L2A"
    db_file, table_name = get_metadata(level = level)
    cc_limit = 100
    date_start = dateutil.parser.isoparse('2022-08-25')
    date_end = dateutil.parser.isoparse('2022-08-30')
    tile = "35TLF"
    result = query(db_file, table_name, cc_limit, date_start, date_end, tile)
    result = get_links(result)
    # Get data
    scenes = result["URL"].tolist()
    for scene in scenes:
        get_data(scene, GOOGLE_SAT_DATA)
    time.end

def main():
    get_DB_L2A_no_geometry()
    get_DB_L2A_with_geometry()
    get_DB_L1C_with_geometry()
    check_unique()
    get_huge_query()
    get_download_data("./test_1.csv", "/../GOOGLE_SAT_DATA")
    get_complete_L2A("/../GOOGLE_SAT_DATA")

if __name__ == '__main__':
    main() 