import os
import datetime as dt
import pandas as pd
import geopandas as gpd
import pkg_resources

from .utils import extract, get_cache_dir, downloader, create_connection, fill_database

# Sentinel 2 metadata index file links to GCP
METADATA_URL = {'L1C': 'http://storage.googleapis.com/gcp-public-data-sentinel-2/index.csv.gz',
                'L2A': 'http://storage.googleapis.com/gcp-public-data-sentinel-2/L2/index.csv.gz',
            }

# Setting available options
OPTIONS = ['L2A', 'L1C']

def get_metadata(filename:str = 'index.csv.gz', level:str = 'L2A', force_update:bool = False):
    
    if level not in OPTIONS:
        raise ValueError("L2A (BOA) or L1C (TOA) are the only available levels.")
    
    # Getting link for user defined level
    url = METADATA_URL.get(level)
    cache = get_cache_dir(subdir = level)
    filename = os.path.join(cache, filename)
    # At first check if the file exists and it is downloaded at the same day if force_update is False
    if force_update is False:
        if os.path.exists(filename):
            file_date = dt.datetime.fromtimestamp(os.path.getctime(filename)).date()
            current_date = dt.datetime.now().date()
            if file_date < current_date:
                file = downloader(url, filename)
                db_file = os.path.join(cache, f"db_{level}.db")
                conn = create_connection(db_file)
                file, metadata = extract(filename)
                fill_database(conn, metadata, name = f"Sentinel-2_{level}")
                conn.close()
            else:
                db_file = os.path.join(cache, f"db_{level}.db")
                if not os.path.exists(db_file):
                    conn = create_connection(db_file)
                    file, metadata = extract(filename)
                    fill_database(conn, metadata, name = f"Sentinel-2_{level}")
                    conn.close()
        else:
            try:
                file = downloader(url, filename)
                db_file = os.path.join(cache, f"db_{level}.db")
                conn = create_connection(db_file)
                file, metadata = extract(filename)
                fill_database(conn, metadata, name = f"Sentinel-2_{level}")
                conn.close()
            except:
                raise FileNotFoundError(f"Could not found {filename}.")

    elif force_update is True:
        file = downloader(url, filename)
        db_file = os.path.join(cache, f"db_{level}.db")
        conn = create_connection(db_file)
        file, metadata = extract(filename)
        fill_database(conn, metadata, name = f"Sentinel-2_{level}")
        conn.close()
    else:
        raise ValueError("Argument force_update is bool.")
    
    return db_file

def query(db_file:str, table:str, cc_limit, date_start, date_end, tile):
    conn = create_connection(db_file)
    cur = conn.cursor()
    try:
        if isinstance(tile, str):
            current_query = f'SELECT BASE_URL, CLOUD_COVER, SENSING_TIME, MGRS_TILE from "{table}" WHERE MGRS_TILE = "{tile}" AND CLOUD_COVER <= {cc_limit} and date("SENSING_TIME") BETWEEN date("{date_start}") AND date("{date_end}")'
        elif isinstance(tile, list):
            tiles = ','.join(['"{}"'.format(t) for t in tile])
            current_query = f'SELECT BASE_URL, CLOUD_COVER, SENSING_TIME, MGRS_TILE from "{table}" WHERE MGRS_TILE IN ({tiles}) AND CLOUD_COVER <= {cc_limit} and date("SENSING_TIME") BETWEEN date("{date_start}") AND date("{date_end}")'
        result = pd.read_sql(current_query, conn)    
    finally:
        cur.close()
    
    return result

def geometry_from_file(geometry:str):
    
    if isinstance(geometry, str):
        data = gpd.read_file(geometry)
    else:
        raise TypeError("Only str paths are supported!")
    
    path = pkg_resources.resource_filename(__name__, os.path.join('aux', 'sentinel-2_tiling_grid.geojson'))
    tiles = gpd.read_file(path)
    joined = gpd.sjoin(tiles, data)
    overlaped_tiles = joined.Name.to_list()
    
    return overlaped_tiles