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

def get_metadata(filename:str = 'index.csv.gz', level:str = 'L2A', force_update:bool = False) -> str:
    """Downloads and updates index files from GCP. Also, creates/updates a database with the same
    data.

    Args:
        filename (str, optional): Name of CSV file. Defaults to 'index.csv.gz'
        level (str, optional): Sentinel 2 data level (L2A->BOA, L1C->TOA). Defaults to 'L2A'
        force_update (bool, optional): Force to update index file. Defaults to False

    Returns:
        str: Path to database file
    """
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

def query(db_file:str, table:str, cc_limit:float, date_start:dt.datetime, date_end:dt.datetime, tile:str) -> pd.DataFrame:
    """Querying Sentinel-2 index database.

    Args:
        db_file (str): Path to database
        table (str): Name of the table in database
        cc_limit (float): Cloud coverage percentage
        date_start (dt.datetime): Starting date
        date_end (dt.datetime): Ending date
        tile (str, list): Grid tile name

    Returns:
        pd.DataFrame: Result of the query
    """
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

def geometry_from_file(geometry:str) -> list:
    """Gets a path of any valid geographic file format and returns the overlaping tile names.

    Args:
        geometry (str): Path to geographic file

    Returns:
        list: List of tile names
    """
    if isinstance(geometry, str):
        data = gpd.read_file(geometry)
    else:
        raise TypeError("Only str paths are supported!")
    
    path = pkg_resources.resource_filename(__name__, os.path.join('aux', 'sentinel-2_tiling_grid.geojson'))
    tiles = gpd.read_file(path)
    joined = gpd.sjoin(tiles, data)
    overlaped_tiles = joined.Name.to_list()
    
    return overlaped_tiles