import os
import datetime as dt

from .utils import extract, get_cache_dir, downloader, create_connection, fill_database

# Sentinel 2 metadata index file links to GCP
METADATA_URL = {'L1C': 'http://storage.googleapis.com/gcp-public-data-sentinel-2/index.csv.gz',
                'L2A': 'http://storage.googleapis.com/gcp-public-data-sentinel-2/L2/index.csv.gz',
            }

# Setting available options
OPTIONS = ['L2A', 'L1C']

def get_metadata(filename:str = 'index.csv.gz', level:str = 'L2A', force_update = False):
    
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
                fill_database(conn, metadata)
            else:
                db_file = os.path.join(cache, f"db_{level}.db")
                if os.path.exists(db_file):
                    conn = create_connection(db_file)
                else:
                    conn = create_connection(db_file)
                    file, metadata = extract(filename)
                    fill_database(conn, metadata)
        else:
            raise FileNotFoundError(f"Could not found {filename}.")

    elif force_update is True:
        file = downloader(url, filename)
        db_file = os.path.join(cache, f"db_{level}.db")
        conn = create_connection(db_file)
        file, metadata = extract(filename)
        fill_database(conn, metadata)
    else:
        raise ValueError("Argument force_update is bool.")

