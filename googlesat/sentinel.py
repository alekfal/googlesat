import os
from datetime import date

from .utils import extract, get_cache_dir, downloader

# Sentinel 2 metadata index file links to GCP
metadata_url = {'L1C': 'http://storage.googleapis.com/gcp-public-data-sentinel-2/index.csv.gz',
                'L2A': 'http://storage.googleapis.com/gcp-public-data-sentinel-2/L2/index.csv.gz',
            }

def check(db, file):
    create_new_db = False
    update_db = False
    if os.path.exists(db):
        db_time_flag = os.stat(db).st_mtime
    else:
        print("SQLlite {} database does not exists...")
        print("A new database will be created...")
        create_new_db = True
    
    if os.path.exists(file):
        file_time_flag = os.stat(db).st_mtime
    else:
        raise Exception("File {file} not found!")
    
    if create_new_db is False:
        if (file_time_flag > db_time_flag):
            print ("Updating database...")
            update_db = True
        
    return create_new_db, update_db

def get_metadata(filename:str = 'index.csv.gz', level:str = 'L2A'):
    
    # Setting available options
    options = ['L2A', 'L1C']
    if level not in options:
        raise ValueError("L2A (BOA) or L1C (TOA) are the only available levels.")
    # Getting link for user defined level
    url = metadata_url.get(level)
    cache = get_cache_dir(subdir = level)
    filename = os.path.join(cache, filename)
    file = downloader(url, filename)
    db_file = os.path.join(cache, f"db_{level}.db")
    
    # Unpacking metadata file
    #file, metadata = extract(filename)



    