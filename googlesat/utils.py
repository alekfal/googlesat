import os
import platformdirs
import sqlite3
import gzip
import pandas as pd
import urllib.request
import time
import sys

def _reporthook(count:int, block_size:float, total_size:float):
    """Generates report for downloading.
    """

    global start_time
    if count == 0:
        start_time = time.time()
        return
    duration = time.time() - start_time
    progress_size = int(count * block_size)
    speed = int(progress_size / (1024 * 1024 * duration + 1)) # Add +1 to avoid division by zero error
    percent = min(int(count * block_size * 100 / total_size), 100)
    sys.stdout.write(f"\rDownloading: {percent}%, {round(progress_size / (1024 * 1024), 1)} MB, {speed} MB/s, {int(duration)} seconds passed.")
    sys.stdout.flush()

def downloader(url:str, name:str, verbose = True) -> str:
    """Download method with urllib library.

    Args:
        url (str): Link to file
        name (str): Saving path with name of the downloaded file
    Returns:
        file (str): Path to file
    """
    if verbose:
        print(f"Getting file {name} from {url}...")
        file = urllib.request.urlretrieve(url, name, _reporthook)
        print("\nDone!")
    else:
        file = urllib.request.urlretrieve(url, name)

    return file

def get_cache_dir(subdir:str=None) -> str:
    """Function for getting cache directory to store reused files like kernels, or scratch space for autotuning, etc.

    Args:
        subdir (str, optional): Directory to save data. Defaults to None

    Returns:
        str: Path to package cache directory
    """

    cache_dir = os.environ.get("GOOGLESAT_CACHE_DIR")
    if cache_dir is None:
        cache_dir = platformdirs.user_cache_dir("googlesat", "googlesat")

    if subdir:
        subdir = subdir if isinstance(subdir, list) else [subdir]
        cache_dir = os.path.join(cache_dir, *subdir)

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return cache_dir

def extract(file:str, chunksize:int = 10**5) -> pd.DataFrame:
    """Extracts a compressed CSV file and stores it into a pandas DataFrame as chunks.

    Args:
        file (str): Path and name of the CSV file
        chunksize (int, optional): Chunk size. Defaults to 10**4

    Returns:
        pd.DataFrame: Pandas DataFrame into chunks
    """

    print(f"Extracting {file}...")
    f = gzip.open(file)
    data = pd.read_csv(f, usecols = ["SENSING_TIME", "MGRS_TILE", "CLOUD_COVER", "BASE_URL"], chunksize = chunksize)
    
    return f, data

def create_table(connection:sqlite3, name:str = "Fill", force = False):
    """Create table to database.

    Args:
        connection (sqlite3): Connection to database
        name (str, optional): Name of the table. Defaults to "Fill"
        force (bool, optional): Force table creation. Defaults to False
    """

    cursor = connection.cursor()

    if force:
        # Creating table
        SQL = f"DROP TABLE IF EXISTS {name}"
        cursor.execute(SQL)
        SQL = f"CREATE TABLE {name} ('index' INTEGER PRIMARY KEY AUTOINCREMENT, SENSING_TIME TEXT NOT NULL, MGRS_TILE TEXT NOT NULL, BASE_URL TEXT NOT NULL, CLOUD_COVER REAL NOT NULL);"
        cursor.execute(SQL)
    else:
        SQL = f"CREATE TABLE IF NOT EXISTS {name} ('index' INTEGER PRIMARY KEY AUTOINCREMENT, SENSING_TIME TEXT NOT NULL, MGRS_TILE TEXT NOT NULL, BASE_URL TEXT NOT NULL, CLOUD_COVER REAL NOT NULL);"
        cursor.execute(SQL)

def delete_dublicates(connection:sqlite3, name:str = "Fill"):
    """Delete dublicates from table.

    Args:
        connection (sqlite3): Connection to database
        name (str, optional): Name of the table. Defaults to "Fill".
    """

    cursor = connection.cursor()
    print("Deleting dublicates in database if exist. This may take a while...")
    SQL = f"DELETE FROM {name} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {name} GROUP BY BASE_URL);"
    cursor.execute(SQL)

def create_index(connection:sqlite3, name:str = "Fill"):
    """Create index for the table.

    Args:
        connection (sqlite3): Connection to database
        name (str, optional): Name of the table. Defaults to "Fill".
    """

    cursor = connection.cursor()
    # Creating index
    SQL =  f"CREATE INDEX IF NOT EXISTS MGRS_TILE_INDEX ON {name}(MGRS_TILE);"
    cursor.execute(SQL)

def create_connection(db_file:str):
    """Create a database connection to a SQLite database.

    Args:
        db_file (str): Path to SQLite database
    """

    print(f"Connecting to {os.path.realpath(db_file)}...")
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except ValueError as error:
        print(error)
    finally:
        if conn:
            return conn

def fill_db(connection:sqlite3, data:pd.DataFrame, name:str = "Fill"):
    """Fill an SQL database from pandas dataframe chunks.

    Args:
        connection (sqlite3): SQLite3 database path
        data (pd.DataFrame): Data in chunks
        name (str, optional): Name of the created table. Defaults to "Fill".
    """

    for d in data:
        # Take maximum allowed chunksize for multiple insertions 
        cols = d.shape[1]
        chunksize = 999 // (cols + 1)
        
        d.to_sql(name, connection, if_exists = "append", method='multi', chunksize = chunksize)

def update_db(connection:sqlite3, data:pd.DataFrame, name:str = "Fill"):
    """Updates database with new entries.
    #BUG: Updating is slower than recreating database from scratch.

    Args:
        connection (sqlite3): SQLite3 database path
        data (pd.DataFrame): Data in chunks
        name (str, optional): Name of the created table. Defaults to "Fill".
    """

    print("Updating database. This may take a while...")
    cursor = connection.cursor()
    for d in data:
        # Take maximum allowed chunksize for multiple insertions
        cols = d.shape[1]
        chunksize = 999 // (cols + 1)
        d.to_sql("temp", connection, if_exists = "replace", method='multi', chunksize = chunksize)

        SQL = f"INSERT OR IGNORE INTO {name} SELECT * FROM temp;"
        cursor.execute(SQL)

    SQL = f"DROP TABLE IF EXISTS temp;"
    cursor.execute(SQL)

def get_links(data:pd.DataFrame) -> pd.DataFrame:
    """Converts google cloud storage links to simple http links

    Args:
        data (pd.DataFrame): DataFrame with the result from querying the database

    Returns:
        pd.DataFrame: New DataFrame with http links
    """

    data["URL"] = data["BASE_URL"]
    data["URL"] = data["URL"].replace("gs://", "https://storage.googleapis.com/", regex = True)

    return data

def clear_cache():
    pass