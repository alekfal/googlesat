import os
import appdirs
import sqlite3
import gzip
import pandas as pd
import urllib.request

def downloader(url:str, name:str) -> str:
    """Download method with urllib library.

    Args:
        url (str): Link to file
        name (str): Saving path with name of the downloaded file
    Returns:
        file (str): Path to file
    """
    print(f"Downloading file {name} from {url}...")
    print("This may take a while...")
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
        cache_dir = appdirs.user_cache_dir("googlesat", "googlesat")

    if subdir:
        subdir = subdir if isinstance(subdir, list) else [subdir]
        cache_dir = os.path.join(cache_dir, *subdir)

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return cache_dir

def extract(file:str, chunksize:int = 10**4) -> pd.DataFrame:
    """Extracts a compressed CSV file and stores it into a pandas DataFrame as chunks.

    Args:
        file (str): Path and name of the CSV file
        chunksize (int, optional): Chunk size. Defaults to 10**4

    Returns:
        pd.DataFrame: Pandas DataFrame into chunks
    """
    print(f"Extracting {file}...")
    f = gzip.open(file)
    data = pd.read_csv(f, chunksize = chunksize)

    return f, data

def create_connection(db_file:str):
    """Create a database connection to a SQLite database.

    Args:
        db_file (str): Path to SQLite database
    """
    print(f"Connecting to {db_file}...")
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except ValueError as error:
        print(error)
    finally:
        if conn:
            return conn

def fill_database(connection:sqlite3, data:pd.DataFrame, name:str = "Fill"):
    """Fill an SQL database from pandas dataframe chunks.

    Args:
        connection (sqlite3): SQLite3 database path
        data (pd.DataFrame): Data in chunks
        name (str, optional): Name of the created table. Defaults to "Fill".
    """
    for d in data:
        d.to_sql(name, connection, if_exists = 'replace')

def get_links(data:pd.DataFrame) -> pd.DataFrame:
    """Converts google cloud storage links to simple http links

    Args:
        data (pd.DataFrame): DataFrame with the result from querying the database

    Returns:
        pd.DataFrame: New DataFrame with http links
    """
    data["URL"] = data["BASE_URL"]
    data["URL"] = data["URL"].replace("gs://", "http://storage.googleapis.com/", regex = True)

    return data