import os
import appdirs
import sqlite3
import gzip
import pandas as pd
from sqlalchemy import create_engine
import urllib.request

def downloader(url:str, name:str):
    """Download method with urllib library.

    Args:
        url (str): Link to file
        name (str): Saving path with name of the downloaded file
    """
    print(f"Downloading file {name} from {url}...")
    print("This may take a while...")
    file = urllib.request.urlretrieve(url, name)
    return file


def get_cache_dir(subdir:str=None):
    """Function for getting cache directory to store reused files like kernels, or scratch space for autotuning, etc.

    Args:
        subdir (str, optional): [description]. Defaults to None

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

def create_db(name:str):
    """Creates an SQLite database to store metadata.

    Args:
        name (str): Database name.

    Returns:
        [type]: [description]
    """
    return create_engine(name)

def update_db():
    pass

def _extract_metadata(file:str, chunksize = 10**4):

    f = gzip.open(file)
    metadata = pd.read_csv(f, chunksize = chunksize)

    return metadata