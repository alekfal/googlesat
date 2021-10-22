import os
from datetime import date

from .utils import get_cache_dir, downloader

# Sentinel 2 metadata index file links to GCP
metadata_url = {'L1C': 'http://storage.googleapis.com/gcp-public-data-sentinel-2/index.csv.gz',
                'L2A': 'http://storage.googleapis.com/gcp-public-data-sentinel-2/L2/index.csv.gz',
            }

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
    print(file)

    