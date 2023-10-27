# googlesat

[![Build Status](https://github.com/alekfal/googlesat/actions/workflows/python-package.yml/badge.svg?branch=main)](https://github.com/alekfal/googlesat/actions)
[![Code coverage](https://codecov.io/gh/alekfal/googlesat/branch/main/graph/badge.svg)](https://codecov.io/gh/alekfal/googlesat)

This software is designed to download Sentinel 2 (L1C &amp; L2A) from Google Cloud Platform and **does not require an google account and a creation of a GCP project** for data searching or downloading.

You can read more about the public Google data access [here](https://cloud.google.com/storage/docs/public-datasets/) and for Sentinel-2 data [here](https://cloud.google.com/storage/docs/public-datasets/sentinel-2).

## Introduction

The ```googlesat``` Python package is developed for searching and downloading data from GCP. In the GCP bucket, Google, to help locate data of interest, has published an index CSV file for both product types (L1C, L2A) of the Sentinel-2 data that are available for downloading.
Using ```googlesat``` the index CSV file is downloaded; either for L1C, L2A or both (a seperate database for each product type is created) and inserted in a SQLite database in order to avoid using Google's BigQuery, that requires an Google account and a creation of a GCP project for data searching. Every time a user runs the software the database **if needed** (CSV file is updated by Google daily) gets the required updates.

Also, a downloader is provided with this software to download the results from querying the database. The downloading procedure is done without using ```gsutils```, so a convertion method of ```gs``` to simple ```https``` links is developed. Then, the downloader use these links to download the ```manifest.xml``` file and extracts all the required informations from the file to build the SAFE format folder structure and download the rest of the files (images, metadata, etc). You can find more information about the SAFE folder structure [here](https://earth.esa.int/eogateway/activities/safe-the-standard-archive-format-for-europe/safe-2.x-basic-information).

## Installation Notes

Install ```googlesat``` using ```pip``` by running the following commands:

```bash
git clone https://github.com/alekfal/googlesat.git
cd googlesat/
pip install .
```

## Quickstart

Search and download data for a specific UTM tile by providing a name,
the product level, cloud coverage limit and start-end date.

```python
import dateutil
import time
import os
import pandas as pd

from googlesat.sentinel import get_metadata, query, geometry_from_file
from googlesat.utils import get_links,  create_connection, get_cache_dir
from googlesat.downloader import get_data

level = "L2A"
db_file, table_name = get_metadata(level = level)
cc_limit = 40
date_start = dateutil.parser.isoparse('2021-10-15')
date_end = dateutil.parser.isoparse('2021-10-30')
tile = "34SEJ"
result = query(db_file, table_name, cc_limit, date_start, date_end, tile)
result = get_links(result)
# Get data
GOOGLE_SAT_DATA = "/path/to/store/"
scenes = result["URL"].tolist()
for scene in scenes:
    get_data(scene, GOOGLE_SAT_DATA)
```

Search and download data for multiple UTM tiles by providing their names,
the product level, cloud coverage limit and start-end date.

```python
import dateutil
import time
import os
import pandas as pd

from googlesat.sentinel import get_metadata, query, geometry_from_file
from googlesat.utils import get_links,  create_connection, get_cache_dir
from googlesat.downloader import get_data

level = "L1C"
db_file, table_name = get_metadata(level = level)
cc_limit = 60
date_start = dateutil.parser.isoparse('2021-10-15')
date_end = dateutil.parser.isoparse('2021-10-30')
tile = ["34TGL", "34TGM", "34SEJ"]
result = query(db_file, table_name, cc_limit, date_start, date_end, tile)
result = get_links(result)
# Get data
GOOGLE_SAT_DATA = "/path/to/store/"
scenes = result["URL"].tolist()
for scene in scenes:
    get_data(scene, GOOGLE_SAT_DATA)
```

Search and download data by providing a geometry file (e.g GeoJSON),
the product level, cloud coverage limit and start-end date.

```python
import dateutil
import time
import os
import pandas as pd

from googlesat.sentinel import get_metadata, query, geometry_from_file
from googlesat.utils import get_links,  create_connection, get_cache_dir
from googlesat.downloader import get_data

db_file, table_name = get_metadata(level = level)
cc_limit = 40
date_start = dateutil.parser.isoparse('2021-10-15')
date_end = dateutil.parser.isoparse('2021-10-30')
file = "./data/test.geojson"
tiles = geometry_from_file(file)
result = query(db_file, table_name, cc_limit, date_start, date_end, tiles)
result = get_links(result)
# Get data
GOOGLE_SAT_DATA = "/path/to/store/"
scenes = result["URL"].tolist()
for scene in scenes:
    get_data(scene, GOOGLE_SAT_DATA)
```

## References

This software is based on the open source project [fetchLandsatSentinelFromGoogleCloud](https://github.com/vascobnunes/fetchLandsatSentinelFromGoogleCloud).