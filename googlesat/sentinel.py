import os
import datetime as dt
import pandas as pd
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
        print(table)
        print(date_start)
        current_query = f'SELECT BASE_URL, CLOUD_COVER, SENSING_TIME, MGRS_TILE from "{table}" WHERE MGRS_TILE = "{tile}" AND CLOUD_COVER <= {cc_limit} and date("SENSING_TIME") BETWEEN date("{date_start}") AND date("{date_end}")'
        result = pd.read_sql(current_query, conn)    
        print(result)
    finally:
        cur.close()
'''
def convert_wkt_to_scene(sat, geometry, include_overlap, thresh=0.0):
    """
    Args:
        sat: 'S2', 'ETM', 'OLI_TIRS'
        geometry: WKT or GeoJSON string
        include_overlap: if True, use predicate 'intersects', else use predicate 'contains'
        thresh (float):
            the fraction of a tile that must intersect and overlap with a
            region.
    Returns:
        List[str]: List of scenes containing the geometry
    Example:
        >>> from fels.fels import *  # NOQA
        >>> sat = 'S2'
        >>> geometry = json.dumps({
        >>>     'type': 'Polygon', 'coordinates': [[
        >>>         [40.4700, -74.2700],
        >>>         [41.3100, -74.2700],
        >>>         [41.3100, -71.7500],
        >>>         [40.4700, -71.7500],
        >>>         [40.4700, -74.2700],
        >>>     ]]})
        >>> include_overlap = True
        >>> sorted(convert_wkt_to_scene('S2', geometry, include_overlap))
        ['37CET', '37CEU', '37CEV', '37DEA']
        >>> sorted(convert_wkt_to_scene('LC', geometry, include_overlap))
        ['140113', '141112', '141113', ...
    """

    if sat == 'S2':
        path = pkg_resources.resource_filename(__name__, os.path.join('data', 'sentinel_2_index_shapefile.shp'))
    else:
        path = pkg_resources.resource_filename(__name__, os.path.join('data', 'WRS2_descending.shp'))

    if isinstance(geometry, dict):
        feat = shp.geometry.shape(geometry)
    elif isinstance(geometry, str):
        try:
            feat = shp.geometry.shape(json.loads(geometry))
        except json.JSONDecodeError:
            feat = shp.wkt.loads(geometry)
    else:
        raise TypeError(type(geometry))

    gdf = _memo_geopandas_read(path)

    if include_overlap:
        if thresh > 0:
            # Requires some minimum overlap
            overlap = gdf.geometry.intersection(feat).area / feat.area
            found = gdf[overlap > thresh]
        else:
            # Any amount of overlap is ok
            found = gdf[gdf.geometry.intersects(feat)]
    else:
        # This is the bottleneck when the downloaded data exists
        found = gdf[gdf.geometry.contains(feat)]

    if sat == 'S2':
        return found.Name.values.tolist()
    else:
        return found.WRSPR.values.tolist()
'''