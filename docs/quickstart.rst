Quickstart
==========

Search and download data for a specific UTM tile by providing a name,
the product level, cloud coverage limit and start-end date.

.. code-block:: python

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

----

Search and download data for multiple UTM tiles by providing their names,
the product level, cloud coverage limit and start-end date.

.. code-block:: python

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

----

Search and download data by providing a geometry file (e.g GeoJSON),
the product level, cloud coverage limit and start-end date.

.. code-block:: python

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
