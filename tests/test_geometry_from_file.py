import pytest
from googlesat.sentinel import geometry_from_file
import geopandas as gpd

gdf = gpd.read_file("./data/test.geojson") 
fake_list = ["fake_file.geojson"]
geometry_from_file_params = [("./data/test.geojson"), (gdf), (fake_list)]

@pytest.mark.parametrize("geometry", geometry_from_file_params,)
def test_geometry_from_file(geometry):
    try:
        tiles = geometry_from_file(geometry)
        assert isinstance(tiles, list)
        assert tiles == ['34SFJ','34SGJ','34TFK', '34TGK', '35SKD', '35TKE']
    except TypeError:
        pass