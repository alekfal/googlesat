import pytest
from googlesat.sentinel import geometry_from_file

geometry_from_file_params = [("./test/data/test.geojson")]

@pytest.mark.parametrize("geometry", geometry_from_file_params,)
def test_geometry_from_file(geometry):
    tiles = geometry_from_file(geometry)
    
    assert isinstance(tiles, list)
    assert tiles == ['34SFJ','34SGJ','34TFK', '34TGK', '35SKD', '35TKE']
