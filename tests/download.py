import dateutil
import time
from googlesat.sentinel import get_metadata, query, geometry_from_file
from googlesat.utils import get_links

class Time:
    """Calculate elapsing time.
    """
    def __init__(self) -> None:
        pass
    
    @property
    def start(self):
        self.start_time = time.time()
        
        return (self.start_time)
    
    @property
    def end(self):
        self.end_time = time.time() - self.start_time
        mins, secs = divmod(self.end_time, 60)
        hours, mins = divmod(mins, 60)
        print ("Elapsed time: {}:{}:{}".format(int(hours), int(mins), round(secs,2)))   
        
        return self.end_time

def test_1():
    time = Time()
    time.start
    db_file = get_metadata()
    cc_limit = 40
    date_start = dateutil.parser.isoparse('2021-10-15')
    date_end = dateutil.parser.isoparse('2021-10-30')
    tile = "34SEJ"
    level = "L2A"
    result = query(db_file, f'Sentinel-2_{level}', cc_limit, date_start, date_end, tile)
    result = get_links(result)
    result.to_csv("test_1.csv")
    time.end

def test_2():
    time = Time()
    time.start
    db_file = get_metadata()
    cc_limit = 40
    date_start = dateutil.parser.isoparse('2021-10-15')
    date_end = dateutil.parser.isoparse('2021-10-30')
    file = "./data/test.geojson"
    tiles = geometry_from_file(file)
    level = "L2A"
    result = query(db_file, f'Sentinel-2_{level}', cc_limit, date_start, date_end, tiles)
    result = get_links(result)
    result.to_csv("test_2.csv")
    time.end

def main():
    #test_1()
    test_2()

if __name__ == '__main__':
    main() 