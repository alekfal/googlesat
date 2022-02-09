import dateutil
import time
from .sentinel import get_metadata, query

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

def run():
    time = Time()
    time.start
    db_file = get_metadata()
    time.end
    cc_limit = 40
    date_start = dateutil.parser.isoparse('2021-10-15')
    date_end = dateutil.parser.isoparse('2021-10-30')
    tile = "34SEJ"
    level = "L2A"
    query(db_file, f'Sentinel-2_{level}', cc_limit, date_start, date_end, tile)

    