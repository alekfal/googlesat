import os

# Set the default output dir to the XDG or System cache dir
# i.e. ~/.cache/googlesat $XDG_DATA_HOME/googlesat %APPDATA%/googlesat or ~/Library/Caches/googlesat
GOOGLESAT_DEFAULT_OUTPUTDIR = os.environ.get('GOOGLESAT_DEFAULT_OUTPUTDIR', '')
print(GOOGLESAT_DEFAULT_OUTPUTDIR)

def get_metadata(url = 1, directory = 1, satellite = 1):
    