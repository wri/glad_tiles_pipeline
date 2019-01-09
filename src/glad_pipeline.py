from stages.download_tiles import download_latest_tiles
from stages.check_availablity import get_most_recent_day
import os
import shutil
import logging
import sys

TILES = [
    "040W_10S_030W_00N",
    "040W_20S_030W_10S",
    "050W_10S_040W_00N",
    "050W_00N_040W_10N",
]
YEARS = [2018, 2019]
ROOTDIR = "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data"

root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)


if os.path.exists(ROOTDIR):
    shutil.rmtree(ROOTDIR)

try:
    tile_date = get_most_recent_day(TILES, YEARS)
except ValueError:
    logging.error("Cannot find recently processes tiles. Aborting")
    pass
else:
    pipe = TILES | download_latest_tiles(YEARS, tile_date, ROOTDIR)

    for tif in pipe.results():
        logging.info("Downloaded TIF: " + tif)
