from stages import check_availablity, download_tiles
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

root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)

if os.path.exists("data"):
    shutil.rmtree("data")

try:
    tile_date = check_availablity.get_most_recent_day(TILES, YEARS)
except ValueError:
    logging.error("Cannot find recently processes tiles. Aborting")
    pass
else:
    pipe = TILES | download_tiles.download_latest_tiles(YEARS, tile_date)

for tif in pipe.results():
    logging.info("Downloaded TIF: " + tif)
