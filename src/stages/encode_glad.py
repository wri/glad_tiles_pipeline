from utils.utils import output_tiles, file_details
from parallelpipe import stage
import subprocess as sp
import logging


@stage(workers=2)
def encode_date_conf(tiles, **kwargs):

    root = kwargs["root"]

    for tile in tiles:
        f_name, tile_id, year = file_details(tile)

        output = output_tiles(root, tile_id, "encode_date_conf", year, f_name)

        cmd = ["encode_date_conf.py", "-i", tile, "-o", output, "-y", str(year)]

        if f_name == "day.tif":
            cmd += ["-m", "date"]
        else:
            cmd += ["-m", "conf"]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError:
            logging.warning("Failed to encode file: " + tile)
        else:
            logging.info("Encode file: " + tile)
            yield output
