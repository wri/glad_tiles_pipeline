from helpers.utils import output_tiles, file_details
from parallelpipe import stage
from pathlib import PurePath
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


@stage(workers=1)  # IMPORTANT to only use one (1) worker!
def date_conf_pairs(tiles):
    tile_pairs = dict()
    for tile in tiles:

        basedir = PurePath(tile).parent.as_posix()
        f_name = PurePath(tile).name

        if basedir not in tile_pairs.keys():
            tile_pairs[basedir] = dict()

        if f_name == "day.tif":
            tile_pairs[basedir]["day"] = tile
        else:
            tile_pairs[basedir]["conf"] = tile

        if len(tile_pairs[basedir]) == 2:
            logging.info("Created pairs for: " + basedir)
            yield tile_pairs[basedir]

    for key, value in tile_pairs.items():
        if len(value) < 2:
            logging.warning("Could not create pair for: " + key)
