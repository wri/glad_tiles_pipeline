from helpers.utils import output_tiles, file_details
from parallelpipe import stage
import subprocess as sp
import logging


@stage(workers=2)
def change_pixel_depth(tiles, **kwargs):

    try:
        # tiles = kwargs["tiles"]
        root = kwargs["root"]
    except KeyError:
        logging.warning("Wrong number of arguments")
    else:

        for tile in tiles:
            f_name, year, folder, tile_id = file_details(tile)

            output = output_tiles(root, tile_id, "pixel_depth", year, f_name)

            try:
                logging.debug(
                    ["pixel_depth.py", "-i", tile, "-o", output, "-d", "UInt16"]
                )
                sp.check_call(
                    ["pixel_depth.py", "-i", tile, "-o", output, "-d", "UInt16"]
                )
            except sp.CalledProcessError:
                logging.warning("Failed to change pixel depth for file: " + tile)
            else:
                logging.info("Changed pixel depth for file: " + tile)
                yield output
