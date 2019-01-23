from helpers.utils import output_file, file_details
import subprocess as sp
import logging


def change_pixel_depth(tiles, **kwargs):

    try:
        root = kwargs["root"]
        name = kwargs["name"]
    except KeyError:
        logging.warning("Wrong number of arguments")
    else:

        for tile in tiles:
            f_name, year, folder, tile_id = file_details(tile)

            output = output_file(root, "tiles", tile_id, name, year, f_name)
            cmd = ["pixel_depth.py", "-i", tile, "-o", output, "-d", "UInt16"]
            try:
                logging.debug(cmd)
                sp.check_call(cmd)
            except sp.CalledProcessError:
                logging.warning("Failed to change pixel depth for file: " + tile)
            else:
                logging.info("Changed pixel depth for file: " + tile)
                yield output
