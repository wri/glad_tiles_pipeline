from glad.utils.utils import output_file, file_details
from glad.utils.tiles import get_bbox_by_tile_id
import subprocess as sp
import logging

TILE_WIDTH = 40000


def realign_pixels(tiles, **kwargs):
    try:
        root = kwargs["root"]
        name = kwargs["name"]
    except KeyError:
        logging.warning("Wrong number of arguments")
    else:
        for tile in tiles:
            f_name, year, folder, tile_id = file_details(tile)

            output = output_file(root, "tiles", tile_id, name, year, f_name)

            min_x, min_y, max_x, max_y = get_bbox_by_tile_id(tile_id)

            cmd = [
                "gdalwarp",
                "-co",
                "COMPRESS=LZW",
                "-co",
                "TILED=YES",
                "-co",
                "SPARSE_OK=TRUE",
                "-r",
                "near",
                "-te",
                str(min_x),
                str(min_y),
                str(max_x),
                str(max_y),
                "-ts",
                str(TILE_WIDTH),
                str(TILE_WIDTH),
                tile,
                output,
            ]

            logging.debug(cmd)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()
            logging.debug(o)
            if p.returncode == 0:
                logging.info("Realigned pixels for tile: " + tile)
                yield output
            else:
                logging.error("Failed to realign pixel depth for file: " + tile)
                logging.error(e)
                raise sp.CalledProcessError
