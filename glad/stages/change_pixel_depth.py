from glad.utils.utils import output_file, file_details
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

            cmd = [
                "gdal_translate",
                "-ot",
                "UInt16",
                "-a_nodata",
                "0",
                "-co",
                "COMPRESS=LZW",
                "-co",
                "TILED=YES",
                "-co",
                "SPARSE_OK=TRUE",
                tile,
                output,
            ]

            logging.debug(cmd)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()
            logging.debug(o)
            if p.returncode == 0:
                logging.info("Changed pixel depth for file: " + tile)
                yield output
            else:
                logging.error("Failed to change pixel depth for file: " + tile)
                logging.error(e)
                raise sp.CalledProcessError
