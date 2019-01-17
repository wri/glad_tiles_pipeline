from helpers.utils import output_tiles, file_details
import helpers.raster_utilities as ras_util
import subprocess as sp
import logging


def resample(tiles, **kwargs):

    root = kwargs["root"]
    name = kwargs["name"]
    resample_method = kwargs["resample_method"]
    zoom = kwargs["zoom"]

    for tile in tiles:

        f_name, year, folder, tile_id = file_details(tile)
        # TODO: review naming - might need to remove year and add datatype instead!
        output = output_tiles(root, tile_id, name, year, "zoom_{}.tif".format(zoom))

        cell_size = str(ras_util.get_cell_size(zoom, "degrees"))
        # mem_pct = ras_util.get_mem_pct()

        cmd = [
            "gdal_translate",
            tile,
            "-co",
            "COMPRESS=DEFLATE",
            "-r",
            resample_method,
            "-tr",
            cell_size,
            cell_size,
            "-co",
            "TILED=YES",
        ]
        # cmd += ['--config', 'GDAL_CACHEMAX', mem_pct]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError:
            logging.warning("Failed to set nodata value for file: " + tile)
        else:
            logging.info("Set nodata value for file: " + tile)
            yield output
