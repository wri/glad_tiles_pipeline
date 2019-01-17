from parallelpipe import stage
from helpers.utils import output_file, file_details
import helpers.raster_utilities as ras_util
import subprocess as sp
import logging


@stage(workers=2)
def resample(tiles, **kwargs):

    root = kwargs["root"]
    name = kwargs["name"]
    resample_method = kwargs["resample_method"]
    zoom = kwargs["zoom"]

    for tile in tiles:

        f_name, year, folder, tile_id = file_details(tile)
        # TODO: review naming - might need to remove year and add datatype instead!
        output = output_file(root, "tiles", tile_id, name, "zoom_{}.tif".format(zoom))

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


@stage(workers=1)  # IMPORTANT to only use one (1) worker!
def build_vrt(tiles, **kwargs):

    root = kwargs["root"]
    name = kwargs["name"]
    zoom = kwargs["zoom"]

    vrt_tiles = list()
    for tile in tiles:
        vrt_tiles.append(tile)

    output = output_file(root, "vrt", name, "zoom_{}.vrt".format(zoom))

    cmd = ["gdalbuildvrt", output] + vrt_tiles

    try:
        sp.check_call(cmd)
    except sp.CalledProcessError:
        logging.warning("Failed to build VRT: " + output)
    else:
        logging.info("Built VRT: " + output)
        yield output
