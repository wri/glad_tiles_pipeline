from glad.utils.utils import output_file, get_tile_id
import glad.utils.raster_utilities as ras_util
import subprocess as sp
import logging


def resample(tiles, **kwargs):

    root = kwargs["root"]
    name = kwargs["name"]
    resample_method = kwargs["resample_method"]
    zoom = kwargs["zoom"]

    for tile in tiles:

        tile_id = get_tile_id(tile)

        output = output_file(
            root,
            "tiles",
            tile_id,
            "resample",
            "zoom_{}".format(zoom),
            "{}.tif".format(name),
        )

        cell_size = str(ras_util.get_cell_size(zoom, "degrees"))
        # mem_pct = ras_util.get_mem_pct()

        cmd = [
            "gdal_translate",
            tile,
            output,
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
        # TODO: figure out how to best manage memory
        # cmd += ['--config', 'GDAL_CACHEMAX', mem_pct]

        try:
            logging.debug(cmd)
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.error("Failed to resample file: " + tile)
            logging.error(e)
            raise sp.CalledProcessError
        else:
            logging.info("Resampled file: " + tile)
            yield output


# IMPORTANT to only use one (1) worker!
def build_vrt(tiles, **kwargs):

    root = kwargs["root"]
    name = kwargs["name"]
    zoom = kwargs["zoom"]

    vrt_tiles = list()
    for tile in tiles:
        vrt_tiles.append(tile)

    output = output_file(root, "vrt", name, "zoom_{}.vrt".format(zoom))

    cmd = ["gdalbuildvrt", output] + vrt_tiles

    logging.debug(cmd)
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
    o, e = p.communicate()
    logging.debug(o)
    if p.returncode == 0:
        logging.info("Built VRT: " + output)
        yield output
    else:
        logging.error("Failed to build VRT: " + output)
        logging.error(e)
        raise sp.CalledProcessError
