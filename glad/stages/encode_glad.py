from glad.utils.utils import output_file, file_details
import glad.utils.raster_utilities as ras_util
from pathlib import PurePath
import subprocess as sp
import logging


def encode_date_conf(tiles, **kwargs):
    """

    :param tiles:
    :param kwargs:
    :return:
    """

    root = kwargs["root"]
    name = kwargs["name"]

    for tile in tiles:
        f_name, year, folder, tile_id = file_details(tile)

        output = output_file(root, "tiles", tile_id, name, year, f_name)

        cmd = ["encode_date_conf.py", "-i", tile, "-o", output, "-y", str(year)]

        if f_name == "day.tif":
            cmd += ["-m", "date"]
        else:
            cmd += ["-m", "conf"]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.error("Failed to encode file: " + tile)
            logging.error(e)
            raise e
        else:
            logging.info("Encoded file: " + tile)
            yield output


def prep_intensity(tiles, **kwargs):
    """
    Reclassify our final year/date raster to 0 | 55
    this will then be resampled at several levels to get our intensity input
    :param tiles:
    :return:
    """

    root = kwargs["root"]
    name = kwargs["name"]
    max_ras_value = 55

    for tile in tiles:
        f_name, year, folder, tile_id = file_details(tile)

        output = output_file(root, "tiles", tile_id, name, year, "source_intensity.tif")

        cmd = ["reclass", tile, output, str(max_ras_value)]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.error("Failed to prepare intensity for file: " + tile)
            logging.error(e)
            raise e
        else:
            logging.info("Prepared intensity for file: " + tile)
            yield output


def unset_no_data_value(tiles):
    """
    Set no data value to 0 for given dataset
    :param tiles: tiles to process
    :return:
    """

    for tile in tiles:

        output = tile
        cmd = ["gdal_edit.py", tile, "-unsetnodata"]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.error("Failed to unset nodata value for file: " + tile)
            logging.error(e)
            raise e
        else:
            logging.info("Unset nodata value for file: " + tile)
            yield output


def encode_rgb(tile_pairs):

    for tile_pair in tile_pairs:
        day_conf = tile_pair[0]
        intensity = tile_pair[1]

        output = output_file(PurePath(day_conf).parent.as_posix(), "rgb.tif")

        cmd = ["build_rgb", day_conf, intensity, output]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.error("Failed to build RGB for: " + str(tile_pair))
            logging.error(e)
            raise e
        else:
            logging.info("Built RGB for: " + str(tile_pair))
            yield output


def project(tiles):
    for tile in tiles:

        output = output_file(PurePath(tile).parent.as_posix(), "rgb_wm.tif")
        zoom = int(PurePath(tile).parts[-2].split("_")[1])

        cell_size = str(ras_util.get_cell_size(zoom, "meters"))

        # custom project str to prevent wrapping around 180 degree dateline
        # source: https://gis.stackexchange.com/questions/34117
        proj_str = (
            "+proj=merc "
            "+a=6378137 "
            "+b=6378137 "
            "+lat_ts=0.0 "
            "+lon_0=0.0 "
            "+x_0=0.0 "
            "+y_0=0 "
            "+k=1.0 "
            "+units=m "
            "+nadgrids=@null "
            "+wktext "
            "+no_defs "
            "+over"
        )

        # DEFLATE compression and tiled data required to (partially) meet COG standard
        # only thing missing is the overviews, but not required as we're generating one tif for each zoom level
        cmd = [
            "gdalwarp",
            "-r",
            "near",
            "-t_srs",
            proj_str,
            "-tap",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "TILED=YES",
            "-dstnodata",
            "0",  # Set nodata value to 0 to avoid blackstrips between tiles
        ]
        # TODO: Figure out best memory allocation
        # cmd += ['--config', 'GDAL_CACHEMAX', ras_util.get_mem_pct(), '-wm', ras_util.get_mem_pct()]
        cmd += ["-tr", cell_size, cell_size, tile, output]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.error("Failed to project file: " + tile)
            logging.error(e)
            raise e
        else:
            logging.info("Projected file: " + tile)
            yield output
