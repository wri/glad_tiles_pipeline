from helpers.utils import output_file, file_details
from parallelpipe import stage
from pathlib import PurePath
import subprocess as sp
import logging


@stage(workers=2)
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
        except sp.CalledProcessError:
            logging.warning("Failed to encode file: " + tile)
        else:
            logging.info("Encoded file: " + tile)
            yield output


@stage(workers=1)  # IMPORTANT to only use one (1) worker!
def date_conf_pairs(tiles):
    """

    :param tiles:
    :return:
    """
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


@stage(workers=2)
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
        except sp.CalledProcessError:
            logging.warning("Failed to prepare intensity for file: " + tile)
        else:
            logging.info("Prepared intensity for file: " + tile)
            yield tile  # !! Note: return input tile, not output b/c we need input in next step


@stage(workers=2)
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
        except sp.CalledProcessError:
            logging.warning("Failed to set nodata value for file: " + tile)
        else:
            logging.info("Set nodata value for file: " + tile)
            yield output
