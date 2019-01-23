from helpers.utils import get_tile_id
from pathlib import PurePath
from helpers.utils import add_tile_to_dict, add_preprocessed_tile_to_dict
import glob
import logging


# IMPORTANT to only use one (1) worker!
def get_preprocessed_tiles(root, years, preprocessed_years):
    preprocessed_tiles = list()

    for tile in glob.iglob(root + "/tiles/**/day_conf.tif", recursive=True):
        try:
            year = int(PurePath(tile).parts[-2])

            if year not in years and year not in preprocessed_years:
                preprocessed_tiles.append(tile)

        except ValueError:
            pass

    return preprocessed_tiles


# IMPORTANT to only use one (1) worker!
def collect_resampled_tiles(root):
    resampled_tiles = dict()

    for tile in glob.iglob(root + "/tiles/**/resample/*/*.tif", recursive=True):

        tile_id = get_tile_id(tile)
        zoom_level = PurePath(tile).parts[-2]

        if (zoom_level, tile_id) not in resampled_tiles.keys():
            resampled_tiles[(zoom_level, tile_id)] = list()
        resampled_tiles[(zoom_level, tile_id)].append(tile)

        if len(resampled_tiles[(zoom_level, tile_id)]) == 2:
            logging.info("Created pairs for: " + str((zoom_level, tile_id)))
            yield sorted(resampled_tiles[(zoom_level, tile_id)])

    for key, value in resampled_tiles.items():
        if len(value) < 2:
            logging.warning("Could not create pair for: " + str(key))


# IMPORTANT to only use one (1) worker!
def collect_rgb_tiles(root):
    rgb_tiles = dict()

    for tile in glob.iglob(root + "/tiles/**/resample/*/rgb_wm.tif", recursive=True):

        zoom = int(PurePath(tile).parts[-2].split("_")[1])

        if zoom not in rgb_tiles.keys():
            rgb_tiles[zoom] = list()
        rgb_tiles[zoom].append(tile)

    for key, value in rgb_tiles.items():
        logging.info("Collect RGB tiles for zoom: " + str(key))
        yield key, value


# IMPORTANT to only use one (1) worker!
def collect_rgb_tile_ids(zoom_tiles):
    tile_list = None
    for tile in zoom_tiles:
        if tile[0] == 12:
            tile_list = tile[1]
            break
    tile_ids = [get_tile_id(tile) for tile in tile_list]
    return tile_ids


# IMPORTANT to only use one (1) worker!
def collect_day_conf_pairs(tiles):
    """
    Collect pairs of day and conf TIFF for each tile
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


# IMPORTANT to only use one (1) worker!
def collect_day_conf(tiles, **kwargs):
    """
    collect day_conf tiles for preprocessed years
    :param tiles:
    :param kwargs:
    :return:
    """

    preprocessed_years = kwargs["preprocessed_years"]

    tile_dicts = dict()
    for tile in tiles:
        basedir = PurePath(tile).parent.parent.as_posix()
        year = PurePath(tile).parts[-2]

        tile_dicts = add_tile_to_dict(tile_dicts, basedir, year, tile)

        if len(tile_dicts[basedir]) == len(preprocessed_years):
            logging.info("Created pairs for: " + basedir)
            yield tile_dicts[basedir]

    for key, value in tile_dicts.items():
        if len(value) < len(preprocessed_years):
            logging.warning("Could not create pair for: " + key)


# IMPORTANT to only use one (1) worker!
def collect_day_conf_all_years(tiles, **kwargs):
    """
    Collect all day_conf tiles of current and preprocessed years
    ie 2019, 2018, 2015_2016_2017
    :param tiles:
    :param kwargs:
    :return:
    """

    root = kwargs["root"]
    years = kwargs["years"]
    preprocessed_years = kwargs["preprocessed_years"]
    preprocessed_tiles = get_preprocessed_tiles(root, years, preprocessed_years)

    tile_dicts = dict()
    for tile in tiles:
        basedir = PurePath(tile).parent.parent.as_posix()
        year = PurePath(tile).parts[-2]

        tile_dicts = add_tile_to_dict(tile_dicts, basedir, year, tile)

        tile_dicts = add_preprocessed_tile_to_dict(
            tile_dicts, basedir, preprocessed_tiles
        )

        if len(tile_dicts[basedir]) == len(years) + 1:
            logging.info("Created pairs for: " + basedir)
            yield tile_dicts[basedir]

    for key, value in tile_dicts.items():
        if len(value) < len(years) + 1:
            logging.warning("Could not create pair for: " + key)
