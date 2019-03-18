from google.cloud.storage.bucket import Bucket
from glad.utils.utils import (
    get_tile_id,
    add_tile_to_dict,
    add_preprocessed_tile_to_dict,
)
from pathlib import PurePath, PosixPath
from glad.utils.google import get_gs_bucket
from typing import Dict, Any, List, Tuple
import datetime
import glob
import logging


def get_most_recent_day(**kwargs: Any) -> Tuple[str, List[str]]:

    tile_ids: List[str] = kwargs["tile_ids"]
    years: List[int] = kwargs["years"]
    num_tiles: int = kwargs["num_tiles"]
    today: datetime.datetime = datetime.datetime.today()

    # check for most recent day of GLAD data
    for day_offset in range(0, 11):
        process_date: str = (today - datetime.timedelta(days=day_offset)).strftime(
            "%Y/%m_%d"
        )

        available_tiles: List[str] = _check_tifs_exist(process_date, tile_ids, years)
        if len(available_tiles) == num_tiles:
            return process_date, available_tiles

    msg: str = (
        "Checked GCS for last 10 days - none had all {} tiled TIFs. "
        "You may want to verify the argument value for --num_tiles".format(num_tiles)
    )
    logging.error(msg)
    raise ValueError(msg)


# IMPORTANT to only use one (1) worker!
def get_preprocessed_tiles(root, include_years=None, exclude_years=None):
    preprocessed_tiles = list()

    for tile in glob.iglob(root + "/tiles/**/day_conf.tif", recursive=True):
        try:
            year = PurePath(tile).parts[-2]

            if include_years and exclude_years:
                if year in include_years and year not in exclude_years:
                    preprocessed_tiles.append(tile)
            elif include_years:
                if year in include_years:
                    preprocessed_tiles.append(tile)
            elif exclude_years:
                if year not in exclude_years:
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
    # TODO: make sure that the right years are returned
    # exclude_years = years + preprocessed_years
    preprocessed_tiles = get_preprocessed_tiles(root, exclude_years=preprocessed_years)

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


def match_emissions(tiles, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]

    for tile in tiles:
        tile_id = get_tile_id(tile)

        path = PurePath(root, "climate", name, tile_id + ".tif")
        if PosixPath(path).exists():
            logging.info("Found matching emission file: " + path.as_posix())
            yield tile, path.as_posix()
        else:
            logging.warning("Could not find matching emission file: " + path.as_posix())
            yield tile, None
            # raise FileNotFoundError


def match_climate_mask(tile_pairs, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]

    for tile_pair in tile_pairs:

        tile_id = get_tile_id(tile_pair[0])

        path = PurePath(root, "climate", name, tile_id + ".tif")
        if PosixPath(path).exists():
            logging.info("Found matching climate mask: " + path.as_posix())
            yield tile_pair[0], tile_pair[1], path.as_posix()
        else:
            # Not all tiles have a corresponding climate mask
            logging.warning("Could not find matching climate mask: " + path.as_posix())
            yield tile_pair[0], tile_pair[1], None


def _check_tifs_exist(
    process_date: str, tile_ids: List[str], years: List[int]
) -> List[str]:

    bucket: Bucket = get_gs_bucket()

    name_list: List[str] = [
        blob.name
        for blob in bucket.list_blobs(prefix="GLADalert/{}".format(process_date))
    ]

    logging.debug("Available TIFFS: " + str(name_list))
    available_tiles: List[str] = list()

    for tile_id in tile_ids:
        c: int = 0
        for year in years:

            year_dig: str = str(year)[2:]

            conf_str: str = "GLADalert/{0}/alert{1}_{2}.tif".format(
                process_date, year_dig, tile_id
            )
            logging.debug("Checking for TIFF: " + conf_str)

            alert_str: str = "GLADalert/{0}/alertDate{1}_{2}.tif".format(
                process_date, year_dig, tile_id
            )
            logging.debug("Checking for TIFF: " + alert_str)

            filtered_names: List[str] = [
                x for x in name_list if conf_str in x or alert_str in x
            ]
            logging.debug("Found: " + str(filtered_names))

            # if both alert and conf rasters exist, tile is ready to download
            if len(filtered_names) == 2:
                c += 1

        # TODO: Need to see if it's better to check for == or >=
        #  currently checking for >= b/c it add some more flexibility incase new tiles were added
        if c >= len(years):
            available_tiles.append(tile_id)

    logging.info(
        "Day {} has {} tiles for years {}".format(
            process_date, len(available_tiles), years
        )
    )

    return available_tiles
