from helpers.utils import get_tile_id
from pathlib import PurePath
import glob
import logging


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


def collect_rgb_tile_ids(zoom_tiles):
    tile_list = None
    for tile in zoom_tiles:
        if tile[0] == 12:
            tile_list = tile[1]
            break
    tile_ids = [get_tile_id(tile) for tile in tile_list]
    return tile_ids
