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
