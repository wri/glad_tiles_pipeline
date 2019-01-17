from parallelpipe import stage
from helpers.utils import (
    output_file,
    file_details,
    add_tile_to_dict,
    add_preprocessed_tile_to_dict,
    get_preprocessed_tiles,
    sort_dict,
    preprocessed_years_str,
)
from pathlib import PurePath
import logging
import subprocess as sp


@stage(workers=2)
def combine_date_conf_pairs(pairs, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]
    for pair in pairs:
        f_name, year, folder, tile_id = file_details(pair["day"])

        output = output_file(root, "tiles", tile_id, name, year, "day_conf.tif")

        try:
            sp.check_call(["add2", pair["day"], pair["conf"], output])
        except sp.CalledProcessError:
            logging.warning("Failed to combine files into: " + output)
        else:
            logging.info("Combined files into: " + output)
            yield output


@stage(workers=1)  # IMPORTANT to only use one (1) worker!
def year_pairs(tiles, **kwargs):
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


@stage(workers=1)  # IMPORTANT to only use one (1) worker!
def all_year_pairs(tiles, **kwargs):

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


@stage(workers=2)
def merge_years(tile_dicts, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]

    for tile_dict in tile_dicts:
        logging.info(str(tile_dict))
        f_name, year, folder, tile_id = file_details(list(tile_dict.values())[0])

        year_str = preprocessed_years_str(sorted(tile_dict.keys()))

        output = output_file(root, "tiles", tile_id, name, year_str, "day_conf.tif")

        input = sort_dict(tile_dict)

        try:
            sp.check_call(["combine{}".format(len(input))] + input + [output])
        except sp.CalledProcessError:
            logging.warning("Failed to combine files: " + str(input))
        else:
            logging.info("Combined files: " + str(input))
            yield output
