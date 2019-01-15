from parallelpipe import stage
from helpers.utils import (
    output_tiles,
    file_details,
    add_tile_to_dict,
    add_preprocessed_tile_to_dict,
    get_preprocessed_tiles,
)
from pathlib import PurePath
import logging
import subprocess as sp


@stage(workers=2)
def combine_date_conf_pairs(pairs, **kwargs):
    root = kwargs["root"]
    for pair in pairs:
        f_name, year, folder, tile_id = file_details(pair["day"])

        output = output_tiles(root, tile_id, "date_conf", year, "day_conf.tif")

        try:
            sp.check_call(["add2", pair["day"], pair["conf"], output])
        except sp.CalledProcessError:
            logging.warning("Failed to compine files into: " + output)
        else:
            logging.info("Combined files into: " + output)
            yield output


@stage(workers=1)  # IMPORTANT to only use one (1) worker!
def year_pairs(tiles, **kwargs):
    preprocessed_years = kwargs["preprocessed_years"]

    tile_pairs = dict()
    for tile in tiles:
        basedir = PurePath(tile).parent.parent.as_posix()
        year = PurePath(tile).parts[-2]

        tile_pairs = add_tile_to_dict(tile_pairs, basedir, year, tile)

        if len(tile_pairs[basedir]) == len(preprocessed_years):
            logging.info("Created pairs for: " + basedir)
            yield tile_pairs[basedir]

    for key, value in tile_pairs.items():
        if len(value) < len(preprocessed_years):
            logging.warning("Could not create pair for: " + key)


@stage(workers=1)  # IMPORTANT to only use one (1) worker!
def all_year_pairs(tiles, **kwargs):

    root = kwargs["root"]
    years = kwargs["years"]
    preprocessed_years = kwargs["preprocessed_years"]
    preprocessed_tiles = get_preprocessed_tiles(root, years, preprocessed_years)

    tile_pairs = dict()
    for tile in tiles:
        basedir = PurePath(tile).parent.parent.as_posix()
        year = PurePath(tile).parts[-2]

        tile_pairs = add_tile_to_dict(tile_pairs, basedir, year, tile)

        tile_pairs = add_preprocessed_tile_to_dict(
            tile_pairs, basedir, preprocessed_tiles
        )

        if len(tile_pairs[basedir]) == len(years) + 1:
            logging.info("Created pairs for: " + basedir)
            yield tile_pairs[basedir]

    for key, value in tile_pairs.items():
        if len(value) < len(years) + 1:
            logging.warning("Could not create pair for: " + key)


@stage(workers=2)
def merge_years(tile_pairs, **kwargs):
    root = kwargs["root"]
    missing_years = kwargs["missing_years"]
    year_str = "_".join(str(year) for year in missing_years)

    for tile_pair in tile_pairs:
        logging.info(str(tile_pair))
        f_name, year, folder, tile_id = file_details(tile_pair[str(missing_years[0])])
        output = output_tiles(root, tile_id, "date_conf", year_str, "day_conf.tif")

        input = list()

        for year in sorted(list(tile_pair.keys())):
            input.append(tile_pair[year])

        try:
            sp.check_call(["combine{}".format(len(input))] + input + [output])
        except sp.CalledProcessError:
            logging.warning("Failed to combine files: " + str(input))
        else:
            logging.info("Combined files: " + str(input))
            yield output


@stage(workers=2)
def merge_all_years(tile_pairs, **kwargs):
    pass
