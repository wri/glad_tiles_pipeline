from parallelpipe import Stage
from stages.download_tiles import (
    download_latest_tiles,
    download_preprocessed_tiles_years,
    download_preprocessed_tiles_year,
)
from stages.check_availablity import get_most_recent_day
from stages.change_pixel_depth import change_pixel_depth
from stages.encode_glad import (
    encode_date_conf,
    date_conf_pairs,
    prep_intensity,
    unset_no_data_value,
    encode_rgb,
    project,
)
from stages.merge_tiles import (
    combine_date_conf_pairs,
    year_pairs,
    merge_years,
    all_year_pairs,
)
from stages.upload_tiles import backup_tiles
from stages.resample import resample, build_vrt
from stages.collectors import collect_resampled_tiles

from helpers.tiles import get_tile_ids_by_bbox
import os
import shutil
import logging
import sys
import argparse
import pathlib

from typing import Dict, Any, List


def str2bool(v):
    """
    Convert various strings to boolean
    :param v: String
    :return: Boolean
    """
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def get_logger(debug=True):
    """
    Build logger
    :param debug: Set Log Level to Debug or Info
    :return: logger
    """

    root = logging.getLogger()
    if debug:
        root.setLevel(logging.DEBUG)
    else:
        root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    root.addHandler(handler)
    return root


def preprocessed_tile_pipe(tile_ids, **kwargs):
    """
    Pipeline to download/ process GLAD alerts of previous years
    :param tile_ids: List of Tile IDs to process
    :param kwargs: Dictonary with keyword arguments
    :return: pipe
    """
    pipe = (
        tile_ids
        | download_preprocessed_tiles_years(name="date_conf", **kwargs)
        | download_preprocessed_tiles_year(name="download", **kwargs)
        | change_pixel_depth(name="pixel_depth", **kwargs)
        | encode_date_conf(name="encode_day_conf", **kwargs)
        | date_conf_pairs()
        | combine_date_conf_pairs(name="day_conf", **kwargs)
        | year_pairs(**kwargs)
        | merge_years(name="day_conf", **kwargs)
        | backup_tiles()
    )
    return pipe


def date_conf_pipe(tile_ids, **kwargs):
    """
    Pipeline to process latest GLAD alerts
    :param tile_ids: List of Tile IDs to process
    :param kwargs: Dictonary with keyword arguments
    :return: pipe
    """
    pipe = (
        tile_ids
        | download_latest_tiles(name="download", **kwargs)
        | change_pixel_depth(name="pixel_depth", **kwargs)
        | encode_date_conf(name="encode_day_conf", **kwargs)
        | date_conf_pairs()
        | combine_date_conf_pairs(name="day_conf", **kwargs)
        | all_year_pairs(**kwargs)
        | merge_years(name="day_conf", **kwargs)
    )
    return pipe


def resample_date_conf_pipe(tiles, **kwargs):

    pipe = (
        tiles
        | Stage(
            resample, name="day_conf", resample_method="near", zoom=12, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=11, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=10, **kwargs
        ).setup(workers=2)
        | build_vrt(name="day_conf", zoom=10, **kwargs)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=9, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=8, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=7, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=6, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=5, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=4, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=3, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=2, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=1, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=0, **kwargs
        ).setup(workers=2)
    )
    return pipe


def intensity_pipe(tiles, **kwargs):
    pipe = (
        tiles
        | unset_no_data_value()
        | prep_intensity(name="day_conf", **kwargs)
        | Stage(
            resample, name="intensity", resample_method="near", zoom=12, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=11, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=10, **kwargs
        ).setup(workers=2)
        | build_vrt(name="intensity", zoom=10, **kwargs)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=9, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=8, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=7, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=6, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=5, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=4, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=3, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=2, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=1, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=0, **kwargs
        ).setup(workers=2)
    )
    return pipe


def rgb_pipeline(**kwargs):
    root = kwargs["root"]

    pipe = collect_resampled_tiles(root) | encode_rgb | project

    return pipe


def get_parser():
    """
    Build parser for command line input
    :return: Parser for command line input
    """
    parser = argparse.ArgumentParser(description="Change the data type of a raster.")
    parser.add_argument(
        "--debug",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Activate debug mode.",
    )
    return parser.parse_args()


def get_data_root():
    """
    Get data root based on current working directory
    :return: Data root path
    """
    cwd = pathlib.Path.cwd()
    return cwd.parent.joinpath("data").as_posix()


def main():

    args = get_parser()

    get_logger(debug=args.debug)

    tile_ids = get_tile_ids_by_bbox(-50, -10, -40, 10)
    root = get_data_root()  # "/home/thomas/shared-drives/glad-pipeline"
    years = [2018, 2019]
    preprocessed_years = range(2015, min(years))

    kwargs: Dict[str, Any] = {
        "years": years,
        "root": root,
        "preprocessed_years": preprocessed_years,
    }

    try:
        kwargs["tile_date"] = get_most_recent_day(tile_ids=tile_ids, **kwargs)
    except ValueError:
        logging.error("Cannot find recently processes tiles. Aborting")
        pass
    else:

        if os.path.exists(root):
            shutil.rmtree(root)

        pipe = preprocessed_tile_pipe(tile_ids=tile_ids, **kwargs)

        for output in pipe.results():
            logging.debug("Preprocess output: " + str(output))
        logging.info("Preprocess - Done")

        pipe = date_conf_pipe(tile_ids, **kwargs)

        date_conf_tiles = list()
        for output in pipe.results():
            date_conf_tiles.append(output)
            logging.debug("Date Conf  output: " + str(output))
        logging.info("Date Conf - Done")

        pipe = resample_date_conf_pipe(date_conf_tiles, **kwargs)
        for output in pipe.results():
            logging.debug("Resample Day Conf output: " + str(output))
        logging.info("Resample Day Conf - Done")

        pipe = intensity_pipe(date_conf_tiles, **kwargs)

        for output in pipe.results():
            logging.debug("Intensity output: " + str(output))
        logging.info("Intensity - Done")

    finally:
        pass


def test():
    # args = get_parser()

    get_logger(debug=True)

    # tile_ids = get_tile_ids_by_bbox(-50, -10, -40, 10)
    root = get_data_root()  # "/home/thomas/shared-drives/glad-pipeline"
    years = [2018, 2019]
    preprocessed_years = range(2015, min(years))

    kwargs: Dict[str, Any] = {
        "years": years,
        "root": root,
        "preprocessed_years": preprocessed_years,
    }
    """
    date_conf_tiles = [
        "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_00N_040W_10N/day_conf/2015_2016_2017_2018_2019/day_conf.tif",
        "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_10S_040W_00N/day_conf/2015_2016_2017_2018_2019/day_conf.tif",
    ]
    pipe = resample_date_conf_pipe(date_conf_tiles, **kwargs)
    for output in pipe.results():
        logging.debug("Resample Day Conf output: " + str(output))
    logging.info("Resample Day Conf - Done")

    pipe = intensity_pipe(date_conf_tiles, **kwargs)

    for output in pipe.results():
        logging.debug("Intensity output: " + str(output))
    logging.info("Intensity - Done")

    """
    tiles = collect_resampled_tiles(kwargs["root"])

    for tile in tiles:
        print(tile)


if __name__ == "__main__":
    # main()
    test()
