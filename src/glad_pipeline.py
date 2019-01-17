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
)
from stages.merge_tiles import (
    combine_date_conf_pairs,
    year_pairs,
    merge_years,
    all_year_pairs,
)
from stages.upload_tiles import backup_tiles
from stages.resample import resample

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
        | download_preprocessed_tiles_year(name="encode_date_conf", **kwargs)
        | change_pixel_depth(name="pixel_depth", **kwargs)
        | encode_date_conf(name="encode_date_conf", **kwargs)
        | date_conf_pairs()
        | combine_date_conf_pairs(name="date_conf", **kwargs)
        | year_pairs(**kwargs)
        | merge_years(name="date_conf", **kwargs)
        | backup_tiles()
    )
    return pipe


def latest_tile_pipe(tile_ids, **kwargs):
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
        | encode_date_conf(name="encode_date_conf", **kwargs)
        | date_conf_pairs()
        | combine_date_conf_pairs(name="date_conf", **kwargs)
        | all_year_pairs(**kwargs)
        | merge_years(name="final", **kwargs)
        | resample(name="resample", resample_method="near", zoom=12, **kwargs)
        | resample(name="resample", resample_method="mode", zoom=11, **kwargs)
        | resample(name="resample", resample_method="mode", zoom=10, **kwargs)
    )
    return pipe


def intensity_pipeline(tiles, **kwargs):
    pipe = (
        tiles
        | unset_no_data_value()
        | prep_intensity(name="final", **kwargs)
        | resample(name="resample", resample_method="near", zoom=12, **kwargs)
        | resample(name="resample", resample_method="bilinear", zoom=11, **kwargs)
        | resample(name="resample", resample_method="bilinear", zoom=10, **kwargs)
    )
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
    root = get_data_root()
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
            logging.debug("Intermediate  output: " + str(output))

        pipe = latest_tile_pipe(tile_ids=tile_ids, **kwargs)

        for output in pipe.results():
            logging.info("Final  output: " + str(output))

    finally:
        pass


if __name__ == "__main__":
    main()
