from stages.download_tiles import (
    download_latest_tiles,
    download_preprocessed_tiles_years,
    download_preprocessed_tiles_year,
)
from stages.check_availablity import get_most_recent_day
from stages.change_pixel_depth import change_pixel_depth
from stages.encode_glad import encode_date_conf, date_conf_pairs
from stages.merge_tiles import (
    combine_date_conf_pairs,
    year_pairs,
    merge_years,
    merge_all_years,
)
import os
import shutil
import logging
import sys
import argparse

from typing import Dict, Any, List

tiles = ["050W_10S_040W_00N", "050W_00N_040W_10N"]

kwargs: Dict[str, Any] = {
    "years": [2018, 2019],
    "root": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data",
}


def str2bool(v):
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def get_logger(debug=True):
    root = logging.getLogger()
    if debug:
        root.setLevel(logging.DEBUG)
    else:
        root.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    if debug:
        handler.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)
    return root


def main():

    parser = argparse.ArgumentParser(description="Change the data type of a raster.")
    parser.add_argument(
        "--debug",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Activate debug mode.",
    )
    args = parser.parse_args()
    get_logger(debug=args.debug)

    root = kwargs["root"]
    kwargs["missing_years"] = range(2015, min(kwargs["years"]))

    try:
        kwargs["tile_date"] = get_most_recent_day(tiles=tiles, **kwargs)
    except ValueError:
        logging.error("Cannot find recently processes tiles. Aborting")
        pass
    else:

        if os.path.exists(root):
            shutil.rmtree(root)

        pipe = (
            tiles
            | download_preprocessed_tiles_years(**kwargs)
            | download_preprocessed_tiles_year(**kwargs)
            | date_conf_pairs()
            | combine_date_conf_pairs(**kwargs)
            | year_pairs(**kwargs)
            | merge_years(**kwargs)
            # | backup_files(**kwargs)
        )

        for output in pipe.results():
            logging.info("Intermediate  output: " + str(output))

        pipe = (
            tiles
            | download_latest_tiles(**kwargs)
            | change_pixel_depth(**kwargs)
            | encode_date_conf(**kwargs)
            | date_conf_pairs()
            | combine_date_conf_pairs(**kwargs)
            | year_pairs(**kwargs)
            | merge_all_years(**kwargs)
        )

        for output in pipe.results():
            logging.info("Final  output: " + str(output))

    finally:
        pass


def test():
    get_logger(debug=True)
    input = [
        {
            "day": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_00N_040W_10N/encode_date_conf/2019/day.tif",
            "conf": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_00N_040W_10N/encode_date_conf/2019/conf.tif",
        },
        {
            "day": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_10S_040W_00N/encode_date_conf/2019/day.tif",
            "conf": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_10S_040W_00N/encode_date_conf/2019/conf.tif",
        },
        {
            "day": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_10S_040W_00N/encode_date_conf/2018/day.tif",
            "conf": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_10S_040W_00N/encode_date_conf/2018/conf.tif",
        },
        {
            "day": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_00N_040W_10N/encode_date_conf/2018/day.tif",
            "conf": "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/data/tiles/050W_00N_040W_10N/encode_date_conf/2018/conf.tif",
        },
    ]

    pipe = input | combine_date_conf_pairs(**kwargs)

    for tif in pipe.results():
        logging.info("Final  output: " + str(tif))


if __name__ == "__main__":
    # main()
    test()
