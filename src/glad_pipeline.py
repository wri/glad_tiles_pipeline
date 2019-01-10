from stages.download_tiles import download_latest_tiles
from stages.check_availablity import get_most_recent_day
from stages.change_pixel_depth import change_pixel_depth
from stages.encode_glad import encode_date_conf, date_conf_pairs
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

    try:
        kwargs["tile_date"] = get_most_recent_day(tiles=tiles, **kwargs)
    except ValueError:
        logging.error("Cannot find recently processes tiles. Aborting")
        pass
    else:
        root = kwargs["root"]
        if os.path.exists(root):
            shutil.rmtree(root)
        pipe = (
            tiles
            | download_latest_tiles(**kwargs)
            | change_pixel_depth(**kwargs)
            | encode_date_conf(**kwargs)
            | date_conf_pairs
        )

        for tif in pipe.results():
            logging.info("Downloaded TIF: " + tif)


if __name__ == "__main__":
    main()
