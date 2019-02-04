from pathlib import Path, PurePath
from datetime import datetime
import subprocess as sp
import glob
import re
import argparse
import logging
import sys
import time
import multiprocessing


def output_mkdir(*path):
    """
    Creates directory for the parent directory
    :param path: List of path elements
    :return: None
    """
    output_dir = PurePath(*path)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    return Path(output_dir).as_posix()


def output_file(*path):
    """
    Creates POSIX path from input list
    and creates directory for the parent directory
    :param path: List of path elements
    :return: POSIX path
    """

    path = [str(p) for p in path if p is not None]
    output_mkdir(*path[:-1])

    return PurePath(*path).as_posix()


def file_details(f):
    p = PurePath(f)
    f_name = p.parts[-1]
    year = p.parts[-2]
    folder = p.parts[-3]
    tile_id = p.parts[-4]

    return f_name, year, folder, tile_id


def get_file_name(f):
    """
    Extract the last element of a POSIX path
    :param f: POSIX path
    :return: File name
    """
    p = PurePath(f)
    return p.parts[-1]


def get_tile_id(f):
    """
    Finds and returns tile id in file name
    Tile id must match the following pattern
    050W_20S_030E_10N

    :param f: File name
    :return: Tile ID or None
    """
    m = re.search("([0-9]{3}[EW]_[0-9]{2}[NS]_?){2}", f)
    if m:
        return m.group(0)
    else:
        return None


def preprocessed_years_str(preprocessed_years):
    return "_".join(str(year) for year in preprocessed_years)


def add_tile_to_dict(tile_dict, basedir, year, tile):
    if basedir not in tile_dict.keys():
        tile_dict[basedir] = dict()

    tile_dict[basedir][year] = tile

    return tile_dict


def add_preprocessed_tile_to_dict(tile_dict, basedir, preprocessed_tiles):
    for tile in preprocessed_tiles:
        year_str = PurePath(tile).parts[-2]
        if basedir == PurePath(tile).parent.parent.as_posix():
            tile_dict[basedir][year_str] = tile
    return tile_dict


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


def sort_dict(tile_dict):

    sorted_list = list()

    for year in sorted(list(tile_dict.keys())):
        sorted_list.append(tile_dict[year])

    return sorted_list


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


def get_data_root():
    """
    Get data root based on current working directory
    :return: Data root path
    """
    cwd = Path.cwd()
    return cwd.parent.joinpath("data").as_posix()


def get_parser():
    """
    Build parser for command line input
    :return: Parser for command line input
    """
    parser = argparse.ArgumentParser(description="Change the data type of a raster.")

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=int(multiprocessing.cpu_count() / 2),
        help="Maximum number of workers per stage",
    )
    parser.add_argument(
        "--bbox",
        "-b",
        type=int,
        nargs="+",
        default=[-120, -40, 180, 30],
        help="Bounding box for area to process (left, bottom, right, top)",
    )
    parser.add_argument(
        "--years", "-y", nargs="+", default=get_current_years(), help="Years to process"
    )
    parser.add_argument(
        "--ignore_preprocessed_years",
        type=str2bool,
        nargs="?",
        default=False,
        const=True,
        help="Ignore preprocessed years prior to years to process",
    )
    parser.add_argument(
        "--debug",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Activate debug mode.",
    )

    parser.add_argument(
        "--include_russia",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Activate debug mode.",
    )

    parser.add_argument(
        "--test",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Tun as test - will not copy any data to S3.",
    )

    parser.add_argument("--max_zoom", type=int, default=12, help="Maximum zoom level")
    parser.add_argument(
        "--min_tile_zoom",
        type=int,
        default=10,
        help="Minimum zoom level for 10x10 degree tiles",
    )
    parser.add_argument(
        "--max_tilecache_zoom",
        type=int,
        default=8,
        help="Maximum zoom level for building tilecache",
    )
    parser.add_argument("--min_zoom", type=int, default=0, help="Minimum zoom level")
    parser.add_argument(
        "--num_tiles",
        "-n",
        type=int,
        default=115,
        help="Number of expected input tiles",
    )

    return parser.parse_args()


def get_logger(debug=True):
    """
    Build logger
    :param debug: Set Log Level to Debug or Info
    :return: logger
    """

    root = logging.getLogger(__name__)
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


def split_list(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def get_current_years():
    now = datetime.now()
    year = now.year
    month = now.month

    if month < 7:
        return [year - 1, year]
    else:
        return [year]


def get_suffix(product):
    if product == "day":
        return "Date"
    else:
        return ""


def get_pro_tiles():
    out = sp.check_output(
        [
            "aws",
            "s3",
            "ls",
            "s3://gfwpro-raster-data",
            "--profile",
            "GFWPro_gfwpro-raster-data_remote",
        ]
    )
    obj_list = out.split(b"\n")

    pro_tiles = {
        obj.split("-glad_")[1].strip(): obj.strip() for obj in obj_list if "glad" in obj
    }
    return pro_tiles
