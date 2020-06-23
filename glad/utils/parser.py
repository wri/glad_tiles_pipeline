import argparse
import multiprocessing
from datetime import datetime


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
        "--shutdown",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Shutdown server once process completed.",
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
        "--env",
        type=str,
        default="test",
        choices=["prod", "staging", "test"],
        help="Will copy data to the selected environment. Won't copy data for test.",
    )
    parser.add_argument("--min_zoom", type=int, default=0, help="Minimum zoom level")
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
    parser.add_argument(
        "--num_tiles",
        "-n",
        type=int,
        default=115,
        help="Number of expected input tiles",
    )
    parser.add_argument(
        "--max_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=datetime.today(),
        help="Max date to use when fetching new tiles",
    )

    return parser.parse_args()


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


def get_current_years():
    now = datetime.now()
    year = now.year
    month = now.month

    if month < 7:
        return [year - 1, year]
    else:
        return [year]
