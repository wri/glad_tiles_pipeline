from glad.pipes import (
    preprocessed_tile_pipe,
    date_conf_pipe,
    resample_date_conf_pipe,
    intensity_pipe,
    rgb_pipe,
    tilecache_pipe,
    download_climate_data,
    csv_export_pipe,
    stats_db,
)
from glad.stages.collectors import get_most_recent_day
from glad.utils.tiles import get_tile_ids_by_bbox
from glad.utils.utils import get_data_root, output_file
from typing import Dict, Any
import os
import shutil
import logging
import sys
import multiprocessing
import argparse
from datetime import datetime


def main():

    args = _get_parser()

    _get_logger(debug=args.debug)

    if not args.ignore_preprocessed_years:
        preprocessed_years = range(2015, min(args.years))
    else:
        preprocessed_years = list()

    tile_ids = get_tile_ids_by_bbox(
        args.bbox[0], args.bbox[1], args.bbox[2], args.bbox[3]
    )

    if args.include_russia:
        tile_ids.append("130E_42N_142E_53N")

    root = get_data_root()

    kwargs: Dict[str, Any] = {
        "workers": args.workers,
        "years": args.years,
        "root": root,
        "preprocessed_years": preprocessed_years,
        "max_zoom": args.max_zoom,
        "min_zoom": args.min_zoom,
        "min_tile_zoom": args.min_tile_zoom,
        "max_tilecache_zoom": args.max_tilecache_zoom,
        "num_tiles": args.num_tiles,
        "test": args.test,
        "db": {
            "db_path": output_file(root, "db", "stats.db"),
            "db_table": "tile_alert_stats",
        },
        "paths": {
            "emissions": "s3://gfw2-data/climate/WHRC_biomass/WHRC_V4/t_co2_pixel/{top}_{left}_mt_co2_pixel_2000.tif",
            "climate_mask": "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/climate/climate_mask/climate_mask_{top}_{left}.tif",
            "preprocessed": "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/tiles/{tile_id}/{product}{year}.tif",
            "stats_db": "s3://palm-risk-poc/data/mvt/stats.db",
        },
    }

    try:
        # TODO
        #  add some logic to skip this step in case we don't deal with current years
        kwargs["tile_date"], tile_ids = get_most_recent_day(tile_ids=tile_ids, **kwargs)
    except ValueError:
        logging.error("Cannot find recently processes tiles. Aborting")
    else:

        if os.path.exists(root):
            # ignore_errors true will allow us to mount the data directory as a docker volume.
            # If not set, this will though an IOError b/c it won't be able to delete mounted volume
            # Data inside the directory/ volume - if any - will still be removed
            shutil.rmtree(root, ignore_errors=True)

        # TODO
        #  add some logic to skip preprocssing step incase igonore_preprocessed_tiles is true
        preprocessed_tile_pipe(tile_ids, **kwargs)
        date_conf_tiles = date_conf_pipe(tile_ids, **kwargs)
        resample_date_conf_pipe(date_conf_tiles, **kwargs)
        intensity_pipe(date_conf_tiles, **kwargs)
        rgb_pipe(**kwargs)
        # copy_vrt_s3_pipe(**kwargs)
        tilecache_pipe(**kwargs)
        download_climate_data(tile_ids, **kwargs)
        csv_export_pipe(**kwargs)
        stats_db(**kwargs)

    finally:
        # TODO
        #  copy all files to S3, including log files
        #  Shut down server
        pass


def _get_parser():
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

    return parser.parse_args()


def _get_logger(debug=True):
    """
    Build logger
    :param debug: Set Log Level to Debug or Info
    :return: logger
    """
    now = datetime.now()
    log_dir = "/usr/data/log"

    # TODO: use SysLogHandler instead of FileHandler
    #  https://stackoverflow.com/questions/36762016/best-practice-to-write-logs-in-var-log-from-a-python-script
    try:
        os.makedirs(log_dir)
    except FileExistsError:
        # directory already exists
        pass

    root = logging.getLogger()
    if debug:
        root.setLevel(logging.DEBUG)
    else:
        root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    sh = logging.StreamHandler(sys.stdout)
    fh = logging.FileHandler(
        "{}/glad-{}.log".format(log_dir, now.strftime("%Y%m%d%H%M%S"))
    )

    fh.setLevel(logging.WARNING)

    sh.setFormatter(formatter)
    fh.setFormatter(formatter)

    root.addHandler(sh)
    root.addHandler(fh)

    return root


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


if __name__ == "__main__":
    main()
