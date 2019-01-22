from stages.pipes import (
    preprocessed_tile_pipe,
    date_conf_pipe,
    resample_date_conf_pipe,
    intensity_pipe,
    rgb_pipe,
    tilecache_pipe,
)
from stages.check_availablity import get_most_recent_day
from helpers.tiles import get_tile_ids_by_bbox
from helpers.utils import get_data_root, get_parser, get_logger, get_current_years
from typing import Dict, Any
import os
import shutil
import logging


def main():

    args = get_parser()

    get_logger(debug=args.debug)

    tile_ids = get_tile_ids_by_bbox(-50, -10, -40, 10)
    root = get_data_root()
    years = get_current_years()
    preprocessed_years = range(2015, min(years))

    max_zoom = 12
    min_tile_zoom = 10
    max_tilecache_zoom = 8
    min_zoom = 0

    kwargs: Dict[str, Any] = {
        "years": years,
        "root": root,
        "preprocessed_years": preprocessed_years,
        "max_zoom": max_zoom,
        "min_zoom": min_zoom,
        "min_tile_zoom": min_tile_zoom,
        "max_tilecache_zoom": max_tilecache_zoom,
    }

    try:
        kwargs["tile_date"] = get_most_recent_day(tile_ids=tile_ids, **kwargs)
    except ValueError:
        logging.error("Cannot find recently processes tiles. Aborting")
        pass
    else:

        if os.path.exists(root):
            shutil.rmtree(root)

        preprocessed_tile_pipe(tile_ids=tile_ids, **kwargs)
        date_conf_tiles = date_conf_pipe(tile_ids, **kwargs)
        resample_date_conf_pipe(date_conf_tiles, **kwargs)
        intensity_pipe(date_conf_tiles, **kwargs)
        rgb_pipe(**kwargs)

    finally:
        pass


def test():
    # args = get_parser()

    get_logger(debug=True)

    tile_ids = get_tile_ids_by_bbox(-50, -10, -40, 10)
    root = get_data_root()
    years = get_current_years()
    preprocessed_years = range(2015, min(years))

    max_zoom = 12
    min_tile_zoom = 10
    max_tilecache_zoom = 8
    min_zoom = 0

    kwargs: Dict[str, Any] = {
        "years": years,
        "root": root,
        "preprocessed_years": preprocessed_years,
        "max_zoom": max_zoom,
        "min_zoom": min_zoom,
        "min_tile_zoom": min_tile_zoom,
        "max_tilecache_zoom": max_tilecache_zoom,
    }

    try:
        kwargs["tile_date"] = get_most_recent_day(tile_ids=tile_ids, **kwargs)
    except ValueError:
        logging.error("Cannot find recently processes tiles. Aborting")
        pass
    else:

        if os.path.exists(root):
            shutil.rmtree(root)

        date_conf_pipe(tile_ids, **kwargs)

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
    # pipe = tilecache_pipe(**kwargs)

    logging.info("Tilecache - Done")


if __name__ == "__main__":
    # main()
    test()
