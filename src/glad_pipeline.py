from stages.pipes import (
    preprocessed_tile_pipe,
    date_conf_pipe,
    resample_date_conf_pipe,
    intensity_pipe,
    rgb_pipe,
    copy_vrt_s3_pipe,
    tilecache_pipe,
    csv_export_pipe,
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
        "paths": {
            "emissions": "s3://gfw2-data/climate/WHRC_biomass/WHRC_V4/t_co2_pixel/{top}_{left}_mt_co2_pixel_2000.tif",
            "climate_mask": "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/climate/climate_mask/climate_mask_{top}_{left}.tif",
            "preprocessed": "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/tiles/{tile_id}/{product}{year}.tif",
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
        preprocessed_tile_pipe(tile_ids=tile_ids, **kwargs)
        date_conf_tiles = date_conf_pipe(tile_ids, **kwargs)
        resample_date_conf_pipe(date_conf_tiles, **kwargs)
        intensity_pipe(date_conf_tiles, **kwargs)
        rgb_pipe(**kwargs)
        # copy_vrt_s3_pipe(**kwargs)
        tilecache_pipe(**kwargs)
        csv_export_pipe(**kwargs)

    finally:
        # TODO
        #  copy all files to S3, including log files
        #  Shut down server
        pass


if __name__ == "__main__":
    main()
