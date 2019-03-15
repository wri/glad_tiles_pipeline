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
from glad.stages.upload_tiles import upload_logs
from glad.utils.tiles import get_tile_ids_by_bbox
from glad.utils.utils import get_data_root, output_file
from glad.utils.parser import get_parser
from glad.utils.logger import get_logger, get_logfile
from typing import Dict, Any

import os
import shutil
import logging


def main():

    args = get_parser()

    logfile = get_logfile()
    get_logger(logfile, debug=args.debug)

    if not args.ignore_preprocessed_years:
        preprocessed_years = range(2015, min(args.years))
    else:
        preprocessed_years = list()

    tile_ids = get_tile_ids_by_bbox(
        args.bbox[0], args.bbox[1], args.bbox[2], args.bbox[3]
    )

    num_tiles = args.num_tiles
    if args.include_russia:
        num_tiles += 1
        tile_ids.append("130E_42N_142E_53N")

    root = get_data_root()

    s3_base_path = "s3://gfw2-data/forest_change/umd_landsat_alerts/"

    kwargs: Dict[str, Any] = {
        "workers": args.workers,
        "years": args.years,
        "root": root,
        "preprocessed_years": preprocessed_years,
        "max_zoom": args.max_zoom,
        "min_zoom": args.min_zoom,
        "min_tile_zoom": args.min_tile_zoom,
        "max_tilecache_zoom": args.max_tilecache_zoom,
        "num_tiles": num_tiles,
        "env": args.env,
        "log": logfile,
        "db": {
            "db_path": output_file(root, "db", "stats.db"),
            "db_table": "tile_alert_stats",
        },
        "paths": {
            "emissions": "s3://gfw2-data/climate/WHRC_biomass/WHRC_V4/t_co2_pixel/{top}_{left}_t_co2_pixel_2000.tif",
            "climate_mask": "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/climate/climate_mask/climate_mask_{top}_{left}.tif",
            "preprocessed": "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/tiles/{tile_id}/{product}{year}.tif",
            "encoded_backup": s3_base_path + "{env}/encoded/{year_str}/{tile_id}.tif",
            "raw_backup": s3_base_path + "{env}/raw/{year}/{product}/{tile_id}.tif",
            "resampled_rgb": s3_base_path + "{env}/rgb/{zoom}/{tile_id}.tif",
            "analysis": s3_base_path + "{env}/analysis/{tile_id}.tif",
            "csv": s3_base_path + "{env}/csv/{tile_id}_{year}.csv",
            "stats_db": s3_base_path + "{env}/db/stats.db",
            "pro": "s3://gfwpro-raster-data/{pro_id}",
            "tilecache": "s3://wri-tiles/glad_{env}/tiles",
            "log": s3_base_path + "{env}/log/{logfile}",
        },
    }

    try:
        # TODO
        #  add some logic to skip this step in case we don't deal with current years
        kwargs["tile_date"], tile_ids = get_most_recent_day(tile_ids=tile_ids, **kwargs)
    except ValueError:
        logging.error("Cannot find recently processes tiles. Aborting")
    else:
        try:

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
            tilecache_pipe(**kwargs)
            download_climate_data(tile_ids, **kwargs)
            csv_export_pipe(**kwargs)
            stats_db(**kwargs)
        except Exception as e:
            logging.exception(e)

    finally:
        upload_logs(**kwargs)

        if args.shutdown:

            logging.warning("Send shutdown signal")

            # signal for docker host to shutdown
            f = open("/var/log/glad/done", "w+")
            f.close()


if __name__ == "__main__":
    main()
