from helpers.utils import get_gs_bucket
import datetime
import logging


def _check_tifs_exist(process_date, tile_ids, years):

    bucket = get_gs_bucket()

    name_list = [
        blob.name
        for blob in bucket.list_blobs(prefix="GLADalert/{}".format(process_date))
    ]

    logging.debug("Available TIFFS: " + str(name_list))
    available_tiles = list()

    for tile_id in tile_ids:
        c = 0
        for year in years:

            year_dig = str(year)[2:]

            conf_str = "GLADalert/{0}/alert{1}_{2}.tif".format(
                process_date, year_dig, tile_id
            )
            logging.debug("Checking for TIFF: " + conf_str)

            alert_str = "GLADalert/{0}/alertDate{1}_{2}.tif".format(
                process_date, year_dig, tile_id
            )
            logging.debug("Checking for TIFF: " + alert_str)

            filtered_names = [x for x in name_list if conf_str in x or alert_str in x]
            logging.debug("Found: " + str(filtered_names))

            # if both alert and conf rasters exist, tile is ready to download
            if len(filtered_names) == 2:
                c += 1

        if c == len(years):
            available_tiles.append(tile_id)

    logging.info(
        "Day {} has {} tiles for years {}".format(
            process_date, len(available_tiles), years
        )
    )

    return available_tiles


def get_most_recent_day(**kwargs):

    tile_ids = kwargs["tile_ids"]
    years = kwargs["years"]

    today = datetime.datetime.today()

    # check for most recent day of GLAD data
    for day_offset in range(0, 11):
        process_date = (today - datetime.timedelta(days=day_offset)).strftime(
            "%Y/%m_%d"
        )

        available_tiles = _check_tifs_exist(process_date, tile_ids, years)
        if available_tiles:
            return process_date, available_tiles

    logging.error("Checked GCS for last 10 days - none had all tiled TIFs")
    raise ValueError("Checked GCS for last 10 days - none had all tiled TIFs")
