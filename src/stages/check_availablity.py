from google.cloud import storage
import datetime
import logging


def _check_tifs_exist(process_date, tile_ids, years):

    client = storage.Client()
    bucket = client.bucket("earthenginepartners-hansen")

    name_list = [
        blob.name
        for blob in bucket.list_blobs(prefix="GLADalert/{}".format(process_date))
    ]

    logging.debug("Available TIFFS: " + str(name_list))
    tif_count = list()

    for year in years:
        c = 0
        year_dig = str(year)[2:]
        for tile_id in tile_ids:
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
        tif_count.append(c)

        logging.info(
            "Day {} has {} out of {} tiles for year {}".format(
                process_date, c, len(tile_ids), year
            )
        )

    return sum(tif_count) == len(tile_ids) * len(years)


def get_most_recent_day(**kwargs):

    tile_ids = kwargs["tile_ids"]
    years = kwargs["years"]

    today = datetime.datetime.today()

    # check for most recent day of GLAD data
    for day_offset in range(0, 11):
        process_date = (today - datetime.timedelta(days=day_offset)).strftime(
            "%Y/%m_%d"
        )

        if _check_tifs_exist(process_date, tile_ids, years):
            return process_date

    logging.error("Checked GCS for last 10 days - none had all tiled TIFs")
    raise ValueError("Checked GCS for last 10 days - none had all tiled TIFs")
