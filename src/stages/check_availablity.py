from google.cloud import storage
import datetime
import logging


def _check_tifs_exist(day_str, tiles, years):

    client = storage.Client()
    bucket = client.bucket("earthenginepartners-hansen")

    name_list = [
        blob.name for blob in bucket.list_blobs(prefix="alert/{}".format(day_str))
    ]

    logging.debug("Available TIFFS: " + str(name_list))

    tif_count = 0

    for year in years:
        year_dig = str(year)[2:]
        for tile in tiles:
            conf_str = "alert/{0}/GLADalert_{0}_alert{1}_{2}.tif".format(
                day_str, year_dig, tile
            )
            logging.debug("Checking for TIFF: " + conf_str)

            alert_str = "alert/{0}/GLADalert_{0}_alertDate{1}_{2}.tif".format(
                day_str, year_dig, tile
            )
            logging.debug("Checking for TIFF: " + alert_str)

            filtered_names = [x for x in name_list if conf_str in x or alert_str in x]
            logging.debug("Found: " + str(filtered_names))

            # if both alert and conf rasters exist, tile is ready to download
            if len(filtered_names) == 2:
                tif_count += 1

        logging.info(
            "Day {} has {} out of {} tiles for year {}".format(
                day_str, tif_count, len(tiles), year
            )
        )

    return tif_count == len(tiles) * len(years)


def get_most_recent_day(**kwargs):

    tiles = kwargs["tiles"]
    years = kwargs["years"]

    today = datetime.datetime.today()
    final_day = None

    # check for most recent day of GLAD data
    for day_offset_int in range(0, 11):
        day_offset_str = (today - datetime.timedelta(days=day_offset_int)).strftime(
            "%m_%d"
        )

        if _check_tifs_exist(day_offset_str, tiles, years):
            final_day = day_offset_str
            break

    final_day = "01_09"  # TODO: remove this line

    if final_day:
        return final_day

    else:
        logging.error("Checked GCS for last 10 days - none had all tiled TIFs")
        raise ValueError("Checked GCS for last 10 days - none had all tiled TIFs")
