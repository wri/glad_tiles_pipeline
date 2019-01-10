from parallelpipe import stage
from utils.utils import output_tiles
import subprocess as sp
import logging


def get_suffix(product):
    if product == "day":
        return "Date"
    else:
        return ""


@stage(workers=2)
def download_latest_tiles(tile_ids, **kwargs):

    years = kwargs["years"]
    tile_date = kwargs["tile_date"]
    root = kwargs["root"]
    url_pattern = "gs://earthenginepartners-hansen/alert/{date}/GLADalert_{date}_alert{product}{year_dig}_{tile_id}.tif"

    for tile_id in tile_ids:
        for year in years:
            year_dig = str(year)[2:]

            for product in ["day", "conf"]:

                tif_url = url_pattern.format(
                    date=tile_date,
                    year_dig=year_dig,
                    tile_id=tile_id,
                    product=get_suffix(product),
                )
                output = output_tiles(root, tile_id, "download", year, product + ".tif")

                try:
                    sp.check_call(["gsutil", "cp", tif_url, output])
                except sp.CalledProcessError:
                    logging.warning("Failed to download file: " + tif_url)
                else:
                    logging.info("Downloaded file: " + tif_url)
                    yield output
