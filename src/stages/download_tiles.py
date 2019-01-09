from parallelpipe import stage
import subprocess as sp
import os
import pathlib
import logging


def get_suffix(product):
    if product == "day":
        return "Date"
    else:
        return ""


@stage(workers=4)
def download_latest_tiles(tiles, years, tile_date, root):

    url_pattern = "gs://earthenginepartners-hansen/alert/{date}/GLADalert_{date}_alert{product}{year_dig}_{tile}.tif"

    for tile in tiles:
        for year in years:
            year_dig = str(year)[2:]

            for product in ["day", "conf"]:
                tif_url = url_pattern.format(
                    date=tile_date,
                    year_dig=year_dig,
                    tile=tile,
                    product=get_suffix(product),
                )
                output_dir = os.path.join(root, "tiles", tile, "download")
                pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
                output_file = os.path.join(output_dir, product + str(year) + ".tif")
                try:
                    sp.check_call(["gsutil", "cp", tif_url, output_file])
                except sp.CalledProcessError:
                    logging.warning("Failed to download file: " + tif_url)
                else:
                    logging.info("Downloaded file: " + tif_url)
                    yield output_file
