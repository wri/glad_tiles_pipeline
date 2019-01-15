from parallelpipe import stage
from helpers.utils import output_tiles, file_details
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
    url_pattern = "gs://earthenginepartners-hansen/GLADalert/{date}/alert{product}{year_dig}_{tile_id}.tif"

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


@stage(workers=2)
def download_preprocessed_tiles_years(tile_ids, **kwargs):
    root = kwargs["root"]
    missing_years = kwargs["missing_years"]

    year_str = "_".join(str(year) for year in missing_years)

    s3_url = "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/tiles/date_conf/{}/day_conf.tif".format(
        year_str
    )

    for tile_id in tile_ids:
        output = output_tiles(root, tile_id, "date_conf", year_str, "day_conf.tif")

        try:
            sp.check_call(["aws", "s3", "cp", s3_url, output])
        except sp.CalledProcessError:
            logging.warning("Failed to download file: " + s3_url)
            yield tile_id
        else:
            logging.info("Downloaded file: " + s3_url)


@stage(workers=2)
def download_preprocessed_tiles_year(tile_ids, **kwargs):
    root = kwargs["root"]
    missing_years = kwargs["missing_years"]

    s3_url = "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/tiles/{}/{}{}.tif"

    for tile_id in tile_ids:
        for year in missing_years:
            for product in ["day", "conf"]:
                output = output_tiles(
                    root, tile_id, "encode_date_conf", year, product + ".tif"
                )

                try:
                    sp.check_call(
                        [
                            "aws",
                            "s3",
                            "cp",
                            s3_url.format(tile_id, product, year),
                            output,
                        ]
                    )
                except sp.CalledProcessError:
                    logging.warning(
                        "Failed to download file: "
                        + s3_url.format(tile_id, product, year)
                    )
                else:
                    logging.info(
                        "Downloaded file: " + s3_url.format(tile_id, product, year)
                    )
                    yield output
