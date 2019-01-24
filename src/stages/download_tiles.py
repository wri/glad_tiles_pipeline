from helpers.utils import output_file, preprocessed_years_str, get_suffix, get_gs_bucket
import subprocess as sp
import logging


def download_latest_tiles(tile_ids, **kwargs):

    years = kwargs["years"]
    tile_date = kwargs["tile_date"]
    root = kwargs["root"]
    name = kwargs["name"]

    url_pattern = "GLADalert/{date}/alert{product}{year_dig}_{tile_id}.tif"

    bucket = get_gs_bucket()

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
                output = output_file(
                    root, "tiles", tile_id, name, year, product + ".tif"
                )

                try:
                    logging.debug("Attempt to download " + tif_url)
                    blob = bucket.blob(tif_url)
                    blob.download_to_filename(output)
                except sp.CalledProcessError:
                    logging.warning("Failed to download file: " + tif_url)
                else:
                    logging.info("Downloaded file: " + tif_url)
                    logging.debug(output)
                    yield output


def download_preprocessed_tiles_years(tile_ids, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]
    preprocessed_years = kwargs["preprocessed_years"]

    year_str = preprocessed_years_str(preprocessed_years)

    for tile_id in tile_ids:

        s3_url = "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/tiles/{}/day_conf/{}/day_conf.tif".format(
            tile_id, year_str
        )

        output = output_file(root, "tiles", tile_id, name, year_str, "day_conf.tif")

        try:
            sp.check_call(["aws", "s3", "cp", s3_url, output])
        except sp.CalledProcessError:
            logging.warning("Failed to download file: " + s3_url)
            yield tile_id
        else:
            logging.info("Downloaded file: " + s3_url)


def download_preprocessed_tiles_year(tile_ids, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]
    preprocessed_years = kwargs["preprocessed_years"]

    s3_url = "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/tiles/{}/{}{}.tif"

    for tile_id in tile_ids:
        for year in preprocessed_years:
            for product in ["day", "conf"]:
                output = output_file(
                    root, "tiles", tile_id, name, year, product + ".tif"
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
