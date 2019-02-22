from glad.utils.utils import (
    output_file,
    preprocessed_years_str,
    get_suffix,
    get_tile_id,
)
from glad.utils.tiles import get_bbox_by_tile_id, get_latitude, get_longitude
from glad.utils.google import get_gs_bucket
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


def download_emissions(tile_ids, **kwargs):
    """
    Downloads carbon emission tiles from S3
    :param tile_ids: list of tile ids to download
    :param kwargs: global keyword arguments
    :return: outfile or input tile_id
    """
    root = kwargs["root"]
    name = kwargs["name"]
    s3_url = kwargs["paths"]["emissions"]
    return_input = kwargs["return_input"]

    for tile_id in tile_ids:

        output = output_file(root, "climate", name, tile_id + ".tif")

        left, bottom, right, top = get_bbox_by_tile_id(tile_id)
        top = get_latitude(top)
        left = get_longitude(left)

        cmd = ["aws", "s3", "cp", s3_url.format(top=top, left=left), output]

        try:
            logging.debug("Download file: " + s3_url.format(top=top, left=left))
            sp.check_call(cmd)
        except sp.CalledProcessError:
            logging.warning(
                "Failed to download file: " + s3_url.format(top=top, left=left)
            )
        else:
            logging.info("Downloaded file: " + s3_url.format(top=top, left=left))
            if not return_input:
                yield output
        finally:
            if return_input:
                yield tile_id


def download_climate_mask(tile_ids, **kwargs):
    """
    Downloads climate mask from S3
    Not all tiles have a climate mask!
    :param tile_ids: list of tile ids to download
    :param kwargs: global keyword arguments
    :return: outfile or input tile_id
    """
    root = kwargs["root"]
    name = kwargs["name"]
    s3_url = kwargs["paths"]["climate_mask"]
    return_input = kwargs["return_input"]

    for tile_id in tile_ids:

        output = output_file(root, "climate", name, tile_id + ".tif")

        left, bottom, right, top = get_bbox_by_tile_id(tile_id)
        top = get_latitude(top)
        left = get_longitude(left)

        try:
            logging.debug("Download file: " + s3_url.format(top=top, left=left))
            sp.check_call(
                ["aws", "s3", "cp", s3_url.format(top=top, left=left), output]
            )
        except sp.CalledProcessError:
            logging.warning(
                "Failed to download file: " + s3_url.format(top=top, left=left)
            )
        else:
            logging.info("Downloaded file: " + s3_url.format(top=top, left=left))
            if not return_input:
                yield output
        finally:
            if return_input:
                yield tile_id


def download_stats_db(**kwargs):
    """
    Downloads stats_db from S3 and stores it in data/db folder
    :param kwargs: global keyword arguments
    :return: location of stats.db
    """

    s3_url = kwargs["paths"]["stats_db"]
    output = kwargs["db"]["db_path"]
    env = kwargs["env"]

    if env == "test":
        env = "stage"

    s3_url = s3_url.format(env=env)

    cmd = ["aws", "s3", "cp", s3_url, output]

    try:
        logging.debug("Download file: " + s3_url)
        sp.check_call(cmd)
    except sp.CalledProcessError as e:
        logging.warning("Failed to download file: " + s3_url)
        raise e
    else:
        logging.info("Downloaded file: " + s3_url)
        return output
