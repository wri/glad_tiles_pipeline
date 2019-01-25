from helpers.utils import file_details, get_file_name
import subprocess as sp
import logging


def upload_preprocessed_tiles_s3(tiles, **kwargs):

    test = kwargs["test"]

    if not test:
        for tile in tiles:
            f_name, year, folder, tile_id = file_details(tile)
            output = "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/tiles/{}/{}/{}/{}".format(
                tile_id, folder, year, f_name
            )

            try:
                sp.check_call(["aws", "s3", "cp", tile, output])
            except sp.CalledProcessError:
                logging.warning("Failed to upload file: " + tile)
            else:
                logging.info("Upload file: " + tile)
                yield tile
    else:
        logging.debug("Test run , skipped upload preprocessed tiles to S3")


def upload_day_conf_s3(tiles, **kwargs):

    test = kwargs["test"]

    if not test:
        for tile in tiles:
            f_name, year, folder, tile_id = file_details(tile)
            output = "s3://palm-risk-poc/data/glad/analysis-staging/{}/{}".format(
                tile_id, f_name
            )

            try:
                sp.check_call(["aws", "s3", "cp", tile, output])
            except sp.CalledProcessError:
                logging.warning("Failed to upload file: " + tile)
            else:
                logging.info("Upload file: " + tile)
                yield tile
    else:
        logging.debug("Test run , skipped upload day_conf to S3 bucket")


def upload_day_conf_s3_gfw_pro(tiles, pro_tiles, **kwargs):

    test = kwargs["test"]

    if not test:
        for tile in tiles:
            f_name, year, folder, tile_id = file_details(tile)
            if tile_id in pro_tiles.keys():
                output = "s3://gfwpro-raster-data/{}".format(pro_tiles[tile_id])

                try:
                    sp.check_call(
                        [
                            "aws",
                            "s3",
                            "cp",
                            tile,
                            output,
                            "--profile",
                            "GFWPro_gfwpro-raster-data_remote",
                        ]
                    )
                except sp.CalledProcessError:
                    logging.warning("Failed to upload file: " + tile)
                else:
                    logging.info("Upload file: " + tile)
                    yield tile
    else:
        logging.debug("Test run , skipped upload day_conf to GFW Pro S3 bucket")


def upload_vrt_s3(zoom_vrts, **kwargs):
    test = kwargs["test"]

    if not test:
        for zoom_vrt in zoom_vrts:
            vrt = zoom_vrt[1]

            vrt_name = get_file_name(vrt)
            output = "s3://palm-risk-poc/data/glad/rgb/{}".format(vrt_name)

            try:
                sp.check_call(["aws", "s3", "cp", vrt, output])
            except sp.CalledProcessError:
                logging.warning("Failed to upload file: " + vrt)
            else:
                logging.info("Upload file: " + vrt)
                yield zoom_vrt
    else:
        logging.debug("Test run , skipped upload day_conf to S3 bucket")


def upload_vrt_tiles_s3(zoom_vrts, zoom_tiles, **kwargs):
    test = kwargs["test"]

    if not test:
        for zoom_vrt in zoom_vrts:
            zoom = zoom_vrt[0]

            for zoom_tile in zoom_tiles:
                if zoom_tile[0] == zoom:
                    for tile in zoom_tile[1]:

                        f_name, zoom_level, folder, tile_id = file_details(tile)

                        output = "s3://palm-risk-poc/data/glad/rgb/{}/{}/{}/{}".format(
                            tile_id, folder, zoom_level, f_name
                        )

                        try:
                            sp.check_call(["aws", "s3", "cp", tile, output])
                        except sp.CalledProcessError:
                            logging.warning("Failed to upload file: " + tile)
                        else:
                            logging.info("Upload file: " + tile)
                            yield tile
    else:
        logging.debug("Test run , skipped upload day_conf to S3 bucket")
