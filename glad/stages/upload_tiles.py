from glad.utils.utils import file_details, output_file, get_tile_id
from pathlib import PurePath
import subprocess as sp
import logging


def upload_preprocessed_tiles_s3(tiles, **kwargs):

    """
    Backups prepocessed tiles for previous years to S3
    :param tiles:
    :param kwargs:
    :return:
    """

    # TODO: This function returns the input and should be marked accordingly

    env = kwargs["env"]
    path = kwargs["paths"]["encoded_backup"]

    for tile in tiles:
        if env == "test":
            logging.info("Test run, skipped upload preprocessed tiles to S3: " + tile)
            yield tile

        else:
            f_name, year_str, folder, tile_id = file_details(tile)
            output = path.format(env=env, year_str=year_str, tile_id=tile_id)

            try:
                sp.check_call(["aws", "s3", "cp", tile, output])
            except sp.CalledProcessError:
                logging.warning("Failed to upload file to  " + output)
            else:
                logging.info("Upload file to " + output)
                yield tile


def upload_raw_tile_s3(tiles, **kwargs):
    """
    Backups raw day and conf tiles for single years
    :param tiles:
    :param kwargs:
    :return:
    """

    # TODO: This function returns the input and should be marked accordingly

    env = kwargs["env"]
    path = kwargs["paths"]["raw_backup"]

    for tile in tiles:
        if env == "test":
            logging.info("Test run, skipped upload preprocessed tiles to S3: " + tile)
            yield tile

        else:
            f_name, year, folder, tile_id = file_details(tile)
            output = path.format(
                env=env, year=year, product=f_name[:-4], tile_id=tile_id
            )

            try:
                sp.check_call(["aws", "s3", "cp", tile, output])
            except sp.CalledProcessError:
                logging.warning("Failed to upload file to  " + output)
            else:
                logging.info("Upload file " + output)
                yield tile


def upload_day_conf_s3(tiles, **kwargs):
    """
    Uploads final encode tiles which include all years
    :param tiles:
    :param kwargs:
    :return:
    """

    # TODO: This function returns the input and should be marked accordingly

    env = kwargs["env"]
    path = kwargs["paths"]["analysis"]

    for tile in tiles:
        if env == "test":
            logging.info("Test run, skipped upload preprocessed tiles to S3: " + tile)
            yield tile

        else:
            tile_id = get_tile_id(tile)
            output = path.format(env=env, tile_id=tile_id)

            try:
                sp.check_call(["aws", "s3", "cp", tile, output])
            except sp.CalledProcessError:
                logging.warning("Failed to upload file to " + output)
            else:
                logging.info("Upload file " + output)
                yield tile


def upload_day_conf_s3_gfw_pro(tiles, pro_tiles, **kwargs):
    """
    Uploads final encode tiles which include all years to S3 GFW-Pro account
    :param tiles:
    :param kwargs:
    :return:
    """

    # TODO: This function returns the input and should be marked accordingly

    env = kwargs["env"]
    path = kwargs["paths"]["pro"]

    for tile in tiles:
        if env != "prod":  # No Staging environment for Pro!
            logging.info("Test run, skipped upload preprocessed tiles to S3: " + tile)
            yield tile

        else:
            tile_id = get_tile_id(tile)
            if tile_id in pro_tiles.keys():
                output = path.format(pro_id=pro_tiles[tile_id])

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
                    logging.warning("Failed to upload file to  " + output)
                else:
                    logging.info("Upload file to " + output)
                    yield tile


def upload_rgb_wm_s3(tiles, **kwargs):

    env = kwargs["env"]
    path = kwargs["paths"]["resampled_rgb"]

    for tile in tiles:
        if env == "test":
            logging.info("Test run, skipped upload preprocessed tiles to S3: " + tile)
            yield tile

        else:
            f_name, zoom, folder, tile_id = file_details(tile)
            output = path.format(env=env, zoom=zoom, tile_id=tile_id)

            try:
                sp.check_call(["aws", "s3", "cp", tile, output])
            except sp.CalledProcessError:
                logging.warning("Failed to upload file to " + output)
            else:
                logging.info("Upload file to " + output)
                yield tile


# Needs to be outside pipe!
def upload_tilecache_s3(**kwargs):
    root = kwargs["root"]
    env = kwargs["env"]
    path = kwargs["paths"]["tilecache"]

    folder = PurePath(root, "tilecache", "tiles").as_posix()

    if env == "test":
        logging.info("Test run, skipped upload tilecache to S3: " + folder)

    else:

        output = path.format(env=env)

        try:
            sp.check_call(["aws", "s3", "cp", folder, output, "--recursive"])
        except sp.CalledProcessError:
            logging.warning("Failed to upload tilecache to " + output)
        else:
            logging.info("Upload tilecache to " + output)


def upload_csv_s3(tile_dfs, name, **kwargs):

    # TODO: remove name from param list, if possible

    env = kwargs["env"]
    path = kwargs["paths"]["csv"]
    root = kwargs["root"]

    for tile_df in tile_dfs:

        year = tile_df[0]
        tile_id = tile_df[1]

        if env == "test":
            logging.info(
                "Test run, skipped upload preprocessed tiles to S3: " + tile_id
            )
            yield tile_df

        else:

            csv = output_file(root, name, "csv", year, tile_id + ".csv")

            output = path.format(env=env, year=year, tile_id=tile_id)

            try:
                sp.check_call(["aws", "s3", "cp", csv, output])
            except sp.CalledProcessError:
                logging.warning("Failed to upload file to " + output)
            else:
                logging.info("Upload file to " + output)
                yield tile_df


# needs to run outside pipe
def upload_statsdb(**kwargs):

    env = kwargs["env"]
    db = kwargs["db"]["db_path"]
    path = kwargs["paths"]["stats_db"]

    if env == "test":
        logging.info("Test run, skipped upload db to S3: " + db)

    else:

        output = path.format(env=env)

        try:
            sp.check_call(["aws", "s3", "cp", db, output, "--recursive"])
        except sp.CalledProcessError:
            logging.warning("Failed to upload db to  " + output)
        else:
            logging.info("Upload db to " + output)
