from glad.utils.utils import file_details, output_file, get_tile_id, preprocessed_years_str
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

            cmd = ["aws", "s3", "cp", tile, output]

            logging.debug(cmd)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()
            logging.debug(o)
            if p.returncode == 0:
                logging.info("Upload file to " + output)
                yield tile
            else:
                logging.error("Failed to upload file to  " + output)
                logging.error(e)
                raise sp.CalledProcessError


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

            cmd = ["aws", "s3", "cp", tile, output]

            logging.debug(cmd)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()
            logging.debug(o)
            if p.returncode == 0:
                logging.info("Upload file " + output)
                yield tile
            else:
                logging.error("Failed to upload file to  " + output)
                logging.error(e)
                raise sp.CalledProcessError


def upload_day_conf_s3(tiles, **kwargs):
    """
    Uploads final encode tiles which include all years
    :param tiles:
    :param kwargs:
    :return:
    """

    # TODO: This function returns the input and should be marked accordingly

    env = kwargs["env"]
    year_str = preprocessed_years_str([2015, 2016, 2017, 2018, 2019])

    for tile in tiles:
        if env == "test":
            logging.info("Test run, skipped upload preprocessed tiles to S3: " + tile)
            yield tile

        else:
            tile_id = get_tile_id(tile)
            path = f"s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/tiles/{tile_id}/day_conf/{year_str}/day_conf.tif"
            output = path

            cmd = ["aws", "s3", "cp", tile, output]

            logging.debug(cmd)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()
            logging.debug(o)
            if p.returncode == 0:
                logging.info("Uploaded file " + output)
                yield tile
            else:
                logging.error("Failed to upload file to " + output)
                logging.error(e)
                raise sp.CalledProcessError


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
                cmd = [
                    "aws",
                    "s3",
                    "cp",
                    tile,
                    output,
                    "--profile",
                    "GFWPro_gfwpro-raster-data_remote",
                ]
                logging.debug(cmd)
                p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
                o, e = p.communicate()
                logging.debug(o)
                if p.returncode == 0:
                    logging.info("Uploaded file to GFW Pro: " + output)
                else:
                    logging.error("Failed to upload file to GFW Pro: " + output)
                    logging.error(e)
                    raise sp.CalledProcessError
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

            cmd = ["aws", "s3", "cp", tile, output]

            logging.debug(cmd)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()
            logging.debug(o)
            if p.returncode == 0:
                logging.info("Upload file to " + output)
                yield tile
            else:
                logging.error("Failed to upload file to " + output)
                logging.error(e)
                raise sp.CalledProcessError


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
        cmd = ["aws", "s3", "cp", folder, output, "--recursive"]

        logging.debug(cmd)
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()
        logging.debug(o)
        if p.returncode == 0:
            logging.info("Upload tilecache to " + output)
        else:
            logging.error("Failed to upload tilecache to " + output)
            logging.error(e)
            raise sp.CalledProcessError


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

            cmd = ["aws", "s3", "cp", csv, output]

            logging.debug(cmd)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()
            logging.debug(o)
            if p.returncode == 0:
                logging.info("Upload file to " + output)
                yield tile_df
            else:
                logging.error("Failed to upload file to " + output)
                logging.error(e)
                raise sp.CalledProcessError


# needs to run outside pipe
def upload_statsdb(**kwargs):

    env = kwargs["env"]
    db = kwargs["db"]["db_path"]
    path = kwargs["paths"]["stats_db"]

    if env == "test":
        logging.info("Test run, skipped upload db to S3: " + db)

    else:

        output = path.format(env=env)

        cmd = ["aws", "s3", "cp", db, output]

        logging.debug(cmd)
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()
        logging.debug(o)
        if p.returncode == 0:
            logging.info("Uploaded db to " + output)
        else:
            logging.error("Failed to upload db to  " + output)
            logging.error(e)
            raise sp.CalledProcessError


# needs to run outside pipe
def upload_logs(**kwargs):
    env = kwargs["env"]
    log = kwargs["log"]
    path = kwargs["paths"]["log"]

    if env == "test":
        logging.info("Test run, skipped upload logfile to S3: " + log)

    else:

        output = path.format(env=env, logfile=PurePath(log).parts[-1])

        cmd = ["aws", "s3", "cp", log, output]

        logging.debug(cmd)
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()
        logging.debug(o)
        if p.returncode == 0:
            logging.info("Uploaded logfile to " + output)
        else:
            logging.error("Failed to upload logfile to  " + output)
            logging.error(e)
            raise sp.CalledProcessError
