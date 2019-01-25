from parallelpipe import Stage
from stages.download_tiles import (
    download_latest_tiles,
    download_preprocessed_tiles_years,
    download_preprocessed_tiles_year,
)
from stages.change_pixel_depth import change_pixel_depth
from stages.encode_glad import (
    encode_date_conf,
    prep_intensity,
    unset_no_data_value,
    encode_rgb,
    project,
)
from stages.merge_tiles import combine_date_conf_pairs, merge_years
from stages.upload_tiles import backup_tiles
from stages.resample import resample, build_vrt
from stages.tiles import (
    generate_tile_list,
    save_tile_lists,
    generate_vrt,
    generate_tilecache_mapfile,
    generate_tilecache_config,
    generate_tiles,
)
from stages.collectors import (
    collect_resampled_tiles,
    collect_rgb_tiles,
    collect_rgb_tile_ids,
    collect_day_conf,
    collect_day_conf_all_years,
    collect_day_conf_pairs,
)
import logging


def preprocessed_tile_pipe(tile_ids, **kwargs):
    """
    Pipeline to download/ process GLAD alerts of previous years
    :param tile_ids: List of Tile IDs to process
    :param kwargs: Dictonary with keyword arguments
    :return: pipe
    """
    workers = kwargs["workers"]
    pipe = (
        tile_ids
        | Stage(download_preprocessed_tiles_years, name="day_conf", **kwargs).setup(
            workers=workers
        )
        | Stage(download_preprocessed_tiles_year, name="download", **kwargs).setup(
            workers=workers
        )
        | Stage(change_pixel_depth, name="pixel_depth", **kwargs).setup(workers=workers)
        | Stage(encode_date_conf, name="encode_day_conf", **kwargs).setup(
            workers=workers
        )
        | Stage(collect_day_conf_pairs).setup(workers=1)  # !Important
        | Stage(combine_date_conf_pairs, name="day_conf", **kwargs).setup(
            workers=workers
        )
        | Stage(collect_day_conf, **kwargs).setup(workers=1)  # Important
        | Stage(merge_years, name="day_conf", **kwargs).setup(workers=workers)
        | Stage(backup_tiles).setup(workers=workers)
    )

    for output in pipe.results():
        logging.debug("Preprocess output: " + str(output))
    logging.info("Preprocess - Done")

    return


def date_conf_pipe(tile_ids, **kwargs):
    """
    Pipeline to process latest GLAD alerts
    :param tile_ids: List of Tile IDs to process
    :param kwargs: Dictonary with keyword arguments
    :return: pipe
    """
    workers = kwargs["workers"]
    pipe = (
        tile_ids
        | Stage(download_latest_tiles, name="download", **kwargs).setup(workers=workers)
        | Stage(change_pixel_depth, name="pixel_depth", **kwargs).setup(workers=workers)
        | Stage(encode_date_conf, name="encode_day_conf", **kwargs).setup(
            workers=workers
        )
        | Stage(collect_day_conf_pairs).setup(workers=1)  # Important!
        | Stage(combine_date_conf_pairs, name="day_conf", **kwargs).setup(
            workers=workers
        )
        | Stage(collect_day_conf_all_years, **kwargs).setup(workers=1)  # Important!
        | Stage(merge_years, name="day_conf", **kwargs).setup(workers=workers)
        # TODO: copy data to s3://palm-risk-poc/data/glad/analysis-staging
        # TODO: copy data to s3://gfwpro-raster-data
    )

    date_conf_tiles = list()
    for output in pipe.results():
        date_conf_tiles.append(output)
        logging.debug("Date Conf  output: " + str(output))
    logging.info("Date Conf - Done")

    return date_conf_tiles


def resample_date_conf_pipe(tiles, **kwargs):

    workers = kwargs["workers"]
    pipe = (
        tiles
        | Stage(
            resample, name="day_conf", resample_method="near", zoom=12, **kwargs
        ).setup(workers=workers)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=11, **kwargs
        ).setup(workers=workers)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=10, **kwargs
        ).setup(workers=workers)
        # | Stage(build_vrt, name="day_conf", zoom=10, **kwargs).setup(
        #    workers=1
        # )  # Important
    )

    for i in range(9, -1, -1):
        pipe = pipe | Stage(
            resample, name="day_conf", resample_method="mode", zoom=i, **kwargs
        ).setup(workers=workers)

    for output in pipe.results():
        logging.debug("Resample Day Conf output: " + str(output))
    logging.info("Resample Day Conf - Done")

    return


def intensity_pipe(tiles, **kwargs):
    workers = kwargs["workers"]
    pipe = (
        tiles
        | Stage(unset_no_data_value).setup(workers=workers)
        | Stage(prep_intensity, name="day_conf", **kwargs).setup(workers=workers)
        | Stage(
            resample, name="intensity", resample_method="near", zoom=12, **kwargs
        ).setup(workers=workers)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=11, **kwargs
        ).setup(workers=workers)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=10, **kwargs
        ).setup(workers=workers)
        # | Stage(build_vrt, name="intensity", zoom=10, **kwargs).setup(
        #     workers=1
        # )  # Important
    )
    for i in range(9, -1, -1):
        pipe = pipe | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=i, **kwargs
        ).setup(workers=workers)

    for output in pipe.results():
        logging.debug("Intensity output: " + str(output))
    logging.info("Intensity - Done")

    return


def rgb_pipe(**kwargs):

    root = kwargs["root"]
    workers = kwargs["workers"]

    tile_pairs = list()
    for pair in collect_resampled_tiles(root):
        tile_pairs.append(pair)

    pipe = (
        tile_pairs
        | Stage(encode_rgb).setup(workers=workers)
        | Stage(project).setup(workers=workers)
    )
    for output in pipe.results():
        logging.debug("RGB output: " + str(output))
    logging.info("RGB - Done")

    return


def copy_vrt_s3_pipe(**kwargs):

    root = kwargs["root"]
    workers = kwargs["workers"]

    # TODO consider moving this to the rbg pipe
    zoom_tiles = list()
    for pair in collect_rgb_tiles(root):
        zoom_tiles.append(pair)

    pipe = (
        zoom_tiles
        | Stage(
            generate_vrt, kwargs["min_tile_zoom"], kwargs["max_zoom"], **kwargs
        ).setup(workers=workers)
        # TODO: copy VRT and tiles to S3
        #  s3://palm-risk-poc/data/glad/rgb z_9.vrt, z_10.vrt, z_11.vrt, z_12.vrt
    )

    for output in pipe.results():
        logging.debug("Copy VRT to S3 output: " + str(output))
    logging.info("opy VRT to S3 - Done")


def tilecache_pipe(**kwargs):

    root = kwargs["root"]
    workers = kwargs["workers"]

    zoom_tiles = list()
    for pair in collect_rgb_tiles(root):
        zoom_tiles.append(pair)

    tile_ids = collect_rgb_tile_ids(zoom_tiles)

    pipe = (
        zoom_tiles
        | Stage(
            generate_vrt, kwargs["min_zoom"], kwargs["max_tilecache_zoom"], **kwargs
        ).setup(workers=workers)
        | Stage(generate_tilecache_mapfile, **kwargs).setup(workers=workers)
        | Stage(generate_tilecache_config, **kwargs).setup(workers=workers)
        | Stage(generate_tile_list, tile_ids=tile_ids, **kwargs).setup(workers=workers)
        | Stage(save_tile_lists, **kwargs).setup(workers=workers)
        | Stage(generate_tiles, **kwargs).setup(workers=workers)
        # TODO: copy tilecache to S3
        #  s3://wri-tiles/glad_{}/tiles/'.format(prod | stage)
    )

    for output in pipe.results():
        logging.debug("Tilecache output: " + str(output))
    logging.info("Tilecache - Done")

    return
