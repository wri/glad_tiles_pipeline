from parallelpipe import Stage
from stages.download_tiles import (
    download_latest_tiles,
    download_preprocessed_tiles_years,
    download_preprocessed_tiles_year,
)
from stages.change_pixel_depth import change_pixel_depth
from stages.encode_glad import (
    encode_date_conf,
    date_conf_pairs,
    prep_intensity,
    unset_no_data_value,
    encode_rgb,
    project,
)
from stages.merge_tiles import (
    combine_date_conf_pairs,
    year_pairs,
    merge_years,
    all_year_pairs,
)
from stages.upload_tiles import backup_tiles
from stages.resample import resample, build_vrt
from stages.collectors import collect_resampled_tiles
import logging


def preprocessed_tile_pipe(tile_ids, **kwargs):
    """
    Pipeline to download/ process GLAD alerts of previous years
    :param tile_ids: List of Tile IDs to process
    :param kwargs: Dictonary with keyword arguments
    :return: pipe
    """
    pipe = (
        tile_ids
        | download_preprocessed_tiles_years(name="date_conf", **kwargs)
        | download_preprocessed_tiles_year(name="download", **kwargs)
        | change_pixel_depth(name="pixel_depth", **kwargs)
        | encode_date_conf(name="encode_day_conf", **kwargs)
        | date_conf_pairs()
        | combine_date_conf_pairs(name="day_conf", **kwargs)
        | year_pairs(**kwargs)
        | merge_years(name="day_conf", **kwargs)
        | backup_tiles()
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
    pipe = (
        tile_ids
        | download_latest_tiles(name="download", **kwargs)
        | change_pixel_depth(name="pixel_depth", **kwargs)
        | encode_date_conf(name="encode_day_conf", **kwargs)
        | date_conf_pairs()
        | combine_date_conf_pairs(name="day_conf", **kwargs)
        | all_year_pairs(**kwargs)
        | merge_years(name="day_conf", **kwargs)
    )

    date_conf_tiles = list()
    for output in pipe.results():
        date_conf_tiles.append(output)
        logging.debug("Date Conf  output: " + str(output))
    logging.info("Date Conf - Done")

    return date_conf_tiles


def resample_date_conf_pipe(tiles, **kwargs):

    pipe = (
        tiles
        | Stage(
            resample, name="day_conf", resample_method="near", zoom=12, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=11, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="day_conf", resample_method="mode", zoom=10, **kwargs
        ).setup(workers=2)
        | build_vrt(name="day_conf", zoom=10, **kwargs)
    )

    for i in range(9, -1, -1):
        pipe = pipe | Stage(
            resample, name="day_conf", resample_method="mode", zoom=i, **kwargs
        ).setup(workers=2)

    for output in pipe.results():
        logging.debug("Resample Day Conf output: " + str(output))
    logging.info("Resample Day Conf - Done")

    return


def intensity_pipe(tiles, **kwargs):
    pipe = (
        tiles
        | unset_no_data_value()
        | prep_intensity(name="day_conf", **kwargs)
        | Stage(
            resample, name="intensity", resample_method="near", zoom=12, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=11, **kwargs
        ).setup(workers=2)
        | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=10, **kwargs
        ).setup(workers=2)
        | build_vrt(name="intensity", zoom=10, **kwargs)
    )
    for i in range(9, -1, -1):
        pipe = pipe | Stage(
            resample, name="intensity", resample_method="bilinear", zoom=i, **kwargs
        ).setup(workers=2)

    for output in pipe.results():
        logging.debug("Intensity output: " + str(output))
    logging.info("Intensity - Done")

    return


def rgb_pipe(**kwargs):

    root = kwargs["root"]

    tile_pairs = list()
    for pair in collect_resampled_tiles(root):
        tile_pairs.append(pair)

    pipe = tile_pairs | encode_rgb | project

    for output in pipe.results():
        logging.debug("RGB output: " + str(output))
    logging.info("RGB - Done")

    return
