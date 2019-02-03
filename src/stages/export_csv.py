from raster2points import raster2df
from helpers.utils import output_file, file_details
import logging
import math


def get_dataframe(tile_pairs):
    for tile_pair in tile_pairs:
        tile = tile_pair[0]
        climate_mask = tile_pair[2]

        f_name, year, folder, tile_id = file_details(tile)

        if climate_mask:
            tiles = list(tile_pair)
        else:
            tiles = list(tile_pair[:-1])

        try:
            logging.info("Extract points for tile: " + str(tiles))
            df = raster2df(*tiles, calc_area=True)
        except Exception as e:
            logging.error("Failed to extract points for tiles: " + str(tiles))
            logging.error(e)
        else:
            if not climate_mask:
                df["val2"] = 0
            yield year, tile_id, df


def _decode_day_conf(value, baseyear=2015):

    conf = math.floor(value / 10000)

    days = value - (conf * 10000)

    next_year = year = baseyear
    max_days = min_days = 0

    while days > max_days:
        min_days = max_days
        year = next_year

        max_days += 365 + int(not (year % 4))
        next_year += 1

    return conf, year, days - min_days


def decode_day_conf(tile_dfs):

    day_conf = "val0"

    for tile_df in tile_dfs:

        year = tile_df[0]
        tile_id = tile_df[1]
        df = tile_df[2]

        df["confidence"], df["year"], df["julian_day"] = zip(
            *map(_decode_day_conf, df[day_conf])
        )
        df.drop(columns=[day_conf])
        yield year, tile_id, df


def save_csv(tile_dfs, **kwargs):

    root = kwargs["root"]

    for tile_df in tile_dfs:

        year = tile_df[0]
        tile_id = tile_df[1]
        df = tile_df[2]

        output = output_file(root, "output", "csv", year, tile_id + ".csv")

        df.to_csv(
            output,
            index=False,
            columns=(
                "long",
                "lat",
                "confidence",
                "year",
                "julian_day",
                "area",
                "val1",
                "val2",
            ),
            header=(
                "long",
                "lat",
                "confidence",
                "year",
                "julian_day",
                "area",
                "emissions",
                "climate_mask",
            ),
        )
        yield output
