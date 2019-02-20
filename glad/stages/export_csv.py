from raster2points import raster2df
from glad.utils.utils import output_file, file_details
import logging
import math
import mercantile
import datetime


def get_dataframe(tile_pairs):
    for tile_pair in tile_pairs:
        tile = tile_pair[0]
        emissions = tile_pair[1]
        climate_mask = tile_pair[2]

        f_name, year, folder, tile_id = file_details(tile)

        tiles = [tile_pair[0]]

        if emissions:
            tiles = tiles + [emissions]

        if climate_mask:
            tiles = tiles + [climate_mask]

        try:
            logging.info("Extract points for tile: " + str(tiles))
            df = raster2df(*tiles, calc_area=True)
        except Exception as e:
            logging.error("Failed to extract points for tiles: " + str(tiles))
            logging.error(e)
        else:
            if not emissions:
                df["val1"] = 0
            if not climate_mask:
                df["val2"] = 0
            yield year, tile_id, df


def decode_day_conf(tile_dfs):

    day_conf = "val0"

    for tile_df in tile_dfs:

        year = tile_df[0]
        tile_id = tile_df[1]
        df = tile_df[2]

        try:
            logging.info("Decode day/conf value for tile: " + tile_id)
            df["confidence"], df["year"], df["julian_day"] = zip(
                *map(_decode_day_conf, df[day_conf])
            )
        except KeyError:
            logging.error(
                "Cannot fine column {} for tile {} and year {}".format(
                    day_conf, tile_id, year
                )
            )
            logging.error(df.head)
        else:
            df = df.drop(columns=[day_conf])
            yield year, tile_id, df


def save_csv(tile_dfs, name, columns, header, return_input, **kwargs):

    root = kwargs["root"]

    for tile_df in tile_dfs:

        year = tile_df[0]
        tile_id = tile_df[1]
        df = tile_df[2]

        output = output_file(root, name, "csv", year, tile_id + ".csv")

        logging.info("Save file: " + output)

        df.to_csv(
            output, index=False, columns=columns, header=header, date_format="%Y-%m-%d"
        )

        if return_input:
            yield tile_df
        else:
            yield output


def convert_julian_date(tile_dfs):

    for tile_df in tile_dfs:

        year = tile_df[0]
        tile_id = tile_df[1]
        df = tile_df[2]

        logging.info("Convert julian date for tile: " + tile_id)
        df["alert_date"] = df["year"].map(
            lambda year: datetime.datetime(year, 1, 1)
        ) + df["julian_day"].map(lambda julian_day: datetime.timedelta(julian_day - 1))
        df["alert_count"] = 1
        df = df.drop(columns=["year", "julian_day", "area", "val1", "val2"])
        yield year, tile_id, df


def convert_latlon_xyz(tile_dfs, **kwargs):

    max_zoom = kwargs["max_zoom"]

    for tile_df in tile_dfs:
        year = tile_df[0]
        tile_id = tile_df[1]
        df = tile_df[2]
        logging.info("Convert Lat/Lon to X/Y/Z for tile: " + tile_id)
        df["z"] = max_zoom  # we need this first for zip(map()) to work
        df["x"], df["y"], df["z"] = zip(
            *map(mercantile.tile, df["lon"], df["lat"], df["z"])
        )
        df = df.drop(columns=["lon", "lat"])
        yield year, tile_id, df


def group_by_xyz(tile_dfs):

    for tile_df in tile_dfs:
        year = tile_df[0]
        tile_id = tile_df[1]
        df = tile_df[2]

        logging.info("Group by X/Y/Z date and conf: " + tile_id)

        groupby_df = (
            df.groupby(["x", "y", "z", "alert_date", "confidence"]).sum().reset_index()
        )
        yield year, tile_id, groupby_df


def convert_to_parent_xyz(tile_dfs):

    for tile_df in tile_dfs:
        year = tile_df[0]
        tile_id = tile_df[1]
        df = tile_df[2]

        logging.info("Convert X/Y/Z to parent X/Y/Z: " + tile_id)
        df["x"], df["y"], df["z"] = zip(
            *map(mercantile.parent, df["x"], df["y"], df["z"])
        )

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
