import pandas as pd
import logging


def climate_filter_df(tiles_df):

    country_list = [
        "BRA",
        "PER",
        "COG",
        "UGA",
        "TLS",
        "CMR",
        "BDI",
        "GAB",
        "BRN",
        "CAF",
        "GNQ",
        "PNG",
        "SGP",
        "RWA",
    ]

    for tile_df in tiles_df:
        df = tile_df[2]

        logging.info("Filter Climate Data Frame")
        df = df[
            ((df["climate_mask"] == 1) | (df["iso"].isin(country_list)))
            & df["confidence"]
            == 3
        ]
        df = df.drop(columns=["julian_day"])

        yield df


def climate_aggregate_df(dfs):

    for df in dfs:

        groupby_list = ["iso", "adm1", "week", "year", "confidence"]
        sum_list = ["alert_count", "emissions", "area"]

        logging.info(
            "Group by admin areas and week of year, count alerts and sum emissions and loss"
        )
        df = df.groupby(groupby_list)[sum_list].sum().reset_index()

        yield df


# Only one (1) worker!
def climate_concate_df(tiles_df):

    frames = list()

    for tile_df in tiles_df:
        df = tile_df[2]

        frames.append(df)

    yield pd.concat(frames)
