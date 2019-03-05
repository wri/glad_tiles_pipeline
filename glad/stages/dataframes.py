from glad.utils.utils import get_file_dir
import pandas as pd
import logging
import os


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
            & (df["confidence"] == 3)
        ]
        df = df.drop(columns=["julian_day"]).reset_index(drop=True)

        yield (df,)


def climate_aggregate_df(dfs):

    for df in dfs:

        df = df[0]
        groupby_list = ["iso", "adm1", "year", "week", "confidence"]
        sum_list = ["alert_count", "emissions", "area"]

        logging.info(
            "Group by admin areas and week of year, count alerts and sum emissions and loss"
        )
        df = df.groupby(groupby_list)[sum_list].sum().reset_index()

        yield (df,)


# Only one (1) worker!
def climate_concate_df(dfs):

    frames = list()

    for df in dfs:
        df = df[0]

        frames.append(df)

    logging.info("Concate dataframes")
    yield (pd.concat(frames),)


def climate_expand_df(df):

    logging.info("Expand dataframe")
    week_year_df = df.groupby(["year", "week"]).size().reset_index()
    iso_adm1_df = df.groupby(["iso", "adm1"]).size().reset_index()

    week_year_df["temp_key"] = 1
    iso_adm1_df["temp_key"] = 1

    temp_df = pd.merge(iso_adm1_df, week_year_df, on="temp_key").drop(
        "temp_key", axis=1
    )

    join_fields = ["iso", "adm1", "year", "week"]
    df = pd.merge(temp_df, df, on=join_fields, how="left")

    df.confidence.fillna(3, inplace=True, downcast="infer")
    df.alert_count.fillna(0, inplace=True, downcast="infer")
    df.emissions.fillna(0, inplace=True)
    df.area.fillna(0, inplace=True)

    df = df.drop("0_x", axis=1).drop("0_y", axis=1).reset_index()
    df = df.sort_values(join_fields)

    return df.drop("index", axis=1)


def climate_cumsum_df(df):

    logging.info("Calculate cumulative sum")
    join_fields = ["iso", "adm1", "year"]

    df["cumulative_deforestation"] = df.groupby(join_fields)["area"].cumsum().round(4)
    df["cumulative_emissions"] = df.groupby(join_fields)["emissions"].cumsum().round(4)

    return df


def climate_emissions_targets(df):

    logging.info("Add emission targets")

    f_dir = get_file_dir(__file__)
    target_csv = os.path.join(f_dir, "fixures/emission_targets_2020.csv")
    target_df = pd.read_csv(target_csv)

    # join target_df to df based on iso
    df = pd.merge(df, target_df, on="iso", how="left")

    df["percent_to_emissions_target"] = (
        (df.cumulative_emissions / df.target_emissions) * 100
    ).round(4)
    df["percent_to_deforestation_target"] = (
        (df.cumulative_deforestation / df.target_deforestation) * 100
    ).round(4)

    # remove joined columns
    df = df.drop("target_emissions", axis=1).drop(df["target_deforestation"], axis=1)

    return df


def climate_format_df(df):

    logging.info("Format table")
    # round emissions and loss area for display
    df.emissions = df.emissions.round(4)

    df.area = (df.area / 10000).round(4)

    df.confidence = "confirmed"

    # add state_iso column to make it easier to query from the front end
    df["state_iso"] = df.iso + df.adm1.astype(str)

    # rename columns to fit regular climate schema
    df = df.rename(
        columns={"adm1": "state_id", "iso": "country_iso", "area": "loss_ha"}
    )

    # sort DF so that elastic guesses each field type correctly
    df = df.sort_values("alert_count", ascending=False)

    return df
