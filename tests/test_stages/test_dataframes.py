from glad.stages.dataframes import (
    climate_filter_df,
    climate_aggregate_df,
    climate_expand_df,
    climate_cumsum_df,
    climate_emissions_targets,
    climate_concate_df,
    climate_format_df,
)
from pandas.util.testing import assert_frame_equal
import pandas as pd
import os

cur_dir = os.path.dirname(os.path.realpath(__file__))

DATAFRAME = pd.read_csv(os.path.join(cur_dir, "fixures/csv/dataframe.csv"))
DATAFRAME_FILTERED = pd.read_csv(
    os.path.join(cur_dir, "fixures/csv/dataframe_filtered.csv")
)
DATAFRAME_AGGREGATE = pd.read_csv(
    os.path.join(cur_dir, "fixures/csv/dataframe_aggregate.csv")
)

DATAFRAME_EXPANDED = pd.read_csv(
    os.path.join(cur_dir, "fixures/csv/dataframe_expanded.csv")
)
DATAFRAME_CUMSUM = pd.read_csv(
    os.path.join(cur_dir, "fixures/csv/dataframe_cumsum.csv")
)


def test_climate_filter_df():

    frames = climate_filter_df([(None, None, DATAFRAME)])

    for df in frames:
        assert_frame_equal(df[0], DATAFRAME_FILTERED)


def test_climate_aggregate_df():

    frames = climate_aggregate_df([(DATAFRAME_FILTERED,)])

    for df in frames:
        print(df[0])
        assert_frame_equal(df[0], DATAFRAME_AGGREGATE)


def test_climate_concate_df():
    pass


def test_climate_expand_df():
    df = climate_expand_df(DATAFRAME_AGGREGATE)
    assert_frame_equal(df, DATAFRAME_EXPANDED)


def test_climate_cumsum_df():
    df = climate_cumsum_df(DATAFRAME_CUMSUM)
    assert_frame_equal(df, DATAFRAME_CUMSUM)


def test_climate_emission_targets():
    pass
