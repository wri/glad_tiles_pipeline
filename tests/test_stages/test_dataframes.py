from glad.stages.dataframes import climate_filter_df, climate_aggregate_df
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
