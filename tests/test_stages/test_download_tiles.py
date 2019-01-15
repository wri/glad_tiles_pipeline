from stages.download_tiles import (
    get_suffix,
    download_latest_tiles,
    download_preprocessed_tiles_years,
)
from helpers.utils import output_tiles
import subprocess as sp
from unittest import mock


def test_get_suffix():
    assert get_suffix("day") == "Date"
    assert get_suffix("conf") == ""


@mock.patch("stages.download_tiles.sp")
def test_download_preprocessed_tiles_years(mock_sp):

    preprocessed_years = [2015, 2016, 2017]
    tile_ids = ["050W_00N_040W_10N"]
    root = "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/tests/data"

    year_str = "_".join(str(year) for year in preprocessed_years)
    output = output_tiles(root, tile_ids[0], "date_conf", year_str, "day_conf.tif")

    mock_sp.check_call.return_value = sp.CalledProcessError

    r = download_preprocessed_tiles_years(
        tile_ids=tile_ids, preprocessed_years=preprocessed_years, root=root
    )

    for x in r.results():
        assert x == output

    # TODO: Is there a better way to check if nothing was returned?
    mock_sp.check_call.return_value = True

    r = download_preprocessed_tiles_years(
        tile_ids=tile_ids, preprocessed_years=preprocessed_years, root=root
    )

    out = False
    for x in r.results():
        out = True
    assert out is False
