from glad.stages.download import get_suffix, download_preprocessed_tiles_years
from parallelpipe import Stage
from glad.utils.utils import output_file
import subprocess as sp
from unittest import mock


def test_get_suffix():
    assert get_suffix("day") == "Date"
    assert get_suffix("conf") == ""


@mock.patch("glad.utils.utils.Path")
@mock.patch("glad.stages.download.sp")
def test_download_preprocessed_tiles_years(mock_sp, mock_path):

    preprocessed_years = [2015, 2016, 2017]
    tile_ids = ["050W_00N_040W_10N"]
    root = "/home/thomas/projects/gfw-sync/glad_tiles_pipeline/tests/data"
    name = "download"
    year_str = "_".join(str(year) for year in preprocessed_years)

    mock_path.mkdir.return_value = "home/user/data"

    output = output_file(
        root, "tiles", tile_ids[0], "date_conf", year_str, "day_conf.tif"
    )

    mock_sp.check_call.return_value = sp.CalledProcessError

    pipe = tile_ids | Stage(
        download_preprocessed_tiles_years,
        preprocessed_years=preprocessed_years,
        root=root,
        name=name,
    )

    for x in pipe.results():
        assert x == output

    # TODO: Is there a better way to check if nothing was returned?
    mock_sp.check_call.return_value = True

    pipe = tile_ids | Stage(
        download_preprocessed_tiles_years,
        preprocessed_years=preprocessed_years,
        root=root,
        name=name,
    )

    out = False
    for x in pipe.results():
        out = True
    assert out is False
