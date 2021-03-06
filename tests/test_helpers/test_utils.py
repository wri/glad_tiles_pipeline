from helpers.utils import (
    preprocessed_years_str,
    add_tile_to_dict,
    add_preprocessed_tile_to_dict,
    get_preprocessed_tiles,
    sort_dict,
    output_file,
    get_tile_id,
    get_file_name,
    get_current_years,
)
from unittest import mock
from datetime import datetime


def test_preprocessed_years_str():
    years = [2015, 2016, 2017]
    assert preprocessed_years_str(years) == "2015_2016_2017"


def test_add_to_tile_pairs():

    tile_pairs = dict()
    basedir = "/my/base/dir"
    year1 = "2015"
    tile1 = "/my/base/dir/2015/tile.tif"

    tile_pairs = add_tile_to_dict(tile_pairs, basedir, year1, tile1)

    assert tile_pairs == {basedir: {year1: tile1}}

    year2 = "2016"
    tile2 = "/my/base/dir/2016/tile.tif"

    tile_pairs = add_tile_to_dict(tile_pairs, basedir, year2, tile2)

    assert tile_pairs == {basedir: {year1: tile1, year2: tile2}}


def test_add_preprocessed_tile_to_dict():

    basedir = "/my/base/dir"
    year1 = "2018"
    tile1 = "/my/base/dir/2018/tile.tif"
    year2 = "2019"
    tile2 = "/my/base/dir/2019/tile.tif"

    tile_pairs = {basedir: {year1: tile1, year2: tile2}}
    preprocessed_tiles = [
        "/my/base/dir/2015_2016_2017/tile.tif",
        "/my/otherbase/dir/2015_2016_2017/tile.tif",
    ]

    tile_pairs = add_preprocessed_tile_to_dict(tile_pairs, basedir, preprocessed_tiles)

    assert tile_pairs == {
        basedir: {
            year1: tile1,
            year2: tile2,
            "2015_2016_2017": "/my/base/dir/2015_2016_2017/tile.tif",
        }
    }


@mock.patch("helpers.utils.glob")
def test_get_preprocessed_tiles(mock_glob):

    root = "/my/root/dir"
    years = [2018, 2019]
    preprocessed_years = [2015, 2016, 2017]

    mock_glob.iglob.return_value = [
        root + "/tiles/2015/day_conf.tif",
        root + "/tiles/2016/day_conf.tif",
        root + "/tiles/2017/day_conf.tif",
        root + "/tiles/2018/day_conf.tif",
        root + "/tiles/2019/day_conf.tif",
        root + "/tiles/2015_2016_2017/day_conf.tif",
        root + "/othertiles/2018_2019/day_conf.tif",
        root + "/othertiles/WRONGFORMAT/day_conf.tif",
    ]
    preprocessed_tiles = get_preprocessed_tiles(root, years, preprocessed_years)

    assert preprocessed_tiles == [
        root + "/tiles/2015_2016_2017/day_conf.tif",
        root + "/othertiles/2018_2019/day_conf.tif",
    ]


def test_sort_dict():
    root = "/my/root/dir"
    tile_dict = {
        "2019": root + "/tiles/2019/day_conf.tif",
        "2018": root + "/tiles/2018/day_conf.tif",
        "2015_2016_2017": root + "/tiles/2015_2016_2017/day_conf.tif",
    }

    sorted_tile_list = sort_dict(tile_dict)

    assert sorted_tile_list == [
        root + "/tiles/2015_2016_2017/day_conf.tif",
        root + "/tiles/2018/day_conf.tif",
        root + "/tiles/2019/day_conf.tif",
    ]


@mock.patch("helpers.utils.Path")
def test_output_file(mock_path):
    path = ["home", "user", "data", "myfile.txt"]
    mock_path.mkdir.return_value = "home/user/data"
    output = output_file(*path)

    assert output == "home/user/data/myfile.txt"

    path = ["home", None, "data", "myfile.txt"]
    mock_path.mkdir.return_value = "home/user/data"
    output = output_file(*path)

    assert output == "home/data/myfile.txt"


def test_output_mkdir():
    # could try to make dir, check if it exists and then delete it
    # skipping for now
    pass


def test_get_tile_id():
    fname = "/data/050W_20S_030E_10N/myfile.txt"
    assert get_tile_id(fname) == "050W_20S_030E_10N"

    fname = "/data/name/000E_20N_130W_01S/myfile.txt"
    assert get_tile_id(fname) == "000E_20N_130W_01S"

    fname = "/data/name/zoom_12.txt"
    assert get_tile_id(fname) is None

    fname = "/data/000E_20N/zoom_12.txt"
    assert get_tile_id(fname) is None


def test_get_file_name():
    fname = "/data/050W_20S_030E_10N/myfile.txt"
    assert get_file_name(fname) == "myfile.txt"


def test_get_current_years():
    now = datetime.now()
    year = now.year
    month = now.month

    if month < 7:
        assert get_current_years() == [year - 1, year]
    else:
        assert get_current_years() == [year]
