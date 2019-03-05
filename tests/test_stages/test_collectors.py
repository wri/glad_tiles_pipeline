import os
from pathlib import PurePath
from glad.stages.collectors import get_preprocessed_tiles


def test_get_preprocessed_tiles():

    root = PurePath(os.path.dirname(os.path.realpath(__file__)), "fixures").as_posix()

    all = ["2015", "2016", "2017", "2018", "2019", "2015_2016_2017"]
    include = ["2015", "2016", "2017", "2018"]
    exclude = ["2018", "2019"]

    f_name = PurePath(root, "tiles/{}/day_conf.tif").as_posix()

    preprocessed = get_preprocessed_tiles(root, include, exclude)
    expected = [f_name.format(x) for x in include if x not in exclude]
    assert sorted(preprocessed) == sorted(expected)

    preprocessed = get_preprocessed_tiles(root, include)
    expected = [f_name.format(x) for x in include]
    assert sorted(preprocessed) == sorted(expected)

    preprocessed = get_preprocessed_tiles(root, exclude_years=exclude)
    expected = [f_name.format(x) for x in all if x not in exclude]
    assert sorted(preprocessed) == sorted(expected)
