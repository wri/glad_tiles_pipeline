from helpers.tiles import (
    upper_bound,
    lower_bound,
    get_longitude,
    get_latitude,
    get_tile_ids_by_bbox,
    format_lat_lon,
    get_bbox_by_tile_id,
)


def test_get_longitude():
    assert get_longitude(0) == "000E"
    assert get_longitude(10) == "010E"
    assert get_longitude(-10) == "010W"
    assert get_longitude(117) == "117E"
    assert get_longitude(-117) == "117W"


def test_get_latitude():
    assert get_latitude(0) == "00N"
    assert get_latitude(10) == "10N"
    assert get_latitude(-10) == "10S"
    assert get_latitude(90) == "90N"
    assert get_latitude(-90) == "90S"


def test_lower_bound():
    assert lower_bound(0) == 0
    assert lower_bound(10) == 10
    assert lower_bound(17) == 10
    assert lower_bound(-10) == -10
    assert lower_bound(-17) == -20


def test_upper_bound():
    assert upper_bound(0) == 0
    assert upper_bound(10) == 10
    assert upper_bound(17) == 20
    assert upper_bound(-10) == -10
    assert upper_bound(-17) == -10


def test_get_tile_ids_by_bbox():
    result1 = [
        "020W_10S_010W_00N",
        "010W_10S_000E_00N",
        "000E_10S_010E_00N",
        "020W_00N_010W_10N",
        "010W_00N_000E_10N",
        "000E_00N_010E_10N",
    ]

    result2 = ["000E_00N_010E_10N"]

    assert get_tile_ids_by_bbox(-11, -4, 1, 4) == result1
    assert get_tile_ids_by_bbox(1, 1, 10, 10) == result2


def test_format_lat_lon():
    assert format_lat_lon("030E") == 30
    assert format_lat_lon("130E") == 130
    assert format_lat_lon("030W") == -30
    assert format_lat_lon("130W") == -130

    assert format_lat_lon("01N") == 1
    assert format_lat_lon("90N") == 90
    assert format_lat_lon("01S") == -1
    assert format_lat_lon("90S") == -90


def test_get_bbox_by_tile_id():
    assert get_bbox_by_tile_id("020W_10S_010W_00N") == (-20, -10, -10, 0)
    assert get_bbox_by_tile_id("000E_00N_010E_10N") == (0, 0, 10, 10)
