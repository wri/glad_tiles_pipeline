from helpers.encode_date_conf import get_year_offset, encode_days, encode_conf
import numpy as np


def test_get_year_offset():

    assert get_year_offset(2015) == 0
    assert get_year_offset(2016) == 0 + 365
    assert get_year_offset(2017) == 0 + 365 + 366
    assert get_year_offset(2018) == 0 + 365 + 366 + 365
    assert get_year_offset(2019) == 0 + 365 + 366 + 365 + 365
    assert get_year_offset(2020) == 0 + 365 + 366 + 365 + 365 + 365
    assert get_year_offset(2021) == 0 + 365 + 366 + 365 + 365 + 365 + 366
    assert get_year_offset(2021, 2016) == 0 + 366 + 365 + 365 + 365 + 366


def test_encode_days():
    i = np.array([[1, 0, 1], [0, 2, 0], [3, 0, 3]])

    o = encode_days(i, 12, 0)
    assert o.all() == np.array([[13, 0, 13], [0, 15, 0], [16, 0, 16]]).all()

    o = encode_days(i, 12, 3)
    assert o.all() == np.array([[13, 12, 13], [12, 15, 12], [3, 12, 3]]).all()


def test_encode_conf():
    i = np.array([[1, 0, 1], [0, 2, 0], [3, 0, 3]])
    o = encode_conf(i)
    assert (
        o.all() == np.array([[10000, 0, 10000], [0, 20000, 0], [30000, 0, 30000]]).all()
    )
