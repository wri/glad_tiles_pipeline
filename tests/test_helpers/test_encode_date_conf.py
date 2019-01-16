from helpers.encode_date_conf import get_year_offset


def test_get_year_offset():

    assert get_year_offset(2015) == 0
    assert get_year_offset(2016) == 0 + 365
    assert get_year_offset(2017) == 0 + 365 + 366
    assert get_year_offset(2018) == 0 + 365 + 366 + 365
    assert get_year_offset(2019) == 0 + 365 + 366 + 365 + 365
    assert get_year_offset(2020) == 0 + 365 + 366 + 365 + 365 + 365
    assert get_year_offset(2021) == 0 + 365 + 366 + 365 + 365 + 365 + 366
    assert get_year_offset(2021, 2016) == 0 + 366 + 365 + 365 + 365 + 366
