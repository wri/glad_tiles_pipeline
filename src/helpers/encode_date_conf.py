# from numba import jit
import numpy as np
import argparse
from osgeo import gdal
import helpers.raster_utilities as ras_util


def main():
    parser = argparse.ArgumentParser(
        description="Get input raster, lookup table, and output."
    )
    parser.add_argument(
        "--mode",
        "-m",
        help="the mode/function to run",
        choices=("date", "conf"),
        required=True,
    )
    parser.add_argument(
        "--year", "-y", help="the year of the dataset", type=int, required=True
    )
    parser.add_argument("--input_raster", "-i", help="the input raster", required=True)
    parser.add_argument(
        "--output_raster", "-o", help="the output raster", required=True
    )
    args = parser.parse_args()

    if args.mode == "date":
        write_total_days_tif(args.input_raster, args.output_raster, args.year)

    else:
        scale_confidence_values(args.input_raster, args.output_raster)


def get_year_offset(year, baseyear=2015):
    """
    Returns number of days between baseyear and year
    :param year: current year (int)
    :param baseyear: reference year (int)
    :return: number of days between baseyear and year (int)
    """
    return (year - baseyear) * 365 + sum(
        [int(not (y % 4)) for y in range(baseyear - 1, year)]
    )


# jit doesn't make this faster. In its current version the last numpy statement is not supported
# also np.select() doesn't work. Excluding the statement from this function ended up being about 1 sec faster
# For readability reasons i stuck with the current implementation
# @jit(nopython=True)
def encode_days(date_array, offset, nodata):
    """
    Build an array of total days for the year (i.e. if it's year 0, this array will be
    all 0s (no day offset needed to get total days). If it's year 1, will be an array
    of 365, year 2, 730, etc
    Build total date_array by adding current date to year offset
    Reset no data value

    :param date_array: input array
    :param year_offset: number to add to each day
    :param date_nodata: inital
    :return:
    """

    year_array = np.array([offset], np.uint16)
    date_array += year_array
    date_array[date_array == nodata + offset] = nodata

    return date_array


def encode_conf(array):
    """
    Multiplies all values of an array by 10000 and forces it to be UINT16
    :param array: Input Array
    :return: Output Array
    """
    array *= np.array([10000], dtype=np.uint16)

    return array


def write_total_days_tif(date_tif, output_raster, year):
    """
    Encodes GLAD date. Adds an offset of number of days between 2015 and current year
    to annual GLAD dates.
    :param date_tif: Input Raster
    :param output_raster: Output Raster
    :param year: Year of Raster
    :return: Output Raster
    """

    year_offset = get_year_offset(year)
    nodata = ras_util.get_no_data_vals(date_tif)[0]

    out_ras = ras_util.create_outfile(date_tif, output_raster, gdal.GDT_UInt16, 1)
    out_band = out_ras.GetRasterBand(1)

    date_array_generator = ras_util.raster2array_large(date_tif)

    for date_array, j, i in date_array_generator:

        date_array = encode_days(date_array, year_offset, nodata)

        out_ras.GetRasterBand(1).WriteArray(date_array, j, i)

    out_band.SetNoDataValue(nodata)
    out_band.FlushCache()

    return output_raster


def scale_confidence_values(confidence_tif, output_raster):
    """
    Multiply all values by 10,000 so we can add them later to get unique date/conf values
    this will gives us values of eithe 20000 or 30000, which we'll then add to our
    0 - 9999 values from scaled julian_day + year_offset
    :param confidence_tif: input Raster
    :param output_raster: output Raster
    :return: Output Raster
    """

    nodata = ras_util.get_no_data_vals(confidence_tif)[0]
    out_ras = ras_util.create_outfile(confidence_tif, output_raster, gdal.GDT_UInt16, 1)
    out_band = out_ras.GetRasterBand(1)

    conf_array_generator = ras_util.raster2array_large(confidence_tif)

    for conf_array, j, i in conf_array_generator:

        conf_array = encode_conf(conf_array)

        out_ras.GetRasterBand(1).WriteArray(conf_array, j, i)

    out_band.SetNoDataValue(nodata * 10000)
    out_band.FlushCache()

    return output_raster


if __name__ == "__main__":
    main()
