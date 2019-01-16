# from numba import jit
import numpy as np
import argparse
import gdal
import helpers.raster_utilities as ras_util


def main():
    # Parse commandline arguments
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


# @jit(nopython=True)
def write_total_days_tif(date_tif, output_raster, year):

    # add a year_offset value to each julian_day pixel, so that when we add
    # all years together, we can see the progression from 2015-01-01 to present
    year_offset = (year - 2015) * 365

    # Figure out what the nodata value will be for this raster
    # The source nodata value is 0; but we're adding a year offset, so need to apply that too
    date_nodata = ras_util.get_no_data_vals(date_tif)[0] + year_offset

    # Get a handle on our out_ras
    out_ras = ras_util.create_outfile(date_tif, output_raster, gdal.GDT_UInt16, 1)
    out_band = out_ras.GetRasterBand(1)

    # Convert input raster to array
    date_array_generator = ras_util.raster2array_large(date_tif)

    for date_array, j, i in date_array_generator:

        # Build an array of total days for the year (i.e. if it's year 0, this array will be
        # all 0s (no day offset needed to get total days). If it's year 1, will be an array
        # of 365, year 2, 730, etc
        year_array = np.array([year_offset], np.uint16)

        # Build total date_array by adding current date to year offset
        date_array += year_array

        # Set no data appropriately - should always be zero
        date_array[date_array == date_nodata] = 0

        out_ras.GetRasterBand(1).WriteArray(date_array, j, i)

    out_band.SetNoDataValue(0)
    out_band.FlushCache()

    return output_raster


# @jit(nopython=True)
def scale_confidence_values(confidence_tif, output_raster):

    out_ras = ras_util.create_outfile(confidence_tif, output_raster, gdal.GDT_UInt16, 1)
    out_band = out_ras.GetRasterBand(1)

    # Convert input raster to array
    conf_array_generator = ras_util.raster2array_large(confidence_tif)

    for conf_array, j, i in conf_array_generator:

        # Multiply all values by 10,000 so we can add them later to get unique date/conf values
        # this will gives us values of eithe 20000 or 30000, which we'll then add to our
        # 0 - 9999 values from scaled julian_day + year_offset
        conf_array *= np.array([10000], dtype=np.uint16)

        # Write to output raster
        out_ras.GetRasterBand(1).WriteArray(conf_array, j, i)

    out_band.SetNoDataValue(0)
    out_band.FlushCache()

    return output_raster


if __name__ == "__main__":
    main()
