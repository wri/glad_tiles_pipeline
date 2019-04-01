import gdal
import argparse
import shutil
import subprocess as sp
import logging

# import raster_utilities as ras_util


def main():

    # little helper CLI to convert a source file to UInt16 bit depth
    # if it's already that format, just copy it

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description="Change the data type of a raster.")
    parser.add_argument(
        "--input_raster", "-i", help="the intensity raster", required=True
    )
    parser.add_argument(
        "--output_raster", "-o", help="the output raster", required=True
    )
    parser.add_argument(
        "--data_type",
        "-d",
        help="the output data type",
        choices=("Int16", "UInt16"),
        required=True,
    )
    parser.add_argument("--bbox", "-b", help="projwin bbox- ulx uly lrx lry", nargs=4)
    args = parser.parse_args()

    src_ds = gdal.Open(args.input_raster)
    band = src_ds.GetRasterBand(1)

    run_gdal = False

    cmd = [
        "gdal_translate",
        args.input_raster,
        "-ot",
        args.data_type,
        "-co",
        "COMPRESS=LZW",
        args.output_raster,
    ]
    cmd += [
        "-a_nodata",
        "0",
    ]  # TODO: ['--config', 'GDAL_CACHEMAX', ras_util.get_mem_pct()]

    if args.bbox:
        cmd += ["-projwin", args.bbox[0], args.bbox[1], args.bbox[2], args.bbox[3]]
        run_gdal = True

    if gdal.GetDataTypeName(band.DataType) != args.data_type:
        run_gdal = True

    # If the initial data type is correct and no bounding box is specified, just copy it to the output
    if run_gdal:
        logging.debug(cmd)
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        o, e = p.communicate()
        logging.debug(o)
        if p.returncode == 0:
            logging.info("Change NoData value and DataType for " + args.input_raster)
        else:
            logging.error(
                "Failed to change NoData value and DataType for " + args.input_raster
            )
            logging.error(e)
            raise sp.CalledProcessError

    else:

        # all GLAD rasters should have a NoData value of 0
        if not band.GetNoDataValue():

            cmd = ["gdal_edit.py", args.input_raster, "-a_nodata", "0"]

            logging.debug(cmd)
            p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            o, e = p.communicate()
            logging.debug(o)
            if p.returncode == 0:
                logging.info("Change No data value for " + args.input_raster)
            else:
                logging.error("Failed to change no data value for " + args.input_raster)
                logging.error(e)
                raise sp.CalledProcessError

        shutil.copy(args.input_raster, args.output_raster)


if __name__ == "__main__":
    main()
