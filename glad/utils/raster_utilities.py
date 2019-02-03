from osgeo import gdal
from osgeo import osr
import multiprocessing


def raster2array_large(rasterfn, return_cols_and_rows=False):
    """
    Modified from:
    https://github.com/rveciana/geoexamples/blob/master/python/gdal-performance/classification_blocks_minmax.py
    :param rasterfn: raster filename
    :param return_cols_and_rows: boolean to return col and row info as we read the raster line by line
    :return:
    """
    ds = gdal.Open(rasterfn)
    band = ds.GetRasterBand(1)

    block_sizes = band.GetBlockSize()
    x_block_size = block_sizes[0]
    y_block_size = block_sizes[1]

    xsize = band.XSize
    ysize = band.YSize

    for i in range(0, ysize, y_block_size):
        if i + y_block_size < ysize:
            rows = y_block_size
        else:
            rows = ysize - i

        for j in range(0, xsize, x_block_size):
            if j + x_block_size < xsize:
                cols = x_block_size
            else:
                cols = xsize - j

            data = band.ReadAsArray(j, i, cols, rows)

            if return_cols_and_rows:
                output = data, j, i, cols, rows
            else:
                output = data, j, i

            yield output


def read_raster_with_block_size(rasterfn, j, i, cols, rows):

    ds = gdal.Open(rasterfn)
    band = ds.GetRasterBand(1)

    data = band.ReadAsArray(j, i, cols, rows)

    return data


def get_no_data_vals(input_raster):
    raster = gdal.Open(input_raster)
    band_count = raster.RasterCount

    nd_list = []

    for i in range(1, band_count + 1):
        band = raster.GetRasterBand(i)
        nd_val = int(band.GetNoDataValue())

        nd_list.append(nd_val)

    return nd_list


def create_outfile(raster_template, output_raster, output_datatype, band_count):
    raster = gdal.Open(raster_template)
    geotransform = raster.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    cols = raster.RasterXSize
    rows = raster.RasterYSize

    driver = gdal.GetDriverByName("GTiff")
    options = ["COMPRESS=NONE"]

    band = raster.GetRasterBand(1)
    block_sizes = band.GetBlockSize()
    x_block_size = block_sizes[0]
    y_block_size = block_sizes[1]

    if x_block_size == 256 and y_block_size == 256:
        options += ["TILED=YES"]

    outRaster = driver.Create(
        output_raster, cols, rows, band_count, output_datatype, options
    )
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))

    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromWkt(raster.GetProjectionRef())
    outRaster.SetProjection(outRasterSRS.ExportToWkt())

    return outRaster


def get_cell_size(zoom_level, measurement):

    # Given a zoom level, look up the approximate raster
    # cell size in meters or degrees

    zoom_level = int(zoom_level)

    zoom_level_dict = {
        0: {"degrees": 1.40625, "meters": 156412.0},
        1: {"degrees": 0.703125, "meters": 78209.0},
        2: {"degrees": 0.3515625, "meters": 39103.0},
        3: {"degrees": 0.17578125, "meters": 19551.0},
        4: {"degrees": 0.087890625, "meters": 9776.0},
        5: {"degrees": 0.0439453125, "meters": 4888.0},
        6: {"degrees": 0.02197265625, "meters": 2444.0},
        7: {"degrees": 0.01098828125, "meters": 1222.0},
        8: {"degrees": 0.0054921875, "meters": 610.984},
        9: {"degrees": 0.00274609375, "meters": 305.492},
        10: {"degrees": 0.001375, "meters": 152.746},
        11: {"degrees": 0.0006875, "meters": 76.373},
        12: {"degrees": 0.00034375, "meters": 38.187},
        13: {"degrees": 0.000171875, "meters": 19.093},
        14: {"degrees": 0.0000859375, "meters": 9.547},
        15: {"degrees": 0.00004296875, "meters": 4.773},
        16: {"degrees": 0.00001953125, "meters": 2.387},
        17: {"degrees": 0.00001171875, "meters": 1.193},
        18: {"degrees": 0.00000390625, "meters": 0.596},
    }

    return zoom_level_dict[zoom_level][measurement]


def get_scale_denominators(zoom):

    scale_denominators = {
        0: {"min": 500000000, "max": None},
        1: {"min": 200000000, "max": 500000000},
        2: {"min": 100000000, "max": 200000000},
        3: {"min": 50000000, "max": 100000000},
        4: {"min": 25000000, "max": 50000000},
        5: {"min": 12500000, "max": 25000000},
        6: {"min": 6500000, "max": 12500000},
        7: {"min": 3000000, "max": 6500000},
        8: {"min": 1500000, "max": 3000000},
    }

    return scale_denominators[zoom]


def get_mem_pct():

    num_cores = multiprocessing.cpu_count()

    memory_parts = num_cores + 1

    mem_pct = int((1.0 / float(memory_parts)) * 100)

    mem_pct_text = "{0}%".format(mem_pct)

    return mem_pct_text
