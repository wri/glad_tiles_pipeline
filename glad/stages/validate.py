from glad.utils.utils import file_details
from osgeo import gdal


def validate_tiles(tiles, **kwargs):
    for tile in tiles:
        f_name, year, folder, tile_id = file_details(tile)

        if f_name == "day.tif":
            expected_min = 0
            expected_max = 366
        else:
            expected_min = 0
            expected_max = 3

        gtif = gdal.Open(tile)
        srcband = gtif.GetRasterBand(1)
        stats = srcband.GetStatistics(True, True)

        min = stats[0]
        max = stats[1]

        if min < expected_min or max > expected_max:
            raise ValueError(f"Tile f{tile_id} in year {year} had min, max of {min, max}, "
                             f"but expected min, max of {expected_min, expected_max}")
