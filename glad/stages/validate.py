from glad.utils.utils import file_details
from osgeo import gdal


def validate_tiles(tiles, **kwargs):
    for tile in tiles:
        f_name, year, folder, tile_id = file_details(tile)

        if f_name == "day.tif":
            expected_range = range(0, 367)
        else:
            expected_range = [0, 2, 3]

        gtif = gdal.Open(tile)
        srcband = gtif.GetRasterBand(1)
        stats = srcband.GetStatistics(True, True)

        min = stats[0]
        max = stats[1]

        if min not in expected_range or max not in expected_range:
            raise ValueError(f"Tile f{tile_id}, year {year}, file {f_name} "
                             f"had min, max of {min, max}, which is outside of the expected range")

        yield tile
