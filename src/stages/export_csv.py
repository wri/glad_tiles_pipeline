from raster2points import raster2df
import logging


def get_dataframe(tiles, **kwargs):
    for tile in tiles:
        try:
            logging.info("Extract points for tile: " + tile)
            df = raster2df(tile, calc_area=True)
        except Exception as e:
            logging.error("Failed to extract points for tile: " + tile)
            logging.error(e)
        else:
            yield df
