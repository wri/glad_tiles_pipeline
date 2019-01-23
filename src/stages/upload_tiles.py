from helpers.utils import file_details
import subprocess as sp
import logging


def backup_tiles(tiles):

    for tile in tiles:
        f_name, year, folder, tile_id = file_details(tile)
        output = "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/tiles/{}/{}/{}/{}".format(
            tile_id, folder, year, f_name
        )

        try:
            sp.check_call(["aws", "s3", "cp", tile, output])
        except sp.CalledProcessError:
            logging.warning("Failed to upload file: " + tile)
        else:
            logging.info("Upload file: " + tile)
            yield output
