from glad.utils.utils import file_details, output_file
from pathlib import PurePosixPath
import subprocess as sp
import sqlite3
import logging


def delete_current_years(db, **kwargs):
    """
    Delete all entries for current years from DB
    :param db:
    :param kwargs:
    :return:
    """
    years = kwargs["years"]

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    for year in years:

        cursor.execute(
            "DELETE FROM tile_alert_stats WHERE alert_date >= '{0}-01-01' AND alert_date <= '{0}-12-31';".format(
                year
            )
        )

    conn.commit()
    conn.close()


def create_vector_tiles(csv_files, **kwargs):

    root = kwargs["root"]

    for csv_file in csv_files:

        f_name, year, format, folder = file_details(csv_file)
        f_name = PurePosixPath(f_name).stem
        f_name = PurePosixPath(f_name).with_suffix(".mbtiles")

        output = output_file(root, "db", year, f_name)

        cmd = ["tippecanoe", "-o", output, "-z12", "-Z12", "-b", "0", csv_file]

        try:
            logging.debug("Create vector tiles for: " + csv_file)
            sp.check_call(cmd)
        except sp.CalledProcessError as e:
            logging.warning("Failed to create vector tiles for: " + csv_file)
            raise e
        else:
            logging.info("Created vector tiles for: " + csv_file)
            return output
