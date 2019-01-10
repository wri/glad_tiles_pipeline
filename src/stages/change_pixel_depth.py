from parallelpipe import stage
import subprocess as sp
import os
import logging
import pathlib
from pathlib import PurePath


@stage(workers=4)
def change_pixel_depth(files, **kwargs):

    try:
        # tiles = kwargs["tiles"]
        root = kwargs["root"]
    except KeyError:
        logging.warning("Wrong number of arguments")
    else:

        for f in files:
            p = PurePath(f)
            f_name = p.parts[-1]
            tile = p.parts[-3]
            output_dir = os.path.join(root, "tiles", tile, "source")
            pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
            output_file = os.path.join(output_dir, f_name)
            try:
                logging.debug(
                    ["pixel_depth.py", "-i", tile, "-o", output_file, "-d", "UInt16"]
                )
                sp.check_call(
                    ["pixel_depth.py", "-i", f, "-o", output_file, "-d", "UInt16"]
                )
            except sp.CalledProcessError:
                logging.warning("Failed to change pixel depth for file: " + f)
            else:
                logging.info("Change pixel depth for file: " + f)
                yield output_file
