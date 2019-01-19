from helpers.utils import output_file, output_mkdir
from helpers.tiles import get_bbox_by_tile_id
import subprocess as sp
import logging


def split_list(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def generate_tile_list(tile_ids):

    for zoom in range(0, 9):

        tile_set = set()
        for tile_id in tile_ids:
            bbox = get_bbox_by_tile_id(tile_id)
            cmd = [
                "tilestache-list",
                "-b",
                bbox[0],
                bbox[1],
                bbox[2],
                bbox[3],
                "-p",
                "1",
                str(zoom),
            ]

            tile_str = sp.check_output(cmd)
            for t in tile_str.split():
                tile_set.add(t)

        yield zoom, list(tile_set)


def split_tiles(tile_lists, **kwargs):

    root = kwargs["root"]

    for tile_list in tile_lists:
        zoom = tile_list[0]
        i = 0
        for tiles in split_list(tile_list[1], 1000):

            output = output_file(root, "tile_lists", "z_{0}_{1}.txt".format(zoom, i))

            with open(output, "wb") as f:
                for tile_coords in tiles:
                    f.write(tile_coords + "\n")

            i += 1
            yield zoom, tiles


def generate_tiles(tile_lists, **kwargs):

    root = kwargs["root"]

    for tile_list in tile_lists:

        zoom = tile_list[0]
        t_list = tile_list[1]

        # TODO: verify that config path is correct
        config = "config/z_{}.cfg".format(zoom)
        output_dir = output_mkdir(root, "tile_cache")

        cmd = ["tilestache-seed", str(zoom), "-c", config, "-q", "-x", "-l", "tiles"]
        cmd += ["--output-directory", output_dir]
        cmd += ["--tile-list", t_list]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError:
            logging.warning("Failed to seed tiles for: " + t_list)
        else:
            logging.info("Seeded tiles for : " + t_list)
            yield output_dir
