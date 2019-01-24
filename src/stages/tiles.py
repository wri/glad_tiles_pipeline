from helpers.utils import output_file, output_mkdir, split_list
from helpers.tiles import get_bbox_by_tile_id
from pathlib import Path, PurePath
import xmltodict as xd
import subprocess as sp
import logging
import json


def generate_vrt(zoom_tiles, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]

    max_tilecache_zoom = kwargs["max_tilecache_zoom"]

    for zoom_tile in zoom_tiles:
        zoom = zoom_tile[0]
        tiles = zoom_tile[1]

        if zoom <= max_tilecache_zoom:
            output = output_file(root, "vrt", name, "zoom_{}.vrt".format(zoom))
            cmd = [
                "gdalbuildvrt",
                "-a_srs",
                "EPSG:3857",
                "-vrtnodata",
                "0",
                output,
            ] + tiles

            try:
                sp.check_call(cmd)
            except sp.CalledProcessError:
                logging.warning("Failed to build VRT: " + output)
            else:
                logging.info("Built VRT: " + output)
                yield zoom, output


def generate_tile_list(zoom_tiles, tile_ids, **kwargs):
    # tilestache wants BBOX formated this way: south, west, north, east
    # we store BBOX internally this way: west, south, east, north

    max_tilecache_zoom = kwargs["max_tilecache_zoom"]

    for zoom_tile in zoom_tiles:
        zoom = zoom_tile[0]
        if zoom <= max_tilecache_zoom:
            tile_set = set()
            for tile_id in tile_ids:
                bbox = get_bbox_by_tile_id(tile_id)
                cmd = [
                    "tilestache-list.py",
                    "-b",
                    str(bbox[1]),
                    str(bbox[0]),
                    str(bbox[3]),
                    str(bbox[2]),
                    "-p",
                    "1",
                    str(zoom),
                ]

                logging.debug(cmd)
                tile_str = sp.check_output(cmd)
                for t in tile_str.split():
                    tile_set.add(t)

            yield zoom, list(tile_set)


def save_tile_lists(zoom_tilelists, **kwargs):

    root = kwargs["root"]

    for zoom_tilelist in zoom_tilelists:
        zoom = zoom_tilelist[0]
        tilelist = zoom_tilelist[1]
        i = 0
        for tiles in split_list(tilelist, 1000):

            output = output_file(
                root, "tilecache", "config", "z_{0}_{1}.txt".format(zoom, i)
            )

            with open(output, "w") as f:
                for tile_coords in tiles:
                    f.write((tile_coords.decode("utf-8") + "\n"))

            i += 1
            yield zoom, output


def generate_tiles(zoom_tilelists, **kwargs):

    root = kwargs["root"]

    for zoom_tilelist in zoom_tilelists:

        zoom = zoom_tilelist[0]
        tilelist = zoom_tilelist[1]

        # TODO: verify that config path is correct
        config = PurePath(
            root, "tilecache", "config", "z{}.cfg".format(zoom)
        ).as_posix()
        output_dir = output_mkdir(root, "tilecache")

        cmd = ["tilestache-seed.py", str(zoom), "-c", config, "-q", "-x", "-l", "tiles"]
        cmd += ["--output-directory", output_dir]
        cmd += ["--tile-list", tilelist]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError:
            logging.warning("Failed to seed tiles for: " + tilelist)
        else:
            logging.info("Seeded tiles for : " + tilelist)
            yield zoom, output_dir


def generate_tilecache_mapfile(zoom_images, **kwargs):

    root = kwargs["root"]

    for zoom_image in zoom_images:
        zoom = zoom_image[0]
        image = zoom_image[1]

        curr_dir = Path(__file__).parent
        mapfile_path = PurePath(curr_dir, "fixures", "mapfile.xml").as_posix()
        with open(mapfile_path) as f:
            mapfile = xd.parse(f.read())

        mapfile["Map"]["Style"]["@name"] = "z{}".format(zoom)
        mapfile["Map"]["Layer"]["@name"] = "z{}".format(zoom)
        mapfile["Map"]["Layer"]["StyleName"] = "z{}".format(zoom)
        mapfile["Map"]["Layer"]["Datasource"]["Parameter"][0]["#text"] = image

        output = output_file(root, "tilecache", "config", "z{}.xml".format(zoom))

        try:
            logging.info("Generating Mapfile " + output)
            with open(output, "w") as f:
                f.write(xd.unparse(mapfile, pretty=True))
            yield zoom, output
        except Exception as e:
            logging.error("Could not generate mapfile " + output)
            logging.error(e)
            raise e


def generate_tilecache_config(zoom_mapfiles, **kwargs):

    root = kwargs["root"]
    tilecache_path = output_mkdir(root, "tilecache")

    for zoom_mapfile in zoom_mapfiles:
        zoom = zoom_mapfile[0]
        mapfile = zoom_mapfile[1]

        curr_dir = Path(__file__).parent
        config_path = PurePath(curr_dir, "fixures", "tilecache.cfg").as_posix()
        with open(config_path) as f:
            config = json.load(f)

        output = output_file(root, "tilecache", "config", "z{}.cfg".format(zoom))

        config["cache"]["path"] = tilecache_path
        config["layers"]["tiles"]["provider"]["mapfile"] = mapfile

        logging.info("Generating tilecache config file " + output)
        with open(output, "w") as f:
            f.write(json.dumps(config, indent=4))

        yield zoom, output
