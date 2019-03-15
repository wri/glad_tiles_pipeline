from glad.utils.utils import output_file, output_mkdir, split_list
from glad.utils.raster_utilities import get_scale_denominators
from glad.utils.tiles import get_bbox_by_tile_id
from pathlib import Path, PurePath
import xmltodict as xd
import subprocess as sp
import logging
import json


def generate_vrt(zoom_tiles, min_zoom_vrt, max_zoom_vrt, **kwargs):
    root = kwargs["root"]

    for zoom_tile in zoom_tiles:
        zoom = zoom_tile[0]
        tiles = zoom_tile[1]

        if min_zoom_vrt <= zoom <= max_zoom_vrt:
            output = output_file(root, "tiles", "z_{}.vrt".format(zoom))
            cmd = [
                "gdalbuildvrt",
                "-a_srs",
                "EPSG:3857",
                "-vrtnodata",
                "None",
                output,
            ] + tiles

            try:
                sp.check_call(cmd)
            except sp.CalledProcessError as e:
                logging.error("Failed to build VRT: " + output)
                logging.error(e)
                raise e
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

                try:
                    logging.info(
                        "Generate tilestache list for zoom {} and tile {}".format(
                            zoom, tile_id
                        )
                    )
                    tile_str = sp.check_output(cmd)
                except sp.CalledProcessError as e:
                    logging.error(
                        "Failed to generate tilestache list for zoom {} and tile {}".format(
                            zoom, tile_id
                        )
                    )
                    logging.error(e)
                    raise e

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
            try:
                logging.info("Try to save tile list #{} for zoom {}".format(i, zoom))
                with open(output, "w") as f:
                    for tile_coords in tiles:
                        f.write((tile_coords.decode("utf-8") + "\n"))
            except Exception as e:
                logging.error(
                    "Failed to save tile list #{} for zoom {}".format(i, zoom)
                )
                logging.error(e)
                raise e

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
        except sp.CalledProcessError as e:
            logging.error("Failed to seed tiles for: " + tilelist)
            logging.error(e)
            raise e
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

        scale_denominators = get_scale_denominators(zoom)

        mapfile["Map"]["Style"]["@name"] = "z{}".format(zoom)
        if not scale_denominators["max"]:
            del mapfile["Map"]["Style"]["Rule"]["MaxScaleDenominator"]
        else:
            mapfile["Map"]["Style"]["Rule"]["MaxScaleDenominator"] = scale_denominators[
                "max"
            ]
        mapfile["Map"]["Style"]["Rule"]["MinScaleDenominator"] = scale_denominators[
            "min"
        ]
        mapfile["Map"]["Layer"]["@name"] = "z{}".format(zoom)
        mapfile["Map"]["Layer"]["StyleName"] = "z{}".format(zoom)
        mapfile["Map"]["Layer"]["Datasource"]["Parameter"][0]["#text"] = image

        output = output_file(root, "tilecache", "config", "z{}.xml".format(zoom))

        try:
            logging.info("Generating Mapfile " + output)
            with open(output, "w") as f:
                f.write(xd.unparse(mapfile, pretty=True))
        except Exception as e:
            logging.error("Could not generate mapfile " + output)
            logging.error(e)
            raise e
        else:
            yield zoom, output


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
        try:
            logging.info("Generating tilecache config file " + output)
            with open(output, "w") as f:
                f.write(json.dumps(config, indent=4))
        except Exception as e:
            logging.error("Failed to generate tilecache config file " + output)
            logging.error(e)
            raise e
        yield zoom, output
