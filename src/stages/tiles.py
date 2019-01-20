from parallelpipe import stage
from helpers.utils import output_file, output_mkdir, split_list
from helpers.tiles import get_bbox_by_tile_id
from collections import OrderedDict
from pathlib import PurePath
import xmltodict as xd
import subprocess as sp
import logging
import json


@stage(workers=2)
def generate_vrt(zoom_tiles, **kwargs):
    # root = kwargs["root"]
    min_tile_zoom = kwargs["min_tile_zoom"]
    max_tilecache_zoom = kwargs["max_tilecache_zoom"]

    for zoom_tile in zoom_tiles:
        zoom = zoom_tile[0]
        if zoom <= max_tilecache_zoom:
            if zoom >= min_tile_zoom:
                # TODO generate VRT function
                output = "vrt"
            else:
                output = zoom_tile[1]

            yield zoom, output


@stage(workers=2)
def generate_tile_list(zoom_tiles, tile_ids, **kwargs):

    max_tilecache_zoom = kwargs["max_tilecache_zoom"]

    for zoom_tile in zoom_tiles:
        zoom = zoom_tile[0]
        if zoom <= max_tilecache_zoom:
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


@stage(workers=2)
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

            with open(output, "wb") as f:
                for tile_coords in tiles:
                    f.write(tile_coords + "\n")

            i += 1
            yield zoom, output


@stage(workers=2)
def generate_tiles(zoom_tilelists, **kwargs):

    root = kwargs["root"]

    for zoom_tilelist in zoom_tilelists:

        zoom = zoom_tilelist[0]
        tilelist = zoom_tilelist[1]

        # TODO: verify that config path is correct
        config = PurePath(
            root, "tilecache", "config", "z{}.cfg".format(zoom)
        ).as_posix()
        output_dir = output_mkdir(root, "tilecache", "tiles")

        cmd = ["tilestache-seed", str(zoom), "-c", config, "-q", "-x", "-l", "tiles"]
        cmd += ["--output-directory", output_dir]
        cmd += ["--tile-list", tilelist]

        try:
            sp.check_call(cmd)
        except sp.CalledProcessError:
            logging.warning("Failed to seed tiles for: " + tilelist)
        else:
            logging.info("Seeded tiles for : " + tilelist)
            yield zoom, output_dir


@stage(workers=2)
def generate_tilecache_mapfile(zoom_images, **kwargs):

    root = kwargs["root"]

    for zoom_image in zoom_images:
        zoom = zoom_image[0]
        image = zoom_image[1]

        output = output_file(root, "tilecache", "config", "z{}.xml".format(zoom))

        mapfile = OrderedDict(
            [
                (
                    "Map",
                    OrderedDict(
                        [
                            (
                                "@srs",
                                "+proj=merc "
                                "+a=6378137 "
                                "+b=6378137 "
                                "+lat_ts=0.0 "
                                "+lon_0=0.0 "
                                "+x_0=0.0 +y_0=0.0 "
                                "+k=1.0 "
                                "+units=m "
                                "+nadgrids=@null "
                                "+wktext "
                                "+no_defs "
                                "+over",
                            ),
                            (
                                "@maximum-extent",
                                "-20037508.34,-20037508.34,20037508.34,20037508.34",
                            ),
                            (
                                "Parameters",
                                OrderedDict(
                                    [
                                        (
                                            "Parameter",
                                            [
                                                OrderedDict(
                                                    [
                                                        ("@name", "bounds"),
                                                        (
                                                            "#text",
                                                            "-180,"
                                                            "-85.05112877980659,"
                                                            "180,"
                                                            "85.05112877980659",
                                                        ),
                                                    ]
                                                ),
                                                OrderedDict(
                                                    [
                                                        ("@name", "center"),
                                                        ("#text", "0,0,2"),
                                                    ]
                                                ),
                                                OrderedDict(
                                                    [
                                                        ("@name", "format"),
                                                        ("#text", "png"),
                                                    ]
                                                ),
                                                OrderedDict(
                                                    [
                                                        ("@name", "minzoom"),
                                                        ("#text", "0"),
                                                    ]
                                                ),
                                                OrderedDict(
                                                    [
                                                        ("@name", "maxzoom"),
                                                        ("#text", "22"),
                                                    ]
                                                ),
                                            ],
                                        )
                                    ]
                                ),
                            ),
                            (
                                "Style",
                                OrderedDict(
                                    [
                                        ("@name", "z{}".format(zoom)),
                                        ("@filter-mode", "first"),
                                        (
                                            "Rule",
                                            OrderedDict(
                                                [
                                                    (
                                                        "MinScaleDenominator",
                                                        "500000000",
                                                    ),
                                                    (
                                                        "RasterSymbolizer",
                                                        OrderedDict(
                                                            [("@opacity", "1")]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                    ]
                                ),
                            ),
                            (
                                "Layer",
                                OrderedDict(
                                    [
                                        ("@name", "z{}".format(zoom)),
                                        (
                                            "@srs",
                                            "+proj=merc "
                                            "+a=6378137 "
                                            "+b=6378137 "
                                            "+lat_ts=0.0 "
                                            "+lon_0=0.0 "
                                            "+x_0=0.0 "
                                            "+y_0=0.0 "
                                            "+k=1.0 "
                                            "+units=m "
                                            "+nadgrids=@null "
                                            "+wktext "
                                            "+no_defs "
                                            "+over",
                                        ),
                                        ("StyleName", "z{}".format(zoom)),
                                        (
                                            "Datasource",
                                            OrderedDict(
                                                [
                                                    (
                                                        "Parameter",
                                                        [
                                                            OrderedDict(
                                                                [
                                                                    ("@name", "file"),
                                                                    ("#text", image),
                                                                ]
                                                            ),
                                                            OrderedDict(
                                                                [
                                                                    ("@name", "type"),
                                                                    ("#text", "gdal"),
                                                                ]
                                                            ),
                                                        ],
                                                    )
                                                ]
                                            ),
                                        ),
                                    ]
                                ),
                            ),
                        ]
                    ),
                )
            ]
        )

        with open(output, "w") as f:
            f.write(xd.unparse(mapfile))

        yield zoom, output


@stage(workers=2)
def generate_tilecache_config(zoom_mapfiles, **kwargs):

    root = kwargs["root"]
    tilecache_path = output_mkdir(root, "tilecache", "tiles")

    for zoom_mapfile in zoom_mapfiles:
        zoom = zoom_mapfile[0]
        mapfile = zoom_mapfiles[1]

        output = output_file(root, "tilecache", "config", "z{}.cgf".format(zoom))

        config = {
            "cache": {
                "name": "Disk",
                "path": tilecache_path,
                "umask": "0000",
                "dirs": "portable",
            },
            "layers": {"tiles": {"provider": {"name": "mapnik", "mapfile": mapfile}}},
        }

        with open(output, "w") as f:
            f.write(json.dumps(config), indent=4)

        yield zoom, output
