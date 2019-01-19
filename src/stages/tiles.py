from helpers.utils import output_file, output_mkdir
from helpers.tiles import get_bbox_by_tile_id
from collections import OrderedDict
import xmltodict as xd
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


def tilecache_mapfile(image_file, zoom):

    output = ""
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
                                                [("@name", "format"), ("#text", "png")]
                                            ),
                                            OrderedDict(
                                                [("@name", "minzoom"), ("#text", "0")]
                                            ),
                                            OrderedDict(
                                                [("@name", "maxzoom"), ("#text", "22")]
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
                                                ("MinScaleDenominator", "500000000"),
                                                (
                                                    "RasterSymbolizer",
                                                    OrderedDict([("@opacity", "1")]),
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
                                                                ("#text", image_file),
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


def tilecache_config():
    tilecache_path = ""
    mapfile = ""
    config = {
        "cache": {
            "name": "Disk",
            "path": tilecache_path,
            "umask": "0000",
            "dirs": "portable",
        },
        "layers": {"tiles": {"provider": {"name": "mapnik", "mapfile": mapfile}}},
    }

    return config
