from math import floor
import logging


def get_longitude(x):
    if x >= 0:
        return str(x).zfill(3) + "E"
    else:
        return str(-x).zfill(3) + "W"


def get_latitude(y):
    if y >= 0:
        return str(y).zfill(2) + "N"
    else:
        return str(-y).zfill(2) + "S"


def lower_bound(y):
    return floor(y / 10) * 10


def upper_bound(y):
    if y == lower_bound(y):
        return y
    else:
        return (floor(y / 10) * 10) + 10


def get_tile_ids_by_bbox(left, bottom, right, top):
    tile_ids = list()
    left = lower_bound(left)
    bottom = lower_bound(bottom)
    right = upper_bound(right)
    top = upper_bound(top)

    for y in range(bottom, top, 10):

        for x in range(left, right, 10):

            west = get_longitude(x)
            south = get_latitude(y)
            east = get_longitude(x + 10)
            north = get_latitude(y + 10)

            tile_ids.append("{}_{}_{}_{}".format(west, south, east, north))

    logging.debug("Tile IDS: " + str(tile_ids))
    return tile_ids


def format_lat_lon(coord):
    if coord[-1:] == "E" or coord[-1:] == "N":
        return int(coord[:-1])
    else:
        return int(coord[:-1]) * -1


def get_bbox_by_tile_id(tile_id):

    left, bottom, right, top = tile_id.split("_")

    left = format_lat_lon(left)
    bottom = format_lat_lon(bottom)
    right = format_lat_lon(right)
    top = format_lat_lon(top)

    return left, bottom, right, top
