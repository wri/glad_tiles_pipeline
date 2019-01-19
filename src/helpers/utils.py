from pathlib import Path, PurePath
import glob
import re


def output_mkdir(*path):
    """
    Creates directory for the parent directory
    :param path: List of path elements
    :return: None
    """
    output_dir = PurePath(*path)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    return Path(output_dir).as_posix()


def output_file(*path):
    """
    Creates POSIX path from input list
    and creates directory for the parent directory
    :param path: List of path elements
    :return: POSIX path
    """

    path = [str(p) for p in path if p is not None]
    output_mkdir(*path[:-1])

    return PurePath(*path).as_posix()


def file_details(f):
    p = PurePath(f)
    f_name = p.parts[-1]
    year = p.parts[-2]
    folder = p.parts[-3]
    tile_id = p.parts[-4]

    return f_name, year, folder, tile_id


def get_file_name(f):
    """
    Extract the last element of a POSIX path
    :param f: POSIX path
    :return: File name
    """
    p = PurePath(f)
    return p.parts[-1]


def get_tile_id(f):
    """
    Finds and returns tile id in file name
    Tile id must match the following pattern
    050W_20S_030E_10N

    :param f: File name
    :return: Tile ID or None
    """
    m = re.search("([0-9]{3}[EW]_[0-9]{2}[NS]_?){2}", f)
    if m:
        return m.group(0)
    else:
        return None


def preprocessed_years_str(preprocessed_years):
    return "_".join(str(year) for year in preprocessed_years)


def add_tile_to_dict(tile_dict, basedir, year, tile):
    if basedir not in tile_dict.keys():
        tile_dict[basedir] = dict()

    tile_dict[basedir][year] = tile

    return tile_dict


def add_preprocessed_tile_to_dict(tile_dict, basedir, preprocessed_tiles):
    for tile in preprocessed_tiles:
        year_str = PurePath(tile).parts[-2]
        if basedir == PurePath(tile).parent.parent.as_posix():
            tile_dict[basedir][year_str] = tile
    return tile_dict


def get_preprocessed_tiles(root, years, preprocessed_years):
    preprocessed_tiles = list()

    for tile in glob.iglob(root + "/tiles/**/day_conf.tif", recursive=True):
        try:
            year = int(PurePath(tile).parts[-2])

            if year not in years and year not in preprocessed_years:
                preprocessed_tiles.append(tile)

        except ValueError:
            pass

    return preprocessed_tiles


def sort_dict(tile_dict):

    sorted_list = list()

    for year in sorted(list(tile_dict.keys())):
        sorted_list.append(tile_dict[year])

    return sorted_list
