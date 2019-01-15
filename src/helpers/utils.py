from pathlib import Path, PurePath
import glob


def output_tiles(root, tile_id, stage, year, name):
    output_dir = PurePath(root, "tiles", tile_id, stage, str(year))
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_file = PurePath(output_dir.as_posix(), name)

    return output_file.as_posix()


def file_details(f):
    p = PurePath(f)
    f_name = p.parts[-1]
    year = p.parts[-2]
    folder = p.parts[-3]
    tile_id = p.parts[-4]

    return f_name, year, folder, tile_id


def preprocessed_years_str(preprocessed_years):
    return "_".join(str(year) for year in preprocessed_years)


def add_tile_to_dict(tile_pairs, basedir, year, tile):
    if basedir not in tile_pairs.keys():
        tile_pairs[basedir] = dict()

    tile_pairs[basedir][year] = tile

    return tile_pairs


def add_preprocessed_tile_to_dict(tile_pairs, basedir, preprocessed_tiles):
    for tile in preprocessed_tiles:
        year_str = PurePath(tile).parts[-2]
        if basedir == PurePath(tile).parent.parent.as_posix():
            tile_pairs[basedir][year_str] = tile
    return tile_pairs


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
