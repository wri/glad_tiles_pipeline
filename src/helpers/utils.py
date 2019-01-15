from pathlib import Path, PurePath


def output_tiles(root, tile_id, stage, year, name):
    output_dir = PurePath(root, "tiles", tile_id, stage, str(year))
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_file = PurePath(output_dir.as_posix(), name)

    return output_file.as_posix()


def file_details(f):
    p = PurePath(f)
    f_name = p.parts[-1]
    tile_id = p.parts[-4]
    year = p.parts[-2]

    return f_name, tile_id, year
