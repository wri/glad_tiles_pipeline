from glad.utils.utils import (
    output_file,
    file_details,
    sort_dict,
    preprocessed_years_str,
)
import logging
import subprocess as sp


def combine_date_conf_pairs(pairs, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]
    for pair in pairs:
        f_name, year, folder, tile_id = file_details(pair["day"])

        output = output_file(root, "tiles", tile_id, name, year, "day_conf.tif")

        try:
            sp.check_call(["add2", pair["day"], pair["conf"], output])
        except sp.CalledProcessError as e:
            logging.error("Failed to combine files into: " + output)
            logging.error(e)
            raise e
        else:
            logging.info("Combined files into: " + output)
            yield output


def merge_years(tile_dicts, **kwargs):
    root = kwargs["root"]
    name = kwargs["name"]

    for tile_dict in tile_dicts:
        logging.info(str(tile_dict))
        f_name, year, folder, tile_id = file_details(list(tile_dict.values())[0])

        year_str = preprocessed_years_str(sorted(tile_dict.keys()))

        output = output_file(root, "tiles", tile_id, name, year_str, "day_conf.tif")

        input = sort_dict(tile_dict)

        try:
            sp.check_call(["combine{}".format(len(input))] + input + [output])
        except sp.CalledProcessError as e:
            logging.error("Failed to combine files: " + str(input))
            logging.error(e)
            raise e
        else:
            logging.info("Combined files: " + str(input))
            yield output
