from glad.stages.collectors import get_most_recent_day
from glad.utils.tiles import get_tile_ids_by_bbox
from glad.utils.parser import get_parser
from glad.utils.logger import get_logger, get_logfile
from typing import Dict, Any
import logging


def main():

    args = get_parser()

    logfile = get_logfile()
    get_logger(logfile, debug=args.debug)

    tile_ids = get_tile_ids_by_bbox(
        args.bbox[0], args.bbox[1], args.bbox[2], args.bbox[3]
    )

    num_tiles = args.num_tiles
    if args.include_russia:
        num_tiles += 1
        tile_ids.append("130E_42N_142E_53N")

    kwargs: Dict[str, Any] = {"years": args.years, "num_tiles": num_tiles}

    try:
        kwargs["tile_date"], tile_ids = get_most_recent_day(
            max_date=args.max_date, tile_ids=tile_ids, **kwargs
        )
    except ValueError:
        logging.warning("Cannot find recently processes tiles.")


if __name__ == "__main__":
    main()
