from datetime import datetime
import logging
import os
import sys


def get_logfile():
    now = datetime.now()
    log_dir = "/var/log/glad"

    # TODO: use SysLogHandler instead of FileHandler
    #  https://stackoverflow.com/questions/36762016/best-practice-to-write-logs-in-var-log-from-a-python-script
    try:
        os.makedirs(log_dir)
    except FileExistsError:
        # directory already exists
        pass

    logfile = "{}/glad-{}.log".format(log_dir, now.strftime("%Y%m%d%H%M%S"))

    return logfile


def get_logger(logfile, debug=True):
    """
    Build logger
    :param logfile: Location of logfile
    :param debug: Set Log Level to Debug or Info
    :return: logger
    """

    root = logging.getLogger()
    if debug:
        root.setLevel(logging.DEBUG)
    else:
        root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    sh = logging.StreamHandler(sys.stdout)
    fh = logging.FileHandler(logfile)

    fh.setLevel(logging.WARNING)

    sh.setFormatter(formatter)
    fh.setFormatter(formatter)

    root.addHandler(sh)
    root.addHandler(fh)

    return root
