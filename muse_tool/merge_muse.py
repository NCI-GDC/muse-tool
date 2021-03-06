#!/usr/bin/env python3
"""
Utility for merging `MuSE call` outputs.

@author: Shenglai Li
"""

import argparse
import logging
import os
import sys
import time


def main(args, logger):
    """
    Main wrapper for merging.
    """
    # Merge
    logger.info("Merging `MuSE call` outputs...")
    first = True
    with open(args.merge_outname, "w") as o:
        for m in args.muse_call_out:
            with open(m) as f:
                for line in f:
                    if first or not line.startswith("#"):
                        o.write(line)
            first = False
    assert os.stat(args.merge_outname).st_size != 0, "Merged VCF is Empty"


def get_args():
    """
    Loads the parser.
    """
    # Main parser
    parser = argparse.ArgumentParser(
        description="Utility for merging `MuSE call` outputs."
    )
    # Args
    required = parser.add_argument_group("Required input parameters")
    required.add_argument("--muse_call_out", action="append", required=True)
    required.add_argument("--merge_outname", required=True)
    return parser.parse_args()


def setup_logger():
    """
    Sets up the logger.
    """
    logger = logging.getLogger("merge_muse.py")
    logger_format = "[%(levelname)s] [%(asctime)s] [%(name)s] - %(message)s"
    logger.setLevel(level=logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(logger_format, datefmt="%Y%m%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


if __name__ == "__main__":
    # CLI Entrypoint.
    start = time.time()
    logger_ = setup_logger()
    logger_.info("-" * 80)
    logger_.info("merge_muse.py")
    logger_.info("Program Args: %s", " ".join(sys.argv))
    logger_.info("-" * 80)

    args_ = get_args()

    # Process
    logger_.info("Processing %s MuSE call outputs", len(args_.muse_call_out))
    main(args_, logger_)

    # Done
    logger_.info("Finished, took %s seconds.", round(time.time() - start, 2))
