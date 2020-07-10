#!/usr/bin/env python
"""
Multithreading MuSE call

@author: Shenglai Li
"""

import os
import sys
import time
import glob
import shlex
import ctypes
import string
import logging
import argparse
import threading
import subprocess
from signal import SIGKILL
from functools import partial
from concurrent.futures import ThreadPoolExecutor


def setup_logger():
    """
    Sets up the logger.
    """
    logger = logging.getLogger("multi_muse_call")
    logger_format = "[%(levelname)s] [%(asctime)s] [%(name)s] - %(message)s"
    logger.setLevel(level=logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(logger_format, datefmt="%Y%m%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def subprocess_commands_pipe(cmd, logger, shell_var=False, lock=threading.Lock()):
    """run pool commands"""
    libc = ctypes.CDLL("libc.so.6")
    pr_set_pdeathsig = ctypes.c_int(1)

    def child_preexec_set_pdeathsig():
        """
        preexec_fn argument for subprocess.Popen,
        it will send a SIGKILL to the child once the parent exits
        """

        def pcallable():
            return libc.prctl(pr_set_pdeathsig, ctypes.c_ulong(SIGKILL))

        return pcallable

    try:
        output = subprocess.Popen(
            shlex.split(cmd),
            shell=shell_var,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=child_preexec_set_pdeathsig(),
        )
        output.wait()
        with lock:
            logger.info("Running command: %s", cmd)
    except BaseException as e:
        output.kill()
        with lock:
            logger.error("command failed %s", cmd)
            logger.exception(e)
    finally:
        output_stdout, output_stderr = output.communicate()
        with lock:
            logger.info(output_stdout.decode("UTF-8"))
            logger.info(output_stderr.decode("UTF-8"))

def tpe_submit_commands(cmds, thread_count, logger, shell_var=False):
    """run commands on number of threads"""
    with ThreadPoolExecutor(max_workers=thread_count) as e:
        for cmd in cmds:
            e.submit(
                partial(subprocess_commands_pipe, logger=logger, shell_var=shell_var),
                cmd,
            )


def get_region(intervals):
    """get region from intervals"""
    interval_list = []
    with open(intervals, "r") as fh:
        line = fh.readlines()
        for bed in line:
            blocks = bed.rstrip().rsplit("\t")
            intv = "{}:{}-{}".format(blocks[0], int(blocks[1]) + 1, blocks[2])
            interval_list.append(intv)
    return interval_list


def get_file_size(filename):
    """ Gets file size """
    fstats = os.stat(filename)
    return fstats.st_size


def cmd_template(dct):
    """cmd template"""
    lst = [
        # "/opt/MuSEv1.0rc_submission_c039ffa",
        "muse",
        "call",
        "-f",
        "${REF}",
        "-r",
        "${REGION}",
        "${TUMOR}",
        "${NORMAL}",
        "-O",
        "${NUM}",
    ]
    template = string.Template(" ".join(lst))
    for i, interval in enumerate(get_region(dct["interval_bed_path"])):
        cmd = template.substitute(
            dict(
                REF=dct["reference_path"],
                REGION=interval,
                TUMOR=dct["tumor_bam"],
                NORMAL=dct["normal_bam"],
                NUM=i
            )
        )
        yield cmd


def get_args():
    """
    Loads the parser.
    """
    # Main parser
    parser = argparse.ArgumentParser(
        "Internal multithreading MuSE call."
    )
    # Required flags.
    parser.add_argument(
        "-f", "--reference_path", required=True, help="Reference path."
    )
    parser.add_argument(
        "-r", "--interval_bed_path", required=True, help="Interval bed file."
    )
    parser.add_argument(
        "-t", "--tumor_bam", required=True, help="Tumor bam file."
    )
    parser.add_argument(
        "-n", "--normal_bam", required=True, help="Normal bam file."
    )
    parser.add_argument(
        "-c", "--thread_count", type=int, required=True, help="Number of thread."
    )
    return parser.parse_args()


def main(args, logger):
    """main"""
    logger.info("Running MuSE...")
    kwargs = vars(args)

    # Start Queue
    tpe_submit_commands(list(cmd_template(kwargs)), kwargs["thread_count"], logger)

    # Check outputs
    outputs = glob.glob("*.MuSE.txt")
    assert len(outputs) == len(
        get_region(kwargs["interval_bed_path"])
    ), "Missing output!"
    if any(get_file_size(x) == 0 for x in outputs):
        logger.error("Empty output detected!")

    # Merge
    merged = "multi_muse_call_merged.MuSE.txt"
    first = True
    with open(merged, "w") as oh:
        for out in outputs:
            with open(out) as fh:
                for line in fh:
                    if first or not line.startswith("#"):
                        oh.write(line)
            first = False


if __name__ == "__main__":
    # CLI Entrypoint.
    start = time.time()
    logger_ = setup_logger()
    logger_.info("-" * 80)
    logger_.info("multi_muse_call.py")
    logger_.info("Program Args: %s", " ".join(sys.argv))
    logger_.info("-" * 80)

    args_ = get_args()

    # Process
    logger_.info(
        "Processing tumor and normal bam files %s, %s",
        args_.tumor_bam,
        args_.normal_bam,
    )
    main(args_, logger_)

    # Done
    logger_.info(
        "Finished, took %s seconds.", round(time.time() - start, 2)
    )
