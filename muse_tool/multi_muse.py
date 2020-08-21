#!/usr/bin/env python3
"""
Multithreading MuSE call

@author: Shenglai Li
"""

import argparse
import concurrent.futures
import logging
import pathlib
import shlex
import subprocess
import sys
from collections import namedtuple
from textwrap import dedent
from types import SimpleNamespace
from typing import IO, Any, Callable, Generator, List, Optional, Tuple

logger = logging.getLogger(__name__)

DI = SimpleNamespace(
    futures=concurrent.futures, pathlib=pathlib, shlex=shlex, subprocess=subprocess
)

CMD_STR = dedent(
    """
    {muse_binary} call
    -f {reference_path}
    -r {region}
    {tumor_bam}
    {normal_bam}
    -O {output_file}
    """
).strip()


def setup_logger():
    """
    Sets up the logger.
    """
    logger_format = "[%(levelname)s] [%(asctime)s] [%(name)s] - %(message)s"
    logger.setLevel(level=logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(logger_format, datefmt="%Y%m%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def subprocess_commands_pipe(cmd, timeout: int = 3600, di=DI) -> Tuple[str, str]:
    """Run given command with subprocess.
    Accepts:
        cmd (str): Command string
        timeout (int=3600): Max time for command to run, in seconds
    Returns:
        Tuple of decoded stdout and stderr
    Raises:
        ValueError: timeout exceeded or other exception
    """
    """run pool commands"""

    output = di.subprocess.Popen(
        shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        # Use only Popen.communicate with PIPE, not wait
        output_stdout, output_stderr = output.communicate(timeout=timeout)
    except Exception:
        output.kill()
        _, output_stderr = output.communicate()
        raise ValueError(output_stderr.decode())
    return output_stdout.decode(), output_stderr.decode()


def tpe_submit_commands(
    cmds: List[Any], thread_count: int, fn: Callable = subprocess_commands_pipe, di=DI,
):
    """Run commands on multiple threads.

    Stdout and stderr are logged on function success.
    Exception logged on function failure.
    Accepts:
        cmds (List[str]): List of inputs to pass to each thread.
        thread_count (int): Threads to run
        fn (Callable): Function to run using threads, must accept each element of cmds
    Returns:
        None
    Raises:
        None
    """
    with di.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(fn, cmd) for cmd in cmds]
        for future in di.futures.as_completed(futures):
            try:
                stdout, stderr = future.result()
                logger.info(stdout.decode())
                logger.info(stderr.decode())
            except Exception as e:
                logger.exception(e)


def get_region(intervals_file: str) -> Generator[str, None, None]:
    """Yield region string from BED file."""
    with open(intervals_file, "r") as fh:
        for line in fh:
            chrom, start, end, *_ = line.strip().split()
            interval = "{}:{}-{}".format(chrom, int(start) + 1, end)
            yield interval


def get_file_size(filename: pathlib.Path) -> int:
    """ Gets file size """
    return filename.stat().st_size


def format_command(
    interval_bed_path: str,
    reference_path: str,
    tumor_bam: str,
    normal_bam: str,
    muse_binary: str = 'muse',
) -> Generator[str, None, None]:
    """Yield commands for each BED interval."""
    for i, interval in enumerate(get_region(interval_bed_path)):
        cmd = CMD_STR.format(
            muse_binary=muse_binary,
            reference_path=reference_path,
            region=interval,
            tumor_bam=tumor_bam,
            normal_bam=normal_bam,
            output_file=i,
        )
        yield cmd


def setup_parser() -> argparse.ArgumentParser:
    """
    Loads the parser.
    """
    # Main parser
    parser = argparse.ArgumentParser("Internal multithreading MuSE call.")
    # Required flags.
    parser.add_argument("-f", "--reference_path", required=True, help="Reference path.")
    parser.add_argument(
        "-r", "--interval_bed_path", required=True, help="Interval bed file."
    )
    parser.add_argument("-t", "--tumor_bam", required=True, help="Tumor bam file.")
    parser.add_argument("-n", "--normal_bam", required=True, help="Normal bam file.")
    parser.add_argument(
        "-c", "--thread_count", type=int, required=True, help="Number of threads."
    )
    parser.add_argument(
        "--muse-binary", required=False, default="muse", help="Path to MuSE binary",
    )
    return parser


def merge_files(muse_outputs: List[pathlib.Path], out_fh: IO):
    """Write contents of outputs to given file handler."""
    # Merge
    first = True
    for file in muse_outputs:
        if get_file_size(file) == 0:
            logger.error("Empty output: %s", file.name)
            continue
        with file.open() as fh:
            for line in fh:
                if first or not line.startswith("#"):
                    out_fh.write(line)
        first = False


def process_argv(argv: Optional[List] = None) -> namedtuple:
    """Processes argv into namedtuple."""

    parser = setup_parser()

    if argv:
        args, unknown_args = parser.parse_known_args(argv)
    else:
        args, unknown_args = parser.parse_known_args()

    args_dict = vars(args)
    args_dict['extras'] = unknown_args
    run_args = namedtuple('RunArgs', list(args_dict.keys()))
    return run_args(**args_dict)


def run(run_args):
    """Main script logic.
    Creates muse commands for each BED region and executes in multiple threads.
    """

    run_commands = list(
        format_command(
            run_args.interval_bed_path,
            run_args.reference_path,
            run_args.tumor_bam,
            run_args.normal_bam,
            run_args.muse_binary,
        )
    )
    # Start Queue
    tpe_submit_commands(
        run_commands, run_args.thread_count,
    )

    # Check and merge outputs
    p = pathlib.Path('.')
    outputs = list(p.glob("*.MuSE.txt"))

    merged = pathlib.Path("multi_muse_call_merged.MuSE.txt")
    with merged.open('w') as fh:
        merge_files(outputs, fh)

    return


def main(argv=None) -> int:
    exit_code = 0

    argv = argv or sys.argv
    args = process_argv(argv)
    setup_logger()

    try:
        run(args)
    except Exception as e:
        logger.exception(e)
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    # CLI Entrypoint.
    retcode = 0

    try:
        retcode = main()
    except Exception as e:
        retcode = 1
        logger.exception(e)

    sys.exit(retcode)


# __END__
