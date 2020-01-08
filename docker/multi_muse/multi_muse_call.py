'''
Multithreading MuSE call

@author: Shenglai Li
'''

import sys
import time
import logging
import argparse
import subprocess
import string
from functools import partial
from multiprocessing.dummy import Lock, Pool

def setup_logger():
    '''
    Sets up the logger.
    '''
    logger = logging.getLogger("multi_muse_call")
    logger_format = '[%(levelname)s] [%(asctime)s] [%(name)s] - %(message)s'
    logger.setLevel(level=logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(logger_format, datefmt='%Y%m%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def do_pool_commands(cmd, logger, shell_var=True, lock=Lock()):
    '''run pool commands'''
    try:
        output = subprocess.Popen(
            cmd,
            shell=shell_var,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        output_stdout, output_stderr = output.communicate()
        with lock:
            logger.info('MuSE Args: %s', cmd)
            logger.info(output_stdout)
            logger.info(output_stderr)
    except BaseException:
        logger.error("command failed %s", cmd)
    return output.wait()

def multi_commands(cmds, thread_count, logger, shell_var=True):
    '''run commands on number of threads'''
    pool = Pool(int(thread_count))
    output = pool.map(partial(do_pool_commands, logger=logger, shell_var=shell_var), cmds)
    return output

def get_region(intervals):
    '''get region from intervals'''
    interval_list = []
    with open(intervals, 'r') as fh:
        line = fh.readlines()
        for bed in line:
            blocks = bed.rstrip().rsplit('\t')
            intv = '{}:{}-{}'.format(blocks[0], int(blocks[1])+1, blocks[2])
            interval_list.append(intv)
    return interval_list

def cmd_template(ref=None, region=None, tumor=None, normal=None):
    '''cmd template'''
    lst = [
        '/opt/MuSEv1.0rc_submission_c039ffa',
        'call',
        '-f', '${REF}',
        '-r', '${REGION}',
        '${TUMOR}', '${NORMAL}', '-O', '${NUM}'
    ]
    template = string.Template(' '.join(lst))
    for i, interval in enumerate(region):
        cmd = template.substitute(
            dict(
                REF=ref,
                REGION=interval,
                TUMOR=tumor,
                NORMAL=normal,
                NUM=i
            )
        )
        yield cmd, '{}.MuSE.txt'.format(i)

def get_args():
    '''
    Loads the parser.
    '''
    # Main parser
    parser = argparse.ArgumentParser('Internal multithreading MuSE call.')
    # Required flags.
    parser.add_argument('-f', '--reference_path', required=True, help='Reference path.')
    parser.add_argument('-r', '--interval_bed_path', required=True, help='Interval bed file.')
    parser.add_argument('-t', '--tumor_bam', required=True, help='Tumor bam file.')
    parser.add_argument('-n', '--normal_bam', required=True, help='Normal bam file.')
    parser.add_argument('-c', '--thread_count', type=int, required=True, help='Number of thread.')
    return parser.parse_args()

def main(args, logger):
    '''main'''
    logger.info("Running MuSE...")
    ref = args.reference_path
    interval = args.interval_bed_path
    tumor = args.tumor_bam
    normal = args.normal_bam
    threads = args.thread_count
    muse_cmds = list(cmd_template(ref=ref, region=get_region(interval), tumor=tumor, normal=normal))
    outputs = multi_commands([i[0] for i in muse_cmds], threads, logger)
    if any(x != 0 for x in outputs):
        logger.error('Failed multi_muse_call')
    else:
        logger.info('Completed multi_muse_call')
        first = True
        with open('multi_muse_call_merged.MuSE.txt', 'w') as oh:
            for _, out in muse_cmds:
                with open(out) as fh:
                    for line in fh:
                        if first or not line.startswith('#'):
                            oh.write(line)
                first = False

if __name__ == '__main__':
    # CLI Entrypoint.
    start = time.time()
    logger_ = setup_logger()
    logger_.info('-'*80)
    logger_.info('multi_muse_call.py')
    logger_.info('Program Args: %s', ' '.join(sys.argv))
    logger_.info('-'*80)

    args_ = get_args()

    # Process
    logger_.info('Processing tumor and normal bam files %s, %s', args_.tumor_bam, args_.normal_bam)
    main(args_, logger_)

    # Done
    logger_.info('Finished, took %s seconds.', round(time.time() - start, 2))
