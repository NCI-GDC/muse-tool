#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import sqlalchemy

from cdis_pipe_utils import pipe_util

import tools.muse_call as muse_call
import tools.muse_sump_wgs as muse_sump_wgs
import tools.muse_sump_wxs as muse_sump_wxs
import tools.verify_util as verify_util

def is_nat(x):
    '''
    Checks that a value is a natural number.
    '''
    if int(x) > 0:
        return int(x)
    raise argparse.ArgumentTypeError('%s must be positive, non-zero' % x)


def main():
    parser = argparse.ArgumentParser('MuSE variant calling pipeline')

    # Logging flags.
    parser.add_argument('-d', '--debug',
        action = 'store_const',
        const = logging.DEBUG,
        dest = 'level',
        help = 'Enable debug logging.',
    )
    parser.set_defaults(level = logging.INFO)

    # Required flags.

    parser.add_argument('-r', '--reference_fasta_name',
                        required = True,
                        help = 'Reference fasta path.',
    )
    parser.add_argument('-snp','--dbsnp_known_snp_sites',
                        required=True,
                        help='Reference SNP path, that should be bgzip compressed, tabix indexed',
    )
    parser.add_argument('-tb', '--analysis_ready_tumor_bam_path',
                        required = True,
                        nargs = '?',
                        default = [sys.stdin],
                        help = 'Source patient tumor bam path.',
    )
    parser.add_argument('-nb', '--analysis_ready_normal_bam_path',
                        required = True,
                        nargs = '?',
                        default = [sys.stdin],
                        help = 'Source patient normal bam path.',
    )
    parser.add_argument('-bs', '--Parallel_Block_Size',
                        type = is_nat,
                        default = 50000000,
                        help = 'Parallel Block Size',
    )
    parser.add_argument('-u', '--uuid',
                        required = True,
                        help = 'analysis_id string',
    )
    parser.add_argument('--tool_name',
                        required = True,
                        help = 'MuSE-pipeline tool'
    )

    args = parser.parse_args()
    tool_name = args.tool_name
    uuid = args.uuid
    thread_count = str(args.thread_count)
    blocksize = str(args.Parallel_Block_Size)

    logger = pipe_util.setup_logging('muse_' + tool_name, args, uuid)
    engine = pipe_util.setup_db(uuid)

    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)

    if tool_name == 'muse_call':
        thread_count = pipe_util.get_param(args, 'thread_count')
        tumor_bam_path = pipe_util.get_param(args, 'tumor_bam_path')
        normal_bam_path = pipe_util.get_param(args, 'normal_bam_path')
        reference_fasta_name = pipe_util.get_param(args, 'reference_fasta_name')
        fai_path = pipe_util.get_param(args, 'fai_path')
        blocksize = pipe_util.get_param(args, 'blocksize')
        muse_call_output_path = muse_call.call_region(uuid, thread_count, tumor_bam_path, normal_bam_path, reference_fasta_name, fai_path, blocksize, engine, logger)

    elif tool_name == 'muse_sump_wxs':
        muse_call_output_path = pipe_util.get_param(args, 'muse_call_output_path')
        dbsnp_known_snp_sites = pipe_util.get_param(args, 'dbsnp_known_snp_sites')
        muse_vcf = muse_sump.sump_wxs(uuid, muse_call_output_path, dbsnp_known_snp_sites, engine, logger)

    elif tool_name == 'muse_sump_wgs':
        muse_call_output_path = pipe_util.get_param(args, 'muse_call_output_path')
        dbsnp_known_snp_sites = pipe_util.get_param(args, 'dbsnp_known_snp_sites')
        muse_vcf = muse_sump.sump_wgs(uuid, muse_call_output_path, dbsnp_known_snp_sites, engine, logger)

    else:
        sys.exit('No recognized tool was selected')

if __name__ == '__main__':
    main()
