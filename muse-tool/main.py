#!/usr/bin/env python3

import argparse
import logging
import os
import sys
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import postgres

import tools.muse_call as muse_call
import tools.muse_sump_wgs as muse_sump_wgs
import tools.muse_sump_wxs as muse_sump_wxs

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
                        required = False,
                        help = 'Reference fasta path.',
    )
    parser.add_argument('-rf', '--reference_fasta_fai',
                        required = False,
                        help = 'Reference fasta fai path.',
    )
    parser.add_argument('-snp','--dbsnp_known_snp_sites',
                        required = False,
                        help='Reference SNP path, that should be bgzip compressed, tabix indexed',
    )
    parser.add_argument('-tb', '--tumor_bam_path',
                        nargs = '?',
                        default = [sys.stdin],
                        required = False,
                        help = 'Source patient tumor bam path.',
    )
    parser.add_argument('-nb', '--normal_bam_path',
                        nargs = '?',
                        default = [sys.stdin],
                        required = False,
                        help = 'Source patient normal bam path.',
    )
    parser.add_argument('-bs', '--Parallel_Block_Size',
                        type = is_nat,
                        default = 50000000,
                        required = False,
                        help = 'Parallel Block Size',
    )
    parser.add_argument('-muse_call_output_path', '--muse_call_output_path',
                        required = False,
                        help = 'muse call output path',
    )
    parser.add_argument('--thread_count',
                        type = is_nat,
                        default = 8,
                        required = False,
                        help = 'thread count'
    )
    parser.add_argument('--tool_name',
                        required = True,
                        help = 'MuSE-pipeline tool'
    )

    db = parser.add_argument_group("Database parameters")
    db.add_argument("--host", default='pgreadwrite.osdc.io', help='hostname for db')
    db.add_argument("--database", default='prod_bioinfo', help='name of the database')
    db.add_argument("--username", default=None, help="username for db access", required=True)
    db.add_argument("--password", default=None, help="password for db access", required=True)

    optional = parser.add_argument_group("optional input parameters")
    optional.add_argument("--normal_id", default="unknown", help="unique identifier for normal dataset")
    optional.add_argument("--tumor_id", default="unknown", help="unique identifier for tumor dataset")
    optional.add_argument("--case_id", default="unknown", help="unique identifier")
    optional.add_argument("--outdir", default="./", help="path for logs etc.")

    args = parser.parse_args()
    tool_name = args.tool_name
    case_id = args.case_id
    thread_count = str(args.thread_count)
    Parallel_Block_Size = str(args.Parallel_Block_Size)

    logger = pipe_util.setup_logging(tool_name, args, case_id)

    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)

    DATABASE = {
        'drivername': 'postgres',
        'host' : args.host,
        'port' : '5432',
        'username': args.username,
        'password' : args.password,
        'database' : args.database
    }


    engine = postgres.db_connect(DATABASE)

    if tool_name == 'muse_call':
        thread_count = pipe_util.get_param(args, 'thread_count')
        tumor_bam_path = pipe_util.get_param(args, 'tumor_bam_path')
        normal_bam_path = pipe_util.get_param(args, 'normal_bam_path')
        reference_fasta_name = pipe_util.get_param(args, 'reference_fasta_name')
        fai_path = pipe_util.get_param(args, 'reference_fasta_fai')
        blocksize = pipe_util.get_param(args, 'Parallel_Block_Size')
        muse_call_output_path = muse_call.call_region(case_id, tumor_id, normal_id, thread_count, tumor_bam_path, normal_bam_path, reference_fasta_name, fai_path, blocksize, engine, logger)

    elif tool_name == 'muse_sump_wxs':
        muse_call_output_path = pipe_util.get_param(args, 'muse_call_output_path')
        dbsnp_known_snp_sites = pipe_util.get_param(args, 'dbsnp_known_snp_sites')
        muse_vcf = muse_sump_wxs.sump_wxs(case_id, tumor_id, normal_id, muse_call_output_path, dbsnp_known_snp_sites, engine, logger)

    elif tool_name == 'muse_sump_wgs':
        muse_call_output_path = pipe_util.get_param(args, 'muse_call_output_path')
        dbsnp_known_snp_sites = pipe_util.get_param(args, 'dbsnp_known_snp_sites')
        muse_vcf = muse_sump_wgs.sump_wgs(case_id, tumor_id, normal_id, muse_call_output_path, dbsnp_known_snp_sites, engine, logger)

    else:
        sys.exit('No recognized tool was selected')

if __name__ == '__main__':
    main()
