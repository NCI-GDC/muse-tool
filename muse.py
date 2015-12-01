#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import sqlalchemy
import pipe_util
import df_util
import time_util
import bam_util
import bam_validate
import verify_util
import RealignerTargetCreator
import IndelRealigner
import BaseRecalibrator
import PrintReads

def is_dir(d):
    '''
    Checks that a directory exists.
    '''
    if os.path.isdir(d):
        return d
    raise argparse.ArgumentTypeError('%s is not a directory' % d)


def is_nat(x):
    '''
    Checks that a value is a natural number.
    '''
    if int(x) > 0:
        return int(x)
    raise argparse.ArgumentTypeError('%s must be positive, non-zero' % x)


def main():
    parser = argparse.ArgumentParser('Broad cocleaning (Inderrealignment and BQSR) pipeline')

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
    parser.add_argument('-indel','--known_1k_genome_indel_sites',
                        required=True,
                        help='Reference INDEL path.',
    )
    parser.add_argument('-snp','--dbsnp_known_snp_sites',
                        required=True,
                        help='Reference SNP path.',
    )
    parser.add_argument('-b', '--harmonized_bam_path',
                        required = False,
                        action="append",
                        help = 'Source bam path.',
    )
    parser.add_argument('-list', '--harmonized_bam_list_path',
                        required = False,
                        help = 'Source bam list path.',
    )
    parser.add_argument('-s', '--scratch_dir',
                        required = False,
                        type = is_dir,
                        help = 'Scratch file directory.',
    )
    parser.add_argument('-l', '--log_dir',
                        required = False,
                        type = is_dir,
                        help = 'Log file directory.',
    )
    parser.add_argument('-j', '--thread_count',
                        required = True,
                        type = is_nat,
                        help = 'Maximum number of threads for execution.',
    )
    parser.add_argument('-u', '--uuid',
                        required = True,
                        help = 'analysis_id string',
    )
    parser.add_argument('-m', '--md5',
                        required = False,
                        action = 'store_true',
                        help = 'calculate final size/MD5',
    )
    parser.add_argument('-e', '--eliminate_intermediate_files',
                        required = False,
                        action = 'store_true',
                        help = 'do not (really) reduce disk usage. set if you want to use more disk space!'
    )

    args = parser.parse_args()
    reference_fasta_name = args.reference_fasta_name
    known_1k_genome_indel_sites = args.known_1k_genome_indel_sites
    dbsnp_known_snp_sites = args.dbsnp_known_snp_sites
    uuid = args.uuid
    harmonized_bam_path = args.harmonized_bam_path
    if not args.harmonized_bam_list_path:
        list_dir = os.path.dirname(harmonized_bam_path[0])
        harmonized_bam_list_path = os.path.join(list_dir, uuid + '_harmonized_bam_list.list')
        with open(harmonized_bam_list_path, "w") as handle:
            for bam in harmonized_bam_path:
                handle.write(bam + "\n")
    else:
        harmonized_bam_list_path = args.harmonized_bam_list_path

    if not args.scratch_dir:
        scratch_dir = os.path.dirname(harmonized_bam_list_path)
    else:
        scratch_dir = args.scratch_dir
    if not args.log_dir:
        log_dir = os.path.dirname(harmonized_bam_list_path)
    else:
        log_dir = args.log_dir
    thread_count = str(args.thread_count)
    if not args.eliminate_intermediate_files:
        eliminate_intermediate_files = True
    else:
        eliminate_intermediate_files = False
    if not args.md5:
        md5 = False
    else:
        md5 = True

    ##logging
    logging.basicConfig(
        filename=os.path.join(log_dir, 'Broad_cocleaning_' + uuid + '.log'),  # /host for docker
        level=args.level,
        filemode='a',
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d_%H:%M:%S_%Z',
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)
    logger.info('harmonized_bam_list_path=%s' % harmonized_bam_list_path)
    if not args.harmonized_bam_path:
        with open(harmonized_bam_list_path) as f:
            harmonized_bam_path = f.read().splitlines()
            for path in harmonized_bam_path:
                logger.info('harmonized_bam_path=%s' % path)
    else:
        for path in harmonized_bam_path:
            logger.info('harmonized_bam_path=%s' % path)

    engine_path = 'sqlite:///' + os.path.join(log_dir, uuid + '_Broad_cocleaning.db')
    engine = sqlalchemy.create_engine(engine_path, isolation_level='SERIALIZABLE')
    
    ##Pipeline
    #check .bai file, call samtools index if not exist
    RealignerTargetCreator.index(uuid, harmonized_bam_list_path, engine, logger)
    
    #call RealignerTargetCreator for harmonized bam list
    harmonized_bam_intervals_path = RealignerTargetCreator.RTC(uuid, harmonized_bam_list_path, thread_count, reference_fasta_name, known_1k_genome_indel_sites, engine, logger)
    
    #call IndelRealigner together but save the reads in the output coresponding to the input that the read came from.
    harmonized_IR_bam_list_path = IndelRealigner.IR(uuid, harmonized_bam_list_path, reference_fasta_name, known_1k_genome_indel_sites, harmonized_bam_intervals_path, engine, logger)
    
    #call BQSR table individually and apply it on bam
    Analysis_ready_bam_list_path = []
    for bam in harmonized_IR_bam_list_path:
        harmonized_IR_bam_BQSR_table_path = BaseRecalibrator.BQSR(uuid, bam, thread_count, reference_fasta_name, dbsnp_known_snp_sites, engine, logger)
        Analysis_ready_bam_path = PrintReads.PR(uuid, bam, thread_count, reference_fasta_name, harmonized_IR_bam_BQSR_table_path, engine, logger)
        bam_validate.bam_validate(uuid, Analysis_ready_bam_path, engine, logger)
        Analysis_ready_bam_list_path.append(Analysis_ready_bam_path)
    
    if md5:
        for bam in Analysis_ready_bam_list_path:
            bam_name = os.path.basename(bam)
            bam_dir = os.path.dirname(bam)
            bam_basename, bam_ext = os.path.splitext(bam_name)
            bai_name = bam_basename + '.bai'
            bai_path = os.path.join(bam_dir, bai_name)
            verify_util.store_md5_size(uuid, bam, engine, logger)
            verify_util.store_md5_size(uuid, bai_path, engine, logger)
    
    if eliminate_intermediate_files:
            pipe_util.remove_file_list(uuid, harmonized_IR_bam_list_path, engine, logger)
    
    for bam in Analysis_ready_bam_list_path:
        validate_file = bam_validate.bam_validate(uuid, bam, engine, logger)

if __name__ == '__main__':
    main()
