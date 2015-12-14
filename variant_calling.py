#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import warnings
import sqlalchemy
import pipe_util
import df_util
import time_util
import index_util
import verify_util
import muse_call
import muse_sump

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
    parser.add_argument('-g', '--Whole_genome_squencing_data',
                        required = False,
                        action = 'store_true',
                        help = 'Whole genome squencing data',
    )
    parser.add_argument('-bs', '--Parallel_Block_Size',
                        type = is_nat,
                        default = 50000000,
                        help = 'Parallel Block Size',
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
    dbsnp_known_snp_sites = args.dbsnp_known_snp_sites
    uuid = args.uuid
    analysis_ready_tumor_bam_path = args.analysis_ready_tumor_bam_path
    analysis_ready_normal_bam_path = args.analysis_ready_normal_bam_path
    blocksize = args.Parallel_Block_Size
    if not args.scratch_dir:
        scratch_dir = os.path.dirname(analysis_ready_tumor_bam_path)
    else:
        scratch_dir = args.scratch_dir
    if not args.log_dir:
        log_dir = os.path.dirname(analysis_ready_tumor_bam_path)
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
        filename=os.path.join(log_dir, 'MuSE_variant_calling' + uuid + '.log'),  # /host for docker
        level=args.level,
        filemode='a',
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d_%H:%M:%S_%Z',
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)
    logger.info('analysis_ready_tumor_bam_path=%s' % analysis_ready_tumor_bam_path)
    logger.info('analysis_ready_normal_bam_path=%s' % analysis_ready_normal_bam_path)
    engine_path = 'sqlite:///' + os.path.join(log_dir, uuid + '_MuSE_variant_calling.db')
    engine = sqlalchemy.create_engine(engine_path, isolation_level='SERIALIZABLE')
    
    ##Pipeline
    #faidx reference fasta file if needed.
    fai_path = reference_fasta_name + '.fai'
    if os.path.isfile(fai_path):
      logger.info('reference_fai_path=%s' % fai_path)
    else:
      fai_path = index_util.samtools_faidx(uuid, reference_fasta_name, engine, logger)
      logger.info('reference_fai_path=%s' % fai_path)
    
    #index input bam files if needed.
    bam_path = []
    bam_path.extend([analysis_ready_tumor_bam_path, analysis_ready_normal_bam_path])
    for path in bam_path:
        bai_path = path + '.bai'
        if os.path.isfile(bai_path):
            logger.info('analysis_ready_bam_bai_path=%s' % bai_path)
        else:
            bai_path = index_util.samtools_bam_index(uuid, path, engine, logger)
            logger.info('analysis_ready_bam_bai_path=%s' % bai_path)
    
    #bgzip compress and tabix index dbsnp file if needed.
    dbsnp_name, dbsnp_ext = os.path.splitext(dbsnp_known_snp_sites)
    dbsnp_tabix_path = dbsnp_known_snp_sites + '.tbi'
    if dbsnp_ext == '.bgz':
        logger.info('dbsnp file is already bgzip compressed =%s' % dbsnp_known_snp_sites)
        if os.path.isfile(dbsnp_tabix_path):
            logger.info('tabix index of dbsnp_bgz file =%s' % dbsnp_tabix_path)
        else:
            dbsnp_tabix_path = index_util.tabix_index(uuid, dbsnp_known_snp_sites, engine, logger)
            logger.info('tabix index of dbsnp_bgz file =%s' % dbsnp_tabix_path)
    else:
        dbsnp_known_snp_sites = index_util.bgzip_compress(uuid, dbsnp_known_snp_sites, engine, logger)
        logger.info('dbsnp file is already bgzip compressed =%s' % dbsnp_known_snp_sites)
        dbsnp_tabix_path = index_util.tabix_index(uuid, dbsnp_known_snp_sites, engine, logger)
        logger.info('tabix index of dbsnp_bgz file =%s' % dbsnp_tabix_path)
        #sys.exit('!!!Reference dbSNP file should be bgzip compressed!!!')
    
    #MuSE call
    muse_call_output_path = muse_call.call(uuid, thread_count, analysis_ready_tumor_bam_path, analysis_ready_normal_bam_path, reference_fasta_name, fai_path, blocksize, engine, logger)

    #MuSE sump
    if not args.Whole_genome_squencing_data:
        muse_vcf = muse_sump.sump_wxs(uuid, muse_call_output_path, dbsnp_known_snp_sites, engine, logger)
    else:
        muse_vcf = muse_sump.sump_wgs(uuid, muse_call_output_path, dbsnp_known_snp_sites, engine, logger)
        
    #picard sortvcf
    muse_srt_vcf = index_util.picard_sortvcf(uuid, muse_vcf, reference_fasta_name, engine, logger)
    
    if eliminate_intermediate_files:
        pipe_util.remove_file_list(uuid, [muse_call_output_path], engine, logger)
        pipe_util.remove_file_list(uuid, [muse_vcf], engine, logger)
        
    if md5:
        verify_util.store_md5_size(uuid, muse_srt_vcf, engine, logger)

if __name__ == '__main__':
    main()
