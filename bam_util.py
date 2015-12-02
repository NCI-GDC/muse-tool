import glob
import os
import logging
import sys
import df_util
import pipe_util
import time_util
import pysam
import pandas as pd

def samtools_bam_index(uuid, bam_path, engine, logger):
    bam_file = os.path.basename(bam_path)
    bam_name, bam_ext = os.path.splitext(bam_file)
    out_dir = os.path.dirname(bam_path)
    bai_path = bam_path + '.bai'
    if pipe_util.already_step(out_dir, bam_name + '_index', logger):
        logger.info('already completed step `samtools index` of %s' % bam_path)
    else:
        logger.info('running step `samtools index` of %s' % bam_path)
        cmd = ['samtools', 'index', bam_path]
        output = pipe_util.do_command(cmd, logger)
        df = time_util.store_time(uuid, cmd, output, logger)
        df['bam_path'] = bam_path
        unique_key_dict = {'uuid': uuid, 'bam_path': bam_path}
        table_name = 'time_mem_samtools_index'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(out_dir, bam_name + '_index', logger)
        logger.info('completed running `samtools index` of %s' % bam_path)
    return bai_path
    
def samtools_faidx(uuid, reference_fasta_name, engine, logger):
    ref_file = os.path.basename(reference_fasta_name)
    fai_path = reference_fasta_name + '.fai'
    out_dir = os.path.dirname(reference_fasta_name)
    if pipe_util.already_step(out_dir, ref_file + '_faidx', logger):
        logger.info('already completed step `samtools faidx` of %s' % reference_fasta_name)
    else:
        logger.info('running step `samtools faidx` of %s' % reference_fasta_name)
        cmd = ['samtools', 'faidx', reference_fasta_name]
        output = pipe_util.do_command(cmd, logger)
        df = time_util.store_time(uuid, cmd, output, logger)
        df['reference_fasta'] = reference_fasta_name
        unique_key_dict = {'uuid': uuid, 'reference_fasta': reference_fasta_name}
        table_name = 'time_mem_samtools_faidx'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(out_dir, ref_file + '_faidx', logger)
        logger.info('completed running `samtools faidx` of %s' % reference_fasta_name)
    return fai_path
