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
        logger.info('completed running `samtools index` of %s' % bam_path)
    return bai_path
