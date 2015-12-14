import glob
import os
import logging
import sys
import df_util
import pipe_util
import time_util
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
        df['bai_path'] = bai_path
        unique_key_dict = {'uuid': uuid, 'bam_path': bam_path, 'bai_path': bai_path}
        table_name = 'time_mem_samtools_index'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(out_dir, bam_name + '_index', logger)
        logger.info('completed running `samtools index` of %s' % bam_path)
    return bai_path

def bgzip_compress(uuid, dbsnp_known_snp_sites, engine, logger):
    dbsnp_file = os.path.basename(dbsnp_known_snp_sites)
    dbsnp_bgz_path = dbsnp_known_snp_sites + '.bgz'
    out_dir = os.path.dirname(dbsnp_known_snp_sites)
    if pipe_util.already_step(out_dir, dbsnp_file + '_bgz', logger):
        logger.info('already completed step `bgzip compress of dbsnp.vcf` of %s' % dbsnp_known_snp_sites)
    else:
        logger.info('running step `bgzip compress of dbsnp.vcf` of %s' % dbsnp_known_snp_sites)
        cmd = [dbsnp_known_snp_sites, '|', 'bgzip', '>', dbsnp_bgz_path]
        shell_cmd = ' '.join(cmd)
        output = pipe_util.do_shell_command(shell_cmd, logger)
        df = time_util.store_time(uuid, shell_cmd, output, logger)
        df['dbsnp_vcf_path'] = dbsnp_known_snp_sites
        df['dbsnp_bgz_path'] = dbsnp_bgz_path
        unique_key_dict = {'uuid': uuid, 'dbsnp_vcf_path': dbsnp_known_snp_sites, 'dbsnp_bgz_path': dbsnp_bgz_path}
        table_name = 'time_mem_bgzip_compress_dbsnp_vcf'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(out_dir, dbsnp_file + '_bgz', logger)
        logger.info('completed running `bgzip compress of dbsnp.vcf` of %s' % dbsnp_known_snp_sites)
    return dbsnp_bgz_path
    
def tabix_index(uuid, dbsnp_known_snp_sites, engine, logger):
    dbsnp_file = os.path.basename(dbsnp_known_snp_sites)
    dbsnp_tbi_path = dbsnp_bgz_path + '.tbi'
    out_dir = os.path.dirname(dbsnp_known_snp_sites)
    if pipe_util.already_step(out_dir, dbsnp_file + '_tbi', logger):
        logger.info('already completed step `tbi index of dbsnp.vcf` of %s' % dbsnp_known_snp_sites)
    else:
        logger.info('running step `tbi index of dbsnp.vcf` of %s' % dbsnp_known_snp_sites)
        cmd = ['tabix', '-p', dbsnp_bgz_path]
        output = pipe_util.do_command(cmd, logger)
        df = time_util.store_time(uuid, cmd, output, logger)
        df['dbsnp_bgz_path'] = dbsnp_bgz_path
        df['dbsnp_tbi_path'] = dbsnp_tbi_path
        unique_key_dict = {'uuid': uuid, 'dbsnp_bgz_path': dbsnp_bgz_path, 'dbsnp_tbi_path': dbsnp_tbi_path}
        table_name = 'time_mem_tabix_index_dbsnp_bgz'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(out_dir, dbsnp_file + '_tbi', logger)
        logger.info('completed running `tbi index of dbsnp.vcf` of %s' % dbsnp_known_snp_sites)
    return dbsnp_tbi_path    
    
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
        df['fai_path'] = fai_path
        unique_key_dict = {'uuid': uuid, 'reference_fasta': reference_fasta_name, 'fai_path': fai_path}
        table_name = 'time_mem_samtools_faidx'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(out_dir, ref_file + '_faidx', logger)
        logger.info('completed running `samtools faidx` of %s' % reference_fasta_name)
        lines = open(fai_path).readlines()
        open(fai_path, 'w').writelines(lines[0:25])
    return fai_path

def picard_CreateSequenceDictionary(uuid, reference_fasta_name, engine, logger):
    sd_dir = os.path.dirname(reference_fasta_name)
    ref_name = os.path.basename(reference_fasta_name)
    ref_base, ref_ext = os.path.splitext(ref_name)
    sd_file = ref_base + '.dict'
    sd_file_path = os.path.join(sd_dir, sd_file)
    if pipe_util.already_step(sd_dir, ref_name + '_dict', logger):
        logger.info('already completed step `Picard CreateSequenceDictionary` of %s' % reference_fasta_name)
    else:
        logger.info('running step `Picard CreateSequenceDictionary` of %s' % reference_fasta_name)
        home_dir = os.path.expanduser('~')
        picard_path = os.path.join(home_dir, 'tools/picard-tools/picard.jar')
        cmd = ['java', '-d64', '-Xmx16G', '-jar', picard_path, 'CreateSequenceDictionary', 'R=' + reference_fasta_name, 'O=' + sd_file_path]
        output = pipe_util.do_command(cmd, logger)
        df = time_util.store_time(uuid, cmd, output, logger)
        df['reference_fasta'] = reference_fasta_name
        df['sequence_dictionary'] = sd_file_path
        unique_key_dict = {'uuid': uuid, 'reference_fasta': reference_fasta_name, 'sequence_dictionary': sd_file_path}
        table_name = 'time_mem_picard_CreateSequenceDictionary'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        logger.info('completed running step `Picard CreateSequenceDictionary` of %s' % reference_fasta_name)
        pipe_util.create_already_step(sd_dir, ref_name + '_dict', logger)
    return sd_file_path

def picard_sortvcf(uuid, muse_vcf, reference_fasta_name, engine, logger):
    sd_dir = os.path.dirname(reference_fasta_name)
    ref_name = os.path.basename(reference_fasta_name)
    ref_base, ref_ext = os.path.splitext(ref_name)
    sd_file = ref_base + '.dict'
    sd_file_path = os.path.join(sd_dir, sd_file)
    if os.path.isfile(sd_file_path):
        logger.info('reference_dict_path=%s' % sd_file_path)
    else:
        sd_file_path = picard_CreateSequenceDictionary(uuid, reference_fasta_name, engine, logger)
        logger.info('reference_dict_path=%s' % sd_file_path)
    srt_dir = os.path.dirname(muse_vcf)
    vcf_name = os.path.basename(muse_vcf)
    vcf_base, vcf_ext = os.path.splitext(vcf_name)
    srt_vcf = vcf_base + '.srt' + vcf_ext
    srt_vcf_path = os.path.join(srt_dir, srt_vcf)
    if pipe_util.already_step(srt_dir, vcf_name + '_sorted', logger):
        logger.info('already completed step `Picard SortVcf` of %s' % muse_vcf)
    else:
        logger.info('running step `Picard SortVcf` of %s' % muse_vcf)
        home_dir = os.path.expanduser('~')
        picard_path = os.path.join(home_dir, 'tools/picard-tools/picard.jar')
        cmd = ['java', '-d64', '-Xmx16G', '-jar', picard_path, 'SortVcf', 'I=' + muse_vcf, 'O=' + srt_vcf_path, 'SD=' + sd_file_path]
        output = pipe_util.do_command(cmd, logger)
        df = time_util.store_time(uuid, cmd, output, logger)
        df['MuSE_VCF'] = muse_vcf
        df['MuSE_sorted_VCF'] = srt_vcf_path
        unique_key_dict = {'uuid': uuid, 'MuSE_VCF': muse_vcf, 'MuSE_sorted_VCF': srt_vcf_path}
        table_name = 'time_mem_picard_SortVcf'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        logger.info('completed running step `Picard SortVcf` of %s' % muse_vcf)
        pipe_util.create_already_step(srt_dir, vcf_name + '_sorted', logger)
    return srt_vcf_path
