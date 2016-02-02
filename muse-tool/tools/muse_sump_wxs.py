import os
import sys
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util
from cdis_pipe_utils import postgres

def sump_wxs(case_id, tumor_id, normal_id, muse_call_output_path, dbsnp_known_snp_sites, engine, logger):
    files = [normal_id, tumor_id]
    step_dir = os.path.join(os.getcwd(), 'sump')
    os.makedirs(step_dir, exist_ok=True)
    logger.info('muse_sump_dir=%s' % step_dir)
    input_name = os.path.basename(muse_call_output_path)
    input_base, input_ext = os.path.splitext(input_name)
    muse_sump_output = input_base + '.vcf'
    output_dir = os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    muse_sump_output_path = os.path.join(output_dir, muse_sump_output)
    logger.info('muse_sump_output_path=%s' % muse_sump_output_path)
    if pipe_util.already_step(step_dir, case_id + '_MuSE_sump', logger):
        logger.info('already completed step `MuSE sump` of: %s' % input_name)
    else:
        logger.info('running step `MuSE sump` of the tumor bam: %s' % input_name)
        home_dir = os.path.expanduser('~')
        muse_path = os.path.join(home_dir, 'tools', 'MuSEv1.0rc_submission_c039ffa')
        cmd = [muse_path, 'sump', '-I', muse_call_output_path, '-E', '-O', muse_sump_output_path, '-D', dbsnp_known_snp_sites]
        output = pipe_util.do_command(cmd, logger)
        metrics = time_util.parse_time(output)
        postgres.add_metrics(engine, 'muse_sump_wxs', case_id, files, metrics, logger)
        pipe_util.create_already_step(step_dir, case_id + '_MuSE_sump', logger)
        logger.info('completed running `MuSE sump` of the tumor bam: %s' % input_name)
    return muse_sump_output_path
