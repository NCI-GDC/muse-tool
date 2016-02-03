import os
import sys
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util
from cdis_pipe_utils import postgres
from tools.postgres import MuSE as MuSE

def sump_wgs(case_id, tumor_id, normal_id, muse_call_output_path, dbsnp_known_snp_sites, output_vcf, engine, logger):
    files = [normal_id, tumor_id]
    step_dir = os.path.join(os.getcwd(), 'sump')
    os.makedirs(step_dir, exist_ok=True)
    logger.info('muse_sump_dir=%s' % step_dir)
    muse_sump_output = output_vcf
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
        cmd = [muse_path, 'sump', '-I', muse_call_output_path, '-G', '-O', muse_sump_output_path, '-D', dbsnp_known_snp_sites]
        output = pipe_util.do_command(cmd, logger)
        metrics = time_util.parse_time(output)
        met = MuSE(case_id = case_id,
                    tool = 'muse_sump_wgs',
                    files=files,
                    systime=metrics['system_time'],
                    usertime=metrics['user_time'],
                    elapsed=metrics['wall_clock'],
                    cpu=metrics['percent_of_cpu'],
                    max_resident_time=metrics['maximum_resident_set_size'])

        postgres.create_table(engine, met)
        postgres.add_metrics(engine, met)
        pipe_util.create_already_step(step_dir, case_id + '_MuSE_sump', logger)
        logger.info('completed running `MuSE sump` of the tumor bam: %s' % input_name)
    return muse_sump_output_path
