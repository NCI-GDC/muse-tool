import os
import sys
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util
from cdis_pipe_utils import postgres
from tools.postgres import MuSE as MuSE

def sump_wgs(case_id, tumor_id, normal_id, muse_call_output_path, dbsnp_known_snp_sites, output_vcf, engine, logger):
    files = [normal_id, tumor_id]
    step_dir = os.getcwd()
    os.makedirs(step_dir, exist_ok=True)
    output_vcf_path = os.path.join(step_dir, output_vcf)
    logger.info('muse_sump_output_path=%s' % output_vcf_path)
    if pipe_util.already_step(step_dir, case_id + '_MuSE_sump', logger):
        logger.info('already completed step `MuSE sump` of: %s' % muse_call_output_path)
    else:
        logger.info('running step `MuSE sump` of the tumor bam: %s' % muse_call_output_path)
        home_dir = os.path.expanduser('~')
        muse_path = os.path.join(home_dir, 'tools', 'MuSEv1.0rc_submission_c039ffa')
        cmd = [muse_path, 'sump', '-I', muse_call_output_path, '-G', '-O', output_vcf_path, '-D', dbsnp_known_snp_sites]
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
        logger.info('completed running `MuSE sump` of the tumor bam: %s' % muse_call_output_path)
    return output_vcf_path
