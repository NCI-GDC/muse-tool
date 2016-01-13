import os
import sys
from cdis_pipe_utils import df_util
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util

def merge_output(uuid, tumor_bam_path, muse_call_output_list, engine, logger):
    step_dir = os.path.join(os.getcwd(), 'merge')
    os.makedirs(step_dir, exist_ok=True)
    logger.info('MuSE_merge_dir=%s' % step_dir)
    if pipe_util.already_step(step_dir, uuid + '_MuSE_merge', logger):
        logger.info('already completed step `MuSE merge outputs` of: %s' % uuid)
    else:
        logger.info('running step `MuSE merge outputs` of: %s' % uuid)
        input_name = os.path.basename(tumor_bam_path)
        sample_base, sample_ext = os.path.splitext(input_name)
        merge_output = os.path.join(step_dir, sample_base) + '.MuSE.txt'
        first = True
        with open (merge_output, "w") as ohandle:
            for out in muse_call_output_list:
                with open(out) as handle:
                    for line in handle:
                        if first or not line.startswith('#'):
                            ohandle.write(line)
                first = False
        pipe_util.create_already_step(step_dir, uuid + '_MuSE_merge', logger)
        logger.info('completed running step `MuSE merge outputs` of: %s' % uuid)
    return merge_output
