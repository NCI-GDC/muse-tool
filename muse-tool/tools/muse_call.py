import os
import sys
import string
from itertools import islice
from functools import partial
from multiprocessing.dummy import Pool, Lock
from cdis_pipe_utils import df_util
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util

def do_pool_commands(cmd, uuid, engine, logger, lock = Lock()):
    logger.info('running muse chunk call: %s' % cmd)
    output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output_stdout = output.communicate()[1]
    with lock:
        logger.info('contents of output=%s' % output_stdout.decode().format())
        df = time_util.store_time(uuid, cmd, output_stdout, logger)
        df['cmd'] = cmd
        unique_key_dict = {'uuid': uuid, 'cmd': cmd}
        table_name = 'time_mem_MuSE_chunk_call_processes'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        logger.info('completed muse chunk call: %s' % str(cmd))
    return output.wait()

def multi_commands(uuid, cmds, thread_count, engine, logger):
    pool = Pool(int(thread_count))
    output = pool.map(partial(do_pool_commands, uuid=uuid, engine=engine, logger=logger), cmds)
    return output

def fai_chunk(fai_path, blocksize):
  seq_map = {}
  with open(fai_path) as handle:
    head = list(islice(handle, 25))
    for line in head:
      tmp = line.split("\t")
      seq_map[tmp[0]] = int(tmp[1])
    for seq in seq_map:
        l = seq_map[seq]
        for i in range(1, l, blocksize):
            yield (seq, i, min(i+blocksize-1, l))

def muse_call_region_cmd_template(muse, ref, fai_path, blocksize, tumor_bam, normal_bam, output_base):
  template = string.Template("/usr/bin/time -v ${MUSE} call -f ${REF} -r ${REGION} ${TUMOR_BAM} ${NORMAL_BAM} -O ${OUTPUT_BASE}.${BLOCK_NUM}")
  for i, block in enumerate(fai_chunk(fai_path, blocksize)):
    cmd = template.substitute(
                              dict(
                                   REF = ref,
                                   REGION = '%s:%s-%s' % (block[0], block[1], block[2]),
                                   BLOCK_NUM = i),
                                   MUSE = muse,
                                   TUMOR_BAM = tumor_bam,
                                   NORMAL_BAM = normal_bam,
                                   OUTPUT_BASE = output_base
    )
    yield cmd, "%s.%s.MuSE.txt" % (output_base, i)

def call_region(uuid, thread_count, tumor_bam_path, normal_bam_path, reference_fasta_name, fai_path, blocksize, engine, logger):
  step_dir = os.path.join(os.getcwd(), 'call')
  logger.info('MuSE_call_dir=%s' % step_dir)
  if pipe_util.already_step(step_dir, tumor_bam_name + '_MuSE_call', logger):
    logger.info('already completed step `MuSE call by regions` of: %s' % analysis_ready_tumor_bam_path)
  else:
    logger.info('running step `MuSE call by regions` of the tumor bam: %s' % analysis_ready_tumor_bam_path)
    home_dir = os.path.expanduser('~')
    muse_path = os.path.join(home_dir, 'tools', 'MuSEv1.0rc_submission_c039ffa')
    cmds = list(muse_call_region_cmd_template(
                                   muse = muse_path,
                                   ref = reference_fasta_name,
                                   fai_path = fai_path,
                                   blocksize = blocksize,
                                   tumor_bam = tumor_bam_path,
                                   normal_bam = normal_bam_path,
                                   output_base = os.path.join(step_dir, 'output'))
    )
    outputs = multi_commands(uuid, list(a[0] for a in cmds), thread_count, engine, logger)
