import os
import sys
import string
import subprocess
from itertools import islice
from functools import partial
from multiprocessing.dummy import Pool, Lock
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util
from cdis_pipe_utils import postgres
from tools.postgres import MuSE as MuSE

def do_pool_commands(cmd, case_id, engine, logger, files, lock = Lock()):
    logger.info('running muse chunk call: %s' % cmd)
    output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output_stdout = output.communicate()[1]
    with lock:
        logger.info('contents of output=%s' % output_stdout.decode().format())
        cmd_list = cmd.split()
        toolname = ('muse_call: %s' % cmd_list[7])
        metrics = time_util.parse_time(output_stdout)
        met = MuSE(case_id = case_id,
                    tool = toolname,
                    files=files,
                    systime=metrics['system_time'],
                    usertime=metrics['user_time'],
                    elapsed=metrics['wall_clock'],
                    cpu=metrics['percent_of_cpu'],
                    max_resident_time=metrics['maximum_resident_set_size'])

        postgres.create_table(engine, met)
        postgres.add_metrics(engine, met)
        logger.info('completed muse chunk call: %s' % str(cmd))
    return output.wait()

def multi_commands(case_id, cmds, thread_count, engine, files, logger):
    pool = Pool(int(thread_count))
    output = pool.map(partial(do_pool_commands, case_id=case_id, engine=engine, logger=logger, files=files), cmds)
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

def call_region(case_id, tumor_id, normal_id, thread_count, tumor_bam_path, normal_bam_path, reference_fasta_name, fai_path, blocksize, engine, logger):
  files = [normal_id, tumor_id]
  step_dir = os.path.join(os.getcwd(), 'call')
  os.makedirs(step_dir, exist_ok=True)
  tumor_bam_name = os.path.basename(tumor_bam_path)
  tb_base, tb_ext = os.path.splitext(tumor_bam_name)
  merge_dir = os.getcwd()
  os.makedirs(merge_dir, exist_ok=True)
  muse_call_output_path = os.path.join(merge_dir, tb_base) + '.MuSE.txt'
  logger.info('MuSE_call_dir=%s' % step_dir)
  if pipe_util.already_step(step_dir, case_id + '_MuSE_call', logger):
    logger.info('already completed step `MuSE call by regions` of: %s' % tumor_bam_path)
  else:
    logger.info('running step `MuSE call by regions` of the tumor bam: %s' % tumor_bam_path)
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
    outputs = multi_commands(case_id, list(a[0] for a in cmds), thread_count, engine, files, logger)
    first = True
    with open (muse_call_output_path, "w") as ohandle:
      for cmd, out in cmds:
        with open(out) as handle:
          for line in handle:
            if first or not line.startswith('#'):
              ohandle.write(line)
        first = False
    pipe_util.create_already_step(step_dir, case_id + '_MuSE_call', logger)
    logger.info('completed running step `MuSE call by regions` of the tumor bam: %s' % tumor_bam_path)
  return muse_call_output_path
