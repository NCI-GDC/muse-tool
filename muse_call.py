import os
import sys
import string
import df_util
import pipe_util
import time_util

def fai_chunk(fai_path, blocksize):
  seq_map = {}
  with open(fai_path) as handle:
    for line in handle:
      tmp = line.split("\t")
      seq_map[tmp[0]] = int(tmp[1])
    for seq in seq_map:
        l = seq_map[seq]
        for i in range(1, l, blocksize):
            yield (seq, i, min(i+blocksize-1, l))

def muse_call_cmd_iter(muse, ref, fai_path, blocksize, tumor_bam, normal_bam, output_base):
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

def call(uuid, thread_count, analysis_ready_tumor_bam_path, analysis_ready_normal_bam_path, reference_fasta_name, fai_path, blocksize, engine, logger):
  call_dir = os.path.dirname(analysis_ready_tumor_bam_path)
  tumor_bam_name = os.path.basename(analysis_ready_tumor_bam_path)
  tb_base, tb_ext = os.path.splitext(tumor_bam_name)
  logger.info('MuSE_call_dir=%s' % call_dir)
  step_dir = call_dir
  muse_call_output = tb_base + '.MuSE.txt'
  muse_call_output_path = os.path.join(call_dir, muse_call_output)
  logger.info('muse_call_output_path=%s' % muse_call_output_path)
  if pipe_util.already_step(step_dir, tumor_bam_name + '_MuSE_call', logger):
    logger.info('already completed step `MuSE call` of: %s' % analysis_ready_tumor_bam_path)
  else:
    logger.info('running step `MuSE call` of the tumor bam: %s' % analysis_ready_tumor_bam_path)
    home_dir = os.path.expanduser('~')
    muse_path = os.path.join(home_dir, 'tools', 'MuSEv1.0rc_submission_c039ffa')
    cmds = list(muse_call_cmd_iter(
                                   muse = muse_path,
                                   ref = reference_fasta_name,
                                   fai_path = fai_path,
                                   blocksize = blocksize,
                                   tumor_bam = analysis_ready_tumor_bam_path,
                                   normal_bam = analysis_ready_normal_bam_path,
                                   output_base = os.path.join(step_dir, 'output.file'))
    )
    outputs = pipe_util.multi_commands(list(a[0] for a in cmds), thread_count)
    first = True
    merge_output = muse_call_output_path
    with open (merge_output, "w") as ohandle:
      for cmd, out in cmds:
        with open(out) as handle:
          for line in handle:
            if first or not line.startswith('#'):
              ohandle.write(line)
    pipe_util.create_already_step(step_dir, tumor_bam_name + '_MuSE_call', logger)
    logger.info('completed running step `MuSE call` of the tumor bam: %s' % analysis_ready_tumor_bam_path)
  return muse_call_output_path
