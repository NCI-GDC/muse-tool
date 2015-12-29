import os
import sys
import string
import tempfile
import time
import shutil
import df_util
import pipe_util
import time_util
import pandas as pd
from itertools import islice

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

def call_region(uuid, thread_count, analysis_ready_tumor_bam_path, analysis_ready_normal_bam_path, reference_fasta_name, fai_path, blocksize, engine, logger):
  call_dir = os.path.dirname(analysis_ready_tumor_bam_path)
  tumor_bam_name = os.path.basename(analysis_ready_tumor_bam_path)
  tb_base, tb_ext = os.path.splitext(tumor_bam_name)
  logger.info('MuSE_call_dir=%s' % call_dir)
  step_dir = call_dir
  muse_call_output = tb_base + '.MuSE.txt'
  muse_call_output_path = os.path.join(call_dir, muse_call_output)
  logger.info('muse_call_output_path=%s' % muse_call_output_path)
  if pipe_util.already_step(step_dir, tumor_bam_name + '_MuSE_call', logger):
    logger.info('already completed step `MuSE call by regions` of: %s' % analysis_ready_tumor_bam_path)
  else:
    logger.info('running step `MuSE call by regions` of the tumor bam: %s' % analysis_ready_tumor_bam_path)
    home_dir = os.path.expanduser('~')
    muse_path = os.path.join(home_dir, 'tools', 'MuSEv1.0rc_submission_c039ffa')
    tmpdir = os.path.abspath(tempfile.mkdtemp(dir=step_dir, prefix="muse_tmp_"))
    cmds = list(muse_call_region_cmd_template(
                                   muse = muse_path,
                                   ref = reference_fasta_name,
                                   fai_path = fai_path,
                                   blocksize = blocksize,
                                   tumor_bam = analysis_ready_tumor_bam_path,
                                   normal_bam = analysis_ready_normal_bam_path,
                                   output_base = os.path.join(tmpdir, 'output.file'))
    ) 
    outputs = pipe_util.multi_commands(uuid, list(a[0] for a in cmds), thread_count, engine, logger)
    merge_output = muse_call_output_path
    first = True
    with open (merge_output, "w") as ohandle:
      for cmd, out in cmds:
        with open(out) as handle:
          for line in handle:
            if first or not line.startswith('#'):
              ohandle.write(line)
        first = False
    logger.info('completed running step `MuSE call by regions` of the tumor bam: %s' % analysis_ready_tumor_bam_path)
    shutil.rmtree(tmpdir)
  return muse_call_output_path

def chunkify(it, n):
  return [it[i::n] for i in range(n)]

def fai_regions(fai_path):
  seq_map = {}
  with open(fai_path) as handle:
    head = list(islice(handle, 25))
    for line in head:
      tmp = line.split("\t")
      seq_map[tmp[0]] = int(tmp[1])
    for seq in seq_map:
        l = seq_map[seq]
        for i in range(1, l, l):
            yield (seq, i, l)

def make_fai_list(fai_path, thread_count):
  fai_dir = os.path.dirname(fai_path)
  list_of_regions = []
  for i, block in enumerate(fai_regions(fai_path)):
    list_of_regions.append('%s:%s-%s' % (block[0], block[1], block[2]))
  fai_list_path = []
  for i in range(thread_count):
    fai_list_path.append(os.path.join(fai_dir, 'ref_list_of_regions_'+str(i)+ '.txt'))
  for item, filename in zip(chunkify(list_of_regions, thread_count), fai_list_path):
    with open(filename, 'w') as output:
        output.writelines('\n'.join(item))
  return fai_list_path
  
def muse_call_list_cmd_template(muse, ref, fai_path, tumor_bam, normal_bam, output_base, thread_count):
  template = string.Template("/usr/bin/time -v ${MUSE} call -f ${REF} -l ${REGION} ${TUMOR_BAM} ${NORMAL_BAM} -O ${OUTPUT_BASE}.${REGION_NUM}")
  for i, block in enumerate(make_fai_list(fai_path, int(thread_count))):
    cmd = template.substitute(
                              dict(
                                   REF = ref,
                                   REGION = block,
                                   BLOCK_NUM = i),
                                   MUSE = muse,
                                   TUMOR_BAM = tumor_bam,
                                   NORMAL_BAM = normal_bam,
                                   OUTPUT_BASE = output_base
    )
    yield cmd, "%s.%s.MuSE.txt" % (output_base, i)

def call_list(uuid, thread_count, analysis_ready_tumor_bam_path, analysis_ready_normal_bam_path, reference_fasta_name, fai_path, engine, logger):
  call_dir = os.path.dirname(analysis_ready_tumor_bam_path)
  tumor_bam_name = os.path.basename(analysis_ready_tumor_bam_path)
  tb_base, tb_ext = os.path.splitext(tumor_bam_name)
  logger.info('MuSE_call_dir=%s' % call_dir)
  step_dir = call_dir
  muse_call_output = tb_base + '.MuSE.txt'
  muse_call_output_path = os.path.join(call_dir, muse_call_output)
  logger.info('muse_call_output_path=%s' % muse_call_output_path)
  if pipe_util.already_step(step_dir, tumor_bam_name + '_MuSE_call', logger):
    logger.info('already completed step `MuSE call by lists` of: %s' % analysis_ready_tumor_bam_path)
  else:
    logger.info('running step `MuSE call by lists` of the tumor bam: %s' % analysis_ready_tumor_bam_path)
    home_dir = os.path.expanduser('~')
    muse_path = os.path.join(home_dir, 'tools', 'MuSEv1.0rc_submission_c039ffa')
    tmpdir = os.path.abspath(tempfile.mkdtemp(dir=step_dir, prefix="muse_tmp_"))
    cmds = list(muse_call_list_cmd_template(
                                   muse = muse_path,
                                   ref = reference_fasta_name,
                                   fai_path = fai_path,
                                   tumor_bam = analysis_ready_tumor_bam_path,
                                   normal_bam = analysis_ready_normal_bam_path,
                                   output_base = os.path.join(tmpdir, 'output.file'),
                                   thread_count = thread_count)
    ) 
    outputs = pipe_util.multi_commands(uuid, list(a[0] for a in cmds), thread_count, engine, logger)
    merge_output = muse_call_output_path
    first = True
    with open (merge_output, "w") as ohandle:
      for cmd, out in cmds:
        with open(out) as handle:
          for line in handle:
            if first or not line.startswith('#'):
              ohandle.write(line)
        first = False
    logger.info('completed running step `MuSE call by lists` of the tumor bam: %s' % analysis_ready_tumor_bam_path)
    shutil.rmtree(tmpdir)
  return muse_call_output_path
