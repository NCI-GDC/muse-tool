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

def muse_call_cmd_template(muse, ref, fai_path, blocksize, tumor_bam, normal_bam, output_base):
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
    tmpdir = os.path.abspath(tempfile.mkdtemp(dir=step_dir, prefix="muse_tmp_"))
    cmds = list(muse_call_cmd_template(
                                   muse = muse_path,
                                   ref = reference_fasta_name,
                                   fai_path = fai_path,
                                   blocksize = blocksize,
                                   tumor_bam = analysis_ready_tumor_bam_path,
                                   normal_bam = analysis_ready_normal_bam_path,
                                   output_base = os.path.join(tmpdir, 'output.file'))
    ) 
    muse_call_log_file = tb_base + '.MuSE_call.log'
    muse_call_log_file_path = os.path.join(call_dir, muse_call_log_file)
    outputs = pipe_util.multi_commands(list(a[0] for a in cmds), thread_count, muse_call_log_file_path)
    with open(muse_call_log_file_path, 'a') as the_file:
      the_file.write(outputs)
    merge_output = muse_call_output_path
    first = True
    with open (merge_output, "w") as ohandle:
      for cmd, out in cmds:
        with open(out) as handle:
          for line in handle:
            if first or not line.startswith('#'):
              ohandle.write(line)
        first = False
    df=pd.DataFrame({'uuid': [uuid], 'muse_call_timeusage': [timeusage], 'analysis_ready_tumor_bam_path': [analysis_ready_tumor_bam_path], 'muse_call_output': [muse_call_output_path]})
    df['analysis_ready_tumor_bam_path'] = analysis_ready_tumor_bam_path
    df['muse_call_timeusage'] = timeusage
    df['muse_call_output'] = muse_call_output_path
    unique_key_dict = {'uuid': uuid, 'muse_call_timeusage': timeusage, 'analysis_ready_tumor_bam_path': analysis_ready_tumor_bam_path, 'muse_call_output': muse_call_output_path}
    table_name = 'time_mem_MuSE_call'
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    pipe_util.create_already_step(step_dir, tumor_bam_name + '_MuSE_call', logger)
    logger.info('completed running step `MuSE call` of the tumor bam: %s' % analysis_ready_tumor_bam_path)
    shutil.rmtree(tmpdir)
  return muse_call_output_path
