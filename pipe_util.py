import logging
import os
import shutil
import subprocess
import sys
import df_util
import time_util
from multiprocessing import Pool
from itertools import repeat

def update_env(logger):
    env = dict()
    env.update(os.environ)
    path = env['PATH']
    logger.info('path=%s' % path)
    home_dir = os.path.expanduser('~')
    new_path = path
    new_path += ':' + os.path.join(home_dir, 'tools', 'MuSEv1.0rc_submission_c039ffa')
    pipe_dir=os.path.dirname(os.path.realpath(sys.argv[0]))
    new_path = path
    new_path+=':'+pipe_dir
    logger.info('new_path=%s' % new_path)
    env['PATH']=new_path
    return env

def do_command(cmd, logger, stdout=subprocess.STDOUT, stderr=subprocess.PIPE, allow_fail=False):
    env = update_env(logger)
    timecmd = cmd
    timecmd.insert(0, '/usr/bin/time')
    timecmd.insert(1, '-v')
    logger.info('running cmd: %s' % timecmd)

    output = b''
    try:
        output = subprocess.check_output(timecmd, env=env, stderr=subprocess.STDOUT)
    except Exception as e:
        output = e.output
        sys.stdout.buffer.write(output)
        logger.debug('failed cmd: %s' % str(timecmd))
        logger.debug('exception: %s' % e)
        if allow_fail:
            if 'ValidateSamFile' in cmd:
                return e.output
            else:
                return None
        else:
            sys.exit('failed cmd: %s' % str(timecmd))
    finally:
        logger.info('contents of output(s)=%s' % output.decode().format())
    logger.info('completed cmd: %s' % str(timecmd))
    return output


def do_shell_command(cmd, logger, stdout=subprocess.STDOUT, stderr=subprocess.PIPE):
    env = update_env(logger)
    timecmd = '/usr/bin/time -v ' + cmd
    logger.info('running cmd: %s' % timecmd)
    try:
        output = subprocess.check_output(timecmd, env=env, stderr=subprocess.STDOUT, shell=True)
        logger.info('contents of output(s)=%s' % output.decode().format())
    except Exception as e:
        logger.debug('failed cmd: %s' % str(timecmd))
        logger.debug(e.output)
        logger.debug('exception: %s' % e)
        sys.exit('failed cmd: %s' % str(timecmd))
    logger.info('completed cmd: %s' % str(timecmd))
    return output


def do_stdout_command(cmd, logger, stdout=subprocess.STDOUT):
    output = str()
    env = update_env(logger)
    timecmd = cmd
    timecmd.insert(0, '/usr/bin/time')
    timecmd.insert(1, '-v')
    logger.info('running cmd: %s' % timecmd)
    if stdout is not subprocess.STDOUT:
        stdout_open = open(stdout, 'wb')
    try:
        with subprocess.Popen(timecmd, stdout=stdout_open, stderr=subprocess.PIPE, env=env) as proc:
            logger.info(proc.stderr.read().decode().format())
            output = proc.stderr.read()
    except Exception as e:
        logger.debug('failed cmd: %s' % str(timecmd))
        logger.debug(e.output)
        logger.debug('exception: %s' % str(e))
        sys.exit('failed cmd: %s' % str(timecmd))
    logger.info('completed cmd: %s' % str(timecmd))
    return output


def do_piped_commands(cmdlist, logger):
    env = update_env(logger)
    popen_list = list()
    first_flag = True
    timecmdlist = cmdlist
    i = 0
    logger.info('running piped cmds: %s' % cmdlist)
    for timecmd in timecmdlist:
        try:
            logger.info('create piped command ' + str(i) + ':' + str(timecmd))
            prev_pipe = popen_list[i - 1]
            popen_list.append(subprocess.Popen(timecmd, stdin=prev_pipe.stdout, env=env))  # stderr=subprocess.STDOUT,
            i += 1
        except Exception as e:
            logger.debug('failed cmd: %s' % str(timecmd))
            #logger.debug(e.output)
            logger.debug('exception: %s' % e)
            sys.exit('failed cmd: %s' % str(timecmd))
    for popencmd in popen_list[:-1]:
        popencmd.stdout.close()
    output = popen_list[-1].communicate()[0]
    logger.info('completed piped cmds: %s' % str(timecmdlist))
    logger.info('contents of piped output=%s' % output)
    output_1 = popen_list[-1].communicate()[1]
    logger.info('output_1=%s' % output_1)
    return output

                
def get_dirlevels(adir):
    dir_split = adir.split('/')
    return len(dir_split)


def remove_frontdir(adir):
    dir_split = adir.split('/')
    dir_join = '/'.join(dir_split[1:])
    return dir_join


def is_file(name, logger):
    if len(os.path.splitext(name)[1]) > 0:
        logger.info('is_file for %s is True' % name)
        return True
    else:
        logger.info('is_file for %s is False' % name)
        return False


def already_have(destination, name, logger):
    logger.info('destination=%s' % destination)
    logger.info('name=%s' % name)
    if is_file(name, logger):
        logger.info('%s is a file' % name)
        out_path = destination
    else:
        logger.info('%s is a directory' % name)
        outdir = name.split('/')[-1]
        out_path = os.path.join(destination, outdir)
    rename = '_'.join(name.split('/'))
    have_flag = os.path.join(out_path, 'have_' + rename)
    if os.path.exists(have_flag):
        logger.info('do have flag: %s' % have_flag)
        return True
    else:
        logger.info('do not have flag: %s' % have_flag)
        return False


def touch(fname, logger, mode=0o666, dir_fd=None, **kwargs):
    logger.info('creating empty file: %s' % fname)
    flags = os.O_CREAT | os.O_APPEND
    with os.fdopen(os.open(fname, flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        os.utime(f.fileno() if os.utime in os.supports_fd else fname,
                 dir_fd=None if os.supports_fd else dir_fd, **kwargs)


def create_have(destination, name, logger):
    if is_file(name, logger):
        base_path = destination
    else:
        outdir = name.split('/')[-1]
        base_path = os.path.join(destination, outdir)
    rename = '_'.join(name.split('/'))
    have_flag = os.path.join(base_path, 'have_' + rename)
    logger.info('creating have flag: %s' % have_flag)
    touch(have_flag, logger)


def already_step(step_dir, step, logger):
    have_step_flag = os.path.join(step_dir, 'have_' + step)
    if os.path.exists(have_step_flag):
        logger.info('step flag exists: %s' % have_step_flag)
        return True
    else:
        logger.info('step flag does not exist: %s' % have_step_flag)
        return False


def create_already_step(step_dir, step, logger):
    have_step_flag = os.path.join(step_dir, 'have_' + step)
    touch(have_step_flag, logger)

    
def get_uuid_from_path(bam_analysis_id):
    dirs = bam_analysis_id.split('/')
    for adir in dirs:
        if len(adir) is 36:
            return adir
    logging.debug('path %s does not contain a uuid' % bam_analysis_id)
    sys.exit(1)


def cleanup(uuid, scratch_dir, logger):
    uuid_dir = os.path.join(scratch_dir, uuid)
    shutil.rmtree(uuid_dir)

def remove_file_list(uuid, file_path_list, engine, logger):
    for file_path in file_path_list:
        logger.info('checking to remove file: %s' % file_path)
        if os.path.isfile(file_path):
            logger.info('removing file: %s' % file_path)
            os.remove(file_path)
            logger.info('removed file: %s' % file_path)
    
def remove_dir(adir, engine, logger):
    logger.info('removing directory: %s' % adir)
    shutil.rmtree(adir)
    logger.info('removed directory: %s' % adir)

def do_pool_commands(cmd):
    output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = output.communicate()
    return output.returncode

def initializer(handle):
    global handler
    handler = handle

def multi_commands(cmds, thread_count, logger):
    handler = logging.FileHandler( 'logFile' )
    p = Pool(int(thread_count), initializer=initializer, initargs=(handler,))
    output = p.map(do_pool_commands, cmds)
    return output
