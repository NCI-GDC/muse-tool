import logging
import os
import sys
import xml.etree.ElementTree
#
import pandas as pd
#
import df_util
import pipe_util
import time_util


def pull_cgquery_xml_to_file(uuid, outputxml, logger):
    file_dir = os.path.dirname(outputxml)
    if pipe_util.already_step(file_dir, 'cgquery_xml', logger):
        logger.info('already completed step `cgquery` of: %s' % uuid)
        return
    else:
        logger.info('running command `cgquery` of: %s' % uuid)
        cmd = ['cgquery', '-a', 'analysis_id=' + uuid, '-o', outputxml]
        output = pipe_util.do_command(cmd, logger)
        pipe_util.create_already_step(file_dir, 'cgquery_xml', logger)
        return


def get_cgquery_xml_to_size_md5_dict(file_path, logger):
    logger.info('file to md5: %s' % file_path)
    file_dir = os.path.dirname(file_path)
    logger.info('file_dir=%s' % file_dir)
    file_name = os.path.basename(file_path)
    logger.info('file_name=%s' % file_name)
    uuid = pipe_util.get_uuid_from_path(file_path)
    logger.info('file uuid: %s' % uuid)
    outputxml = os.path.join(file_dir, 'cgquery.xml')
    pull_cgquery_xml_to_file(uuid, outputxml, logger)
    tree = xml.etree.ElementTree.parse(outputxml)
    root = tree.getroot()
    logger.info('root.getchildren()=%s' % root.getchildren())
    result = root.find('Result')
    logger.info('result.getchildren()=%s' % result.getchildren())
    files = result.find('files')
    size_md5_dict = dict()
    for queryfile in files:
        query_filename = queryfile.find('filename').text
        query_filesize = queryfile.find('filesize').text
        query_checksum = queryfile.find('checksum').text
        logger.info('query_filename=%s' % query_filename)
        if query_filename == file_name:
            size_md5_dict['filesize'] = query_filesize
            size_md5_dict['checksum'] = query_checksum
            return size_md5_dict
    logger.debug('%s not found in cgquery' % file_name)
    sys.exit(1)
    return


def get_s3_md5(s3_bucket, s3_object, logger):
    s3_path = os.path.join('s3://', s3_bucket, analysis_id, s3_object)
    cmd = ['s3cmd', 'info', s3_path]
    output = pipe_util.do_command(cmd, logger)
    s3_md5 = str()
    for line in output:
        if 'MD5' in line:
            md5_s3 = line.split(':')[1].strip()
            return md5_s3
    return None


def verify_size(uuid, file_path, cgquery_size_md5_dict, engine, logger):
    filesize = get_file_size(uuid, file_path, engine, logger)
    logger.info('query_filesize=%s' % cgquery_size_md5_dict['filesize'])
    if filesize == cgquery_size_md5_dict['filesize']:
        logger.info('the file size is correct: %s' % filesize)
        return int(filesize)
    else:
        logger.error('the file size %s does not match the cgquery size %s.' % (filesize, cgquery_size_md5_dict['filesize']))
        sys.exit(1)
    return None


def get_file_size(uuid, file_path, engine, logger):
    cmd = ['ls', '-l', file_path]
    output = pipe_util.do_command(cmd, logger)
    filesize = output.split()[4].decode()
    logger.info('%s filesize=%s' % (file_path, filesize))
    return filesize


def get_file_md5(uuid, file_path, engine, logger):
    file_dir = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    file_shortname, file_ext = os.path.splitext(file_name)
    file_md5_name = file_name + '.md5'
    file_md5_path = os.path.join(file_dir, file_md5_name)
    if pipe_util.already_step(file_dir, file_name + '_md5sum', logger):
        logger.info('already completed step `md5sum` of: %s' % file_path)
        with open(file_md5_path, 'r') as file_md5_path_open:
            file_md5 = file_md5_path_open.readline().strip()
            return file_md5
    else:
        cmd = ['md5sum', file_path]
        output = pipe_util.do_command(cmd, logger)
        file_md5 = output.split()[0].decode()
        file_md5_path_open = open(file_md5_path, 'w')
        file_md5_path_open.write(file_md5)
        file_md5_path_open.close()
        df = time_util.store_time(uuid, cmd, output, logger)
        df['file_path'] = file_path
        logger.info('df=%s' % df)
        unique_key_dict = {'uuid': uuid, 'file_path': file_path}
        table_name = 'time_mem_md5'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(file_dir, file_name + '_md5sum', logger)
        return file_md5
    return None


def verify_md5(uuid, file_path, cgquery_size_md5_dict, engine, logger):
    file_dir = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    file_shortname, file_ext = os.path.splitext(file_name)
    file_md5 = get_file_md5(uuid, file_path, engine, logger)
    assert (file_md5 is not None)
    if file_md5 == cgquery_size_md5_dict['checksum']:
        logger.info('the md5 is correct: %s' % file_md5)
        return file_md5
    else:
        logger.error('the file md5(%s) does not match the s3 md5(%s)' % (file_checksum, cgquery_size_md5_dict['checksum']))
        sys.exit(1)
    return


def store_md5_size(uuid, file_path, engine, logger):
    file_dir = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    if pipe_util.already_step(file_dir, file_name + '_store_md5_size', logger):
        logger.info('already_completed step store md5_size of: %s' % file_path)
    else:
        logger.info('running step store md5_size of: %s' % file_path)
        file_md5 = get_file_md5(uuid, file_path, engine, logger)
        file_size = get_file_size(uuid, file_path, engine, logger)
        df = pd.DataFrame({'uuid': [uuid],
                         'file_path': file_path,
                         'file_size': file_size,
                         'file_md5': file_md5})
        logger.info('df=%s' % df)
        table_name = 'file_size_md5'
        unique_key_dict = {'uuid': uuid, 'file_path': file_path}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(file_dir, file_name + '_store_md5_size', logger)
    return


def verify_cgquery(uuid, file_path, engine, logger):
    file_dir = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    file_shortname, file_ext = os.path.splitext(file_name)
    if pipe_util.already_step(file_dir, file_name + '_verify_cgquery', logger):
        logger.info('already completed step `verify_cgquery` of: %s' % file_path)
    else:
        cgquery_size_md5_dict = get_cgquery_xml_to_size_md5_dict(file_path, logger)
        file_size = verify_size(uuid, file_path, cgquery_size_md5_dict, engine, logger)
        file_md5 = verify_md5(uuid, file_path, cgquery_size_md5_dict, engine, logger)
        assert (file_size is not None)
        assert (file_md5 is not None)

        #store size/md5 results to db. uuid+file_path are unique key
        df = pd.DataFrame({'uuid': [uuid],
                         'file_path': file_path,
                         'file_size': file_size,
                         'file_md5': file_md5})  # ,index=['uuid','file_path'])
        logger.info('df=%s' % df)
        table_name = 'file_size_md5'
        unique_key_dict = {'uuid': uuid, 'file_path': file_path}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(file_dir, file_name + '_verify_cgquery', logger)
    return
