import ast
import json
import logging
import os
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker


def delete_record_from_table(unique_key_dict, table_name, engine, logger):
    unique_key_list = sorted(unique_key_dict.keys())
    first_where = True
    statement = 'DELETE FROM ' + table_name + ' WHERE'
    for unique_key in unique_key_list:
        if first_where:
            statement += ' ' + '\"' + unique_key + '\"' + '=' + '\"' + unique_key_dict[unique_key] + '\"'
            first_where = False
        else:
            statement += ' AND ' + '\"' + unique_key + '\"' + '=' + '\"' + unique_key_dict[unique_key] + '\"'
    statement += ';'
    sql = sqlalchemy.sql.text(statement)
    conn = engine.connect()
    trans = conn.begin()
    try:
        result = conn.execute(sql)
        trans.commit()
        conn.close()
        logger.info('result=%s' % result)
    except Exception as e:
        trans.rollback()
        conn.close()
        logger.debug('exception: %s' % e)
        sys.exit(1)

def get_json_data(json_path, logger):
    logger.info('get_json_data() json_path=%s' % json_path)
    with open(json_path, 'r') as json_open:
        logger.info('json_open=%s' % json_open)
        json_data = json.load(json_open)
    logger.info('get_json_data() json_data=%s' % json_data)
    return json_data

def append_json(json_data, df, logger):
    new_index = len(json_data)
    logger.info('append_json() new_index=%s' % new_index)
    df_json = df.to_json(orient='index')
    df_json = df_json.replace('null','"null"')
    df_dict = ast.literal_eval(df_json)
    logger.info('append_json df_dict=%s' % df_dict)
    logger.info('type(df_dict)=%s' % type(df_dict))
    logger.info('len(df_dict)=%s' % str(len(df_dict)))
    json_data[new_index] = df_dict['0']
    logger.info('append_json() json_data=%s' % json_data)
    return json_data


def save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger):
    logger.info('df=%s\n' % df)
    uuid = df['uuid'][0]
    logger.info('uuid=%s' % uuid)
    logger.info('type(uuid)=%s' % type(uuid))
    if engine.has_table(table_name):  # table already exists
        logger.info('table %s already exists' % table_name)

        delete_record_from_table(unique_key_dict, table_name, engine, logger)

        try:
            logger.info('writing sql to existing table: %s' % table_name)
            df.to_sql(table_name, engine, if_exists='append')
            logger.info('wrote sql to existing table: %s' % table_name)
        except Exception as e:
            logger.debug('exception: %s' % e)
            sys.exit(1)
    else:  # first creation of table
        logger.info('table %s does not yet exist' % table_name)
        try:
            df.to_sql(table_name, engine, if_exists='fail')
            logger.info('wrote sql to table: %s' % table_name)
        except Exception as e:
            logger.debug('exception: %s' % e)
            sys.exit(1)
