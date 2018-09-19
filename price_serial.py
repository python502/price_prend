#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/10 10:38
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : price_serial.py
# @Software: PyCharm
# @Desc    :
import os
import re
import logging
import logging.handlers
import pandas as pd
import shutil

from datetime import datetime
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
ORIGINAL_CSV_FILE = 'original.csv'
CSV_FILE = 'price_serial.csv'
S = 'date'
SPLICE = '^'
COLUMN = [S, 'app', 'type', 'geo', 'payout', 'tier', 'descr']
LEN_COLUMN = len(COLUMN)-1
format_dict = {
    logging.DEBUG: logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'),
    logging.INFO: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.WARNING: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.ERROR: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s'),
    logging.CRITICAL: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
}

class Logger():
    __cur_logger = logging.getLogger()
    def __init__(self,loglevel):
        #set name and loglevel
        new_logger = logging.getLogger(__name__)
        new_logger.setLevel(loglevel)
        formatter = format_dict[loglevel]
        filehandler = logging.handlers.RotatingFileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'price_serial.log'), mode='w')
        filehandler.setFormatter(formatter)
        new_logger.addHandler(filehandler)
        #create handle for stdout
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)
        #add handle to new_logger
        new_logger.addHandler(streamhandler)
        Logger.__cur_logger = new_logger

    @classmethod
    def getlogger(cls):
        return cls.__cur_logger


logger = Logger(logging.DEBUG).getlogger()


def set_file_datas(file_name, datas, mode='a', header=False):
    df = pd.DataFrame(datas)
    df.to_csv(file_name, index=False, mode=mode, header=header, columns=COLUMN)


def renew_original(original_dir):
    original_file = os.path.join(original_dir, ORIGINAL_CSV_FILE)
    pandas_data = pd.read_csv(original_file, sep='\t')
    pandas_data = pandas_data.drop(pandas_data.loc[pandas_data['date'] == 'date'].index)
    pandas_data.to_csv(original_file, index=False)


def set_com(data1, data2):
    same = set(data1) & set(data2)
    only_1 = list(set(data1)-same)
    only_2 = list(set(data2)-same)
    same = list(same)
    all_data = list(set(data1) | set(data2))
    return (same, only_1, only_2, all_data)

def format_datas(datas):
    result = dict()
    format_str = SPLICE.join(['{}','{}','{}'])
    for data in datas:
        if len(data) != LEN_COLUMN:
            logger.error('data:{} len error')
            continue
        #['app', 'type', 'geo', 'payout', 'tier', 'descr']
        result[format_str.format(data[0], data[1], data[2])] = [data[3], data[4], data[5]]
    return result

def compare_file(data1, data2, time_in):
    try:
        write_lines = []
        data1 = [tuple((data.get(COLUMN[i]) for i in range(1, len(COLUMN)))) for data in data1]
        data2 = [tuple((data.get(COLUMN[i]) for i in range(1, len(COLUMN)))) for data in data2]
        result = set_com(data1, data2)
        dif_data1 = result[1]
        dif_data2 = result[2]
        dif_data1 = format_datas(dif_data1)
        dif_data2 = format_datas(dif_data2)
        keys_1 = dif_data1.keys()
        keys_2 = dif_data2.keys()
        result = set_com(keys_1, keys_2)
        all_key = result[3]
        for x in all_key:
            #默认Tier1
            # value = dif_data2.get(x, [0.0, 'Tire1', ''])
            value = dif_data2.get(x) if dif_data2.get(x) else ['0.0', dif_data1.get(x)[1], dif_data1.get(x)[2]]
            line = x.split(SPLICE)
            line.extend(value)
            line.insert(0, time_in)
            write_lines.append(dict(zip(COLUMN, line)))
        return write_lines
    except Exception:
        raise


def save_serial_file(before_pt, after_pt, csv_file, pandas_data, first_day):
    time_in = after_pt
    data1 = pandas_data.loc[pandas_data[S] == before_pt][COLUMN].to_dict('records')
    if first_day:
        #第一天数据全部插入
        logger.debug('length of begin_data is:{}'.format(len(data1)))
        set_file_datas(csv_file, data1, 'w', True)
    data2 = pandas_data.loc[pandas_data[S] == after_pt][COLUMN].to_dict('records')
    write_data = compare_file(data1, data2, time_in)
    if not write_data:
        logger.info('before_pt:{} and after_pt:{} data is same'.format(before_pt, after_pt))
    else:
        logger.debug('length of write_data is:{}'.format(len(write_data)))
        set_file_datas(csv_file, write_data)


def generate_price_serial(operate_dir):
    logger.info('generate price serial begin, operate dir is:{}'.format(operate_dir))
    original_csv = os.path.join(operate_dir, ORIGINAL_CSV_FILE)
    if not os.path.exists(original_csv):
        logger.error('original csv not exist,file pith is:{}'.format(ORIGINAL_CSV_FILE))
        return False
    result_csv = os.path.join(operate_dir, CSV_FILE)
    if os.path.exists(result_csv):
        os.remove(result_csv)
    dates = get_all_date(original_csv, 'date')
    # if n < 2:
    #     logger.info('file:{} get pt info less:{},so only copy it'.format(original_csv, dates))
    #     shutil.copy(original_csv, result_csv)
    #     return True
    pandas_data = format_global_data(original_csv, False)

    # pandas_data = pd.read_csv(r'./temporary.csv', dtype=str)
    if pandas_data.empty:
        logger.error('not get valid pandas data')
        return

    n = len(dates)
    if n < 2:
        set_file_datas(result_csv, pandas_data)
        return True

    first_day = True
    for i in range(n-1):
        before_pt = dates[i]
        after_pt = dates[i+1]
        logger.debug('begin_pt:{}, end_pt:{} begin'.format(before_pt, after_pt))
        save_serial_file(before_pt, after_pt, result_csv, pandas_data, first_day)
        logger.debug('begin_pt:{}, end_pt:{} end'.format(before_pt, after_pt))
        first_day = False


def format_global_data(original_csv, write_tmp=r'./temporary.csv'):
    pandas_data = pd.read_csv(original_csv, dtype=str)
    pandas_data = pandas_data.drop('geo', axis=1).join(
        pandas_data['geo'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('geo')).reset_index(drop=True)
    pandas_data = pandas_data.drop_duplicates()
    indexs = pandas_data.loc[pandas_data['tier'].isin(['Global', 'Global-TH'])].index
    # indexs = pandas_data.loc[pandas_data['tier'].isin(['Global'])].index

    logger.info('get Global row:{}'.format(len(indexs)))
    add_geo_global = dict()
    row = 0
    date = ''
    tmp_pandas_data = None
    for index in indexs:
        if not row % 500:
            logger.info('row [{}] is running now'.format(row+1))
        row += 1
        logger.debug('row [{}] is running now'.format(row))
        data = pandas_data.loc[index]
        if data['date'] != date:
            date = data['date']
            tmp_pandas_data = pandas_data.loc[pandas_data['date'] == data['date']]
        #data['tier'] == 'Global'
        add_geo_global[(data['date'], data['app'], data['type'], 'global', 'Done')] = [data['payout'], data['descr']]
        map_info = tmp_pandas_data.loc[(tmp_pandas_data['date'] == data['date'])&\
                                   (tmp_pandas_data['app'] == data['app'])&\
                                   (tmp_pandas_data['type'] == data['type'])&\
                                   (tmp_pandas_data['geo'] == data['geo'])&\
                                   (tmp_pandas_data['tier'].isin(['Tier1', 'Tier2', 'Tier3', 'Tier4', 'Tier5']))]
        #没发现其他相同的但是tier的 所以价格改为0.0
        if map_info.empty:
            pandas_data.loc[index]['payout', 'tier'] = [0.0, 'Done']
        else:
        #发现其他的 所以此条记录删除
            pandas_data = pandas_data.drop(index)
    add_data = list()
    logger.info('add row which geo is "global" begin')
    for key, value in add_geo_global.iteritems():
        row_add = dict()
        row_add['date'] = key[0]
        row_add['app'] = key[1]
        row_add['type'] = key[2]
        row_add['geo'] = key[3]
        row_add['tier'] = key[4]
        row_add['payout'] = value[0]
        row_add['descr'] = value[1]
        add_data.append(row_add)
    pandas_add = pd.DataFrame(add_data)
    pandas_data = pandas_data.append(pandas_add, ignore_index=True)
    pandas_data = pandas_data.sort_values(by="date")
    if write_tmp:
        pandas_data.to_csv(write_tmp, index=False, columns=COLUMN)
    return pandas_data


def get_all_date(original_csv, specific=S):
    pandas_data = pd.read_csv(original_csv, dtype=str)
    dates = pandas_data.loc[:, specific].unique().tolist()#unique()获取唯一值
    # dates = pandas_data.loc[:, specific].drop_duplicates().tolist()
    dates.sort()
    pts = filter(lambda pt: True if isinstance(pt, str) and re.search('^\d{4}-\d{2}-\d{2}$', pt) else False, dates)
    return pts


def drop_desrc(original_dir):
    import numpy as np
    original_file = os.path.join(original_dir, CSV_FILE)
    pandas_data = pd.read_csv(original_file)
    pandas_data = pandas_data.drop(['descr'], axis=1)
    pandas_data['descr'] = np.nan
    pandas_data.to_csv(original_file, index=False)


def main():
    # drop_desrc(r'./datas')
    # renew_original(r'./datas')
    generate_price_serial(r'./datas')
    # format_global_data(r'./datas/original.csv')


if __name__ == '__main__':
    startTime = datetime.now()
    main()
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))