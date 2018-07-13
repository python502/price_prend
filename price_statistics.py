#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/11 16:07
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : price_statistics.py
# @Software: PyCharm
# @Desc    :
import os
import pandas as pd
from datetime import datetime
from logger import logger

S = 'date'
SPLICE = '^'
CSV_FILE = 'price_trend.csv'
ORIGINAL_CSV_FILE = 'original.csv'
COLUMN = [S, 'app', 'type', 'geo', 'payout']
csv_data = {}
insert_zero = True
base_data = {}

def format_file(file):
    with open(file, 'r') as fd:
        datas = fd.readlines()
        datas = [i.strip() + '\n' for i in datas if i and i != '\n']
    with open(file, 'w') as fd:
        fd.writelines(datas)

def get_datas(pt, pandas_data):
    try:
        data = pandas_data[(pandas_data[S] == pt)][COLUMN[1:]].to_dict('list')
        keys = data.keys()
        values = zip(*data.values())
        results = []
        for value in values:
            data = dict(zip(keys, value))
            results.append(data)
        return results
    except Exception, ex:
        logger.error('get_datas error:{}'.format(ex))
        raise
def set_file_datas(file_name, datas, header=True):
    df = pd.DataFrame(datas)
    df.to_csv(file_name, index=False, mode='a', header=header, columns=COLUMN, sep='\t')


def remove_none():
    keys = []
    for key, value in base_data.iteritems():
        if not value:
            keys.append(key)
    for key in keys:
        del base_data[key]

def save_statistic_file(pt, csv_file):
    data = csv_data[pt]
    data = {tuple((d.get(COLUMN[i]) for i in range(1, len(COLUMN) - 1))): d.get('payout') for d in data}
    header = False if base_data else True
    x1 = len(base_data)
    base_data.update(data)
    x2 = len(base_data)
    if not insert_zero:
        remove_none()
    x3 = len(base_data)

    write_data = []
    for key, value in base_data.iteritems():
        t_l = {}
        # t_l[COLUMN[0]] = pt
        t_l[COLUMN[1]] = key[0]
        t_l[COLUMN[2]] = key[1]
        t_l[COLUMN[3]] = key[2]
        t_l[COLUMN[4]] = value
        write_data.append(t_l)
    if not write_data:
        logger.error('pt:{} no data to write'.format(pt))
        logger.error('x1:{},x2;{},x3;{}'.format(x1, x2, x3))
        return
    write_data = merge_datas(pt, write_data)
    set_file_datas(csv_file, write_data, header)

def get_all_datas(original_csv, specific=S):
    global csv_data
    pandas_data = pd.read_csv(original_csv)
    pts = pandas_data[specific].drop_duplicates().tolist()
    pts.sort()
    pts = filter(lambda pt: True if isinstance(pt, str) and re.search('^\d{4}-\d{2}-\d{2}$', pt) else False, pts)
    for pt in pts:
        value = get_datas(pt, pandas_data)
        csv_data[pt] = value
    return pts

def merge_datas(pt, datas):
    format_str = SPLICE.join(['{}', '{}', '{}'])
    tmp_dict = {}
    for data in datas:
        key = format_str.format(data.get(COLUMN[1]), data.get(COLUMN[2]), data.get(COLUMN[4]))
        if tmp_dict.has_key(key):
            tmp_dict[key] = tmp_dict.get(key)+','+data.get('geo')
        else:
            tmp_dict[key] = data.get('geo')
    results = []
    for key, value in tmp_dict.iteritems():
        result = {S:pt}
        t = key.split(SPLICE)
        result[COLUMN[1]] = t[0]
        result[COLUMN[2]] = t[1]
        result[COLUMN[4]] = t[2]
        result['geo'] = value
        results.append(result)
    return results

def main(dir_name):
    original_csv = os.path.join(dir_name, CSV_FILE)
    if not os.path.exists(dir_name) or not os.path.exists(original_csv):
        logger.error('csv dir or original csv not exist')
        return
    result_csv = os.path.join(dir_name, ORIGINAL_CSV_FILE)
    if os.path.exists(result_csv):
        os.remove(result_csv)

    pts = get_all_datas(original_csv)
    n = len(pts)
    if n == 0:
        logger.error('file:{} get not data'.format(original_csv))
        return
    for pt in pts:
        logger.debug('pt:{} begin'.format(pt))
        save_statistic_file(pt, result_csv)
        logger.debug('pt:{} end'.format(pt))
if __name__=='__main__':
    startTime = datetime.now()
    dir_name = r'D:\price_trend\data_statistics'
    main(dir_name)
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))