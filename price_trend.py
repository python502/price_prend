#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/10 15:40
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : price_trend.py
# @Software: PyCharm
# @Desc    :
import os
import shutil
import pandas as pd
from logger import logger

LEN_COLUMN = 4
CSV_FILE = 'price_trend.csv'
ORIGINAL_CSV_FILE = 'original.csv'
# COLUMN = ['app', 'utm_type', 'geo', 'payout']
COLUMN = ['pt', 'a', 'b', 'c', 'd']
pandas_data = None

def get_datas(pt):
    try:
        data = pandas_data[(pandas_data['pt']==pt)][COLUMN].to_dict('list')
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
    df.to_csv(file_name, index=False, mode='a', header=header, columns=COLUMN)


def get_files(path, type2):
    name = []
    for file in os.walk(path):
        for each_list in file[2]:
            if each_list.endswith(type2):
            # os.walk()函数返回三个参数：路径，子文件夹，路径下的文件，利用字符串拼接file[0]和file[2]得到文件的路径
                name.append(each_list)
    name.sort()
    paths = [os.path.join(path, x.strip()) for x in name]
    return paths

def format_datas(datas):
    result = {}
    format_str = '{}_{}_{}'
    for data in datas:
        if len(data) != LEN_COLUMN:
            logger.error('data:{} len error')
            continue
        result[format_str.format(data[0], data[1], data[2])] = data[3]
    return result

def set_com(data1, data2):
    same = set(data1) & set(data2)
    only_1 = list(set(data1)-same)
    only_2 = list(set(data2)-same)
    same = list(same)
    all_data = list(set(data1) | set(data2))
    return (same, only_1, only_2, all_data)

def compare_file(data1, data2, time_in):
    try:
        write_lines = []
        data1 = [tuple((data.get(COLUMN[i]) for i in range(1, len(COLUMN)))) for data in data1]
        # data1 = [(data.get(COLUMN[1]), data.get(COLUMN[2]), data.get(COLUMN[3]), data.get(COLUMN[4])) for data in data1]
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
            value = dif_data2.get(x, 0)
            line = x.split('_')
            line.append(value)
            line.insert(0, time_in)
            write_lines.append(dict(zip(COLUMN, line)))
        return write_lines
    except Exception:
        raise

def save_compare_file(before_pt, after_pt, csv_file, before_data, only_once = False):
    time_in = after_pt
    if not before_data and not only_once:
        #第一天数据全部插入
        data1 = get_datas(before_pt)
        set_file_datas(csv_file, data1, True)
    else:
        data1 = before_data
    data2 = get_datas(after_pt)
    write_data = compare_file(data1, data2, time_in)
    if not write_data:
        logger.info('before_pt:{} and after_pt:{} data is same'.format(before_pt, after_pt))
    else:
        set_file_datas(csv_file, write_data, only_once)
    return data2

def get_specific_data(original_csv, specific='pt'):
    global pandas_data
    pandas_data = pd.read_csv(original_csv)
    # column = pandas_data.to_dict('list').keys()
    pts = pandas_data[specific].drop_duplicates().tolist()
    pts.sort()
    return pts

def main(dir_name):
    original_csv = os.path.join(dir_name, ORIGINAL_CSV_FILE)
    if not os.path.exists(dir_name) or not os.path.exists(original_csv):
        logger.error('csv dir or original csv not exist')
        return
    result_csv = os.path.join(dir_name, CSV_FILE)
    if os.path.exists(result_csv):
        os.remove(result_csv)

    pts = get_specific_data(original_csv)
    n = len(pts)

    if n < 2:
        logger.info('file:{} get pt info less:{},so only copy it'.format(original_csv, pts))
        shutil.copy(original_csv, result_csv)
        return

    before_data = None
    for i in range(n-1):
        before_pt = pts[i]
        after_pt = pts[i+1]
        before_data = save_compare_file(before_pt, after_pt, result_csv, before_data)
        if not before_data:
            logger.error('before_pt:{}  after_pt:{} not get result data'.format(before_pt, after_pt))
            return

if __name__=='__main__':
    dir_name = r'D:\price_trend\datas'
    main(dir_name)
