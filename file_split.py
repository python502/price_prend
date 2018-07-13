#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/12 17:04
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : file_split.py
# @Software: PyCharm
# @Desc    :
import os
import sys
import re
import shutil
import logging
import logging.handlers
import pandas as pd
import numpy as np
from datetime import datetime

format_dict = {
    logging.DEBUG: logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'),
    logging.INFO: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.WARNING: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.ERROR: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s'),
    logging.CRITICAL: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
}

current_dir = os.path.abspath(os.path.dirname(__file__))

class Logger():
    __cur_logger = logging.getLogger()
    def __init__(self,loglevel):
        #set name and loglevel
        new_logger = logging.getLogger(__name__)
        new_logger.setLevel(loglevel)
        formatter = format_dict[loglevel]
        filehandler = logging.handlers.RotatingFileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'file_split.log'), mode='a')
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

# logger = Logger(logging.DEBUG).getlogger()
logger = Logger(logging.INFO).getlogger()

def get_csv_files(path):
    name = []
    for file in os.walk(path):
        for each_list in file[2]:
            if re.search('^\d{4}-\d{2}-\d{2}.csv$', each_list):
            # os.walk()函数返回三个参数：路径，子文件夹，路径下的文件，利用字符串拼接file[0]和file[2]得到文件的路径
                name.append(each_list)
    name.sort()
    paths = [os.path.join(path, x.strip()) for x in name]
    return paths

def get_csv_data(pandas_data, throw_columns, check_column, format_column, add_columns, add_map_columns):
    data = pandas_data.drop(throw_columns, axis=1)
    # import pdb;pdb.set_trace()
    if check_column:
        data_check = data[check_column].fillna(0)
        data = data.drop(check_column, axis=1)
        data = data.join(data_check)
    for column in throw_columns:
        data[column] = 0

    for key, value in add_columns.iteritems():
        data[key] = value

    for key, value in add_map_columns.iteritems():
        flag = False
        map_key = value[0]
        map_value = value[1]
        map_key_list = data[map_key].drop_duplicates().tolist()
        for key_list in map_key_list:
            map_key_value = map_value.loc[map_value[map_key] == key_list][key].tolist()
            if map_key_value:
                data.loc[data[map_key] == key_list, key] = map_key_value[0]
                flag = True
    # data.columns.values
        if not flag:
            data[key] = np.nan

    for key, value in format_column.iteritems():
        x = data[value].astype(key)
        data = data.drop(value, axis=1)
        data = data.join(x)

    return data


def file_split(csv_file, column_info):
    file_name = os.path.basename(csv_file)
    base_dir = os.path.dirname(csv_file)
    child_dir = os.path.join(base_dir, file_name[:-4])
    if os.path.exists(child_dir):
        if os.path.isfile(child_dir):
            os.remove(child_dir)
        elif os.path.isdir(child_dir):
            shutil.rmtree(child_dir)
        else:
            logger.error('{} is exist,but not file or dir')
            return False
    os.mkdir(child_dir)

    pandas_data = pd.read_csv(csv_file, header=None, names=column_info.get('columns_original'))
    #add package
    if pandas_data.empty:
        logger.error('csv_file: {} no data'.format(csv_file))
        return

    for key, value in column_info.get('split_data').iteritems():
        csv_name = file_name[:-4]+key+'.csv'
        csv_file = os.path.join(child_dir, csv_name)
        throw = list(set(tuple(column_info.get('columns_original')))-set(tuple(value)))
        check = list(set(tuple(value)) - set(tuple(column_info.get('uncheck_columns'))))
        datas = get_csv_data(pandas_data, throw, check, column_info.get('format_columns'), column_info.get('add_columns', {}), column_info.get('add_map_columns', {}))
        if datas.empty:
            logger.error('csv_name: {} no need write'.format(csv_name))
            return
        set_csv_datas(csv_file, datas, column_info.get('write_columns'))

def set_csv_datas(file_name, datas, columns, header=False):
    datas.to_csv(file_name, index=False, mode='w', header=header, columns=columns)




def cost_dau_split():
    cost_dau = os.path.join(current_dir, 'cost_dau')
    csv_files = get_csv_files(cost_dau)
    app_map_package_csv = os.path.join(current_dir, 'app_map_package.csv')
    app_map_package = pd.read_csv(app_map_package_csv)
    column_info = {'columns_original': ['data_time', 'app', 'geo', 'utm_type', 'dnu', 'dau', 'wau', 'mau', 'real_dau', 'real_wau',\
                                        'real_mau', 'retains_1', 'retains_2', 'retains_3', 'retains_7', 'retains_14', 'retains_21', 'retains_30', 'real_retains_1', 'real_retains_2', \
                                        'real_retains_3', 'real_retains_7', 'real_retains_14', 'real_retains_21', 'real_retains_30', 'install', 'cost', 'unit_price'],
                   'split_data': {'_0': ['data_time', 'app', 'geo', 'utm_type', 'dnu', 'dau', 'wau', 'mau', 'real_dau', 'real_wau', 'real_mau', 'install', 'cost', 'unit_price'],
                                  '_1': ['data_time', 'app', 'geo', 'utm_type', 'retains_1', 'real_retains_1'],
                                  '_2': ['data_time', 'app', 'geo', 'utm_type', 'retains_2', 'real_retains_2'],
                                    '_3': ['data_time', 'app', 'geo', 'utm_type', 'retains_3', 'real_retains_3'],
                                    '_7': ['data_time', 'app', 'geo', 'utm_type', 'retains_7', 'real_retains_7'],
                                    '_14': ['data_time', 'app', 'geo', 'utm_type', 'retains_14', 'real_retains_14'],
                                    '_21': ['data_time', 'app', 'geo', 'utm_type', 'retains_21', 'real_retains_21'],
                                  '_30': ['data_time', 'app', 'geo', 'utm_type', 'retains_30', 'real_retains_30'],
                                  },
                   'write_columns': ['package_name', 'app', 'geo', 'utm_type', 'dnu', 'dau', 'wau', 'mau', 'real_dau', 'real_wau',\
                                        'real_mau',  'install', 'cost', 'unit_price','retains_1', 'retains_2', 'retains_3', 'retains_7', 'retains_14', 'retains_21', \
                                       'retains_30', 'real_retains_1', 'real_retains_2','real_retains_3', 'real_retains_7', 'real_retains_14', 'real_retains_21', \
                                    'real_retains_30', 'dau_0_7', 'dau_8_30', 'dau_31_60', 'dau_61_90', 'dau_91_u', 'dau_retain_0_7', 'dau_retain_8_30', 'dau_reatain_31_60', 'dau_retain_61_90', 'dau_retain_91_u'],
                   'add_columns': {'dau_0_7': 0, 'dau_8_30': 0, 'dau_31_60': 0, 'dau_61_90': 0, 'dau_91_u': 0, 'dau_retain_0_7': 0, 'dau_retain_8_30': 0, 'dau_reatain_31_60': 0, 'dau_retain_61_90': 0, 'dau_retain_91_u': 0},
                   'uncheck_columns': ['data_time', 'app', 'geo', 'utm_type'],
                   'format_columns': {'int': ['dnu', 'dau', 'wau', 'mau', 'real_dau', 'real_wau',\
                                              'real_mau', 'retains_1', 'retains_2', 'retains_3', 'retains_7', 'retains_14', 'retains_21', 'retains_30', 'real_retains_1', 'real_retains_2', \
                                              'real_retains_3', 'real_retains_7', 'real_retains_14', 'real_retains_21', 'real_retains_30', 'install'], 'float': ['cost', 'unit_price']},
                   'add_map_columns': {'package_name': ['app', app_map_package]}
                   }
    for file in csv_files:
        file_split(file, column_info)

def geo_dau_split():
    geo_dau = os.path.join(current_dir, 'geo_dau_non_repeat')
    csv_files = get_csv_files(geo_dau)
    column_info = {'columns_original': ['data_time', 'geo', 'dnu', 'dau', 'wau', 'mau', 'real_dau', 'real_wau', 'real_mau', 'retains_1',\
                                        'retains_2', 'retains_3', 'retains_7', 'retains_14', 'retains_21', 'retains_30', 'real_retains_1', 'real_retains_2', 'real_retains_3', 'real_retains_7', \
                                         'real_retains_21','real_retains_14', 'real_retains_30'],
                   'split_data': {'_0': ['geo', 'dnu', 'dau', 'wau', 'mau', 'real_dau', 'real_wau', 'real_mau'],
                                  '_1': ['geo', 'retains_1', 'real_retains_1'],
                                  '_2': ['geo', 'retains_2', 'real_retains_2'],
                                    '_3': ['geo', 'retains_3', 'real_retains_3'],
                                    '_7': ['geo',  'retains_7', 'real_retains_7'],
                                    '_14': ['geo',  'retains_14', 'real_retains_14'],
                                    '_21': ['geo', 'retains_21', 'real_retains_21'],
                                  '_30': ['geo',  'retains_30', 'real_retains_30'],
                                  },
                   'write_columns': ['geo', 'dnu', 'dau', 'wau', 'mau', 'real_dau', 'real_wau', 'real_mau', 'retains_1',\
                                        'retains_2', 'retains_3', 'retains_7', 'retains_14', 'retains_21', 'retains_30', 'real_retains_1', 'real_retains_2', 'real_retains_3', 'real_retains_7', \
                                         'real_retains_21','real_retains_14', 'real_retains_30'],
                   'uncheck_columns': ['geo'],
                   'format_columns': {'int': ['dnu', 'dau', 'wau', 'mau', 'real_dau', 'real_wau', 'real_mau', 'retains_1',\
                                              'retains_2', 'retains_3', 'retains_7', 'retains_14', 'retains_21', 'retains_30', 'real_retains_1', 'real_retains_2', 'real_retains_3', 'real_retains_7',\
                                              'real_retains_21','real_retains_14', 'real_retains_30']}
                   }
    for file in csv_files:
        file_split(file, column_info)

def main(argv):
    if len(argv) == 1:
        logger.info('cost_dau begin')
        cost_dau_split()
        logger.info('cost_dau finish')
        logger.info('geo_dau begin')
        geo_dau_split()
        logger.info('geo_dau finish')

    elif sys.argv[1] == 'cost_dau':
        logger.info('cost_dau begin')
        cost_dau_split()
        logger.info('cost_dau finish')

    elif sys.argv[1] == 'geo_dau':
        logger.info('geo_dau begin')
        geo_dau_split()
        logger.info('geo_dau finish')

    else:
        logger.info('Input parameter error, must be cost_dau or geo_dau or not input')

if __name__ == '__main__':
    startTime = datetime.now()
    main(sys.argv)
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))