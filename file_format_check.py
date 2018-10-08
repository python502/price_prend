#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/27 13:31
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : file_format_check.py
# @Software: PyCharm
# @Desc    :
import pandas as pd
import argparse
import re
import os
import logging
import logging.handlers

format_dict = {
    logging.DEBUG: logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'),
    logging.INFO: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.WARNING: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.ERROR: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s'),
    logging.CRITICAL: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
}

COLUMN_FORMAT = {'beg_date': re.compile('^\d{4}-\d{2}-\d{2}$')
                 , 'app': re.compile('^\w+$')
                 , 'type': re.compile('^FB$|^APX$|^ADW$|^cashcash$|^superads$|^rupiahkilat$|^Pinjamall$|^paydayloans$|^aggrex_int$|^kreditmart_int$|^qreditku_int$|^heiner_int$|^newsinpalm$|^raja_invites$|^shkyad_int$|^adxmi$|^Appnext$|^Unity$|^yahoo$')
                 , 'geo': re.compile('^global$|^[A-Z,]+$')
                 , 'payout': re.compile('^[0-9]+[0-9.]*$')
                 , 'tier': re.compile('^Tier\S*$|^Global$|^Done$')
                 , 'descr': None}


class Logger(object):
    __cur_logger = logging.getLogger()

    def __init__(self, loglevel):
        #set name and loglevel
        new_logger = logging.getLogger(__name__)
        new_logger.setLevel(loglevel)
        formatter = format_dict[loglevel]
        #create handle for stdout
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)
        #add handle to new_logger
        new_logger.addHandler(streamhandler)
        Logger.__cur_logger = new_logger

    @classmethod
    def getlogger(cls):
        return cls.__cur_logger


logger = Logger(logging.INFO).getlogger()


def check_format(file_path):
    try:
        error_num = 0
        result = True
        pandas_data = pd.read_csv(file_path, dtype=str)
        #得到csv文件的列名
        column = pandas_data.columns.tolist()
        column.sort()
        # 得到配置的列名
        column_key = COLUMN_FORMAT.keys()
        column_key.sort()
        if column != column_key:
            logger.error('column:{} is Incorrect, must be:{}'.format(column, column_key))
            return False
        indexs = pandas_data.index
        for index in indexs:
            data = pandas_data.loc[index]
            for key, value in COLUMN_FORMAT.iteritems():
                check_value = data[key]
                try:
                    if value and not value.search(check_value):
                        logger.error('index:{} column:{}/value:{} is Incorrect'.format(index, key, check_value))
                        error_num += 1
                except Exception, ex:
                    logger.error('index:{} column:{}/value:{} is Incorrect, ex:{}'.format(index, key, check_value, ex))
                    error_num += 1
    except Exception, ex:
        result = False
        logger.error('check_format have error:{}'.format(ex))
    finally:
        if error_num:
            logger.error('csv file:{} find error num:{}, Please fix it before upload'.format(file_path, error_num))
            result = False
        if result:
            logger.info('The file format is correct')
        else:
            logger.info('The file format is Incorrect')


def main():
    parser = argparse.ArgumentParser(description='the csv file which upload to dotc_payout_info_upload')
    parser.add_argument('-f', '--file', default='./upload.csv', type=str,
                        help='the target csv file full path')
    args = parser.parse_args()
    file = args.file
    if not os.path.exists(file):
        logger.error('input csv is not exist:{}'.format(file))
        return False
    check_format(file)


if __name__ == '__main__':
    main()