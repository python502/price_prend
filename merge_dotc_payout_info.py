#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/12 11:49
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : merge_dotc_payout_info.py
# @Software: PyCharm
# @Desc    :
import argparse
import time
from sqlalchemy import *
from datetime import datetime
import pandas as pd
import sys
import logging
import logging.handlers

format_dict = {
    logging.DEBUG: logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'),
    logging.INFO: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.WARNING: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.ERROR: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s'),
    logging.CRITICAL: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
}


class Logger(object):
    __cur_logger = logging.getLogger()

    def __init__(self, loglevel, file='default.log', mode='w'):
        #set name and loglevel
        new_logger = logging.getLogger(__name__)
        new_logger.setLevel(loglevel)
        formatter = format_dict[loglevel]
        # filehandler = logging.handlers.RotatingFileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), file), mode=mode)
        # filehandler.setFormatter(formatter)
        # new_logger.addHandler(filehandler)
        #create handle for stdout
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)
        #add handle to new_logger
        new_logger.addHandler(streamhandler)
        Logger.__cur_logger = new_logger

    @classmethod
    def getlogger(cls):
        return cls.__cur_logger


logger = Logger(logging.INFO, 'merge_dotc_payout_info.log').getlogger()

DB_INFO = {'DB_USER': 'root',
           'DB_PASS': '111111',
           'DB_HOST': '127.0.0.1',
           'DB_PORT': 3306,
           'DATABASE': 'capture'}

TABLE_PAYOUT_INFO = 'dotc_payout_info'
TABLE_PAYOUT_INFO_UPLOAD = 'dotc_payout_info_upload'
DEFAULT_TIME = 943891200


def format_global_data(pandas_data, write_tmp=r'./temporary_merge_upload.csv'):
    try:
        #将payout float   create_time int 都转化为str
        x = pandas_data[['payout', 'create_time']].astype(str)
        pandas_data = pandas_data.drop(['payout', 'create_time'], axis=1)
        pandas_data = pandas_data.join(x)
        pandas_data = pandas_data.drop('geo', axis=1).join(
            pandas_data['geo'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('geo')).reset_index(drop=True)
        pandas_data = pandas_data.drop_duplicates()
        indexs = pandas_data.loc[pandas_data['tier'].str.lower() == 'global'].index
        logger.debug('get Global row:{}'.format(len(indexs)))
        add_geo_global = dict()
        # row = 0
        beg_date = ''
        create_time = ''
        tmp_pandas_data = None
        for index in indexs:
            # if not row % 500:
            #     logger.debug('row [{}] is running now'.format(row + 1))
            # row += 1
            # logger.debug('row [{}] is running now'.format(row))
            data = pandas_data.loc[index]
            if data['beg_date'] != beg_date or data['create_time'] != create_time:
                beg_date = data['beg_date']
                create_time = data['create_time']
                tmp_pandas_data = pandas_data.loc[(pandas_data['beg_date'] == data['beg_date']) & (pandas_data['create_time'] == data['create_time'])]
            # data['tier'] == 'Global'
            add_geo_global[(data['beg_date'], data['app'], data['type'], 'global', 'Done', data['create_time'])] = [data['payout'], data['descr']]
            map_info = tmp_pandas_data.loc[(tmp_pandas_data['beg_date'] == data['beg_date']) & \
                                           (tmp_pandas_data['app'] == data['app']) & \
                                           (tmp_pandas_data['type'] == data['type']) & \
                                           (tmp_pandas_data['geo'] == data['geo']) & \
                                           # (tmp_pandas_data['tier'].isin(['Tier', 'Tier1', 'Tier2', 'Tier3', 'Tier4', 'Tier5']))]
                                           (tmp_pandas_data['tier'].str.startswith('Tier', na=False))]
            # 没发现其他相同的但是tier的 所以价格改为0.0
            if map_info.empty:
                pandas_data.loc[index]['payout', 'tier'] = [0.0, 'Done']
            else:
                # 发现其他的 所以此条记录删除
                pandas_data = pandas_data.drop(index)
        add_data = list()
        logger.debug('add row which geo is "global" begin')
        for key, value in add_geo_global.iteritems():
            row_add = dict()
            row_add['beg_date'] = key[0]
            row_add['app'] = key[1]
            row_add['type'] = key[2]
            row_add['geo'] = key[3]
            row_add['tier'] = key[4]
            row_add['create_time'] = key[5]
            row_add['payout'] = value[0]
            row_add['descr'] = value[1]

            add_data.append(row_add)
        pandas_add = pd.DataFrame(add_data)
        pandas_data = pandas_data.append(pandas_add, ignore_index=True)
        pandas_data = pandas_data.sort_values(by="beg_date")
        if write_tmp:
            pandas_data.to_csv(write_tmp, encoding='utf-8', index=False, columns=['beg_date', 'app', 'type', 'geo', 'payout', 'tier', 'descr', 'create_time'])
        return pandas_data
    except Exception:
        raise


def get_newest(pandas_data):
    datas_groupby_date = pandas_data.groupby(['beg_date', 'app', 'type', 'geo'])
    all_datas = dict()
    for row in datas_groupby_date:
        beg_date = row[0]
        row_date = row[1]
        datas_groupby_create_time = row_date.groupby(['create_time'])
        all_create_time = list()
        tmp_datas = dict()
        for row_create in datas_groupby_create_time:
            all_create_time.append(row_create[0])
            tmp_datas[(beg_date, row_create[0])] = row_create[1]
        all_create_time.sort(reverse=True)
        all_datas[(beg_date, all_create_time[0])] = tmp_datas[(beg_date, all_create_time[0])]

    pandas_data = pd.DataFrame(columns=['beg_date', 'app', 'type', 'geo', 'payout', 'tier', 'descr', 'create_time'])
    # 获取最新的df
    for key, value in all_datas.iteritems():
        logger.debug('key: {}'.format(key))
        pandas_data = pandas_data.append(value, ignore_index=True)
    if pandas_data.empty:
        logger.error('df is empty')
        return pd.DataFrame.empty
    return pandas_data


def merge_dotc_payout_info(pt):
    try:
        #connect to db
        connect_info = 'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DATABASE}?charset=utf8'.format(**DB_INFO)
        engine = create_engine(connect_info, echo=False)
        metadata = MetaData(engine)
        connect = engine.connect()
        new_merge_time = pd.read_sql('select update_time from {} order by update_time desc limit 1'.format(TABLE_PAYOUT_INFO), con=engine)['update_time'].tolist()
        new_merge_time = new_merge_time[0] if new_merge_time else DEFAULT_TIME

        if new_merge_time != DEFAULT_TIME:
            desc_table_count = pd.read_sql('select count(*) from {} where update_time={}'.format(TABLE_PAYOUT_INFO, new_merge_time), con=engine)['count(*)'].tolist()[0]
            source_table_count = pd.read_sql('select count(*) from {} where create_time={}'.format(TABLE_PAYOUT_INFO_UPLOAD, new_merge_time), con=engine)['count(*)'].tolist()[0]
            if desc_table_count == source_table_count:
                logger.info('new_merge_time:{} is complete,so skip it'.format(new_merge_time))
                new_merge_time += 1
        need_merge_data = pd.read_sql('select beg_date, app, type, geo, payout, tier, descr, create_time from {} where create_time BETWEEN {} and {} order by create_time,beg_date'.format(TABLE_PAYOUT_INFO_UPLOAD, new_merge_time, pt), con=engine)
        # need_merge_data = pd.read_sql('select beg_date, app, type, geo, payout, tier, descr, create_time from {} where create_time>={} and create_time<={} order by create_time,beg_date '.format(TABLE_PAYOUT_INFO_UPLOAD, new_merge_time, pt), con=engine)

        if need_merge_data.empty:
            logger.error('no data need merge')
            return True
        need_merge_data = format_global_data(need_merge_data)

        if need_merge_data.empty:
            logger.error('no data need merge after format global data')
            return False
        need_merge_data = get_newest(need_merge_data)

        if need_merge_data.empty:
            logger.error('no data need merge after get newest data')
            return False
        dates = need_merge_data.loc[:, 'beg_date'].unique().tolist()
        dates = ['"'+d+'"' for d in dates]
        pandas_payout_info = pd.read_sql('select beg_date, app, type, geo, payout, tier, descr, update_time from {} where beg_date in ({})'.format(TABLE_PAYOUT_INFO, ','.join(dates)), con=engine)
        x = pandas_payout_info[['payout', 'update_time']].astype(str)
        pandas_payout_info = pandas_payout_info.drop(['payout', 'update_time'], axis=1)
        pandas_payout_info = pandas_payout_info.join(x)
        dotc_payout_info = Table(TABLE_PAYOUT_INFO, metadata, autoload=True)
        beg_date = ''
        tmp_pandas_payout_info = None
        for i in range(len(need_merge_data)):
            item = need_merge_data.iloc[i].to_dict()
            item = {key: str(value).strip() for key, value in item.iteritems()}
            if item['beg_date'] != beg_date:
                beg_date = item['beg_date']
                tmp_pandas_payout_info = pandas_payout_info.loc[pandas_payout_info['beg_date'] == item['beg_date']]
            if tmp_pandas_payout_info.loc[(tmp_pandas_payout_info['beg_date'] == item['beg_date']) & \
                                           (tmp_pandas_payout_info['app'] == item['app']) & \
                                           (tmp_pandas_payout_info['type'] == item['type']) & \
                                           (tmp_pandas_payout_info['geo'] == item['geo'])].empty:
                add(item, dotc_payout_info, connect)
            else:
                if tmp_pandas_payout_info.loc[(tmp_pandas_payout_info['beg_date'] == item['beg_date']) & \
                        (tmp_pandas_payout_info['app'] == item['app']) & \
                        (tmp_pandas_payout_info['type'] == item['type']) & \
                        (tmp_pandas_payout_info['geo'] == item['geo'])& \
                        (tmp_pandas_payout_info['payout'] == item['payout']) & \
                        (tmp_pandas_payout_info['update_time'] == item['create_time']) & \
                        (tmp_pandas_payout_info['tier'] == item['tier']) &\
                        (tmp_pandas_payout_info['descr'] == item['descr'])].empty:
                    update(item, dotc_payout_info, connect)
                else:
                    logger.debug('item:{} same'.format(item))
                    continue
        return True
    except Exception, ex:
        logger.error('merge_dotc_payout_info error,ex:{}'.format(ex))
        return False


def update(item, table, conn):
    if isinstance(item['descr'], unicode):
        item['descr'] = item['descr'].encode('utf-8')
    u = table.update().values(beg_date=item['beg_date'],
                              app=item['app'],
                              type=item['type'],
                              geo=item['geo'],
                              tier=item['tier'],
                              payout=float(item['payout']),
                              descr=item['descr'],
                              update_time=int(item['create_time']))\
        .where(table.c.beg_date == item['beg_date'])\
        .where(table.c.app == item['app'])\
        .where(table.c.type == item['type'])\
        .where(table.c.geo == item['geo'])
    conn.execute(u)


def add(item, table, conn):
    if isinstance(item['descr'], unicode):
        item['descr'] = item['descr'].encode('utf-8')
    i = table.insert().values(beg_date=item['beg_date'],
                              app=item['app'],
                              type=item['type'],
                              geo=item['geo'],
                              tier=item['tier'],
                              payout=float(item['payout']),
                              descr=item['descr'],
                              update_time=int(item['create_time']))
    conn.execute(i)


def main():
    parser = argparse.ArgumentParser(description='merge dotc_payout_info_upload data to dotc_payout_info')
    parser.add_argument('-p', '--pt', default=int(time.time()), type=int,
                        help='The pt of task')
    args = parser.parse_args()
    pt = args.pt
    sys.exit(0) if merge_dotc_payout_info(pt) else sys.exit(1)


if __name__ == '__main__':
    startTime = datetime.now()
    main()
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))