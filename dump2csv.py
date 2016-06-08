#!/usr/bin/env python
# encoding: utf-8

'''
Usage:
  dump2csv.py -s SID -u REDIS_URL -d DIR [-m COUNT] [-l DIR] [-v] [<table>...]
  dump2csv.py -c CONFIG [-v] [<table>...]
  dump2csv.py (-h | --help | --version)

Arguments:
  <table>                       Specify tables to dump like: "testdb.testtable"
                                It will dump all tables if specify nothing
Options:
  -h --help                     Show this help message and exit
  --version                     Show version and exit
  -c --config_file=CONFIG_FILE  Specify config file
  -v --verbose                  Print the running status message
  -s --server_id=SID            Specify mysql server id
  -u --cache_url=REDIS_URL      Specify the redis cache url like:
                                "redis://host:port/db"
  -d --dump_dir=DIR             Specify the dir of dump result
  -l --log_dir=DIR              Specify the dir of logging
  -m --max_rows=COUNT           Specify max rows of one csv file [default: 1000000]
'''

import csv
import os
import time
from functools import partial
from docopt import docopt
from collections import defaultdict
import json

import rcache
import mwlogger
from datetime import datetime

__version__ = "Version0.1"


def group_by_field(rows):
    '''
    return {(field1, field2...):[row1, row2...] ....}
    '''
    g_rows = defaultdict(list)

    for row in rows:
        fields = row.keys()
        fields.sort()
        g_rows[tuple(fields)].append(row)
    return g_rows

def save2csv(logger, dump_dir, table, trows):
    """
    save table's rows into csv_files. csv_file like
     'db.table.timestamp.csv'
    :param logger:
    :param dump_dir:
    :param table:
    :param trows:
    :return: None
    """
    try:
        if len(trows) == 0:
            logger.info("table[{}] has no rows to dump".format(table))
            return
        g_rows = group_by_field(trows)
        table_alter = False
        if len(g_rows) > 1:
            logger.warn("table[{}] maybe altered.".format(table))
            table_alter = True
        for fieldnames, rows in g_rows.items():
            save_dir = os.path.join(dump_dir, datetime.strftime(datetime.today(), "%Y%m%d"))
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            suffix = "tmp" if table_alter else "csv"
            csv_file = os.path.join(save_dir,
                "{}.{:.6f}.{}".format(table, time.time(), suffix))
            logger.info("dump to {}, rows:{}".format(csv_file, len(rows)))
            exists = os.path.exists(csv_file)
            with open(csv_file, 'ab+') as fp:
                dict_writer = csv.DictWriter(fp, fieldnames=fieldnames)
                if not exists:
                    dict_writer.writeheader()
                dict_writer.writerows(rows)
            logger.info("{} dump Done.".format(csv_file))
        logger.info("table:{}, rows:{} dump OK!".format(table, len(trows)))
    except:
        logger.error("{} dump Error".format(table), exc_info=True)
        raise


def create_logger(log_dir, verbose):
    if verbose:
        log_file = None
    elif log_dir:
        log_file = os.path.join(log_dir, "dump.log")
    else:
        log_file = "dump.log"

    return mwlogger.MwLogger("dump", log_file)

def main():

    '''
    {'--cache_url': 'redis://127.0.0.1/1',
    '--config_file': False,
    '--dump_out_put': '/tmp/dumps',
    '--help': False,
    '--log_output': None,
    '--max_rows': '1000000',
    '--server_id': '1',
    '--version': False,
    '-v': False,
    'CONFIG_FILE': None}
    '''

    options = docopt(__doc__, version=__version__)

    config_file = options['CONFIG_FILE']
    verbose = options['--verbose']

    if config_file:
        cfg = json.load(file(config_file))
        cache_url = cfg['cache_url']
        server_id = cfg['server_id']
        max_rows = cfg['max_rows']
        log_dir = cfg.get('log_dir', None)
        dump_dir = cfg['dump_dir']
    else:
        cache_url = options['--cache_url']
        server_id = options['--server_id']
        max_rows = options['--max_rows']
        log_dir = options['--log_dir']
        dump_dir = options['--dump_dir']

    dump_tables = options['<table>']

    cache = rcache.Rcache(cache_url, server_id)
    logger = create_logger(log_dir, verbose)

    logger.info("start dump from cache to csv files")

    callback = partial(save2csv, logger, dump_dir)
    cache.dump_t(callback, max_rows, dump_tables)
    logger.info("dump complete!")


if __name__ == "__main__":
    main()
