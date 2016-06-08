
#!/usr/bin/env python
# encoding: utf-8

'''
Usage:
  loadcsv.py <csv_file> <primary_key> [<primary_key>...] -s SID -u REDIS_URL [-l DIR] [-v]
  loadcsv.py  <csv_file> <primary_key> [<primary_key>...] -c CONFIG_FILE [-v]
  loadcsv.py (-h | --help | --version)

Arguments:
  <csv_file>                    Specify csv file to load
  <primary_key>                 Specify csv file's primary_key, support multiple keys

Options:
  -h --help                     Show this help message and exit
  --version                     Show version and exit
  -c --config_file=CONFIG_FILE  Specify config file
  -v --verbose                  Print the running status message
  -s --server_id=SID            Specify mysql server id
  -u --cache_url=REDIS_URL      Specify the redis cache url like:
                                "redis://host:port/db"
  -l --log_dir=DIR              Specify the dir of logging
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

__version__ = "v0.1"
__author__ = "deng_lingfei"


def main():
    options = docopt(__doc__, version=__version__)
    config_file = options['--config_file']
    verbose = options['--verbose']

    csv_file = options['<csv_file>']
    primary_key = options['<primary_key>']

    if config_file:
        cfg = json.load(file(config_file))
        cache_url = cfg['cache_url']
        server_id = cfg['server_id']
        log_dir = cfg.get('log_dir', None)
    else:
        cache_url = options['--cache_url']
        server_id = options['--server_id']
        log_dir = options['--log_dir']

    cache = rcache.Rcache(cache_url, server_id)
    logger = create_logger(log_dir, verbose)

    logger.info("start load {} to redis".format(csv_file))

    table = _get_table_name(csv_file)
    rows = list(readcsv(csv_file))
    try:
        cache.save(table, primary_key, rows)
    except Exception as err:
        logger.error("load {} failed, reason is {}".format(table, err))
    logger.info("load {} ok, rows:{} ".format(csv_file, len(rows)))

def create_logger(log_dir, verbose):
    if verbose:
        log_file = None
    elif log_dir:
        log_file = os.path.join(log_dir, "load.log")
    else:
        log_file = "load.log"

    return mwlogger.MwLogger("load", log_file)

def _get_table_name(csv_file):
    '''
    support csv_file like "db.table.csv" and "db.table.timestamp.csv"
    :param csv_file:
    :return: db.table
    '''
    return ".".join(os.path.basename(csv_file).split(".")[:2])

def readcsv(csv_file):
    with open(csv_file) as fp:
        for row in csv.DictReader(fp):
            yield row

if __name__ == '__main__':
    main()


