#!/usr/bin/env python
# encoding: utf-8

'''
Usage:
  dump2csv.py -s SID -u REDIS_URL -d DIR [-m COUNT] [-l DIR] [-v] [<table>...] [-g GSTORAGE]
  dump2csv.py -c CONFIG_FILE [-v] [<table>...]
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
  -g --gs_url=GSTORSGE          Specify the gs url for storaging dumping files
'''

import csv
import os
import time
from functools import partial
from docopt import docopt
from collections import defaultdict
import json
from Queue import Queue
import threading
import commands

import rcache
import mwlogger
from datetime import datetime

__version__ = "Version0.1"


rqueue = Queue()
glogger = None

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

def save2csv(dump_dir, table, trows, gs_url):
    """
    save table's rows into csv_files. csv_file like
     'db.table.timestamp.csv'
    :param dump_dir:
    :param table:
    :param trows:
    :param gs_url:
    :return: None
    """
    try:
        if len(trows) == 0:
            glogger.info("table[{}] has no rows to dump".format(table))
            return
        g_rows = group_by_field(trows)
        table_alter = False
        if len(g_rows) > 1:
            glogger.warn("table[{}] maybe altered.".format(table))
            table_alter = True
        for fieldnames, rows in g_rows.items():
            save_dir = os.path.join(dump_dir, datetime.strftime(datetime.today(), "%Y%m%d"))
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            suffix = "tmp" if table_alter else "csv"
            csv_file = os.path.join(save_dir,
                "{}.{:.6f}.{}".format(table, time.time(), suffix))
            glogger.info("dump to {}, rows:{}".format(csv_file, len(rows)))
            exists = os.path.exists(csv_file)
            with open(csv_file, 'ab+') as fp:
                dict_writer = csv.DictWriter(fp, fieldnames=fieldnames)
                if not exists:
                    dict_writer.writeheader()
                dict_writer.writerows(rows)
            glogger.info("{} dump Done.".format(csv_file))
            if gs_url:
                glogger.info("dispatch {} to rqueue".format(csv_file))
                rqueue.put(csv_file)
        glogger.info("table:{}, rows:{} dump OK!".format(table, len(trows)))
    except:
        glogger.error("{} dump Error".format(table), exc_info=True)
        raise


def create_logger(log_dir, verbose):
    if verbose:
        log_file = None
    elif log_dir:
        log_file = os.path.join(log_dir, "dump.log")
    else:
        log_file = "dump.log"

    return mwlogger.MwLogger("dump", log_file)


def _upload_by_date(csv_file, gs_url):
    '''
    -m: multi-thread
    -n: skip files exist in gstorage
    -L: record the file uploaded info
    '''
    csv_pdir = os.path.dirname(csv_file)
    date = os.path.basename(csv_pdir)
    cmd = "gsutil -m cp -n -L {log} -r {src} {dst}".format(
        log=os.path.join(csv_pdir, "upload.info"),
        src=os.path.join(csv_pdir, "*.csv"),
        dst=os.path.join(gs_url, date)
    )
    for tries in range(3):
        ret, out = commands.getstatusoutput(cmd)
        if ret == 0 or tries == 2:
            break
        else:
            time.sleep(2)
    return ret >> 8, out


def async_upload2gstorage_ex(gs_url):
    while 1:
        csv_file = rqueue.get()
        if csv_file is None:
            glogger.info("all csv_files upload ok, thread exit!")
            break
        else:
            ret, output = _upload_by_date(csv_file, gs_url)
            if ret != 0:
                glogger.error("upload failed, return code:{}, out:{}".format(ret, output))
            else:
                glogger.info("upload ok!")


def group_lst(csvs):
    to_ups = []
    pre_date = cur_date = None
    for csv_f in csvs:
        csv_pdir = os.path.dirname(csv_f)
        cur_date = os.path.basename(csv_pdir)
        if pre_date is None or pre_date == cur_date:
            to_ups.append(csv_f)
            if len(to_ups) >= 8:
                to_ups = yield to_ups
                yield  # yield to send
        else:
            to_ups = yield to_ups
            yield  # yield to send
            to_ups.append(csv)
        pre_date = cur_date
    to_ups = yield to_ups
    yield  # yield to send


def upload_csvs(gs_url, csvs):
    gen = group_lst(csvs)
    for gcsvs in gen:
        if len(gcsvs) == 0:
            break
        glogger.info("start uploading {} to gstorage".format(str(gcsvs)))
        csv_pdir = os.path.dirname(gcsvs[0])
        date = os.path.basename(csv_pdir)
        cmd = "gsutil -m cp -n -L {log} {src} {dst}/".format(
            log=os.path.join(csv_pdir, "upload.info"),
            src=' '.join(gcsvs),
            dst=os.path.join(gs_url, date)
        )
        ret, out = _run_cmd_retry(cmd, 3)
        if ret == 0:
            glogger.info("upload successfully, files count:{}".format(len(gcsvs)))
            gen.send([])
        else:
            # TODO: need alarm
            glogger.error("{} run error. ret:{}, out:{}".format(
                cmd, ret, out
            ))
            # parse success from log_file, upload.info's schema:
            # Source,Destination,Start,End,Md5,UploadId,Source Size,Bytes Transferred,Result,Description
            log = os.path.join(csv_pdir, "upload.info")
            with open(log) as fp:
                _ups = list(csv.DictReader(fp))
            sources = [up['Source'].strip("file://") for up in _ups]
            gen.send(list(set(gcsvs) - set(sources)))

def _run_cmd_retry(cmd, tries):
    for tries in range(tries):
        ret, out = commands.getstatusoutput(cmd)
        if ret == 0 or tries == tries - 1:
            break
        else:
            time.sleep(2)
    return ret >> 8, out


def async_upload2gstorage(gs_url):
    csvs = []
    while 1:
        while not rqueue.empty():
            csvs.append(rqueue.get())
        if not csvs:
            time.sleep(1)
            continue
        if csvs[-1] is None:
            del csvs[-1]
            break
        upload_csvs(gs_url, csvs)
        del csvs[:]
    if len(csvs) > 0:
        upload_csvs(gs_url, csvs)

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
    config_file = options['--config_file']
    verbose = options['--verbose']

    if config_file:
        cfg = json.load(file(config_file))
        cache_url = cfg['cache_url']
        server_id = cfg['server_id']
        max_rows = cfg['max_rows']
        log_dir = cfg.get('log_dir', None)
        dump_dir = cfg['dump_dir']
        gs_url = cfg.get('gs_url', None)
    else:
        cache_url = options['--cache_url']
        server_id = options['--server_id']
        max_rows = options['--max_rows']
        log_dir = options['--log_dir']
        dump_dir = options['--dump_dir']
        gs_url = options['--gs_url']


    dump_tables = options['<table>']

    cache = rcache.Rcache(cache_url, server_id)
    global glogger
    glogger = create_logger(log_dir, verbose)

    if gs_url:
        gs_url = os.path.join(gs_url, str(server_id))
        upload_thr = threading.Thread(target=async_upload2gstorage, args=(gs_url,))
        upload_thr.setDaemon(True)
        upload_thr.start()
        glogger.info("upload csv files to {} thread running...".format(gs_url))

    glogger.info("start dump from cache to csv files")

    callback = partial(save2csv, dump_dir, gs_url=gs_url)
    cache.dump_t(callback, max_rows, dump_tables)
    glogger.info("dump complete!")
    if gs_url:
        glogger.info("wait csv files uploading completed......")
        rqueue.put(None)
        upload_thr.join()


if __name__ == "__main__":
    main()
