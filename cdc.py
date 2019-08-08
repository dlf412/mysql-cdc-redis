#!/usr/bin/env python                                                                                                                                                                  
# encoding: utf-8
 
'''
required:
    1. pymysql
    2. pymysqlreplication
    3. redis: Records log_file and log_postion for resuming
 
mysql configures: /etc/mysql/my.cnf
   server-id       = 1
   log_bin         = /var/log/mysql/mysql-bin.log
   expire_logs_days    = 10
   max_binlog_size         = 100M
   binlog_format=row
'''
 
from __future__ import absolute_import
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
import redis
import commands
import time
 
import rcache
from cdc_config import (
    redis_url, cache_url, server_id,
    mysql_settings, schemas, tables,
    tables_without_primary_key,
    blocking, events, dump_command,
    log_level, cache_max_rows, binlog_max_latency
    )
import mwlogger
 
logger = mwlogger.MwLogger('mysql-cdc', 'syslog',
            log_level=log_level, facility='local0')
 
def _trans_events(ets):
    aevents = {
            "insert": WriteRowsEvent,
            "update": UpdateRowsEvent,
            "delete": DeleteRowsEvent
            }
    return [aevents[et] for et in ets]
 
def _trigger_dumping():
    status, output = commands.getstatusoutput(dump_command)
    if status != 0:
        # alarm
        logger.error("dump failed: {}".format(output))
    else:
        logger.info("dump OK!")
    return status
 
def _get_row_values(binlogevent):
    vals_lst = []
    for row in binlogevent.rows:
        if isinstance(binlogevent, DeleteRowsEvent):
            vals = row["values"]
            vals['cdc_action'] = 'delete'
        elif isinstance(binlogevent, UpdateRowsEvent):
            vals = row["after_values"]
            vals['cdc_action'] = 'update'
        elif isinstance(binlogevent, WriteRowsEvent):
            vals = row["values"]
            vals['cdc_action'] = 'insert'
        vals['cdc_ts'] = binlogevent.timestamp
        vals_lst.append(vals)
    return vals_lst
 
def main():
    rclient = redis.from_url(redis_url)
    cache = rcache.Rcache(cache_url, server_id)
 
    log_file = rclient.get("log_file")
    log_pos = rclient.get("log_pos")
    log_pos = int(log_pos) if log_pos else None
 
    only_events = _trans_events(events)
    only_events.append(RotateEvent)
 
    stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=server_id,
        blocking=blocking,
        only_events=only_events,                                                                                                                                                       
        only_tables=tables,
        only_schemas=schemas,
        resume_stream=True,  # for resuming
        freeze_schema=False, # do not support alter table event for faster
        log_file=log_file,
        log_pos=log_pos)
    row_count = 0
 
    for binlogevent in stream:
        if int(time.time()) - binlogevent.timestamp > binlog_max_latency:
            logger.warn("latency[{}] too large".format(
                int(time.time()) - binlogevent.timestamp))
        logger.debug("catch {}".format(binlogevent.__class__.__name__))
        if isinstance(binlogevent, RotateEvent):  #listen log_file changed event
            rclient.set("log_file", binlogevent.next_binlog)
            rclient.set("log_pos", binlogevent.position)
            logger.info("log_file:{}, log_position:{}".format(
                binlogevent.next_binlog, binlogevent.position))
        else:
            row_count += 1
            table = "%s.%s" % (binlogevent.schema, binlogevent.table)
            vals_lst = _get_row_values(binlogevent)
            if not binlogevent.primary_key:
                binlogevent.primary_key = tables_without_primary_key.get(table, None)
                if not binlogevent.primary_key:
                    logger.error("{} has neither primary_key nor unique key configure".format(table))
                    exit(1)                    
            try:
                cache.save(table, binlogevent.primary_key, vals_lst)
                logger.debug("save {} {} rows to cache".format(
                    table, len(vals_lst)))
            except rcache.SaveIgnore as err:
                logger.warning(str(err))
            except rcache.FullError as err:
                logger.info("cache OOM occured: {}.trigger dump command".format(
                    str(err)))
                dump_code = _trigger_dumping()
                cache.save(table, binlogevent.primary_key, vals_lst)
            if cache_max_rows and cache.size > cache_max_rows:
                logger.info("cache size:{} >= {}, trigger dumping".format(
                   cache.size, cache_max_rows))
                _trigger_dumping()
            rclient.set("log_pos", binlogevent.packet.log_pos)
        if row_count % 1000 == 0:
            logger.info("save {} changed rows".format(row_count))
 
    stream.close()
 
if __name__ == "__main__":
    main()      
