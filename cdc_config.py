#!/usr/bin/env python
# encoding: utf-8
 
 
# redis for saving binlog file and position
# please reverse the db + 1 for saving mysql changed data
redis_url = "redis://127.0.0.1/0"
cache_url = "redis://127.0.0.1/1"
 
# mysql server id
server_id = 1 
 
# mysql connection setting
mysql_settings = {'host': '192.168.1.34',
                  'port': 3306,
                  'user': 'mediawise',
                  'passwd': '123'}
 
# watch databases setting
# it can be set None or a tuple.
# Watch all databases if set None
# value format: None or database tuple
schemas = None
 
# watch tables setting like databases setting if with primary_key
# value format: None or table tuple
#tables = ("task",)
tables = None
# please set the unique key if without primary_key
# value format:
# {} or {"table_name": fields tuple, ....}
tables_without_primary_key = {"db_test.task_test": ("uuid", )}
 
# Read on binlog stream is blocking, default is True
# If set to False, the cdc problem will exit when reading binlog over
blocking = True
 
# watch event setting
events = ["insert", "update", "delete"]
 
# turn off dumping trigger if set to 0
cache_max_rows = 2000000
 
dump_command = "python dump2csv.py -c dump.conf"
 
log_level = "INFO"
 
binlog_max_latency = 60000
