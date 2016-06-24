#!/usr/bin/env python
# encoding: utf-8

'''
Usage:
  bq_schema_from_mysql.py [-h HOST] -u USER -p PASSWORD [-P PORT] <database> [<table>...] -s SID -S SYSTEM
  bq_schema_from_mysql.py (--help | --version)

Arguments:
  <database>                Specify the mysql database
  <table>                   Specify the tables. It will translate all table's schema
                            to bigquery_schema if specify nothing
Options:
  --help                    Show this help message and exit
  --version                 Show version and exit
  -h --host=HOST            Specify mysqld server host [default: localhost]
  -u --user=USER            Specify connection db's user
  -p --password=PASSWORD    Specify connection db's password
  -P --port=PORT            Specify mysqld server port [default: 3306]
  -s --server_id=SID        Specify mysqld server id
  -S --system=SYSTEM        Specify the app system. etc: VTWeb, MediaWise.
'''

__author__ = 'deng_lingfei'

'''
bigquery schema reference:
[
  {
    "name": "time_micros",
    "type": "integer",
    "mode": "NULLABLE"
  },

  {
    "name": "c_ip",
    "type": "string",
    "mode": "NULLABLE"
  }
]
'''

import pymysql
import os
import sys
import json
from docopt import docopt
from collections import defaultdict

schema_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bq_schema")

print schema_dir

type_mapping = {
    "tinyint" : "integer",	# A very small integer
    "smallint": "integer",	# A small integer
    "mediumint": "integer", # A medium-sized integer
    "int": "integer",	    # A standard integer
    "bigint": "integer", 	# A large integer
    "decimal": "float",     # A fixed-point number
    "float": "float",       # A single-precision floating point number
    "double": "float",      # A double-precision floating point number
    "bit": "integer",       # A bit field

    "char": "string", # a fixed-length nonbinary (character) string
    "varchar": "string", # a variable-length non-binary string
    "binary": "string", # a fixed-length binary string
    "varbinary": "string", # a variable-length binary string
    "tinyblob": "string", # a very small blob (binary large object)
    "blob": "string", # a small blob
    "mediumblob": "string", # a medium-sized blob
    "longblob": "string", # a large blob
    "tinytext": "string", # a very small non-binary string
    "text": "string", # a small non-binary string
    "mediumtext": "string", # a medium-sized non-binary string
    "longtext": "string", # a large non-binary string
    "enum": "string", # an enumeration; each column value may be assigned one enumeration member
    "set": "string", # a set; each column value may be assigned zero or more set members

    "date": "string", # a date value in ‘ccyy-mm-dd’ format
    "time": "string", # a time value in ‘hh:mm:ss’ format
    "datetime": "timestamp", # a date and time value in ‘ccyy-mm-dd hh:mm:ss’ format
    "timestamp": "timestamp", # a timestamp value in ‘ccyy-mm-dd hh:mm:ss’ format
    "year": "string" # a year value in CCYY or YY format
}

def type_trans(mysql_type):
    return type_mapping.get(mysql_type, "string")

def show_table(cur):
    cur.execute("show tables")
    return [one[0] for one in cur.fetchall()]

def trans_schema(cur):
    '''bigquery schema reference:
    [
        {
            "name": "time_micros",
            "type": "integer",
            "mode": "NULLABLE"
        },

        {
            "name": "c_ip",
            "type": "string",
            "mode": "NULLABLE"
        }
    ]
    '''
    pre_table = cur_table = None
    table_schema = []
    for (column, data_type, table, _) in cur:
        cur_table = table
        if pre_table != cur_table and pre_table is not None:
            yield (pre_table, table_schema)
            del table_schema[:]
        pre_table = cur_table
        table_schema.append({"name": column, "type": type_trans(data_type)})
    yield (cur_table, table_schema)



if __name__ == '__main__':
    args = docopt(__doc__, version="v0.1")
    host = args['--host']
    port = int(args['--port'])
    user = args['--user']
    password = args['--password']
    database = args['<database>']
    tables = args['<table>']
    sid = args['--server_id']
    system = args['--system']

    cli = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cur = cli.cursor()

    if len(tables) == 0:
        tables = show_table(cur)

    if len(tables) == 0:
        print "no any table"
        exit(0)

    db_path = os.path.join(schema_dir, system, sid, database)
    if not os.path.exists(db_path):
        os.makedirs(db_path)

    tables = ["'{}'".format(t) for t in tables]

    sql = "SELECT column_name, data_type, table_name, table_schema FROM information_schema.columns  where " \
          "table_schema='{}' and table_name in ({});".format(database, ','.join(tables))
    cur.execute(sql)
    for table, schema in trans_schema(cur):
        with open(os.path.join(db_path, table), 'w') as fp:
            schema.append({"name": "cdc_action", "type": "string"})
            schema.append({"name": "cdc_ts", "type": "string"})
            schema.sort(key=lambda k:k["name"])
            json.dump(schema, fp)
        print "generate {}.{} google cloud bigquery's schema OK!".format(database, table)
    cur.close()
    cli.close()
