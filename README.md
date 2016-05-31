# mysql-cdc-redis
capture changed data from mysql using binlog
* functions:                                                                                                                                                                           
 1. capture mysql database changed data from binlog and save to redis
 2. support dump the changed data from redis to xml files
 
* configure files:
 1. cdc_config.py: 
     You can set the mysql connection settings, schemas, tables, binlog events and so on.
     Please see the cdc_config.py's comments for details
 
 2. dump.conf:
    Setting the dump2csv.py running arguments and options 
 
* mysqld config:
   server-id       = 1
   log_bin         = /var/log/mysql/mysql-bin.log
   expire_logs_days    = 10
   max_binlog_size         = 100M
   binlog_format=row
 
* requires:
   See requirements.txt. Run "sudo pip install -r requirements.txt" to install all deps
 
* How to run?
 1. changed data capture: python cdc.py 
 2. dump to csv files:  python dump2csv.py -c dump.conf [table1] [table2]... 
 
* watch running logging
 1. cdc.py:  syslog 
 2. dump2csv.py:  dump.log in dump.conf
 
* redis
 1. Using 2GB at least 
 2. HA
 3. turn on AOF configure
