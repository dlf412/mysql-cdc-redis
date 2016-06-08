#!/usr/bin/env python
# encoding: utf-8

import redis
import time
import threading
import os

class SaveIgnore(Exception):
    pass

class FullError(Exception):
    pass

class Rcache(object):
    def __init__(self, redis_url, mysql_server_id):
        self._redis_url = redis_url
        self._client = redis.from_url(redis_url)
        self._server_id = mysql_server_id
        self._key_prefix = "{}#".format(mysql_server_id)
        self._locking_key = "{}#locking".format(mysql_server_id)
        self._row_ids_prefix = "{}#row_ids#".format(mysql_server_id)
        self._table_key_offset = len(self._row_ids_prefix)
        self._lock_timer = None

    @property
    def size(self):
        return self._client.dbsize()


    def table_size(self, table):
        row_ids_key = self._row_ids_prefix + table
        return self._client.scard(row_ids_key)


    def _lock(self, ex=60):
        '''
        return true if locking_key not exists
        and set the locking key for avioding others get it
        '''
        return bool(self._client.set(self._locking_key, "",
            ex=ex, nx=True))


    def _free_lock(self):
        self._client.delete(self._locking_key)


    def _fresh_lock(self, ex=60):
        self._client.expire(self._locking_key, ex),
        self._lock_timer = threading.Timer(ex - 10,
                self._fresh_lock, (ex,))
        self._lock_timer.setDaemon(True)
        self._lock_timer.start()


    def dump_r(self, callback):
        '''
        callback args: (table, row)
        dump data row by row
        '''
        if not callable(callback):
            return
        try:
            self._get_lock()
            self._fresh_lock()
            for (table, row) in self:
                callback(table, row)
            self.clear()
        finally:
            self._unfresh_lock()
            self._free_lock()



    def _clear_table(self, table):
        row_ids_key = self._row_ids_prefix + table
        rids = self._client.smembers(row_ids_key)
        keys = ["{}{}.{}".format(self._key_prefix, table, rid) for rid in rids]
        keys.append(row_ids_key)
        self._client.delete(*keys)

    def dump_t(self, callback, max_rows=0, dump_tables=None):
        '''
        callback args: (table, rows)
        dump data table by table
        '''
        if not callable(callback):
            return
        try:
            self._get_lock()
            self._fresh_lock()
            if not dump_tables:
                for (over, table, rows) in self.iter_rows(max_rows):
                    callback(table, rows)
                    if over:
                        self._clear_table(table)
            else:
                for table in dump_tables:
                    for over, rows in self.iter_rows_by_table(table):
                        callback(table, rows)
                        if over:
                            self._clear_table(table)
        finally:
            self._unfresh_lock()
            self._free_lock()

    def clear(self):
        self._client.flushdb()

    def _unfresh_lock(self):
        if self._lock_timer:
            self._lock_timer.cancel()
            del self._lock_timer
            self._lock_timer = None


    def _iter_row_ids(self):
        return self._client.scan_iter(match="{}*".format(self._row_ids_prefix),
                count=100)


    def tables(self):
        '''
        return [schema.table1, schema.table2, .....]
        '''
        keys = list(self._iter_row_ids())
        return [table[self._table_key_offset:] for table in keys]


    def __iter__(self):
        """
        return a generator like (table, row)
        """
        for row_ids in self._iter_row_ids():
            for row_id in self._client.sscan_iter(row_ids, count=1000):
                table = row_ids[self._table_key_offset:]
                row_key = "{}{}.{}".format(self._key_prefix, table, row_id)
                row = self._client.hgetall(row_key)
                yield (table, row)


    def iter_rows(self, max_rows=0):
        '''
        return a generator like (table, rows)
        set max_rows for avoid OOM
        yield (iter_over_flag, table, rows)
        '''
        for row_ids in self._iter_row_ids():
            rows = []
            table = row_ids[self._table_key_offset:]
            for row_id in self._client.sscan_iter(row_ids, count=1000):
                row_key = "{}{}.{}".format(self._key_prefix, table, row_id)
                row = self._client.hgetall(row_key)
                rows.append(row)
                if max_rows and len(rows) >= max_rows:
                    yield (False, table, rows)
                    del rows[:]
            yield (True, table, rows)


    def iter_rows_by_table(self, table, max_row=0):
        '''
        return a generator like [row1, row2...]
        '''
        rows = []
        row_ids_key = "{}{}".format(self._row_ids_prefix, table)
        for row_id in self._client.sscan_iter(row_ids_key, count=1000):
            row_key = "{}{}.{}".format(self._key_prefix, table, row_id)
            rows.append(self._client.hgetall(row_key))
            if max_row and len(rows) >= max_row:
                yield False, rows
                del rows[:]
        yield True, rows


    def _get_lock(self, block=True, interval=1):
        if block:
            while not self._lock():
                time.sleep(interval)
            return True
        else:
            return self._lock()


    @staticmethod
    def _gen_rid(row, primary_key):
        if primary_key is None:
            return None
        elif isinstance(primary_key, tuple) or isinstance(primary_key, list):
            return '&'.join([str(row[key]) for key in primary_key])
        else:
            return row[primary_key]


    @staticmethod
    def _merge_row(old, new):
        '''
        merge mode:
        None & new : new
        insert & delete : None
        insert & update : insert
        delete & insert : update

        delete & update : error
        update & insert : error
        '''
        if not old:
            return new

        old_act = old["cdc_action"]
        new_act = new["cdc_action"]

        if new_act == "delete" and old_act == "insert":
            return None
        elif new_act == "update" and old_act == "insert":
            new["cdc_action"] = "insert"
        elif new_act == "insert" and old_act == "delete":
            new["cdc_action"] = "update"
        elif new_act == "insert" and old_act == "update":
            new["cdc_action"] = "update"  # maybe truncate table happened
        return new

    def save(self, table, primary_key, rows):
        '''
        save changed data
        table with schema. like: "test.test4"
        row must has two columns:
          "cdc_action": "delete/insert/update"
          "cdc_ts": timestamp
        return size of  saved rows
        '''
        try:
            self._get_lock()
            table_key = "{}{}".format(self._key_prefix, table)
            row_ids_key = "{}{}".format(self._row_ids_prefix, table)
            if not isinstance(rows, list):
                rows = [rows]

            for row in rows:
                rid = self._gen_rid(row, primary_key)
                if rid is None:
                    raise SaveIgnore(
                        "Do not support table[{}] without primary_key".format(
                            table))
                key = "{}.{}".format(table_key, rid)
                old_row = self._client.hgetall(key)
                merged_row = self._merge_row(old_row, row)
                if merged_row:
                    self._client.hmset(key, merged_row)
                    self._client.sadd(row_ids_key, rid)
                else:
                    self._client.delete(key)
                    self._client.srem(row_ids_key, rid)
        except redis.ResponseError, err:
            if "OOM command not allowed" in str(err):
                raise FullError(str(err))
        finally:
            self._free_lock()






