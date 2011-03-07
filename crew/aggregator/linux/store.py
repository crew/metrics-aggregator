#!/usr/bin/env python
import sys
import json
import hashlib
import logging
import sqlite3
from crew.metrics.httpapi import HttpAPI


class LinuxLastDate(object):

    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.setup_table()

    def setup_table(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS hosts
            (hostname text, lastsecond integer, lastmicrosecond integer,
            PRIMARY KEY (hostname))''')
        self.conn.commit()

    def update_lastdate(self, host, lastsecond, lastmicrosecond):
        c = self.conn.cursor()
        c.execute('''INSERT OR REPLACE INTO hosts
            (hostname, lastsecond, lastmicrosecond) VALUES (?, ?, ?)''',
            [host, lastsecond, lastmicrosecond])
        self.conn.commit()

    def fetch_lastdate(self, host):
        c = self.conn.cursor()
        c.execute('''SELECT lastsecond, lastmicrosecond FROM hosts
            WHERE (hostname = ?) LIMIT 1''', [host])
        row = c.fetchone()
        if row is None:
            return
        return row[0], row[1]


class BoundLinuxLastDate(LinuxLastDate):

    def __init__(self, filename, hostname):
        super(self.__class__, self).__init__(filename)
        self.hostname = hostname

    def update(self, *args):
        self.update_lastdate(self.hostname, *args)

    def fetch(self):
        return self.fetch_lastdate(self.hostname)


class LinuxDataCounter(object):

    def __init__(self, api, db, hostname):
        self.api = api
        self.db = db
        self.hostname = hostname

    @staticmethod
    def get_records_from_json(filename):
        f = open(filename, 'rb')
        for line in f.readlines():
            yield json.loads(line.strip())

    def parse_all(self, filename):
        for record in self.get_records_from_json(filename):
            self.parse(record)

    def parse_from_wtmpjson(self, w):
        last_date = self.db.fetch()
        if last_date is None:
            for record in w.all_records():
                self.parse(record)
        else:
            for record in w.all_records_after(*last_date):
                self.parse(record)

    def store(self, timestamp=None, *args, **kwargs):
        if timestamp is None:
            raise Exception('timestamp is required.')
        ts = timestamp[0] + (timestamp[1] / 1000000.0)
        kwargs['timestamp'] = ts
        logging.info('args: %r kwargs: %r', args, kwargs)
        self.api.store(*args, **kwargs)
        self.db.update(*timestamp)

    def parse(self, record):
        user = record['user']
        login_type = record['type']
        device = record['device']
        device_type = device[:3]
        timestamp = (record['seconds'], record['useconds'])
        last_date = self.db.fetch()
        if last_date is not None and last_date >= timestamp:
            return
        if device_type not in ('tty', 'pts'):
            return
        if login_type == 'BOOT_TIME' and user == 'reboot':
            # Check for reboots.
            self.store(event='reboot', timestamp=timestamp,
                hostname=self.hostname)
            return
        if login_type not in ('USER_PROCESS', 'DEAD_PROCESS'):
            return
        # Transform into a more human readable format.
        event = {
            'USER_PROCESS': 'login',
            'DEAD_PROCESS': 'logout',
        }[login_type]
        fake_id = '%s-%s-%s-%d' % (self.hostname, device, user, record['pid'])
        event_id = hashlib.sha1(fake_id).hexdigest()
        is_local = device_type == 'tty'
        self.store(event=event, is_local=is_local, timestamp=timestamp,
            event_id=event_id, hostname=self.hostname)
