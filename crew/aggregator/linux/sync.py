#!/usr/bin/env python
import os
import sys
import errno
import shutil
import sqlite3
import logging
import datetime
import tempfile
import threading
import subprocess


def fetch_date(hostname, key='/home/lee/.ssh/id_dsa'):
    p = subprocess.Popen(['ssh', '-o', 'StrictHostKeyChecking=no', '-q',
        '-i', key, hostname, 'date', '+%Y-%m-%d'], stdout=subprocess.PIPE)
    y, m, d = map(int, p.stdout.read().split('-'))
    return datetime.date(y, m, d)


def previous_month(d):
    """
    :param d: A `datetime.date` object.
    :returns: A `datetime.date` object which has the previous month.
    """
    m = d.month - 1
    if m == 0:
        return datetime.date(d.year - 1, 12, 1)
    return datetime.date(d.year, m, 1)


def _scp(key, hostname, filename, dest):
    return subprocess.Popen(['scp', '-o', 'StrictHostKeyChecking=no',
        '-q', '-i', key, '%s:%s' % (hostname, filename), dest])


def scp(key, hostname, dest):
    return _scp(key, hostname, '/var/log/wtmp', dest)


def scp_archive(key, hostname, dest):
    return _scp(key, hostname, '/var/log/wtmp.1.gz', dest)


class DownloadWTMP(threading.Thread):

    def __init__(self, *args):
        threading.Thread.__init__(self, args=args, target=self.target)

    def target(self, hostname, dest, dest_stat, dest_dir, key):
        tmpfd, tmppath = tempfile.mkstemp(suffix='.tmp', prefix=hostname)
        logging.debug('%s exists, saving to %s', dest, tmppath)
        # Wait on the download to determine the size.
        scp(key, hostname, tmppath).wait()
        # Check if the file shrunk (a.k.a rotated)
        if dest_stat.st_size > os.fstat(tmpfd).st_size:
            logging.info('%s shrunk, it has been rotated.', dest)
            # Determine previous month and archive.
            d = previous_month(fetch_date(hostname, key))
            a = os.path.join(dest_dir, 'wtmp-%d-%02d.gz' % (d.year, d.month))
            logging.info('Archiving to %s', a)
            scp_archive(key, hostname, a)
        logging.debug('Moving %s to %s', tmppath, dest)
        shutil.move(tmppath, dest)

    def wait(self):
        return self.join()


def fetch_wtmp(hostname, dest_dir, key='/home/lee/.ssh/id_dsa'):
    """
    :param hostname: The hostname.
    :param dest_dir: The destination directory.
    """
    dest = os.path.join(dest_dir, 'wtmp')
    try:
        dest_stat = os.stat(dest)
        p = DownloadWTMP(hostname, dest, dest_stat, dest_dir, key)
        p.start()
    except OSError as e:
        if e.errno == errno.ENOENT:
            # No file or directory
            p = scp(key, hostname, dest)
        else:
            logging.error(e)
            raise e
    return p


def get_hosts_from_file(filename):
    with open(filename, 'rb') as f:
        for line in f.readlines():
            yield line.strip()


def sync_linux_host(host, dir_name):
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
    return fetch_wtmp(host, dir_name)


def sync_linux(hosts, sync_dir, block=True):
    if not os.path.isdir(sync_dir):
        os.mkdir(sync_dir)
    procs = []
    for host in hosts:
        d = os.path.join(sync_dir, host)
        if not os.path.isdir(d):
            os.mkdir(d)
        p = fetch_wtmp(host, d)
        if block:
            p.wait()
        procs.append(p)
    if not block:
        for p in procs:
            p.wait()
