#!/usr/bin/env python
import os
import sys
import logging
from crew.metrics.httpapi import HttpAPI
from crew.aggregator.linux import sync, wtmpjson, store
import gflags


gflags.DEFINE_string('dbfile', 'crewlinux.sqlite3', 'The path to the '
    'database file.')
gflags.DEFINE_string('apikey', 'apikey', 'The apikey.')
gflags.DEFINE_string('machines', None, 'Path to the list of linuxmachines.')
gflags.DEFINE_string('syncdir', 'wtmp', 'The directory to put wtmp files.')
gflags.DEFINE_string('url', 'http://localhost:2000', 'The url of '
    'the flamongo endpoint.')
gflags.DEFINE_string('namespace', 'linux', 'The namespace.')
FLAGS = gflags.FLAGS


def main(hosts, sync_dir, ns, apikey, url, db_filename):
    hosts = list(hosts)
    sync.sync_linux(hosts, sync_dir)
    api = HttpAPI(namespace=ns, apikey=apikey, url=url)
    for h in hosts:
        logging.info('Processing %s', h)
        w = wtmpjson.WTMP(os.path.join(sync_dir, h))
        db = store.BoundLinuxLastDate(db_filename, h)
        store.LinuxDataCounter(api, db, h).parse_from_wtmpjson(w)


if __name__ == '__main__':
    FLAGS(sys.argv)
    logging.basicConfig(level=logging.DEBUG)
    if FLAGS.machines:
        hosts = sync.get_hosts_from_file(FLAGS.machines)
    else:
        from crew.aggregator.constants import LINUXMACHINES
        hosts = LINUXMACHINES
    main(hosts, FLAGS.syncdir, FLAGS.namespace, FLAGS.apikey, FLAGS.url,
        FLAGS.dbfile)
