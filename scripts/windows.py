from datetime import datetime
import logging
import time
import os
import errno
from crew.metrics.httpapi import HttpAPI
import gflags
import sys


gflags.DEFINE_string('source', 'data.txt', 'The source filename.')
gflags.DEFINE_integer('interval', 600, 'The wait interval in seconds.')
gflags.DEFINE_string('logfile', 'aggregator.log', 'The appliction log file.')
gflags.DEFINE_string('lastread', 'lastread.log', 'The log file for the '
    'last read position.')
gflags.DEFINE_string('namespace', 'ns', 'the namespace of the metric.')
gflags.DEFINE_string('apikeyfile', 'apikey.txt', 'The path to the file '
    'containing the api key.')
gflags.DEFINE_string('url', 'http://localhost:2000', 'The endpoint.')
FLAGS = gflags.FLAGS


class LastDate(object):
    """Serializer and deserializer for storing the last known datetime
    and the last known seek position (the last read byte)."""

    def __init__(self, filename):
        """
        :param filename: The filename.
        """
        self.filename = filename

    def get(self):
        """
        :returns: A tuple of last date (epoch float) and the last seek
        position.
        """
        try:
            with open(self.filename, 'rb') as f:
                lines = list(f.readlines())
                if len(lines) < 2:
                    return None, 0
                return float(lines[0]), int(lines[1])
        except IOError as e:
            if e.errno == errno.ENOENT:
                # No such file exists.
                return None, 0
            # Other IOError
            logging.error('%s', e)
            raise
        except Exception as e:
            logging.error('%s', e)
            raise

    def set(self, date, seek_pos=0):
        """
        :param date: The date.
        :param seek_pos: The last known seek position.
        """
        with open(self.filename, 'wb') as f:
            f.write('%s\n%d\n' % (date, seek_pos))
            f.flush()


class WindowsData(object):

    def __init__(self, filename, lastdate, ns, apikey, url):
        logging.info('Opening file...')
        self.fd = open(filename, 'rb')
        self.ld = lastdate
        self.ns = ns
        self.apikey = apikey
        self.url = url

    def seek_to_last_known(self):
        _, pos = self.ld.get()
        assert pos >= 0
        if pos:
            stat = os.fstat(self.fd.fileno())
            if stat.st_size >= pos:
                logging.info('Seeking to %d', pos)
                self.fd.seek(pos)
            else:
                logging.warn('File shrunk.')
                self.fd.seek(0)

    @staticmethod
    def parse_date(s):
        FORMAT='%Y %b %d %H:%M:%S'
        x = '%d %s' % (datetime.now().year, s)
        return time.strptime(x, FORMAT)

    @staticmethod
    def parse_line(line):
        # Split the data via spaces
        chunks = line.split(' ')
        # The date string is the first three "chunks"
        dt = WindowsData.parse_date(' '.join(chunks[:3]))
        # The data is the rest of it in HOSTNAME,DOMAIN
        data = [x.split(',') for x in chunks[3:]]
        return dt, data

    def parselines(self):
        self.seek_to_last_known()
        for line in self.fd.readlines():
            # Strip the unicode BOM and newline and carriage return
            line = line.strip('\r\n\xef\xbb\xbf')
            try:
                timestamp, data = self.parse_line(line)
                # The seconds since epoch.
                yield time.mktime(timestamp), data
            except Exception as e:
                logging.error('Error parsing line: >%s<' % line.rstrip('\r\n'))
                logging.error('Exception: %s', e)
                continue

    def store(self):
        api = HttpAPI(namespace=self.ns, apikey=self.apikey, url=self.url)
        for timestamp, data in self.parselines():
            # A successful read
            last, pos = self.ld.get()
            if last:
                logging.info('Last update: %.6f' % last)
            if last is None or timestamp > last:
                for host, domain in data:
                    # Upload data.
                    logging.debug('store: hostname=%s domain=%s timestamp=%s',
                        host.lower(), domain, timestamp)
                    resp = api.store(hostname=host.lower(), domain=domain,
                        timestamp=timestamp)
                    if not resp['code'] == 201:
                        raise Exception('Endpoint error.')
                # TODO get seek pos.
                self.ld.set(str(timestamp), pos)


def main(filename, lastread, ns, apikey, url):
    # Create the generator that produces the last updated date.
    ld = LastDate(lastread)
    wd = WindowsData(filename, ld, ns, apikey, url)
    wd.store()
    # Set the last known seek position.
    # XXX this should really be done in the loop WindowsData.store
    last, _ = ld.get()
    wd.fd.seek(0, os.SEEK_END)
    ld.set(str(last), wd.fd.tell())


if __name__ == '__main__':
    FLAGS(sys.argv)
    interval = FLAGS.interval
    # Setup logging.
    logging.basicConfig(filename=FLAGS.logfile, level=logging.DEBUG)
    filename = FLAGS.source
    lastread = FLAGS.lastread
    ns = FLAGS.namespace
    apikey = open(FLAGS.apikeyfile, 'rb').read().strip()
    url = FLAGS.url
    logging.info('Starting...')
    logging.info('Data filename: %s', filename)
    logging.info('Last-read log filename: %s', lastread)
    logging.info('Namespace: %s', ns)
    logging.info('Endpoint: %s', url)
    logging.info('Poll interval: %d', interval)
    # Start looping.
    count = 0
    while True:
        logging.info('Loop count: %d', count)
        try:
            main(filename, lastread, ns, apikey, url)
        except Exception as e:
            logging.error(e)
        time.sleep(interval)
        count += 1
