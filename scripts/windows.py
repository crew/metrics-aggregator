from datetime import datetime
import logging
import time
import os
import errno
from crew.metrics.httpapi import HttpAPI


class LastDate(object):

    def __init__(self, filename):
        self.filename = filename

    def get(self):
        try:
            with open(self.filename, 'rb') as f:
                lines = list(f.readlines())
                if len(lines) < 2:
                    return None, 0
                return float(lines[0]), int(lines[1])
        except IOError, e:
            if e.errno == errno.ENOENT:
                # No such file exists.
                return None, 0
            # Other IOError
            logging.error('%s', e)
            raise
        except Exception, e:
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

    def __init__(self, filename, lastdate):
        logging.info('Opening file...')
        self.fd = open(filename, 'rb')
        self.ld = lastdate

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
            except Exception, e:
                logging.error('Error parsing line: >%s<' % line.rstrip('\r\n'))
                logging.error('Exception: %s', e)
                continue

    def store(self):
        api = HttpAPI(namespace='ns', apikey='apikey', url='http://localhost:2000')
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
                    api.store(hostname=host.lower(), domain=domain,
                        timestamp=timestamp)
                # TODO get seek pos.
                self.ld.set(str(timestamp), pos)


def main():
    # TODO Command line options.
    logging.basicConfig(filename='/tmp/aggregator.log', level=logging.DEBUG)
    filename = 'data.txt'
    lastread_filename = 'lastread.log'
    logging.info('Starting...')
    logging.info('Data filename: %s', filename)
    logging.info('Last-read log filename: %s', lastread_filename)
    # Create the generator that produces the last updated date.
    ld = LastDate(lastread_filename)
    wd = WindowsData(filename, ld)
    wd.store()
    # Set the last known seek position.
    # XXX this should really be done in the loop WindowsData.store
    last, _ = ld.get()
    wd.fd.seek(0, os.SEEK_END)
    ld.set(str(last), wd.fd.tell())
    logging.info('Exiting...')


if __name__ == '__main__':
    main()
