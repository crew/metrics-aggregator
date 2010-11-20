from datetime import datetime
import logging
import time


def parse_date(s):
    FORMAT='%Y %b %d %H:%M:%S'
    x = '%d %s' % (datetime.now().year, s)
    return time.strptime(x, FORMAT)


def parse_line(line):
    # Split the data via spaces
    chunks = line.split(' ')
    # The date string is the first three "chunks"
    dt = parse_date(' '.join(chunks[:3]))
    # The data is the rest of it in HOSTNAME,DOMAIN
    data = [x.split(',') for x in chunks[3:]]
    return dt, data


def last_date_gen(filename):
    """
    Creates a generator that fetchs the last updated date.
    """
    def g():
        try:
            with open(filename, 'r') as f:
                line = f.read()
                if not line:
                    return None
                return float(line)
        except IOError, e:
            if e.errno == 2:
                return None
            logging.error('%s', e)
            raise e
        except Exception, e:
            logging.error('%s', e)
            raise e
    return g


def write_last_date(filename, date):
    """
    :param filename: The filename.
    :param date: The date.
    """
    with open(filename, 'w') as f:
        f.write(date)


def main():
    logging.basicConfig(filename='/tmp/aggregator.log', level=logging.DEBUG)
    filename = 'data.txt'
    lastread_filename = 'lastread.log'
    logging.info('Starting...')
    logging.info('Data filename: %s', filename)
    logging.info('Last-read log filename: %s', lastread_filename)
    # Create the generator that produces the last updated date.
    last_date = last_date_gen(lastread_filename)
    # Get the last updated date.
    last = last_date()
    if last:
        logging.info('Current last date: %.6f' % last)
    else:
        logging.warn('Running for the first time.')
    logging.info('Opening file...')
    try:
        f = open(filename, 'r')
    except Exception, e:
        logging.error('Error opening file: %s', e)
        raise
    logging.info('Opened: %s' % f.name)
    lineno = 0
    for line in f.readlines():
        # Strip the unicode BOM and newline and carriage return
        line = line.strip('\r\n\xef\xbb\xbf')
        lineno += 1
        try:
            timestamp, data = parse_line(line)
        except Exception, e:
            logging.error('Error parsing line: >%s<' % line.rstrip('\r\n'))
            logging.error('Exception: %s', e)
        # The seconds since epoch.
        since_epoch = time.mktime(timestamp)
        logging.info('Date: %.6f' % since_epoch)
        last = last_date()
        if last:
            logging.info('Last update: %.6f' % last)
        if last is None or since_epoch > last:
            logging.info('Calling the backend...')
            # Upload data.
            print last, 'write! %s' % data
            write_last_date('lastread.log', str(since_epoch))
        else:
            logging.info('Already read: %s ...' % line.strip()[:60])
    logging.info('Exiting...')


if __name__ == '__main__':
    main()
