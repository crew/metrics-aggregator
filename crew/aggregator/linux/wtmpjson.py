import os
import re
import json
import datetime
import subprocess


class WTMP(object):
    """
    The directory should at least have a file named ``wtmp``. There also may
    be archived wtmp files (by month) in this format: ``wtmp-YYYY-MM.gz``
    where YYYY is the year and MM is the 0-prefix month.
    """

    def __init__(self, dir_name):
        self.dir_name = dir_name

    def _read_wtmp(self, filename):
        if filename.endswith('.gz'):
            return self._read_wtmp_gz(filename)
        return self._read_wtmp_plain(filename)

    def _read_wtmp_plain(self, filename):
        p = subprocess.Popen(['wtmp2json', filename], stdout=subprocess.PIPE)
        for line in p.stdout.readlines():
            yield json.loads(line.strip())

    def _read_wtmp_gz(self, filename):
        p1 = subprocess.Popen(['zcat', filename], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['wtmp2json', '--'], stdin=p1.stdout,
            stdout=subprocess.PIPE)
        p1.stdout.close()
        for line in p2.stdout.readlines():
            yield json.loads(line.strip())

    RECORD_FILES_REGEX = re.compile('wtmp-(\d+)-(\d+)\.gz')
    def record_files(self):
        files = os.listdir(self.dir_name)
        files.sort()
        acc = []
        for f in files:
            m = self.RECORD_FILES_REGEX.match(f)
            if m:
                year, month = int(m.group(1)), int(m.group(2))
                acc.append((year, month, os.path.join(self.dir_name, f)))
        if 'wtmp' in files:
            acc.append((None, None, os.path.join(self.dir_name, 'wtmp')))
        return acc

    def all_records(self):
        # Get all records.
        for _, _, filename in self.record_files():
            for record in self._read_wtmp(filename):
                yield record

    def all_records_after(self, start, start_timestamp_micro=0):
        """
        :param start: The epoch in seconds (integer).
        :param start_timestamp_micro: The microseconds if any (integer).
        """
        if start is None:
            for r in self.all_records():
                yield r
        # Find the start.
        start_timestamp = start
        start = datetime.datetime.fromtimestamp(start)
        for y, m, filename in self.record_files():
            if y and m and start < datetime.datetime(y, m, 1):
                continue
            for record in self._read_wtmp(filename):
                if (record['seconds'] > start_timestamp or
                    record['seconds'] == start_timestamp and
                    record['useconds'] > start_timestamp_micro):
                    yield record
