#!/usr/bin/env python
import sys
import subprocess

# /var/log/wtmp

def get_wtmp(host):
    dest = "./%s.wtmp" % host
    p = subprocess.Popen(["scp", "-q", "-o", "StrictHostKeyChecking=no", "%s:/var/log/wtmp" % host, dest])
    # p.wait()

def get_hosts_from_file(filename):
    f = open(filename,"rb")
    hosts=[]
    for line in f.readlines():
        hosts.append(line.strip())
    return hosts

def main(filename):
    for host in get_hosts_from_file(filename):
        get_wtmp(host)
    


if __name__ == '__main__':
    main(sys.argv[1])
