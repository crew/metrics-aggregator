#!/usr/bin/env python
import subprocess
import json


# Test the default.
p = subprocess.Popen(['./wtmp2json'], stdout=subprocess.PIPE)
for line in p.stdout.readlines():
    repr(json.loads(line.rstrip('\n')))
